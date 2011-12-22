from twisted.internet.defer import fail, succeed

from txaws.ec2.exception import EC2Error

from juju.errors import MachinesNotFound, ProviderInteractionError

from juju.lib.testing import TestCase
from .common import EC2TestMixin


class SomeError(Exception):
    pass


class GetMachinesTest(EC2TestMixin, TestCase):

    def assert_not_found(self, d, instance_ids):
        self.assertFailure(d, MachinesNotFound)

        def verify(error):
            self.assertEquals(error.instance_ids, instance_ids)
        d.addCallback(verify)
        return d

    def test_get_all_filters(self):
        """
        The machine iteration api of the provider should list all running
        machines associated to the provider.
        """
        self.ec2.describe_instances()
        self.mocker.result(succeed([
            self.get_instance("i-amrunning", dns_name="x1.example.com"),
            self.get_instance("i-amdead", "terminated"),
            self.get_instance("i-amalien", groups=["other"]),
            self.get_instance("i-ampending", "pending")]))
        self.mocker.replay()

        provider = self.get_provider()
        d = provider.get_machines()

        def verify(result):
            (running, pending,) = result
            self.assert_machine(running, "i-amrunning", "x1.example.com")
            self.assert_machine(pending, "i-ampending", "")
        d.addCallback(verify)
        return d

    def test_get_all_no_results(self):
        self.ec2.describe_instances()
        self.mocker.result(succeed([]))
        self.mocker.replay()

        provider = self.get_provider()
        d = provider.get_machines()

        def verify(result):
            self.assertEquals(result, [])
        d.addCallback(verify)
        return d

    def test_get_some_bad_state(self):
        self.ec2.describe_instances("i-amfine", "i-amdead")
        self.mocker.result(succeed([
            self.get_instance("i-amfine", dns_name="x1.example.com"),
            self.get_instance("i-amdead", "terminated")]))
        self.mocker.replay()

        provider = self.get_provider()
        d = provider.get_machines(["i-amfine", "i-amdead"])
        return self.assert_not_found(d, ["i-amdead"])

    def test_get_some_bad_group(self):
        self.ec2.describe_instances("i-amfine", "i-amalien")
        self.mocker.result(succeed([
            self.get_instance("i-amfine", dns_name="x1.example.com"),
            self.get_instance("i-amalien", groups=["random"])]))
        self.mocker.replay()

        provider = self.get_provider()
        d = provider.get_machines(["i-amfine", "i-amalien"])
        return self.assert_not_found(d, ["i-amalien"])

    def test_get_some_too_few(self):
        self.ec2.describe_instances("i-amfine", "i-ammissing")
        self.mocker.result(succeed([self.get_instance("i-amfine")]))
        self.mocker.replay()

        provider = self.get_provider()
        d = provider.get_machines(["i-amfine", "i-ammissing"])
        return self.assert_not_found(d, ["i-ammissing"])

    def test_get_some_success(self):
        self.ec2.describe_instances("i-amrunning", "i-ampending")
        self.mocker.result(succeed([
            self.get_instance("i-amrunning", dns_name="x1.example.com"),
            self.get_instance("i-ampending", "pending")]))
        self.mocker.replay()

        provider = self.get_provider()
        d = provider.get_machines(["i-amrunning", "i-ampending"])

        def verify(result):
            (running, pending,) = result
            self.assert_machine(running, "i-amrunning", "x1.example.com")
            self.assert_machine(pending, "i-ampending", "")
        d.addCallback(verify)
        return d

    def test_describe_known_failure(self):
        self.ec2.describe_instances("i-acf059", "i-amfine", "i-920fda")
        error = EC2Error("<error/>", 400)
        error.errors = [{
            "Code": "InvalidInstanceID.NotFound",
            "Message": "blah i-acf059, i-920fda blah"}]
        self.mocker.result(fail(error))
        self.mocker.replay()

        provider = self.get_provider()
        d = provider.get_machines(["i-acf059", "i-amfine", "i-920fda"])
        return self.assert_not_found(d, ["i-acf059", "i-920fda"])

    def test_describe_unknown_failure(self):
        self.ec2.describe_instances("i-brokeit", "i-msorry")
        self.mocker.result(fail(
            self.get_ec2_error("splat! kerpow!", "unhelpful noises (%r)")))
        self.mocker.replay()

        provider = self.get_provider()
        d = provider.get_machines(["i-brokeit", "i-msorry"])
        self.assertFailure(d, ProviderInteractionError)

        def verify(error):
            self.assertEquals(
                str(error),
                "Unexpected EC2Error getting machines i-brokeit, i-msorry: "
                "unhelpful noises ('splat! kerpow!')")
        d.addCallback(verify)
        return d

    def test_describe_error(self):
        self.ec2.describe_instances("i-amdeadly")
        self.mocker.result(fail(SomeError()))
        self.mocker.replay()

        provider = self.get_provider()
        d = provider.get_machines(["i-amdeadly"])
        self.assertFailure(d, SomeError)
        return d

    def test_get_one_error(self):
        self.ec2.describe_instances("i-amfatal")
        self.mocker.result(fail(SomeError()))
        self.mocker.replay()

        provider = self.get_provider()
        d = provider.get_machine("i-amfatal")
        self.assertFailure(d, SomeError)
        return d

    def test_get_one_not_found(self):
        self.ec2.describe_instances("i-amgone")
        self.mocker.result(succeed([]))
        self.mocker.replay()

        provider = self.get_provider()
        d = provider.get_machine("i-amgone")
        return self.assert_not_found(d, ["i-amgone"])

    def test_get_one(self):
        self.ec2.describe_instances("i-amgood")
        self.mocker.result(succeed([self.get_instance("i-amgood")]))
        self.mocker.replay()

        provider = self.get_provider()
        d = provider.get_machine("i-amgood")
        d.addCallback(self.assert_machine, "i-amgood", "")
        return d
