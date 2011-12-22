from twisted.internet.defer import fail, succeed

from txaws.ec2.exception import EC2Error
from txaws.s3.exception import S3Error

from yaml import dump

from juju.errors import EnvironmentNotFound
from juju.lib.testing import TestCase
from juju.providers.ec2.machine import EC2ProviderMachine
from juju.providers.ec2.tests.common import EC2TestMixin


def _invalid_id_error():
    e = EC2Error("<error/>", 400)
    e.errors = [{"Code": "InvalidInstanceID.NotFound",
                 "Message": "blah i-abef014589 blah"}]
    return e


class EC2FindZookeepersTest(EC2TestMixin, TestCase):

    def mock_load_state(self, result):
        self.s3.get_object(self.env_name, "provider-state")
        self.mocker.result(result)

    def assert_no_environment(self):
        provider = self.get_provider()
        d = provider.get_zookeeper_machines()
        self.failUnlessFailure(d, EnvironmentNotFound)
        return d

    def verify_no_environment(self, load_result):
        self.mock_load_state(load_result)
        self.mocker.replay()
        return self.assert_no_environment()

    def test_no_state(self):
        """
        When loading saved state from S3, the provider method gracefully
        handles the scenario where there is no saved state.
        """
        error = S3Error("<error/>", 404)
        error.errors = [{"Code": "NoSuchKey"}]
        return self.verify_no_environment(fail(error))

    def test_empty_state(self):
        """
        When loading saved state from S3, the provider method gracefully
        handles the scenario where there is no saved zookeeper state.
        """
        return self.verify_no_environment(succeed(dump([])))

    def test_no_hosts(self):
        """
        If the saved state from s3 exists, but has no zookeeper hosts,
        the provider method correctly detects this and raises
        EnvironmentNotFound.
        """
        return self.verify_no_environment(succeed(dump({"abc": 123})))

    def test_machines_not_running(self):
        """
        If the saved state exists but only contains zookeeper hosts that
        are not actually running, the provider method detects this and raises
        EnvironmentNotFound.
        """
        self.mock_load_state(succeed(dump({"zookeeper-instances": ["i-x"]})))
        self.ec2.describe_instances("i-x")
        self.mocker.result(succeed([]))
        self.mocker.replay()

        return self.assert_no_environment()

    def check_good_instance_state(self, state):
        self.s3.get_object(self.env_name, "provider-state")
        self.mocker.result(succeed(dump(
            {"zookeeper-instances": ["i-foobar"]})))
        self.ec2.describe_instances("i-foobar")
        self.mocker.result(succeed([self.get_instance("i-foobar", state)]))
        self.mocker.replay()

        provider = self.get_provider()
        d = provider.get_zookeeper_machines()

        def verify(machines):
            (machine,) = machines
            self.assertEquals(machine.instance_id, "i-foobar")
            self.assertTrue(isinstance(machine, EC2ProviderMachine))
        d.addCallback(verify)
        return d

    def test_pending_ok(self):
        return self.check_good_instance_state("pending")

    def test_running_ok(self):
        return self.check_good_instance_state("running")

    def test_eventual_success(self):
        """
        When the S3 state contains valid zookeeper hosts,
        return a one-element list containing the first one
        encountered.
        """
        self.s3.get_object(self.env_name, "provider-state")
        self.mocker.result(succeed(dump(
            {"zookeeper-instances": ["i-abef014589",
                                     "i-amnotyours",
                                     "i-amdead",
                                     "i-amok",
                                     "i-amtoo"]})))

        # Zk instances are checked individually to handle invalid ids correctly
        self.ec2.describe_instances("i-abef014589")
        self.mocker.result(fail(_invalid_id_error()))
        self.ec2.describe_instances("i-amnotyours")
        self.mocker.result(succeed([
            self.get_instance("i-amnotyours", groups=["bad"])]))
        self.ec2.describe_instances("i-amdead")
        self.mocker.result(succeed([
            self.get_instance("i-amnotyours", "terminated")]))
        self.ec2.describe_instances("i-amok")
        self.mocker.result(succeed([self.get_instance("i-amok", "pending")]))
        self.ec2.describe_instances("i-amtoo")
        self.mocker.result(succeed([self.get_instance("i-amtoo", "running")]))

        self.mocker.replay()

        provider = self.get_provider()
        d = provider.get_zookeeper_machines()

        def verify_machines(machines):
            (foobaz, foobop) = machines
            self.assertEquals(foobaz.instance_id, "i-amok")
            self.assertTrue(isinstance(foobaz, EC2ProviderMachine))
            self.assertEquals(foobop.instance_id, "i-amtoo")
            self.assertTrue(isinstance(foobop, EC2ProviderMachine))
        d.addCallback(verify_machines)
        return d
