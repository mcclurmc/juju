from twisted.internet.defer import succeed, fail, inlineCallbacks

from juju.lib.testing import TestCase
from juju.providers.ec2.tests.common import (
    EC2TestMixin, MATCH_GROUP, Observed, MockInstanceState)

from juju.machine import ProviderMachine

from juju.errors import MachinesNotFound, ProviderError
from juju.providers.ec2.machine import EC2ProviderMachine


class SomeError(Exception):
    pass


class EC2ShutdownMachineTest(EC2TestMixin, TestCase):

    @inlineCallbacks
    def test_shutdown_machine(self):
        system_state = MockInstanceState(
            self, ["i-foobar"], [0],
            [["running"],
             ["shutting-down"],
             ["shutting-down"],
             ["shutting-down"],
             ["terminated"]])

        self.ec2.describe_instances("i-foobar")
        self.mocker.call(system_state.get_round)
        self.mocker.count(5)

        self.ec2.terminate_instances("i-foobar")
        self.mocker.result(succeed([("i-foobar", "running", "shutting-down")]))

        self.ec2.delete_security_group("juju-moon-0")
        deleted_groups = Observed()
        self.mocker.call(deleted_groups.add)

        self.mocker.replay()

        machine = EC2ProviderMachine("i-foobar")
        provider = self.get_provider()
        machine = yield provider.shutdown_machine(machine)
        self.assertTrue(isinstance(machine, EC2ProviderMachine))
        self.assertEquals(machine.instance_id, "i-foobar")
        # Notably, juju-moon is not mock deleted
        self.assertEquals(deleted_groups.items, set(["juju-moon-0"]))

    def test_shutdown_machine_invalid_group(self):
        """
        Attempting to shutdown a machine that does not belong to this
        provider instance raises an exception.
        """
        instance = self.get_instance("i-foobar", groups=["whatever"])
        self.ec2.describe_instances("i-foobar")
        self.mocker.result(succeed([instance]))
        self.mocker.replay()

        machine = EC2ProviderMachine("i-foobar")
        provider = self.get_provider()
        d = provider.shutdown_machine(machine)
        self.failUnlessFailure(d, MachinesNotFound)

        def verify(error):
            self.assertEquals(str(error), "Cannot find machine: i-foobar")
        d.addCallback(verify)
        return d

    def test_shutdown_machine_invalid_machine(self):
        """
        Attempting to shutdown a machine that from a different provider
        type will raise a `ProviderError`.
        """
        self.mocker.replay()
        machine = ProviderMachine("i-foobar")
        provider = self.get_provider()
        d = provider.shutdown_machine(machine)
        self.assertFailure(d, ProviderError)

        def check_error(error):
            self.assertEquals(str(error),
                              "Can only shut down EC2ProviderMachines; got a "
                              "<class 'juju.machine.ProviderMachine'>")
        d.addCallback(check_error)
        return d

    @inlineCallbacks
    def test_shutdown_machines_none(self):
        self.mocker.replay()
        provider = self.get_provider()
        result = yield provider.shutdown_machines([])
        self.assertEquals(result, [])

    @inlineCallbacks
    def test_shutdown_machines_some_invalid(self):
        self.ec2.describe_instances("i-amkillable", "i-amalien", "i-amdead")
        self.mocker.result(succeed([
            self.get_instance("i-amkillable"),
            self.get_instance("i-amalien", groups=["other"]),
            self.get_instance("i-amdead", "shutting-down")]))
        self.mocker.replay()

        provider = self.get_provider()
        ex = yield self.assertFailure(
            provider.shutdown_machines([
                    EC2ProviderMachine("i-amkillable"),
                    EC2ProviderMachine("i-amalien"),
                    EC2ProviderMachine("i-amdead")]),
            MachinesNotFound)
        self.assertEquals(str(ex),
                          "Cannot find machines: i-amalien, i-amdead")

    @inlineCallbacks
    def test_shutdown_machines_some_success(self):
        """Verify that shutting down some machines works.

        In particular, the environment as a whole is not removed
        because there's still the environment security group left."""
        system_state = MockInstanceState(
            self,
            ["i-amkillable",   "i-amkillabletoo"], [0, 2],
            [["running",       "running"],
             ["shutting-down", "shutting-down"],
             ["shutting-down", "shutting-down"],
             ["shutting-down", "terminated"],
             ["terminated"]])

        self.ec2.describe_instances("i-amkillable", "i-amkillabletoo")
        self.mocker.call(system_state.get_round)
        self.mocker.count(4)

        self.ec2.describe_instances("i-amkillable")
        self.mocker.call(system_state.get_round)

        self.ec2.terminate_instances("i-amkillable", "i-amkillabletoo")
        self.mocker.result(succeed([
            ("i-amkillable", "running", "shutting-down"),
            ("i-amkillabletoo", "running", "shutting-down")]))

        deleted_groups = Observed()
        self.ec2.delete_security_group("juju-moon-0")
        self.mocker.call(deleted_groups.add)

        self.ec2.delete_security_group("juju-moon-2")
        self.mocker.call(deleted_groups.add)

        self.mocker.replay()

        provider = self.get_provider()
        machine_1, machine_2 = yield provider.shutdown_machines([
                EC2ProviderMachine("i-amkillable"),
                EC2ProviderMachine("i-amkillabletoo")])
        self.assertTrue(isinstance(machine_1, EC2ProviderMachine))
        self.assertEquals(machine_1.instance_id, "i-amkillable")
        self.assertTrue(isinstance(machine_2, EC2ProviderMachine))
        self.assertEquals(machine_2.instance_id, "i-amkillabletoo")
        self.assertEquals(
            deleted_groups.items,
            set(["juju-moon-0", "juju-moon-2"]))


class EC2DestroyTest(EC2TestMixin, TestCase):

    @inlineCallbacks
    def test_destroy_environment(self):
        """
        The destroy_environment operation terminates all running and pending
        instances associated to the `MachineProvider` instance.
        """
        self.s3.put_object("moon", "provider-state", "{}\n")
        self.mocker.result(succeed(None))
        self.ec2.describe_instances()
        instances = [
            self.get_instance("i-canbekilled", machine_id=0),
            self.get_instance("i-amdead", machine_id=1, state="terminated"),
            self.get_instance("i-dontbelong", groups=["unknown"]),
            self.get_instance(
                "i-canbekilledtoo", machine_id=2, state="pending")]
        self.mocker.result(succeed(instances))
        self.ec2.describe_instances("i-canbekilled", "i-canbekilledtoo")
        self.mocker.result(succeed([
                    self.get_instance("i-canbekilled"),
                    self.get_instance("i-canbekilledtoo", state="pending")]))
        self.ec2.terminate_instances("i-canbekilled", "i-canbekilledtoo")
        self.mocker.result(succeed([
                    ("i-canbekilled", "running", "shutting-down"),
                    ("i-canbekilledtoo", "pending", "shutting-down")]))

        self.ec2.describe_instances("i-canbekilled", "i-canbekilledtoo")
        states = [
            self.get_instance(
                "i-canbekilled", state="terminated", machine_id=0),
            self.get_instance(
                "i-canbekilledtoo", state="terminated", machine_id=2)]
        self.mocker.result(succeed(states))

        self.ec2.delete_security_group(MATCH_GROUP)
        deleted_groups = Observed()
        self.mocker.call(deleted_groups.add)
        self.mocker.count(3)

        self.mocker.replay()

        provider = self.get_provider()
        machine_1, machine_2 = yield provider.destroy_environment()
        self.assertTrue(isinstance(machine_1, EC2ProviderMachine))
        self.assertEquals(machine_1.instance_id, "i-canbekilled")
        self.assertTrue(isinstance(machine_2, EC2ProviderMachine))
        self.assertEquals(machine_2.instance_id, "i-canbekilledtoo")
        self.assertEquals(
            deleted_groups.items,
            set(["juju-moon-0", "juju-moon-2", "juju-moon"]))

    @inlineCallbacks
    def test_s3_failure(self):
        """Failing to store empty state should not stop us killing machines"""
        self.s3.put_object("moon", "provider-state", "{}\n")
        self.mocker.result(fail(SomeError()))
        self.ec2.describe_instances()
        self.mocker.result(succeed([self.get_instance("i-canbekilled")]))
        system_state = MockInstanceState(
            self, ["i-canbekilled"], [0],
            [["running"],
             ["shutting-down"],
             ["shutting-down"],
             ["shutting-down"],
             ["terminated"]])
        self.ec2.describe_instances("i-canbekilled")
        self.mocker.call(system_state.get_round)
        self.mocker.count(5)
        self.ec2.terminate_instances("i-canbekilled")
        self.mocker.result(succeed([
            ("i-canbekilled", "running", "shutting-down")]))
        self.ec2.delete_security_group("juju-moon-0")
        self.mocker.result(succeed(True))
        self.ec2.delete_security_group("juju-moon")
        self.mocker.result(succeed(True))
        self.mocker.replay()

        provider = self.get_provider()
        machine, = yield provider.destroy_environment()
        self.assertTrue(isinstance(machine, EC2ProviderMachine))
        self.assertEquals(machine.instance_id, "i-canbekilled")

    @inlineCallbacks
    def test_shutdown_no_instances(self):
        """
        If there are no instances to shutdown, running the destroy_environment
        operation does nothing.
        """
        self.s3.put_object("moon", "provider-state", "{}\n")
        self.mocker.result(succeed(None))
        self.ec2.describe_instances()
        self.mocker.result(succeed([]))
        self.ec2.delete_security_group("juju-moon")
        self.mocker.result(fail(
                self.get_ec2_error(
                    "juju-moon",
                    format="The security group %r does not exist"
                    )))
        self.mocker.replay()

        provider = self.get_provider()
        result = yield provider.destroy_environment()
        self.assertEquals(result, [])
