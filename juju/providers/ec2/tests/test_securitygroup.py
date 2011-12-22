import logging

from twisted.internet.defer import fail, succeed, inlineCallbacks, TimeoutError
from txaws.ec2.model import IPPermission, SecurityGroup

from juju.errors import ProviderInteractionError
from juju.lib.testing import TestCase
from juju.machine import ProviderMachine
from juju.providers.ec2.securitygroup import (
    open_provider_port, close_provider_port, get_provider_opened_ports,
    remove_security_groups, destroy_environment_security_group)
from juju.providers.ec2.tests.common import (
    EC2TestMixin, MATCH_GROUP, Observed, MockInstanceState)


class EC2PortMgmtTest(EC2TestMixin, TestCase):

    @inlineCallbacks
    def test_open_provider_port(self):
        """Verify open port op will use the correct EC2 API."""
        log = self.capture_logging("juju.ec2", level=logging.DEBUG)
        machine = ProviderMachine("i-foobar", "x1.example.com")
        self.ec2.authorize_security_group(
            "juju-moon-machine-1", ip_protocol="tcp", from_port="80",
            to_port="80", cidr_ip="0.0.0.0/0")
        self.mocker.result(succeed(True))
        self.mocker.replay()

        provider = self.get_provider()
        yield open_provider_port(provider, machine, "machine-1", 80, "tcp")
        self.assertIn(
            "Opened 80/tcp on provider machine 'i-foobar'",
            log.getvalue())

    @inlineCallbacks
    def test_close_provider_port(self):
        """Verify close port op will use the correct EC2 API."""
        log = self.capture_logging("juju.ec2", level=logging.DEBUG)
        machine = ProviderMachine("i-foobar", "x1.example.com")
        self.ec2.revoke_security_group(
            "juju-moon-machine-1", ip_protocol="tcp", from_port="80",
            to_port="80", cidr_ip="0.0.0.0/0")
        self.mocker.result(succeed(True))
        self.mocker.replay()

        provider = self.get_provider()
        yield close_provider_port(provider, machine, "machine-1", 80, "tcp")
        self.assertIn(
            "Closed 80/tcp on provider machine 'i-foobar'",
            log.getvalue())

    @inlineCallbacks
    def test_get_provider_opened_ports(self):
        """Verify correct parse of IP perms from describe_security_group."""
        self.ec2.describe_security_groups("juju-moon-machine-1")
        self.mocker.result(succeed([
                    SecurityGroup(
                        "juju-%s-machine-1" % self.env_name,
                        "a security group name",
                        ips=[
                            IPPermission("udp", "53", "53", "0.0.0.0/0"),
                            IPPermission("tcp", "80", "80", "0.0.0.0/0"),
                            # The range 8080-8082 will be ignored
                            IPPermission("tcp", "8080", "8082", "0.0.0.0/0"),
                            # Ignore permissions that are not 0.0.0.0/0
                            IPPermission("tcp", "443", "443", "10.1.2.3")
                            ])]))
        self.mocker.replay()

        provider = self.get_provider()
        machine = ProviderMachine(
            "i-foobar", "x1.example.com")
        opened_ports = yield get_provider_opened_ports(
            provider, machine, "machine-1")
        self.assertEqual(opened_ports, set([(53, "udp"), (80, "tcp")]))

    @inlineCallbacks
    def test_open_provider_port_unknown_instance(self):
        """Verify open port op will use the correct EC2 API."""
        machine = ProviderMachine("i-foobar", "x1.example.com")
        self.ec2.authorize_security_group(
            "juju-moon-machine-1", ip_protocol="tcp", from_port="80",
            to_port="80", cidr_ip="0.0.0.0/0")
        self.mocker.result(fail(self.get_ec2_error("i-foobar")))
        self.mocker.replay()

        provider = self.get_provider()
        ex = yield self.assertFailure(
            open_provider_port(provider, machine, "machine-1", 80, "tcp"),
            ProviderInteractionError)
        self.assertEqual(
            str(ex),
            "Unexpected EC2Error opening 80/tcp on machine i-foobar: "
            "The instance ID 'i-foobar' does not exist")

    @inlineCallbacks
    def test_close_provider_port_unknown_instance(self):
        """Verify open port op will use the correct EC2 API."""
        machine = ProviderMachine("i-foobar", "x1.example.com")
        self.ec2.revoke_security_group(
            "juju-moon-machine-1", ip_protocol="tcp", from_port="80",
            to_port="80", cidr_ip="0.0.0.0/0")
        self.mocker.result(fail(self.get_ec2_error("i-foobar")))
        self.mocker.replay()

        provider = self.get_provider()
        ex = yield self.assertFailure(
            close_provider_port(provider, machine, "machine-1", 80, "tcp"),
            ProviderInteractionError)
        self.assertEqual(
            str(ex),
            "Unexpected EC2Error closing 80/tcp on machine i-foobar: "
            "The instance ID 'i-foobar' does not exist")

    @inlineCallbacks
    def test_get_provider_opened_ports_unknown_instance(self):
        """Verify open port op will use the correct EC2 API."""
        self.ec2.describe_security_groups("juju-moon-machine-1")
        self.mocker.result(fail(self.get_ec2_error("i-foobar")))
        self.mocker.replay()

        provider = self.get_provider()
        machine = ProviderMachine("i-foobar", "x1.example.com")
        ex = yield self.assertFailure(
            get_provider_opened_ports(provider, machine, "machine-1"),
            ProviderInteractionError)
        self.assertEqual(
            str(ex),
            "Unexpected EC2Error getting open ports on machine i-foobar: "
            "The instance ID 'i-foobar' does not exist")


class EC2RemoveGroupsTest(EC2TestMixin, TestCase):

    @inlineCallbacks
    def test_remove_security_groups(self):
        """Test normal running seen in polling instances for their state."""
        system_state = MockInstanceState(
            self,
            ["i-amkillable",   "i-amkillabletoo"], [0, 2],
            [["shutting-down", "shutting-down"],
             ["shutting-down", "shutting-down"],
             ["shutting-down", "terminated"],
             ["terminated"]])

        self.ec2.describe_instances("i-amkillable", "i-amkillabletoo")
        self.mocker.call(system_state.get_round)
        self.mocker.count(3)

        self.ec2.describe_instances("i-amkillable")
        self.mocker.call(system_state.get_round)

        self.ec2.delete_security_group(MATCH_GROUP)
        deleted_groups = Observed()
        self.mocker.call(deleted_groups.add)
        self.mocker.count(2)
        self.mocker.replay()

        provider = self.get_provider()
        yield remove_security_groups(
            provider, ["i-amkillable", "i-amkillabletoo"])
        self.assertEquals(
            deleted_groups.items,
            set(["juju-moon-0", "juju-moon-2"]))

    @inlineCallbacks
    def test_machine_not_in_machine_security_group(self):
        """Old environments do not have machines in security groups.

        TODO It might be desirable to change this behavior to log as an
        error, or even raise as a provider exception.
        """
        log = self.capture_logging()

        # Instance is not in a machine security group, just its env
        # security group (juju-moon)
        self.ec2.describe_instances("i-amkillable")
        self.mocker.result(succeed([
            self.get_instance("i-amkillable", "terminated", groups=[])]))
        self.mocker.replay()

        provider = self.get_provider()
        yield remove_security_groups(provider, ["i-amkillable"])
        self.assertIn(
            "Ignoring missing machine security group for instance "
            "'i-amkillable'",
            log.getvalue())

    @inlineCallbacks
    def test_cannot_delete_machine_security_group(self):
        """Verify this raises error.

        Note it is possible for a malicious admin to start other
        machines with an existing machine security group, so need to
        test this removal is robust in this case. Another scenario
        that might cause this is that there's an EC2 timeout.
        """
        system_state = MockInstanceState(
            self, ["i-amkillable"], [0],
            [["shutting-down"],
             ["shutting-down"],
             ["shutting-down"],
             ["terminated"]])

        self.ec2.describe_instances("i-amkillable")
        self.mocker.call(system_state.get_round)
        self.mocker.count(4)

        self.ec2.delete_security_group("juju-moon-0")
        self.mocker.result(fail(
                self.get_ec2_error(
                    "juju-moon-0",
                    format="There are active instances using security group %r"
                    )))

        self.mocker.replay()

        provider = self.get_provider()
        ex = yield self.assertFailure(
            remove_security_groups(provider, ["i-amkillable"]),
            ProviderInteractionError)
        self.assertEquals(
            str(ex),
            "EC2 error when attempting to delete group juju-moon-0: "
            "Error Message: There are active instances using security group "
            "'juju-moon-0'")

    @inlineCallbacks
    def test_remove_security_groups_takes_too_long(self):
        """Verify that removing security groups doesn't continue indefinitely.

        This scenario might happen if the instance is taking too long
        to report as going into the terminated state, although not
        likely after 1000 polls!
        """
        log = self.capture_logging()
        states = [["shutting-down", "shutting-down"],
                  ["shutting-down", "shutting-down"],
                  ["shutting-down", "terminated"]]
        keeps_on_going = [["shutting-down"] for i in xrange(1000)]
        states.extend(keeps_on_going)

        system_state = MockInstanceState(
            self, ["i-amkillable", "i-amkillabletoo"], [0, 2], states)

        self.ec2.describe_instances("i-amkillable", "i-amkillabletoo")
        self.mocker.call(system_state.get_round)
        self.mocker.count(3)

        # Keep the rounds going until it exits with the error message
        self.ec2.describe_instances("i-amkillable")
        self.mocker.call(system_state.get_round)
        self.mocker.count(197)

        # Verify that at least this machine security group was deleted
        deleted_groups = Observed()
        self.ec2.delete_security_group("juju-moon-2")
        self.mocker.call(deleted_groups.add)

        self.mocker.replay()

        provider = self.get_provider()
        yield remove_security_groups(
            provider, ["i-amkillable", "i-amkillabletoo"])
        self.assertEquals(deleted_groups.items, set(["juju-moon-2"]))
        self.assertIn(
            "Instance shutdown taking too long, "
            "could not delete groups juju-moon-0",
            log.getvalue())

    @inlineCallbacks
    def test_remove_security_groups_raises_http_timeout(self):
        """Verify that a timeout in txaws raises an appropiate error.

        This is an example of a scenario where txaws may not raise an
        EC2Error, but will instead raise some other error.
        """
        system_state = MockInstanceState(
            self, ["i-amkillable"], [0],
            [["shutting-down"],
             ["shutting-down"],
             ["shutting-down"],
             ["terminated"]])

        self.ec2.describe_instances("i-amkillable")
        self.mocker.call(system_state.get_round)
        self.mocker.count(4)

        self.ec2.delete_security_group("juju-moon-0")
        self.mocker.result(fail(
                TimeoutError(
                    "Getting https://x.example.com?Action=DeleteSecurityGroup"
                    "&etc=andsoforth took longer than 30 seconds.")))
        self.mocker.replay()

        provider = self.get_provider()
        ex = yield self.assertFailure(
            remove_security_groups(provider, ["i-amkillable"]),
            TimeoutError)
        self.assertEquals(
            str(ex),
            "Getting https://x.example.com?Action=DeleteSecurityGroup"
            "&etc=andsoforth took longer than 30 seconds.")

    @inlineCallbacks
    def test_destroy_environment_security_group(self):
        """Verify the deletion of the security group for the environment"""
        self.ec2.delete_security_group("juju-moon")
        self.mocker.result(succeed(True))
        self.mocker.replay()

        provider = self.get_provider()
        destroyed = yield destroy_environment_security_group(provider)
        self.assertTrue(destroyed)

    @inlineCallbacks
    def test_destroy_environment_security_group_missing(self):
        """Verify ignores errors in deleting the env security group"""
        log = self.capture_logging(level=logging.DEBUG)
        self.ec2.delete_security_group("juju-moon")
        self.mocker.result(fail(
                self.get_ec2_error(
                    "juju-moon",
                    format="The security group %r does not exist"
                    )))
        self.mocker.replay()

        provider = self.get_provider()
        destroyed = yield destroy_environment_security_group(provider)
        self.assertFalse(destroyed)
        self.assertIn(
            "Ignoring EC2 error when attempting to delete group "
            "juju-moon: Error Message: The security group "
            "'juju-moon' does not exist",
            log.getvalue())
