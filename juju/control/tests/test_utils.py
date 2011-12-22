import os

from twisted.internet.defer import inlineCallbacks, returnValue
from yaml import dump

from juju.environment.tests.test_config import EnvironmentsConfigTestBase
from juju.control.tests.common import ControlToolTest
from juju.control.utils import (
    get_environment, get_ip_address_for_machine, get_ip_address_for_unit,
    expand_path, parse_passthrough_args, ParseError)
from juju.environment.config import EnvironmentsConfig
from juju.environment.errors import EnvironmentsConfigError
from juju.lib.testing import TestCase
from juju.state.errors import ServiceUnitStateMachineNotAssigned
from juju.state.tests.test_service import ServiceStateManagerTestBase


class FakeOptions(object):
    pass


class LookupTest(ServiceStateManagerTestBase, EnvironmentsConfigTestBase):

    @inlineCallbacks
    def setUp(self):
        yield super(LookupTest, self).setUp()
        config = {
            "environments": {
                "firstenv": {
                    "type": "dummy", "admin-secret": "homer"}}}
        self.write_config(dump(config))
        self.config.load()
        self.environment = self.config.get_default()
        self.provider = self.environment.get_machine_provider()

    @inlineCallbacks
    def start_machine(self, dns_name):
        machine_state = yield self.machine_state_manager.add_machine_state()
        provider_machine, = yield self.provider.start_machine(
            {"machine-id": machine_state.id, "dns-name": dns_name})
        yield machine_state.set_instance_id(provider_machine.instance_id)
        returnValue(machine_state)

    @inlineCallbacks
    def test_get_ip_address_for_machine(self):
        """Verify can retrieve dns name, machine state with machine id."""
        machine_state = yield self.machine_state_manager.add_machine_state()
        provider_machine, = yield self.provider.start_machine(
            {"machine-id": machine_state.id, "dns-name": "steamcloud-1.com"})
        yield machine_state.set_instance_id(provider_machine.instance_id)

        dns_name, lookedup_machine_state = yield get_ip_address_for_machine(
            self.client, self.provider, machine_state.id)
        self.assertEqual(dns_name, "steamcloud-1.com")
        self.assertEqual(lookedup_machine_state.id, machine_state.id)

    @inlineCallbacks
    def test_get_ip_address_for_unit(self):
        """Verify can retrieve dns name, unit state with unit name."""
        service_state = yield self.add_service("wordpress")
        unit_state = yield service_state.add_unit_state()
        machine_state = yield self.start_machine("steamcloud-1.com")
        yield unit_state.assign_to_machine(machine_state)
        yield unit_state.set_public_address("steamcloud-1.com")

        dns_name, lookedup_unit_state = yield get_ip_address_for_unit(
            self.client, self.provider, "wordpress/0")
        self.assertEqual(dns_name, "steamcloud-1.com")
        self.assertEqual(lookedup_unit_state.unit_name, "wordpress/0")

    @inlineCallbacks
    def test_get_ip_address_for_unit_with_unassigned_machine(self):
        """Service unit exists, but it doesn't have an assigned machine."""
        service_state = yield self.add_service("wordpress")
        yield service_state.add_unit_state()
        e = yield self.assertFailure(
            get_ip_address_for_unit(self.client, self.provider, "wordpress/0"),
            ServiceUnitStateMachineNotAssigned)
        self.assertEqual(
            str(e),
            "Service unit 'wordpress/0' is not assigned to a machine")


class PathExpandTest(TestCase):

    def test_expand_path(self):
        self.assertEqual(
            os.path.abspath("."), expand_path("."))
        self.assertEqual(
            os.path.expanduser("~/foobar"), expand_path("~/foobar"))


class GetEnvironmentTest(ControlToolTest):

    def test_get_environment(self):
        config = {
            "environments": {"firstenv": {"type": "dummy"}}}
        self.write_config(dump(config))

        env_config = EnvironmentsConfig()
        env_config.load_or_write_sample()
        options = FakeOptions()
        options.environment = None
        options.environments = env_config

        environment = get_environment(options)
        self.assertEqual(environment.name, "firstenv")

    def test_get_environment_default_with_multiple(self):
        config = {
            "environments": {"firstenv": {"type": "dummy"},
                             "secondenv": {"type": "dummy"}}}
        self.write_config(dump(config))

        env_config = EnvironmentsConfig()
        env_config.load_or_write_sample()
        options = FakeOptions()
        options.environment = None
        options.environments = env_config

        error = self.assertRaises(
            EnvironmentsConfigError,
            get_environment,
            options)

        self.assertIn(
            "There are multiple environments and no explicit default",
            str(error))

    def test_get_nonexistant_environment(self):
        config = {
            "environments": {"firstenv": {"type": "dummy"},
                             "secondenv": {"type": "dummy"}}}
        self.write_config(dump(config))

        env_config = EnvironmentsConfig()
        env_config.load_or_write_sample()
        options = FakeOptions()
        options.environment = "volcano"
        options.environments = env_config

        error = self.assertRaises(
            EnvironmentsConfigError,
            get_environment,
            options)

        self.assertIn("Invalid environment 'volcano'", str(error))


class ParsePassthroughArgsTest(ControlToolTest):

    def test_parse_typical_ssh(self):
        """Verify that flags and positional args are properly partitioned."""
        ssh_flags = "bcDeFIiLlmOopRSWw"
        self.assertEqual(
            parse_passthrough_args(
                ["-L8080:localhost:80", "-o", "Volume 11", "mysql/0", "ls a*"],
                ssh_flags),
            (["-L8080:localhost:80", "-o", "Volume 11"], ["mysql/0", "ls a*"]))

        self.assertEqual(
            parse_passthrough_args(
                ["-L8080:localhost:80", "-aC26", "0", "foobar", "do", "123"],
                ssh_flags),
            (["-L8080:localhost:80", "-aC26"], ["0", "foobar", "do", "123"]))

        self.assertEqual(
            parse_passthrough_args(["mysql/0"], ssh_flags),
            ([], ["mysql/0"]))

        self.assertEqual(
            parse_passthrough_args(
                ["mysql/0", "command", "-L8080:localhost:80"], ssh_flags),
            ([], ["mysql/0", "command", "-L8080:localhost:80"]))

    def test_parse_flag_taking_args(self):
        """Verify that arg-taking flags properly combine with args"""
        # some sample flags, from the ssh command
        ssh_flags = "bcDeFIiLlmOopRSWw"

        for flag in ssh_flags:
            # This flag properly combines, either of the form -Xabc or -X abc
            self.assertEqual(
                parse_passthrough_args(
                    ["-" + flag + "XYZ", "-1X", "-" + flag, "XYZ", "mysql/0"],
                    ssh_flags),
                (["-" + flag + "XYZ", "-1X", "-" + flag, "XYZ"], ["mysql/0"]))

            # And requires that it is combined
            e = self.assertRaises(
                ParseError,
                parse_passthrough_args, ["-" + flag], ssh_flags)
            self.assertEqual(str(e), "argument -%s: expected one argument" % flag)
