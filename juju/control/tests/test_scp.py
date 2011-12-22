import logging
import os

from yaml import dump

from twisted.internet.defer import inlineCallbacks

from juju.environment.environment import Environment
from juju.state.machine import MachineStateManager
from juju.state.tests.test_service import ServiceStateManagerTestBase
from juju.lib.mocker import ARGS, KWARGS

from juju.control import main

from .common import ControlToolTest


class SCPTest(ServiceStateManagerTestBase, ControlToolTest):

    @inlineCallbacks
    def setUp(self):
        yield super(SCPTest, self).setUp()
        config = {
            "environments": {
                "firstenv": {
                    "type": "dummy", "admin-secret": "homer"}}}
        self.write_config(dump(config))
        self.setup_exit(0)

        self.config.load()
        self.environment = self.config.get_default()
        self.provider = self.environment.get_machine_provider()

        # Setup a machine in the provider
        self.provider_machine = (yield self.provider.start_machine(
            {"machine-id": 0, "dns-name": "antigravity.example.com"}))[0]

        # Setup the zk tree with a service, unit, and machine.
        self.service = yield self.add_service_from_charm("mysql")
        self.unit = yield self.service.add_unit_state()
        yield self.unit.set_public_address(
            "%s.example.com" % self.unit.unit_name.replace("/", "-"))

        self.machine_manager = MachineStateManager(self.client)
        self.machine = yield self.machine_manager.add_machine_state()
        yield self.machine.set_instance_id(0)
        yield self.unit.assign_to_machine(self.machine)

        # capture the output.
        self.output = self.capture_logging(
            "juju.control.cli", level=logging.INFO)

        self.stderr = self.capture_stream("stderr")

    @inlineCallbacks
    def test_scp_unit_name(self):
        """Verify scp command is invoked against the host for a unit name."""
        mock_environment = self.mocker.patch(Environment)
        mock_environment.get_machine_provider()
        self.mocker.result(self.provider)

        # Verify expected call against scp
        mock_exec = self.mocker.replace(os.execvp)
        mock_exec("scp", [
            "scp", "ubuntu@mysql-0.example.com:/foo/*", "10.1.2.3:."])

        # But no other calls
        calls = []
        mock_exec(ARGS, KWARGS)
        self.mocker.count(0, None)
        self.mocker.call(lambda *args, **kwargs: calls.append((args, kwargs)))

        finished = self.setup_cli_reactor()
        self.mocker.replay()

        yield self.unit.connect_agent()
        main(["scp", "mysql/0:/foo/*", "10.1.2.3:."])
        yield finished
        self.assertEquals(calls, [])

    @inlineCallbacks
    def test_scp_machine_id(self):
        """Verify scp command is invoked against the host for a machine ID."""
        mock_environment = self.mocker.patch(Environment)
        mock_environment.get_machine_provider()
        self.mocker.result(self.provider)

        # Verify expected call against scp
        mock_exec = self.mocker.replace(os.execvp)
        mock_exec(
            "scp",
            ["scp", "ubuntu@antigravity.example.com:foo/*", "10.1.2.3:."])

        # But no other calls
        calls = []
        mock_exec(ARGS, KWARGS)
        self.mocker.count(0, None)
        self.mocker.call(lambda *args, **kwargs: calls.append((args, kwargs)))

        finished = self.setup_cli_reactor()
        self.mocker.replay()

        yield self.unit.connect_agent()
        main(["scp", "0:foo/*", "10.1.2.3:."])
        yield finished
        self.assertEquals(calls, [])

    @inlineCallbacks
    def test_passthrough_args(self):
        """Verify that args are passed through to the underlying scp command.

        For example, something like the following command should be valid::

          $ juju scp -o "ConnectTimeout 60" foo mysql/0:/foo/bar
        """
        mock_environment = self.mocker.patch(Environment)
        mock_environment.get_machine_provider()
        self.mocker.result(self.provider)

        # Verify expected call against scp
        mock_exec = self.mocker.replace(os.execvp)
        mock_exec("scp", [
            "scp", "-r", "-o", "ConnectTimeout 60",
            "foo", "ubuntu@mysql-0.example.com:/foo/bar"])

        # But no other calls
        calls = []
        mock_exec(ARGS, KWARGS)
        self.mocker.count(0, None)
        self.mocker.call(lambda *args, **kwargs: calls.append((args, kwargs)))

        finished = self.setup_cli_reactor()
        self.mocker.replay()

        main(["scp", "-r", "-o", "ConnectTimeout 60",
              "foo", "mysql/0:/foo/bar"])
        yield finished
        self.assertEquals(calls, [])


class ParseErrorsTest(ServiceStateManagerTestBase, ControlToolTest):

    @inlineCallbacks
    def setUp(self):
        yield super(ParseErrorsTest, self).setUp()
        config = {
            "environments": {"firstenv": {"type": "dummy"}}}

        self.write_config(dump(config))
        self.config.load()
        self.stderr = self.capture_stream("stderr")

    def test_passthrough_args_parse_error(self):
        """Verify that bad passthrough args will get an argparse error."""
        e = self.assertRaises(
            SystemExit, main, ["scp", "-P", "mysql/0"])
        self.assertEqual(e.code, 2)
        self.assertIn("juju scp: error: too few arguments",
                      self.stderr.getvalue())
