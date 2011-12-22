import logging
import os

from yaml import dump

from twisted.internet.defer import inlineCallbacks, succeed, Deferred

from juju.environment.environment import Environment
from juju.charm.tests.test_repository import RepositoryTestBase
from juju.state.machine import MachineStateManager, MachineState
from juju.state.service import ServiceUnitState
from juju.state.tests.test_service import ServiceStateManagerTestBase
from juju.lib.mocker import ARGS, KWARGS

from juju.control import main

from .common import ControlToolTest


class ControlShellTest(
    ServiceStateManagerTestBase, ControlToolTest, RepositoryTestBase):

    @inlineCallbacks
    def setUp(self):
        yield super(ControlShellTest, self).setUp()
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
    def test_shell_with_unit(self):
        """
        'juju ssh mysql/0' will execute ssh against the machine
        hosting the unit.
        """
        mock_environment = self.mocker.patch(Environment)
        mock_environment.get_machine_provider()
        self.mocker.result(self.provider)

        mock_exec = self.mocker.replace(os.execvp)
        mock_exec("ssh", [
            "ssh",
            "-o",
            "ControlPath " + self.tmp_home + "/.juju/ssh/master-%r@%h:%p",
            "-o", "ControlMaster no",
            "ubuntu@mysql-0.example.com"])

        # Track unwanted calls:
        calls = []
        mock_exec(ARGS, KWARGS)
        self.mocker.count(0, None)
        self.mocker.call(lambda *args, **kwargs: calls.append((args, kwargs)))

        finished = self.setup_cli_reactor()
        self.mocker.replay()
        yield self.unit.connect_agent()

        main(["ssh", self.unit.unit_name])
        yield finished

        self.assertEquals(calls, [])
        self.assertIn(
            "Connecting to unit mysql/0 at mysql-0.example.com",
            self.output.getvalue())

    @inlineCallbacks
    def test_passthrough_args(self):
        """Verify that args are passed through to the underlying ssh command.

        For example, something like the following command should be valid::

          $ juju ssh -L8080:localhost:8080 -o "ConnectTimeout 60" mysql/0 ls /
        """
        mock_environment = self.mocker.patch(Environment)
        mock_environment.get_machine_provider()
        self.mocker.result(self.provider)

        mock_exec = self.mocker.replace(os.execvp)
        mock_exec("ssh", [
            "ssh",
            "-o",
            "ControlPath " + self.tmp_home + "/.juju/ssh/master-%r@%h:%p",
            "-o", "ControlMaster no",
            "-L8080:localhost:8080", "-o", "ConnectTimeout 60",
            "ubuntu@mysql-0.example.com", "ls *"])

        # Track unwanted calls:
        calls = []
        mock_exec(ARGS, KWARGS)
        self.mocker.count(0, None)
        self.mocker.call(lambda *args, **kwargs: calls.append((args, kwargs)))

        finished = self.setup_cli_reactor()
        self.mocker.replay()
        yield self.unit.connect_agent()

        main(["ssh", "-L8080:localhost:8080", "-o", "ConnectTimeout 60",
              self.unit.unit_name, "ls *"])
        yield finished

        self.assertEquals(calls, [])
        self.assertIn(
            "Connecting to unit mysql/0 at mysql-0.example.com",
            self.output.getvalue())

    @inlineCallbacks
    def test_shell_with_unit_and_unconnected_unit_agent(self):
        """If a unit doesn't have a connected unit agent,
        the ssh command will wait till one exists before connecting.
        """
        mock_environment = self.mocker.patch(Environment)
        mock_environment.get_machine_provider()
        self.mocker.result(self.provider)

        mock_unit = self.mocker.patch(ServiceUnitState)
        mock_unit.watch_agent()
        self.mocker.result((succeed(False), succeed(True)))
        mock_exec = self.mocker.replace(os.execvp)
        mock_exec("ssh", [
            "ssh",
            "-o",
            "ControlPath " + self.tmp_home + "/.juju/ssh/master-%r@%h:%p",
            "-o", "ControlMaster no",
            "ubuntu@mysql-0.example.com"])

        # Track unwanted calls:
        calls = []
        mock_exec(ARGS, KWARGS)
        self.mocker.count(0, None)
        self.mocker.call(lambda *args, **kwargs: calls.append((args, kwargs)))

        finished = self.setup_cli_reactor()
        self.mocker.replay()
        yield self.unit.connect_agent()
        main(["ssh", "mysql/0"])
        yield finished

        self.assertEquals(calls, [])

        self.assertIn(
            "Waiting for unit to come up", self.output.getvalue())

    @inlineCallbacks
    def test_shell_with_machine_and_unconnected_machine_agent(self):
        """If a machine doesn't have a connected machine agent,
        the ssh command will wait till one exists before connecting.
        """
        mock_environment = self.mocker.patch(Environment)
        mock_environment.get_machine_provider()
        self.mocker.result(self.provider)

        mock_machine = self.mocker.patch(MachineState)
        mock_machine.watch_agent()
        self.mocker.result((succeed(False), succeed(True)))
        mock_exec = self.mocker.replace(os.execvp)
        mock_exec("ssh", [
            "ssh",
            "-o",
            "ControlPath " + self.tmp_home + "/.juju/ssh/master-%r@%h:%p",
            "-o", "ControlMaster no",
            "ubuntu@antigravity.example.com"])

        # Track unwanted calls:
        calls = []
        mock_exec(ARGS, KWARGS)
        self.mocker.count(0, None)
        self.mocker.call(lambda *args, **kwargs: calls.append((args, kwargs)))

        finished = self.setup_cli_reactor()
        self.mocker.replay()
        yield self.machine.connect_agent()
        main(["ssh", "0"])
        yield finished

        self.assertEquals(calls, [])

        self.assertIn(
            "Waiting for machine to come up", self.output.getvalue())

    @inlineCallbacks
    def test_shell_with_unit_and_unset_dns(self):
        """If a machine agent isn't connects, its also possible that
        the provider machine may not yet have a dns name, if the
        instance hasn't started. In that case after the machine agent
        has connected, verify the provider dns name is valid."""

        mock_environment = self.mocker.patch(Environment)
        mock_environment.get_machine_provider()
        self.mocker.result(self.provider)

        mock_unit = self.mocker.patch(ServiceUnitState)
        mock_unit.watch_agent()

        address_set = Deferred()

        @inlineCallbacks
        def set_unit_dns():
            yield self.unit.set_public_address("mysql-0.example.com")
            address_set.callback(True)

        self.mocker.call(set_unit_dns)
        self.mocker.result((succeed(False), succeed(False)))

        mock_exec = self.mocker.replace(os.execvp)
        mock_exec("ssh", [
            "ssh",
            "-o",
            "ControlPath " + self.tmp_home + "/.juju/ssh/master-%r@%h:%p",
            "-o", "ControlMaster no",
            "ubuntu@mysql-0.example.com"])

        # Track unwanted calls:
        calls = []
        mock_exec(ARGS, KWARGS)
        self.mocker.count(0, None)
        self.mocker.call(lambda *args, **kwargs: calls.append((args, kwargs)))

        finished = self.setup_cli_reactor()
        self.mocker.replay()

        yield self.unit.set_public_address(None)

        main(["ssh", "mysql/0"])

        # Wait till we've set the unit address before connecting the agent.
        yield address_set
        yield self.unit.connect_agent()
        yield finished

        self.assertEquals(calls, [])

        self.assertIn(
            "Waiting for unit to come up", self.output.getvalue())

    @inlineCallbacks
    def test_shell_with_machine_and_unset_dns(self):
        """If a machine agent isn't connects, its also possible that
        the provider machine may not yet have a dns name, if the
        instance hasn't started. In that case after the machine agent
        has connected, verify the provider dns name is valid."""

        mock_environment = self.mocker.patch(Environment)
        mock_environment.get_machine_provider()
        self.mocker.result(self.provider)

        mock_machine = self.mocker.patch(MachineState)
        mock_machine.watch_agent()

        def set_machine_dns():
            self.provider_machine.dns_name = "antigravity.example.com"

        self.mocker.call(set_machine_dns)
        self.mocker.result((succeed(False), succeed(False)))

        mock_exec = self.mocker.replace(os.execvp)
        mock_exec("ssh", [
            "ssh",
            "-o",
            "ControlPath " + self.tmp_home + "/.juju/ssh/master-%r@%h:%p",
            "-o", "ControlMaster no",
            "ubuntu@antigravity.example.com"])

        # Track unwanted calls:
        calls = []
        mock_exec(ARGS, KWARGS)
        self.mocker.count(0, None)
        self.mocker.call(lambda *args, **kwargs: calls.append((args, kwargs)))

        finished = self.setup_cli_reactor()
        self.mocker.replay()
        self.provider_machine.dns_name = None
        yield self.machine.connect_agent()
        main(["ssh", "0"])
        yield finished

        self.assertEquals(calls, [])

        self.assertIn(
            "Waiting for machine to come up", self.output.getvalue())

    @inlineCallbacks
    def test_shell_with_machine_id(self):
        """
        'juju ssh <machine_id>' will execute ssh against the machine
        with the corresponding id.
        """
        mock_environment = self.mocker.patch(Environment)
        mock_environment.get_machine_provider()
        self.mocker.result(self.provider)

        mock_exec = self.mocker.replace(os.execvp)
        mock_exec("ssh", [
            "ssh",
            "-o",
            "ControlPath " + self.tmp_home + "/.juju/ssh/master-%r@%h:%p",
            "-o", "ControlMaster no",
            "ubuntu@antigravity.example.com",
        ])

        # Track unwanted calls:
        calls = []
        mock_exec(ARGS, KWARGS)
        self.mocker.count(0, None)
        self.mocker.call(lambda *args, **kwargs: calls.append((args, kwargs)))

        finished = self.setup_cli_reactor()
        self.mocker.replay()
        yield self.machine.connect_agent()
        main(["ssh", "0"])
        yield finished

        self.assertEquals(calls, [])

        self.assertIn(
            "Connecting to machine 0 at antigravity.example.com",
            self.output.getvalue())

    @inlineCallbacks
    def test_shell_with_unassigned_unit(self):
        """If the service unit is not assigned, attempting to
        connect, raises an error."""
        mock_environment = self.mocker.patch(Environment)
        mock_environment.get_machine_provider()
        self.mocker.result(self.provider)

        finished = self.setup_cli_reactor()
        self.mocker.replay()

        unit_state = yield self.service.add_unit_state()

        main(["ssh", unit_state.unit_name])
        yield finished

        self.assertIn(
            "Service unit 'mysql/1' is not assigned to a machine",
            self.stderr.getvalue())

    @inlineCallbacks
    def test_shell_with_invalid_machine(self):
        """If the machine does not exist, attempting to
        connect, raises an error."""
        mock_environment = self.mocker.patch(Environment)
        mock_environment.get_machine_provider()
        self.mocker.result(self.provider)

        finished = self.setup_cli_reactor()
        self.mocker.replay()

        main(["ssh", "1"])
        yield finished

        self.assertIn("Machine 1 was not found", self.stderr.getvalue())


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
            SystemExit, main, ["ssh", "-L", "mysql/0"])
        self.assertEqual(e.code, 2)
        self.assertIn("juju ssh: error: too few arguments",
                      self.stderr.getvalue())

