import logging
import os

import yaml

from twisted.internet.defer import (
    inlineCallbacks, returnValue, succeed, Deferred)

from juju.control import main
from juju.charm.tests.test_repository import RepositoryTestBase
from juju.environment.environment import Environment
from juju.state.machine import MachineStateManager
from juju.state.service import ServiceUnitState

from juju.lib.mocker import ANY
from juju.control.tests.common import ControlToolTest
from juju.state.tests.test_service import ServiceStateManagerTestBase


class ControlDebugHookTest(
    ServiceStateManagerTestBase, ControlToolTest, RepositoryTestBase):

    @inlineCallbacks
    def setUp(self):
        yield super(ControlDebugHookTest, self).setUp()
        config = {
            "environments": {
                "firstenv": {
                    "type": "dummy", "admin-secret": "homer"}}}
        self.write_config(yaml.dump(config))
        self.config.load()

        self.environment = self.config.get_default()
        self.provider = self.environment.get_machine_provider()

        # Setup a machine in the provider
        self.provider_machine = (yield self.provider.start_machine(
            {"machine-id": 0, "dns-name": "antigravity.example.com"}))[0]

        # Setup the zk tree with a service, unit, and machine.
        self.service = yield self.add_service_from_charm("mysql")
        self.unit = yield self.service.add_unit_state()

        self.machine_manager = MachineStateManager(self.client)
        self.machine = yield self.machine_manager.add_machine_state()
        yield self.machine.set_instance_id(0)
        yield self.unit.assign_to_machine(self.machine)

        # capture the output.
        self.output = self.capture_logging(
            "juju.control.cli", level=logging.INFO)

        self.stderr = self.capture_stream("stderr")

        self.setup_exit(0)

    @inlineCallbacks
    def test_debug_hook_invalid_hook_name(self):
        """If an invalid hookname is used an appropriate error
        message is raised that references the charm.
        """
        mock_environment = self.mocker.patch(Environment)
        mock_environment.get_machine_provider()
        self.mocker.result(self.provider)
        finished = self.setup_cli_reactor()
        self.mocker.replay()

        main(["debug-hooks", "mysql/0", "bad-happened"])
        yield finished

        self.assertIn(
            "Charm 'local:series/mysql-1' does not contain hook "
            "'bad-happened'",
            self.stderr.getvalue())

    @inlineCallbacks
    def test_debug_hook_invalid_unit_name(self):
        """An invalid unit causes an appropriate error.
        """
        finished = self.setup_cli_reactor()
        self.mocker.replay()

        main(["debug-hooks", "mysql/42"])
        yield finished
        self.assertIn(
            "Service unit 'mysql/42' was not found",
            self.stderr.getvalue())

    @inlineCallbacks
    def test_debug_hook_invalid_service(self):
        """An invalid service causes an appropriate error.
        """
        finished = self.setup_cli_reactor()
        self.mocker.replay()

        main(["debug-hooks", "magic/42"])
        yield finished
        self.assertIn(
            "Service 'magic' was not found",
            self.stderr.getvalue())

    @inlineCallbacks
    def test_debug_hook_unit_agent_not_available(self):
        """Simulate the unit agent isn't available when the command is run.

        The command will set the debug flag, and wait for the unit
        agent to be available.
        """
        mock_unit = self.mocker.patch(ServiceUnitState)

        # First time, doesn't exist, will wait on watch
        mock_unit.watch_agent()
        self.mocker.result((succeed(False), succeed(True)))

        # Second time, setup the unit address
        mock_unit.watch_agent()

        def setup_unit_address():
            set_d = self.unit.set_public_address("x1.example.com")
            exist_d = Deferred()
            set_d.addCallback(lambda result: exist_d.callback(True))
            return (exist_d, succeed(True))

        self.mocker.call(setup_unit_address)

        # Intercept the ssh call
        self.patch(os, "system", lambda x: True)

        #mock_environment = self.mocker.patch(Environment)
        #mock_environment.get_machine_provider()
        #self.mocker.result(self.provider)
        finished = self.setup_cli_reactor()
        self.mocker.replay()

        main(["debug-hooks", "mysql/0"])
        yield finished
        self.assertIn("Waiting for unit", self.output.getvalue())
        self.assertIn("Unit running", self.output.getvalue())
        self.assertIn("Connecting to remote machine x1.example.com",
                      self.output.getvalue())

    @inlineCallbacks
    def test_debug_hook(self):
        """The debug cli will setup unit debug setting and ssh to a screen.
        """
        system_mock = self.mocker.replace(os.system)
        system_mock(ANY)

        def on_ssh(command):
            self.assertStartsWith(command, "ssh -t ubuntu@x2.example.com")
            # In the function, os.system yields to faciliate testing.
            self.assertEqual(
                (yield self.unit.get_hook_debug()),
                {"debug_hooks": ["*"]})
            returnValue(True)

        self.mocker.call(on_ssh)

        finished = self.setup_cli_reactor()
        self.mocker.replay()

        # Setup the unit address.
        yield self.unit.set_public_address("x2.example.com")

        main(["debug-hooks", "mysql/0"])
        yield finished

        self.assertIn(
            "Connecting to remote machine", self.output.getvalue())

        self.assertIn(
            "Debug session ended.", self.output.getvalue())

    @inlineCallbacks
    def test_standard_named_debug_hook(self):
        """A hook can be debugged by name.
        """
        yield self.verify_hook_debug("start")
        self.mocker.reset()
        self.setup_exit(0)
        yield self.verify_hook_debug("stop")
        self.mocker.reset()
        self.setup_exit(0)
        yield self.verify_hook_debug("server-relation-changed")
        self.mocker.reset()
        self.setup_exit(0)
        yield self.verify_hook_debug("server-relation-changed",
                                     "server-relation-broken")
        self.mocker.reset()
        self.setup_exit(0)
        yield self.verify_hook_debug("server-relation-joined",
                                     "server-relation-departed")

    @inlineCallbacks
    def verify_hook_debug(self, *hook_names):
        """Utility function to verify hook debugging by name
        """
        mock_environment = self.mocker.patch(Environment)
        mock_environment.get_machine_provider()
        self.mocker.result(self.provider)

        system_mock = self.mocker.replace(os.system)
        system_mock(ANY)

        def on_ssh(command):
            self.assertStartsWith(command, "ssh -t ubuntu@x11.example.com")
            self.assertEqual(
                (yield self.unit.get_hook_debug()),
                {"debug_hooks": list(hook_names)})
            returnValue(True)

        self.mocker.call(on_ssh)

        finished = self.setup_cli_reactor()
        self.mocker.replay()

        yield self.unit.set_public_address("x11.example.com")

        args = ["debug-hooks", "mysql/0"]
        args.extend(hook_names)
        main(args)

        yield finished

        self.assertIn(
            "Connecting to remote machine", self.output.getvalue())

        self.assertIn(
            "Debug session ended.", self.output.getvalue())
