import logging
import yaml

from twisted.internet.defer import inlineCallbacks

from juju.control import main, terminate_machine
from juju.control.tests.common import MachineControlToolTest
from juju.errors import CannotTerminateMachine
from juju.state.errors import MachineStateInUse, MachineStateNotFound


class ControlTerminateMachineTest(MachineControlToolTest):

    @inlineCallbacks
    def setUp(self):
        yield super(ControlTerminateMachineTest, self).setUp()
        config = {
            "environments": {
                "firstenv": {
                    "type": "dummy", "admin-secret": "homer"}}}
        self.write_config(yaml.dump(config))
        self.config.load()
        self.output = self.capture_logging()
        self.stderr = self.capture_stream("stderr")

    @inlineCallbacks
    def test_terminate_machine_method(self):
        """Verify that underlying method works as expected."""
        environment = self.config.get("firstenv")
        mysql_service_state = yield self.add_service_from_charm("mysql")
        mysql_unit_state = yield mysql_service_state.add_unit_state()
        mysql_machine_state = \
            yield self.machine_state_manager.add_machine_state()
        yield mysql_unit_state.assign_to_machine(mysql_machine_state)
        wordpress_service_state = \
            yield self.add_service_from_charm("wordpress")
        wordpress_unit_state = yield wordpress_service_state.add_unit_state()
        wordpress_machine_state = \
            yield self.machine_state_manager.add_machine_state()
        yield wordpress_unit_state.assign_to_machine(wordpress_machine_state)
        yield wordpress_unit_state.unassign_from_machine()
        yield terminate_machine.terminate_machine(
            self.config, environment, False,
            logging.getLogger("juju.control.cli"), [2])
        yield self.assert_machine_states([0, 1], [2])

    @inlineCallbacks
    def test_terminate_machine_method_root(self):
        """Verify supporting method throws `CannotTerminateMachine`."""
        environment = self.config.get("firstenv")
        mysql_service_state = yield self.add_service_from_charm("mysql")
        mysql_unit_state = yield mysql_service_state.add_unit_state()
        mysql_machine_state = \
            yield self.machine_state_manager.add_machine_state()
        yield mysql_unit_state.assign_to_machine(mysql_machine_state)
        ex = yield self.assertFailure(
            terminate_machine.terminate_machine(
                self.config, environment, False,
                logging.getLogger("juju.control.cli"), [0]),
            CannotTerminateMachine)
        self.assertEqual(
            str(ex),
            "Cannot terminate machine 0: environment would be destroyed")

    @inlineCallbacks
    def test_terminate_machine_method_in_use(self):
        """Verify supporting method throws `MachineStateInUse`."""
        environment = self.config.get("firstenv")
        mysql_service_state = yield self.add_service_from_charm("mysql")
        mysql_unit_state = yield mysql_service_state.add_unit_state()
        mysql_machine_state = \
            yield self.machine_state_manager.add_machine_state()
        yield mysql_unit_state.assign_to_machine(mysql_machine_state)
        ex = yield self.assertFailure(
            terminate_machine.terminate_machine(
                self.config, environment, False,
                logging.getLogger("juju.control.cli"), [1]),
            MachineStateInUse)
        self.assertEqual(ex.machine_id, 1)
        yield self.assert_machine_states([0, 1], [])

    @inlineCallbacks
    def test_terminate_machine_method_unknown(self):
        """Verify supporting method throws `MachineStateInUse`."""
        environment = self.config.get("firstenv")
        mysql_service_state = yield self.add_service_from_charm("mysql")
        mysql_unit_state = yield mysql_service_state.add_unit_state()
        mysql_machine_state = \
            yield self.machine_state_manager.add_machine_state()
        yield mysql_unit_state.assign_to_machine(mysql_machine_state)
        ex = yield self.assertFailure(
            terminate_machine.terminate_machine(
                self.config, environment, False,
                logging.getLogger("juju.control.cli"), [42]),
            MachineStateNotFound)
        self.assertEqual(ex.machine_id, 42)
        yield self.assert_machine_states([0, 1], [])

    @inlineCallbacks
    def test_terminate_unused_machine(self):
        """Verify a typical allocation, unassignment, and then termination."""
        wait_on_reactor_stopped = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()
        wordpress_service_state = \
            yield self.add_service_from_charm("wordpress")
        wordpress_unit_state = yield wordpress_service_state.add_unit_state()
        wordpress_machine_state = \
            yield self.machine_state_manager.add_machine_state()
        yield wordpress_unit_state.assign_to_machine(wordpress_machine_state)
        riak_service_state = yield self.add_service_from_charm("riak")
        riak_unit_state = yield riak_service_state.add_unit_state()
        riak_machine_state = \
            yield self.machine_state_manager.add_machine_state()
        yield riak_unit_state.assign_to_machine(riak_machine_state)
        mysql_service_state = yield self.add_service_from_charm("mysql")
        mysql_unit_state = yield mysql_service_state.add_unit_state()
        mysql_machine_state = \
            yield self.machine_state_manager.add_machine_state()
        yield mysql_unit_state.assign_to_machine(mysql_machine_state)
        yield wordpress_unit_state.unassign_from_machine()
        yield mysql_unit_state.unassign_from_machine()
        yield self.assert_machine_states([0, 1, 2, 3], [])
        main(["terminate-machine", "1", "3"])
        yield wait_on_reactor_stopped
        self.assertIn(
            "Machines terminated: 1, 3", self.output.getvalue())
        yield self.assert_machine_states([0, 2], [1, 3])

    @inlineCallbacks
    def test_attempt_terminate_unknown_machine(self):
        """Try to terminate a used machine and get an in use error in log."""
        wait_on_reactor_stopped = self.setup_cli_reactor()
        self.setup_exit(0) # XXX should be 1, see bug #697093
        self.mocker.replay()
        mysql_service_state = yield self.add_service_from_charm("mysql")
        mysql_unit_state = yield mysql_service_state.add_unit_state()
        mysql_machine_state = \
            yield self.machine_state_manager.add_machine_state()
        yield mysql_unit_state.assign_to_machine(mysql_machine_state)
        main(["terminate-machine", "42", "1"])
        yield wait_on_reactor_stopped
        self.assertIn("Machine 42 was not found", self.output.getvalue())
        yield self.assert_machine_states([1], [])

    @inlineCallbacks
    def test_attempt_terminate_root_machine(self):
        """Try to terminate root machine and get corresponding error in log."""
        wait_on_reactor_stopped = self.setup_cli_reactor()
        self.setup_exit(0) # XXX should be 1, see bug #697093
        self.mocker.replay()
        mysql_service_state = yield self.add_service_from_charm("mysql")
        mysql_unit_state = yield mysql_service_state.add_unit_state()
        mysql_machine_state = \
            yield self.machine_state_manager.add_machine_state()
        yield mysql_unit_state.assign_to_machine(mysql_machine_state)
        main(["terminate-machine", "0", "1"])
        yield wait_on_reactor_stopped
        self.assertIn(
            "Cannot terminate machine 0: environment would be destroyed",
            self.output.getvalue())
        yield self.assert_machine_states([0, 1], [])

    @inlineCallbacks
    def test_do_nothing(self):
        """Verify terminate-machine can take no args and then does nothing."""
        wait_on_reactor_stopped = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()
        mysql_service_state = yield self.add_service_from_charm("mysql")
        mysql_unit_state = yield mysql_service_state.add_unit_state()
        mysql_machine_state = \
            yield self.machine_state_manager.add_machine_state()
        yield mysql_unit_state.assign_to_machine(mysql_machine_state)
        main(["terminate-machine"])
        yield wait_on_reactor_stopped
        yield self.assert_machine_states([0, 1], [])

    def test_wrong_arguments_provided_non_integer(self):
        """Test command rejects non-integer args."""
        self.assertRaises(
            SystemExit, main, ["terminate-machine", "bar"])
        self.assertIn(
            "juju terminate-machine: error: argument ID: "
            "invalid int value: 'bar'",
            self.stderr.getvalue())

    @inlineCallbacks
    def test_invalid_environment(self):
        """Test command with an environment that hasn't been set up."""
        wait_on_reactor_stopped = self.setup_cli_reactor()
        self.setup_exit(1)
        self.mocker.replay()
        mysql_service_state = yield self.add_service_from_charm("mysql")
        mysql_unit_state = yield mysql_service_state.add_unit_state()
        mysql_machine_state = \
            yield self.machine_state_manager.add_machine_state()
        yield mysql_unit_state.assign_to_machine(mysql_machine_state)
        wordpress_service_state = \
            yield self.add_service_from_charm("wordpress")
        wordpress_unit_state = yield wordpress_service_state.add_unit_state()
        wordpress_machine_state = \
            yield self.machine_state_manager.add_machine_state()
        yield wordpress_unit_state.assign_to_machine(wordpress_machine_state)
        main(["terminate-machine", "--environment", "roman-candle",
              "1", "2"])
        yield wait_on_reactor_stopped
        self.assertIn(
            "Invalid environment 'roman-candle'",
            self.stderr.getvalue())
        yield self.assert_machine_states([0, 1, 2], [])
