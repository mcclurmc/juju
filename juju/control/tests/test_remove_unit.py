import logging
import sys

from twisted.internet.defer import inlineCallbacks
from yaml import dump
import zookeeper

from .common import ControlToolTest

from juju.charm.tests.test_repository import RepositoryTestBase
from juju.control import main
from juju.state.machine import MachineStateManager
from juju.state.tests.test_service import ServiceStateManagerTestBase


class ControlRemoveUnitTest(
    ServiceStateManagerTestBase, ControlToolTest, RepositoryTestBase):

    @inlineCallbacks
    def setUp(self):
        yield super(ControlRemoveUnitTest, self).setUp()
        config = {
            "environments": {"firstenv": {"type": "dummy"}}}

        self.write_config(dump(config))
        self.config.load()

        self.environment = self.config.get_default()
        self.provider = self.environment.get_machine_provider()

        # Setup some service units.
        self.service_state1 = yield self.add_service_from_charm("mysql")
        self.service_unit1 = yield self.service_state1.add_unit_state()
        self.service_unit2 = yield self.service_state1.add_unit_state()
        self.service_unit3 = yield self.service_state1.add_unit_state()

        # Add an assigned machine to one of them.
        self.machine_manager = MachineStateManager(self.client)
        self.machine = yield self.machine_manager.add_machine_state()
        yield self.machine.set_instance_id(0)
        yield self.service_unit1.assign_to_machine(self.machine)

        # Setup a machine in the provider matching the assigned.
        self.provider_machine = yield self.provider.start_machine(
            {"machine-id": 0, "dns-name": "antigravity.example.com"})

        self.output = self.capture_logging(level=logging.DEBUG)
        self.stderr = self.capture_stream("stderr")

    @inlineCallbacks
    def test_remove_unit(self):
        """
        'juju remove-unit <unit_name>' will remove the given unit.
        """
        unit_names = yield self.service_state1.get_unit_names()
        self.assertEqual(len(unit_names), 3)

        finished = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()
        main(["remove-unit", "mysql/0"])
        yield finished

        topology = yield self.get_topology()
        self.assertFalse(topology.has_service_unit(
            self.service_state1.internal_id, self.service_unit1.internal_id))

        topology = yield self.get_topology()
        self.assertTrue(topology.has_service_unit(
            self.service_state1.internal_id, self.service_unit2.internal_id))

        self.assertFalse(
            topology.get_service_units_in_machine(self.machine.internal_id))

        self.assertIn(
            "Unit 'mysql/0' removed from service 'mysql'",
            self.output.getvalue())

    @inlineCallbacks
    def test_remove_multiple_units(self):
        """
        'juju remove-unit <unit_name1> <unit_name2>...' removes desired units.
        """
        unit_names = yield self.service_state1.get_unit_names()
        self.assertEqual(len(unit_names), 3)

        finished = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()
        main(["remove-unit", "mysql/0", "mysql/2"])
        yield finished

        topology = yield self.get_topology()
        self.assertFalse(topology.has_service_unit(
            self.service_state1.internal_id, self.service_unit1.internal_id))

        topology = yield self.get_topology()
        self.assertTrue(topology.has_service_unit(
            self.service_state1.internal_id, self.service_unit2.internal_id))

        self.assertFalse(
            topology.get_service_units_in_machine(self.machine.internal_id))

        self.assertIn(
            "Unit 'mysql/0' removed from service 'mysql'",
            self.output.getvalue())
        self.assertIn(
            "Unit 'mysql/2' removed from service 'mysql'",
            self.output.getvalue())

    @inlineCallbacks
    def test_remove_unassigned_unit(self):
        """Remove unit also works if the unit is unassigned to a machine.
        """
        unit_names = yield self.service_state1.get_unit_names()
        self.assertEqual(len(unit_names), 3)

        finished = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()
        main(["remove-unit", "mysql/1"])
        yield finished

        # verify the unit and its machine assignment.
        unit_names = yield self.service_state1.get_unit_names()
        self.assertEqual(len(unit_names), 2)

        topology = yield self.get_topology()
        topology = yield self.get_topology()
        self.assertFalse(topology.has_service_unit(
            self.service_state1.internal_id, self.service_unit2.internal_id))

        topology = yield self.get_topology()
        self.assertTrue(topology.has_service_unit(
            self.service_state1.internal_id, self.service_unit1.internal_id))

    @inlineCallbacks
    def test_remove_unit_unknown_service(self):
        """If the service doesn't exist, return an appropriate error message.
        """
        finished = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()
        main(["remove-unit", "volcano/0"])
        yield finished
        self.assertIn(
            "Service 'volcano' was not found", self.stderr.getvalue())

    @inlineCallbacks
    def test_remove_unit_bad_parse(self):
        """Verify that a bad service unit name results in an appropriate error.
        """
        finished = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()
        main(["remove-unit", "volcano-0"])
        yield finished
        self.assertIn(
            "Not a proper unit name: 'volcano-0'", self.stderr.getvalue())

    @inlineCallbacks
    def test_remove_unit_unknown_unit(self):
        """If the unit doesn't exist an appropriate error message is returned.
        """
        finished = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()
        main(["remove-unit", "mysql/3"])
        yield finished
        self.assertIn(
            "Service unit 'mysql/3' was not found", self.stderr.getvalue())

    @inlineCallbacks
    def test_zookeeper_logging_default(self):
        """By default zookeeper logging is turned off, unless in verbose
        mode.
        """
        log_file = self.makeFile()

        def reset_zk_log():
            zookeeper.set_debug_level(0)
            zookeeper.set_log_stream(sys.stdout)

        self.addCleanup(reset_zk_log)
        finished = self.setup_cli_reactor()
        self.setup_exit(0)
        # Do this as late as possible to prevent zk background logging
        # from causing problems.
        zookeeper.set_debug_level(zookeeper.LOG_LEVEL_INFO)
        zookeeper.set_log_stream(open(log_file, "w"))
        self.mocker.replay()
        main(["remove-unit", "mysql/3"])

        yield finished
        output = open(log_file).read()
        self.assertEqual(output, "")

    @inlineCallbacks
    def test_zookeeper_logging_enabled(self):
        """By default zookeeper logging is turned off, unless in verbose
        mode.
        """
        log_file = self.makeFile()
        zookeeper.set_debug_level(10)
        zookeeper.set_log_stream(open(log_file, "w"))

        def reset_zk_log():
            zookeeper.set_debug_level(0)
            zookeeper.set_log_stream(sys.stdout)

        self.addCleanup(reset_zk_log)
        finished = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()
        main(["-v", "remove-unit", "mysql/3"])

        yield finished
        output = open(log_file).read()
        self.assertTrue(output)
        self.assertIn("ZOO_DEBUG", output)
