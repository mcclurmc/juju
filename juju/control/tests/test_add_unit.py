from twisted.internet.defer import inlineCallbacks
from yaml import dump

from juju.control import main

from .common import MachineControlToolTest


class ControlAddUnitTest(MachineControlToolTest):

    @inlineCallbacks
    def setUp(self):
        yield super(ControlAddUnitTest, self).setUp()
        config = {
            "environments": {"firstenv": {"type": "dummy"}}}

        self.write_config(dump(config))
        self.config.load()
        self.service_state1 = yield self.add_service_from_charm("mysql")
        self.service_unit1 = yield self.service_state1.add_unit_state()
        self.machine_state1 = yield self.machine_state_manager.\
            add_machine_state()
        yield self.service_unit1.assign_to_machine(self.machine_state1)

        self.output = self.capture_logging()
        self.stderr = self.capture_stream("stderr")

    @inlineCallbacks
    def test_add_unit(self):
        """
        'juju add-unit <service_name>' will add a new service
        unit of the given service.
        """
        unit_names = yield self.service_state1.get_unit_names()
        self.assertEqual(len(unit_names), 1)

        finished = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()
        main(["add-unit", "mysql"])
        yield finished

        # verify the unit and its machine assignment.
        unit_names = yield self.service_state1.get_unit_names()
        self.assertEqual(len(unit_names), 2)

        topology = yield self.get_topology()
        unit = yield self.service_state1.get_unit_state("mysql/1")
        machine_id = topology.get_service_unit_machine(
            self.service_state1.internal_id, unit.internal_id)
        self.assertNotEqual(machine_id, None)
        self.assertIn(
            "Unit 'mysql/1' added to service 'mysql'",
            self.output.getvalue())
        yield self.assert_machine_assignments("mysql", [1, 2])

    @inlineCallbacks
    def test_add_multiple_units(self):
        """
        'juju add-unit <service_name>' will add a new service
        unit of the given service.
        """
        unit_names = yield self.service_state1.get_unit_names()
        self.assertEqual(len(unit_names), 1)

        finished = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()
        main(["add-unit", "--num-units", "5", "mysql"])
        yield finished

        # verify the unit and its machine assignment.
        unit_names = yield self.service_state1.get_unit_names()
        self.assertEqual(len(unit_names), 6)

        topology = yield self.get_topology()
        unit = yield self.service_state1.get_unit_state("mysql/1")
        machine_id = topology.get_service_unit_machine(
            self.service_state1.internal_id, unit.internal_id)
        self.assertNotEqual(machine_id, None)
        for i in xrange(1, 6):
            self.assertIn(
                "Unit 'mysql/%d' added to service 'mysql'" % i,
                self.output.getvalue())
        yield self.assert_machine_assignments("mysql", [1, 2, 3, 4, 5, 6])

    @inlineCallbacks
    def test_add_unit_unknown_service(self):
        finished = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()
        main(["add-unit", "volcano"])
        yield finished
        self.assertIn(
            "Service 'volcano' was not found", self.stderr.getvalue())

    @inlineCallbacks
    def test_add_unit_reuses_machines(self):
        """Verify that if machines are not in use, add-unit uses them."""
        # add machine to wordpress, then destroy and reallocate later
        # in this test to mysql as mysql/1's machine
        wordpress_service_state = yield self.add_service_from_charm(
            "wordpress")
        wordpress_unit_state = yield wordpress_service_state.add_unit_state()
        wordpress_machine_state = \
            yield self.machine_state_manager.add_machine_state()
        yield wordpress_unit_state.assign_to_machine(wordpress_machine_state)
        yield wordpress_unit_state.unassign_from_machine()
        finished = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()
        main(["add-unit", "mysql"])
        yield finished
        self.assertIn(
            "Unit 'mysql/1' added to service 'mysql'",
            self.output.getvalue())
        yield self.assert_machine_assignments("wordpress", [None])
        yield self.assert_machine_assignments("mysql", [1, 2])

    @inlineCallbacks
    def test_policy_from_environment(self):
        config = {
            "environments": {"firstenv": {
                    "placement": "local",
                    "type": "dummy"}}}

        self.write_config(dump(config))
        self.config.load()

        ms0 = yield self.machine_state_manager.get_machine_state(0)
        yield self.service_unit1.unassign_from_machine()
        yield self.service_unit1.assign_to_machine(ms0)

        unit_names = yield self.service_state1.get_unit_names()
        self.assertEqual(len(unit_names), 1)

        finished = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()
        main(["add-unit", "mysql"])
        yield finished

        # Verify the local policy was used
        topology = yield self.get_topology()
        unit = yield self.service_state1.get_unit_state("mysql/1")
        machine_id = topology.get_service_unit_machine(
            self.service_state1.internal_id, unit.internal_id)
        self.assertNotEqual(machine_id, None)
        self.assertIn(
            "Unit 'mysql/1' added to service 'mysql'",
            self.output.getvalue())
        # adding a second unit still assigns to machine 0 with local policy
        yield self.assert_machine_assignments("mysql", [0, 0])
