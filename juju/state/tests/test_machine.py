import functools

import yaml

from twisted.internet.defer import inlineCallbacks, Deferred, returnValue

from juju.charm.tests import local_charm_id
from juju.state.charm import CharmStateManager
from juju.state.machine import MachineStateManager
from juju.state.service import ServiceStateManager
from juju.state.errors import (
    MachineStateNotFound, MachineStateInUse, StateChanged)

from juju.state.tests.common import StateTestBase


class MachineStateManagerTest(StateTestBase):

    @inlineCallbacks
    def setUp(self):
        yield super(MachineStateManagerTest, self).setUp()
        self.charm_state_manager = CharmStateManager(self.client)
        self.machine_state_manager = MachineStateManager(self.client)
        self.service_state_manager = ServiceStateManager(self.client)
        self.charm_state = yield self.charm_state_manager.add_charm_state(
            local_charm_id(self.charm), self.charm, "")

    @inlineCallbacks
    def add_service(self, service_name):
        service_state = yield self.service_state_manager.add_service_state(
            service_name, self.charm_state)
        returnValue(service_state)

    @inlineCallbacks
    def test_add_machine(self):
        """
        Adding a machine state should register it in zookeeper.
        """
        machine_state1 = yield self.machine_state_manager.add_machine_state()
        machine_state2 = yield self.machine_state_manager.add_machine_state()

        self.assertEquals(machine_state1.id, 0)
        self.assertEquals(machine_state1.internal_id, "machine-0000000000")
        self.assertEquals(machine_state2.id, 1)
        self.assertEquals(machine_state2.internal_id, "machine-0000000001")

        children = yield self.client.get_children("/machines")
        self.assertEquals(sorted(children),
                          ["machine-0000000000", "machine-0000000001"])

        topology = yield self.get_topology()
        self.assertTrue(topology.has_machine("machine-0000000000"))
        self.assertTrue(topology.has_machine("machine-0000000001"))

    @inlineCallbacks
    def test_machine_str_representation(self):
        """The str(machine) value includes the machine id.
        """
        machine_state1 = yield self.machine_state_manager.add_machine_state()
        self.assertEqual(
            str(machine_state1), "<MachineState id:machine-%010d>" % (0))

    @inlineCallbacks
    def test_remove_machine(self):
        """
        Adding a machine state should register it in zookeeper.
        """
        machine_state1 = yield self.machine_state_manager.add_machine_state()
        yield self.machine_state_manager.add_machine_state()

        removed = yield self.machine_state_manager.remove_machine_state(
            machine_state1.id)
        self.assertTrue(removed)
        children = yield self.client.get_children("/machines")
        self.assertEquals(sorted(children),
                          ["machine-0000000001"])

        topology = yield self.get_topology()
        self.assertFalse(topology.has_machine("machine-0000000000"))
        self.assertTrue(topology.has_machine("machine-0000000001"))

        # Removing a non-existing machine again won't fail, since the end
        # intention is preserved.  This makes dealing with concurrency easier.
        # However, False will be returned in this case.
        removed = yield self.machine_state_manager.remove_machine_state(
            machine_state1.id)
        self.assertFalse(removed)

    @inlineCallbacks
    def test_remove_machine_with_agent(self):
        """Removing a machine with a connected machine agent should succeed.

        The removal signals intent to remove a working machine (with an agent)
        with the provisioning agent to remove it subsequently.
        """

        # Add two machines.
        machine_state1 = yield self.machine_state_manager.add_machine_state()
        yield self.machine_state_manager.add_machine_state()

        # Connect an agent
        yield machine_state1.connect_agent()

        # Remove a machine
        removed = yield self.machine_state_manager.remove_machine_state(
            machine_state1.id)
        self.assertTrue(removed)

        # Verify the second one is still present
        children = yield self.client.get_children("/machines")
        self.assertEquals(sorted(children),
                          ["machine-0000000001"])

        # Verify the topology state.
        topology = yield self.get_topology()
        self.assertFalse(topology.has_machine("machine-0000000000"))
        self.assertTrue(topology.has_machine("machine-0000000001"))

    @inlineCallbacks
    def test_get_machine_and_check_attributes(self):
        """
        Getting a machine state should be possible using both the
        user-oriented id and the internal id.
        """
        yield self.machine_state_manager.add_machine_state()
        yield self.machine_state_manager.add_machine_state()
        machine_state = yield self.machine_state_manager.get_machine_state(0)
        self.assertEquals(machine_state.id, 0)

        machine_state = yield self.machine_state_manager.get_machine_state("0")
        self.assertEquals(machine_state.id, 0)

        yield self.assertFailure(
            self.machine_state_manager.get_machine_state("a"),
            MachineStateNotFound)

    @inlineCallbacks
    def test_get_machine_not_found(self):
        """
        Getting a machine state which is not available should errback
        a meaningful error.
        """
        # No state whatsoever.
        try:
            yield self.machine_state_manager.get_machine_state(0)
        except MachineStateNotFound, e:
            self.assertEquals(e.machine_id, 0)
        else:
            self.fail("Error not raised")

        # Some state.
        yield self.machine_state_manager.add_machine_state()
        try:
            yield self.machine_state_manager.get_machine_state(1)
        except MachineStateNotFound, e:
            self.assertEquals(e.machine_id, 1)
        else:
            self.fail("Error not raised")

    @inlineCallbacks
    def test_get_all_machine_states(self):
        machines = yield self.machine_state_manager.get_all_machine_states()
        self.assertFalse(machines)

        yield self.machine_state_manager.add_machine_state()
        machines = yield self.machine_state_manager.get_all_machine_states()
        self.assertEquals(len(machines), 1)

        yield self.machine_state_manager.add_machine_state()
        machines = yield self.machine_state_manager.get_all_machine_states()
        self.assertEquals(len(machines), 2)

    @inlineCallbacks
    def test_set_functions(self):
        m1 = yield self.machine_state_manager.add_machine_state()
        m2 = yield self.machine_state_manager.add_machine_state()

        m3 = yield self.machine_state_manager.get_machine_state(0)
        m4 = yield self.machine_state_manager.get_machine_state(1)

        self.assertEquals(hash(m1), hash(m3))
        self.assertEquals(hash(m2), hash(m4))
        self.assertEquals(m1, m3)
        self.assertEquals(m2, m4)

        self.assertNotEqual(m1, object())
        self.assertNotEqual(m1, m2)

    @inlineCallbacks
    def test_set_and_get_instance_id(self):
        """
        Each provider must have its own notion of an id for machines it offers.
        The MachineState enables keeping track of that for reference, so we
        must be able to get and set with simple accessor methods.
        """
        machine_state0 = yield self.machine_state_manager.add_machine_state()
        yield machine_state0.set_instance_id("custom-id")

        machine_state1 = yield self.machine_state_manager.get_machine_state(
            machine_state0.id)
        instance_id = yield machine_state1.get_instance_id()

        self.assertEquals(instance_id, "custom-id")

        content, stat = yield self.client.get("/machines/machine-0000000000")
        self.assertEquals(yaml.load(content),
                          {"provider-machine-id": "custom-id"})

    @inlineCallbacks
    def test_set_instance_id_preserves_existing_data(self):
        """
        If there's more data in the machine node, it will be preserved.
        """
        machine_state = yield self.machine_state_manager.add_machine_state()
        yield self.client.set("/machines/machine-0000000000",
                              yaml.dump({"foo": "bar"}))
        yield machine_state.set_instance_id("custom-id")

        content, stat = yield self.client.get("/machines/machine-0000000000")
        self.assertEquals(yaml.load(content),
                          {"provider-machine-id": "custom-id",
                           "foo": "bar"})

    @inlineCallbacks
    def test_set_instance_id_if_machine_state_is_removed(self):
        """
        The set_instance_id method shouldn't attempt to recreate the zk node
        in case it gets removed.  Instead, it should raise a StateChanged
        exception.
        """
        machine_state = yield self.machine_state_manager.add_machine_state()
        removed = yield self.machine_state_manager.remove_machine_state(
            machine_state.id)
        self.assertTrue(removed)
        d = machine_state.set_instance_id(123)
        yield self.assertFailure(d, StateChanged)
        exists = yield self.client.exists("/machines/machine-0000000000")
        self.assertFalse(exists)

    @inlineCallbacks
    def test_get_unset_instance_id(self):
        """
        When the instance_id is still unset, None is returned.
        """
        machine_state = yield self.machine_state_manager.add_machine_state()
        instance_id = yield machine_state.get_instance_id()
        self.assertEquals(instance_id, None)

    @inlineCallbacks
    def test_get_instance_id_when_machine_is_removed(self):
        """
        When the instance_id is still unset, None is returned.
        """
        machine_state = yield self.machine_state_manager.add_machine_state()
        removed = yield self.machine_state_manager.remove_machine_state(
            machine_state.id)
        self.assertTrue(removed)
        d = machine_state.get_instance_id()
        yield self.assertFailure(d, StateChanged)

    @inlineCallbacks
    def test_get_unset_instance_id_when_node_has_data(self):
        """
        When the instance_id is still unset, None is returned, *even*
        if the node has some other data in it.
        """
        machine_state = yield self.machine_state_manager.add_machine_state()
        yield self.client.set("/machines/machine-0000000000",
                              yaml.dump({"foo": "bar"}))

        instance_id = yield machine_state.get_instance_id()
        self.assertEquals(instance_id, None)

    @inlineCallbacks
    def test_machine_agent(self):
        """A machine state has an associated machine agent.
        """
        machine_state = yield self.machine_state_manager.add_machine_state()
        exists_d, watch_d = machine_state.watch_agent()
        exists = yield exists_d
        self.assertFalse(exists)
        yield machine_state.connect_agent()
        event = yield watch_d
        self.assertEqual(event.type_name, "created")
        self.assertEqual(event.path,
                         "/machines/%s/agent" % machine_state.internal_id)

    @inlineCallbacks
    def test_watch_machines_initial_callback(self):
        """Watch machine processes initial state before returning.

        Note the callback is only executed if there is some meaningful state
        change.
        """
        results = []

        def callback(*args):
            results.append(True)

        yield self.machine_state_manager.add_machine_state()
        yield self.machine_state_manager.watch_machine_states(callback)
        self.assertTrue(results)

    @inlineCallbacks
    def test_watch_machines_when_being_created(self):
        """
        It should be possible to start watching machines even
        before they are created.  In this case, the callback will
        be made when it's actually introduced.
        """
        wait_callback = [Deferred() for i in range(10)]

        calls = []

        def watch_machines(old_machines, new_machines):
            calls.append((old_machines, new_machines))
            wait_callback[len(calls) - 1].callback(True)

        # Start watching.
        self.machine_state_manager.watch_machine_states(watch_machines)

        # Callback is still untouched.
        self.assertEquals(calls, [])

        # Add a machine, and wait for callback.
        yield self.machine_state_manager.add_machine_state()
        yield wait_callback[0]

        # The first callback must have been fired, and it must have None
        # as the first argument because that's the first machine seen.
        self.assertEquals(len(calls), 1)
        old_machines, new_machines = calls[0]
        self.assertEquals(old_machines, None)
        self.assertEquals(new_machines, set([0]))

        # Add a machine again.
        yield self.machine_state_manager.add_machine_state()
        yield wait_callback[1]

        # Now the watch callback must have been fired with two
        # different machine sets.  The old one, and the new one.
        self.assertEquals(len(calls), 2)
        old_machines, new_machines = calls[1]
        self.assertEquals(old_machines, set([0]))
        self.assertEquals(new_machines, set([0, 1]))

    @inlineCallbacks
    def test_watch_machines_may_defer(self):
        """
        The watch machines callback may return a deferred so that it
        performs some of its logic asynchronously.  In this case, it
        must not be called a second time before its postponed logic
        is finished completely.
        """
        wait_callback = [Deferred() for i in range(10)]
        finish_callback = [Deferred() for i in range(10)]

        calls = []

        def watch_machines(old_machines, new_machines):
            calls.append((old_machines, new_machines))
            wait_callback[len(calls) - 1].callback(True)
            return finish_callback[len(calls) - 1]

        # Start watching.
        self.machine_state_manager.watch_machine_states(watch_machines)

        # Create the machine.
        yield self.machine_state_manager.add_machine_state()

        # Hold off until callback is started.
        yield wait_callback[0]

        # Add another machine.
        yield self.machine_state_manager.add_machine_state()

        # Give a chance for something bad to happen.
        yield self.sleep(0.3)

        # Ensure we still have a single call.
        self.assertEquals(len(calls), 1)

        # Allow the first call to be completed, and wait on the
        # next one.
        finish_callback[0].callback(None)
        yield wait_callback[1]
        finish_callback[1].callback(None)

        # We should have the second change now.
        self.assertEquals(len(calls), 2)
        old_machines, new_machines = calls[1]
        self.assertEquals(old_machines, set([0]))
        self.assertEquals(new_machines, set([0, 1]))

    @inlineCallbacks
    def test_watch_machines_with_changing_topology(self):
        """
        If the topology changes in an unrelated way, the machines
        watch callback should not be called with two equal
        arguments.
        """
        wait_callback = [Deferred() for i in range(10)]

        calls = []

        def watch_machines(old_machines, new_machines):
            calls.append((old_machines, new_machines))
            wait_callback[len(calls) - 1].callback(True)

        # Start watching.
        self.machine_state_manager.watch_machine_states(watch_machines)

        # Callback is still untouched.
        self.assertEquals(calls, [])

        # Add a machine, and wait for callback.
        yield self.machine_state_manager.add_machine_state()
        yield wait_callback[0]

        # Give some time to prevent changes from grouping.
        yield self.sleep(0.1)

        # Now change the topology in an unrelated way.
        yield self.service_state_manager.add_service_state(
            "wordpress", self.charm_state)

        # Give some time to prevent changes from grouping.
        yield self.sleep(0.1)

        # Add a machine again.
        yield self.machine_state_manager.add_machine_state()
        yield wait_callback[1]

        # Finally, give a chance for the third call to happen.
        yield self.sleep(0.3)

        # But it *shouldn't* have happened.
        self.assertEquals(len(calls), 2)

    @inlineCallbacks
    def test_watch_service_units_processes_current_state(self):
        """
        The watch creation method only returns after processing
        initial state.

        Note, the callback is only invoked if there is a state change
        that needs processing.
        """
        machine_state0 = yield self.machine_state_manager.add_machine_state()
        service_state0 = yield self.service_state_manager.add_service_state(
            "wordpress", self.charm_state)
        unit_state0 = yield service_state0.add_unit_state()
        yield unit_state0.assign_to_machine(machine_state0)

        results = []

        def callback(*args):
            results.append(True)

        yield machine_state0.watch_assigned_units(callback)
        self.assertTrue(results)

    @inlineCallbacks
    def test_watch_service_units_in_machine(self):
        """
        We can also watch for service units which are assigned or
        unassigned from a specific machine.  This enables the
        service agent to keep an eye on things it's supposed to
        deploy/undeploy.
        """
        wait_callback = [Deferred() for i in range(30)]

        calls = []

        def watch_units(machine_id, old_units, new_units):
            calls.append((machine_id, old_units, new_units))
            wait_callback[len(calls) - 1].callback(True)

        watch_units0 = functools.partial(watch_units, 0)
        watch_units1 = functools.partial(watch_units, 1)

        # Create a couple of machines.
        machine_state0 = yield self.machine_state_manager.add_machine_state()
        machine_state1 = yield self.machine_state_manager.add_machine_state()

        # Set their watches.
        machine_state0.watch_assigned_units(watch_units0)
        machine_state1.watch_assigned_units(watch_units1)

        # Create some services and units.
        service_state0 = yield self.service_state_manager.add_service_state(
            "wordpress", self.charm_state)
        service_state1 = yield self.service_state_manager.add_service_state(
            "mysql", self.charm_state)

        unit_state0 = yield service_state0.add_unit_state()
        unit_state1 = yield service_state0.add_unit_state()
        unit_state2 = yield service_state1.add_unit_state()
        unit_state3 = yield service_state1.add_unit_state()

        # With all this setup in place, no unit was actually assigned to a
        # machine, so no callbacks should have happened yet.
        self.assertEquals(calls, [])

        # So assign a unit, and wait for the first callback.
        yield unit_state0.assign_to_machine(machine_state0)
        yield wait_callback[0]

        self.assertEquals(calls, [(0, None, set(["wordpress/0"]))])

        # Try it again with a different unit, same service and machine.
        yield unit_state1.assign_to_machine(machine_state0)
        yield wait_callback[1]

        self.assertEquals(len(calls), 2, calls)
        self.assertEquals(calls[1],
                          (0, set(["wordpress/0"]),
                              set(["wordpress/0", "wordpress/1"])))

        # Try it with a different service now, same machine.
        yield unit_state2.assign_to_machine(machine_state0)
        yield wait_callback[2]

        self.assertEquals(len(calls), 3, calls)
        self.assertEquals(calls[2],
                          (0, set(["wordpress/0", "wordpress/1"]),
                              set(["wordpress/0", "wordpress/1", "mysql/0"])))

        # Try it with a different machine altogether.
        yield unit_state3.assign_to_machine(machine_state1)
        yield wait_callback[3]

        self.assertEquals(len(calls), 4, calls)
        self.assertEquals(calls[3],
                          (1, None, set(["mysql/1"])))

        # Now let's unassign a unit from a machine.
        yield unit_state1.unassign_from_machine()
        yield wait_callback[4]

        self.assertEquals(len(calls), 5, calls)
        self.assertEquals(calls[4],
                          (0, set(["wordpress/0", "wordpress/1", "mysql/0"]),
                              set(["wordpress/0", "mysql/0"])))

        # And finally, let's *delete* a machine.  To do that, though, we must
        # first unassign the unit from it. This will trigger the callback
        # already, with an empty set of units.
        yield unit_state3.unassign_from_machine()
        yield wait_callback[5]

        self.assertEquals(len(calls), 6, calls)
        self.assertEquals(calls[5],
                          (1, set(["mysql/1"]), set()))

        # Now we can remove the machine itself, but that won't cause any
        # callbacks, since there were no units already.
        removed = yield self.machine_state_manager.remove_machine_state(
            machine_state1.id)
        self.assertTrue(removed)
        self.assertEquals(len(calls), 6, calls)

    test_watch_service_units_in_machine.timeout = 10

    @inlineCallbacks
    def test_watch_units_may_defer(self):
        """
        The watch units callback may return a deferred so that it
        performs some of its logic asynchronously.  In this case, it
        must not be called a second time before its postponed logic
        is finished completely.
        """
        wait_callback = [Deferred() for i in range(10)]
        finish_callback = [Deferred() for i in range(10)]

        calls = []

        def watch_units(old_units, new_units):
            calls.append((old_units, new_units))
            wait_callback[len(calls) - 1].callback(True)
            return finish_callback[len(calls) - 1]

        # Create the basic state and set the watch.
        machine_state = yield self.machine_state_manager.add_machine_state()
        machine_state.watch_assigned_units(watch_units)
        service_state = yield self.service_state_manager.add_service_state(
            "wordpress", self.charm_state)
        unit_state0 = yield service_state.add_unit_state()
        unit_state1 = yield service_state.add_unit_state()

        # Shouldn't have any callbacks yet.
        self.assertEquals(calls, [])

        # Assign first unit.
        yield unit_state0.assign_to_machine(machine_state)

        # Hold off until callback is started.
        yield wait_callback[0]

        # Assign another unit.
        yield unit_state1.assign_to_machine(machine_state)

        # Give a chance for something bad to happen.
        yield self.sleep(0.3)

        # Ensure we still have a single call.
        self.assertEquals(len(calls), 1)

        # Allow the first call to be completed, and wait on the
        # next one.
        finish_callback[0].callback(None)
        yield wait_callback[1]
        finish_callback[1].callback(None)

        # We should have the second change now.
        self.assertEquals(len(calls), 2)

    @inlineCallbacks
    def test_watch_unassignment_and_removal_at_once(self):
        """
        If units get quickly unassigned and the machine is
        removed, the change may be observed as a single modification
        in state, and the watch has to pretend that it saw the
        unassigned units rather than blowing up with the missing
        machine.
        """
        wait_callback = [Deferred() for i in range(10)]

        calls = []

        def watch_units(old_units, new_units):
            calls.append((old_units, new_units))
            wait_callback[len(calls) - 1].callback(True)

        # Create the state ahead of time.
        machine_state = yield self.machine_state_manager.add_machine_state()
        service_state = yield self.service_state_manager.add_service_state(
            "wordpress", self.charm_state)
        unit_state = yield service_state.add_unit_state()

        # Add the watch.
        machine_state.watch_assigned_units(watch_units)

        # Assign unit.
        yield unit_state.assign_to_machine(machine_state)

        # Wait for the callback, and discard it.
        yield wait_callback[0]
        self.assertEquals(len(calls), 1)

        # Grab the topology to ensure that unassignment and
        # machine removal are perceived at once.
        topology = yield self.get_topology()
        topology.unassign_service_unit_from_machine(
            service_state.internal_id, unit_state.internal_id)
        topology.remove_machine(machine_state.internal_id)
        yield self.set_topology(topology)

        # Hold off until callback is started.
        yield wait_callback[1]

        # Ensure we have a single call.
        self.assertEquals(len(calls), 2, calls)
        self.assertEquals(calls[1], (set(['wordpress/0']), set()))

    @inlineCallbacks
    def test_machine_cannot_be_removed_if_assigned(self):
        """Verify that a machine cannot be removed before being unassigned"""
        machine_state = yield self.machine_state_manager.add_machine_state()
        service_state = yield self.service_state_manager.add_service_state(
            "wordpress", self.charm_state)
        unit_state = yield service_state.add_unit_state()
        yield unit_state.assign_to_machine(machine_state)
        ex = yield self.assertFailure(
            self.machine_state_manager.remove_machine_state(machine_state.id),
            MachineStateInUse)
        self.assertEqual(ex.machine_id, 0)
        yield unit_state.unassign_from_machine()
        topology = yield self.get_topology()
        self.assertTrue(topology.has_machine("machine-0000000000"))
        removed = yield self.machine_state_manager.remove_machine_state(
            machine_state.id)
        self.assertTrue(removed)
        topology = yield self.get_topology()
        self.assertFalse(topology.has_machine("machine-0000000000"))
        # can do this multiple times
        removed = yield self.machine_state_manager.remove_machine_state(
            machine_state.id)
        self.assertFalse(removed)

    @inlineCallbacks
    def test_get_all_service_unit_states(self):
        """Verify retrieval of service unit states related to machine state."""

        # check with one service unit state for a machine, as
        # currently supported by provisioning
        machine_state = yield self.machine_state_manager.add_machine_state()
        wordpress = yield self.add_service("wordpress")
        wordpress_0 = yield wordpress.add_unit_state()
        yield wordpress_0.assign_to_machine(machine_state)
        unit_states = yield machine_state.get_all_service_unit_states()
        self.assertEqual(len(unit_states), 1)
        self.assertEqual(unit_states[0].unit_name, "wordpress/0")

        # check against multiple service units
        mysql = yield self.add_service("mysql")
        mysql_0 = yield mysql.add_unit_state()
        yield mysql_0.assign_to_machine(machine_state)
        wordpress_1 = yield wordpress.add_unit_state()
        yield wordpress_1.assign_to_machine(machine_state)
        unit_states = yield machine_state.get_all_service_unit_states()
        self.assertEqual(len(unit_states), 3)
        self.assertEqual(
            set(unit_state.unit_name for unit_state in unit_states),
            set(["wordpress/0", "wordpress/1", "mysql/0"]))

        # then check after unassigning the service unit to the machine
        yield wordpress_0.unassign_from_machine()
        unit_states = yield machine_state.get_all_service_unit_states()
        self.assertEqual(len(unit_states), 2)
        self.assertEqual(
            set(unit_state.unit_name for unit_state in unit_states),
            set(["wordpress/1", "mysql/0"]))

    @inlineCallbacks
    def test_get_all_service_unit_states_chaining(self):
        """Verify going from one service unit state, to machine, and back."""

        # to be extra cautious, create an extra machine to avoid
        # possibly having machine id = 0 be in some way spurious
        yield self.machine_state_manager.add_machine_state()

        # create one machine state, assigned to mysql/0 and wordpress/0
        machine_state = yield self.machine_state_manager.add_machine_state()
        wordpress = yield self.add_service("wordpress")
        wordpress_0 = yield wordpress.add_unit_state()
        yield wordpress_0.assign_to_machine(machine_state)
        mysql = yield self.add_service("mysql")
        mysql_0 = yield mysql.add_unit_state()
        yield mysql_0.assign_to_machine(machine_state)

        # verify get back an equivalent machine state
        machine_id = yield wordpress_0.get_assigned_machine_id()
        self.assertEqual(machine_state.id, machine_id)

        # and verify we have the right service unit states, including
        # the one we started with (wordpress/0)
        unit_states = yield machine_state.get_all_service_unit_states()
        self.assertEqual(len(unit_states), 2)
        self.assertEqual(
            set(unit_state.unit_name for unit_state in unit_states),
            set(["mysql/0", "wordpress/0"]))
