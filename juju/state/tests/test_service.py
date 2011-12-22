import os.path

import yaml
import zookeeper

from twisted.internet.defer import inlineCallbacks, Deferred, returnValue

from juju.charm.errors import CharmURLError
from juju.charm.directory import CharmDirectory
from juju.charm.tests import local_charm_id
from juju.charm.tests.test_metadata import test_repository_path
from juju.state.charm import CharmStateManager
from juju.state.endpoint import RelationEndpoint
from juju.state.service import (
    ServiceStateManager, NO_HOOKS, RETRY_HOOKS, parse_service_name)
from juju.state.machine import MachineStateManager
from juju.state.relation import RelationStateManager
from juju.state.errors import (
    StateChanged, ServiceStateNotFound, ServiceUnitStateNotFound,
    ServiceUnitStateMachineAlreadyAssigned, ServiceStateNameInUse,
    BadDescriptor, BadServiceStateName, ServiceUnitDebugAlreadyEnabled,
    MachineStateNotFound, NoUnusedMachines, ServiceUnitResolvedAlreadyEnabled,
    ServiceUnitRelationResolvedAlreadyEnabled, StopWatcher)


from juju.state.tests.common import StateTestBase


class ServiceStateManagerTestBase(StateTestBase):

    @inlineCallbacks
    def setUp(self):
        yield super(ServiceStateManagerTestBase, self).setUp()
        self.charm_state_manager = CharmStateManager(self.client)
        self.service_state_manager = ServiceStateManager(self.client)
        self.machine_state_manager = MachineStateManager(self.client)
        self.charm_state = yield self.charm_state_manager.add_charm_state(
            local_charm_id(self.charm), self.charm, "")
        self.relation_state_manager = RelationStateManager(self.client)

    @inlineCallbacks
    def add_service(self, name):
        service_state = yield self.service_state_manager.add_service_state(
            name, self.charm_state)
        returnValue(service_state)

    @inlineCallbacks
    def add_service_from_charm(self, service_name, charm_id=None):
        charm_dir = CharmDirectory(os.path.join(
            test_repository_path, "series", service_name))
        if charm_id is None:
            charm_state = yield self.charm_state_manager.add_charm_state(
                local_charm_id(charm_dir), charm_dir, "")
        else:
            charm_state = yield self.charm_state_manager.get_charm_state(
                charm_id)
        service_state = yield self.service_state_manager.add_service_state(
            service_name, charm_state)
        returnValue(service_state)

    @inlineCallbacks
    def add_relation(self, relation_type, *services):
        endpoints = []
        for service_meta in services:
            service_state, relation_name, relation_role = service_meta
            endpoints.append(RelationEndpoint(
                service_state.service_name,
                relation_type,
                relation_name,
                relation_role))
        relation_state = yield self.relation_state_manager.add_relation_state(
            *endpoints)
        returnValue(relation_state[0])

    @inlineCallbacks
    def remove_service(self, internal_service_id):
        topology = yield self.get_topology()
        topology.remove_service(internal_service_id)
        yield self.set_topology(topology)

    @inlineCallbacks
    def remove_service_unit(self, internal_service_id, internal_unit_id):
        topology = yield self.get_topology()
        topology.remove_service_unit(internal_service_id, internal_unit_id)
        yield self.set_topology(topology)

    @inlineCallbacks
    def assert_machine_states(self, present, absent):
        """Assert that machine IDs are either `present` or `absent` in topo."""
        for machine_id in present:
            self.assertTrue(
                (yield self.machine_state_manager.get_machine_state(
                        machine_id)))
        for machine_id in absent:
            ex = yield self.assertFailure(
                self.machine_state_manager.get_machine_state(machine_id),
                MachineStateNotFound)
            self.assertEqual(ex.machine_id, machine_id)

    @inlineCallbacks
    def assert_machine_assignments(self, service_name, machine_ids):
        """Assert that `service_name` is deployed on `machine_ids`."""
        topology = yield self.get_topology()
        service_id = topology.find_service_with_name(service_name)
        assigned_machine_ids = [
            topology.get_service_unit_machine(service_id, unit_id)
            for unit_id in topology.get_service_units(service_id)]
        internal_machine_ids = []
        for machine_id in machine_ids:
            if machine_id is None:
                # corresponds to get_service_unit_machine API in this case
                internal_machine_ids.append(None)
            else:
                internal_machine_ids.append("machine-%010d" % machine_id)
        self.assertEqual(
            set(assigned_machine_ids), set(internal_machine_ids))

    @inlineCallbacks
    def get_unit_state(self):
        """Simple test helper to get a unit state."""
        service_state = yield self.service_state_manager.add_service_state(
            "wordpress", self.charm_state)
        unit_state = yield service_state.add_unit_state()
        returnValue(unit_state)


class ServiceStateManagerTest(ServiceStateManagerTestBase):

    @inlineCallbacks
    def test_add_service(self):
        """
        Adding a service state should register it in zookeeper,
        including the requested charm id.
        """
        yield self.service_state_manager.add_service_state(
            "wordpress", self.charm_state)
        yield self.service_state_manager.add_service_state(
            "mysql", self.charm_state)
        children = yield self.client.get_children("/services")

        self.assertEquals(sorted(children),
                          ["service-0000000000", "service-0000000001"])

        content, stat = yield self.client.get("/services/service-0000000000")
        details = yaml.load(content)
        self.assertTrue(details)
        self.assertEquals(details.get("charm"), "local:series/dummy-1")
        self.assertFalse(isinstance(details.get("charm"), unicode))

        topology = yield self.get_topology()
        self.assertEquals(topology.find_service_with_name("wordpress"),
                          "service-0000000000")
        self.assertEquals(topology.find_service_with_name("mysql"),
                          "service-0000000001")

    @inlineCallbacks
    def test_add_service_with_duplicated_name(self):
        """
        If a service is added with a duplicated name, a meaningful
        error should be raised.
        """
        yield self.service_state_manager.add_service_state(
            "wordpress", self.charm_state)

        try:
            yield self.service_state_manager.add_service_state(
                "wordpress", self.charm_state)
        except ServiceStateNameInUse, e:
            self.assertEquals(e.service_name, "wordpress")
        else:
            self.fail("Error not raised")

    @inlineCallbacks
    def test_get_service_and_check_attributes(self):
        """
        Getting a service state should be possible, and the service
        state identification should be available through its
        attributes.
        """
        yield self.service_state_manager.add_service_state(
            "wordpress", self.charm_state)
        service_state = yield self.service_state_manager.get_service_state(
            "wordpress")
        self.assertEquals(service_state.service_name, "wordpress")
        self.assertEquals(service_state.internal_id, "service-0000000000")

    @inlineCallbacks
    def test_get_service_not_found(self):
        """
        Getting a service state which is not available should errback
        a meaningful error.
        """
        try:
            yield self.service_state_manager.get_service_state("wordpress")
        except ServiceStateNotFound, e:
            self.assertEquals(e.service_name, "wordpress")
        else:
            self.fail("Error not raised")

    def test_get_unit_state(self):
        """A unit state can be retrieved by name from the service manager."""
        self.assertFailure(self.service_state_manager.get_unit_state(
            "wordpress/1"), ServiceStateNotFound)

        self.assertFailure(self.service_state_manager.get_unit_state(
            "wordpress1"), ServiceUnitStateNotFound)

        wordpress_state = yield self.service_state_manager.add_service_state(
            "wordpress", self.charm_state)

        self.assertFailure(self.service_state_manager.get_unit_state(
            "wordpress/1"), ServiceUnitStateNotFound)

        wordpress_unit = wordpress_state.add_unit_state()

        unit_state = yield self.service_state_manager.get_unit_state(
                "wordpress/1")

        self.assertEqual(unit_state.internal_id, wordpress_unit.internal_id)

    @inlineCallbacks
    def test_get_service_charm_id(self):
        """
        The service state should make its respective charm id available.
        """
        yield self.service_state_manager.add_service_state(
            "wordpress", self.charm_state)
        service_state = yield self.service_state_manager.get_service_state(
            "wordpress")
        charm_id = yield service_state.get_charm_id()
        self.assertEquals(charm_id, "local:series/dummy-1")

    @inlineCallbacks
    def test_set_service_charm_id(self):
        """
        The service state should allow its charm id to be set.
        """
        yield self.service_state_manager.add_service_state(
            "wordpress", self.charm_state)
        service_state = yield self.service_state_manager.get_service_state(
            "wordpress")
        yield service_state.set_charm_id("local:series/dummy-2")
        charm_id = yield service_state.get_charm_id()
        self.assertEquals(charm_id, "local:series/dummy-2")

    @inlineCallbacks
    def test_remove_service_state(self):
        """
        A service state can be removed along with its relations, units,
        and zookeeper state.
        """
        service_state = yield self.service_state_manager.add_service_state(
            "wordpress", self.charm_state)

        relation_state = yield self.add_relation(
            "rel-type2", [service_state, "app", "server"])

        unit_state = yield service_state.add_unit_state()
        machine_state = yield self.machine_state_manager.add_machine_state()
        yield unit_state.assign_to_machine(machine_state)

        yield self.service_state_manager.remove_service_state(service_state)

        topology = yield self.get_topology()
        self.assertFalse(topology.has_relation(relation_state.internal_id))
        self.assertFalse(topology.has_service(service_state.internal_id))
        self.assertFalse(
            topology.get_service_units_in_machine(machine_state.internal_id))

        exists = yield self.client.exists(
            "/services/%s" % service_state.internal_id)
        self.assertFalse(exists)

    @inlineCallbacks
    def test_add_service_unit_and_check_attributes(self):
        """
        A service state should enable adding a new service unit
        under it, and again the unit should offer attributes allowing
        its identification.
        """
        service_state0 = yield self.service_state_manager.add_service_state(
            "wordpress", self.charm_state)
        service_state1 = yield self.service_state_manager.add_service_state(
            "mysql", self.charm_state)

        unit_state0 = yield service_state0.add_unit_state()
        unit_state1 = yield service_state1.add_unit_state()
        unit_state2 = yield service_state0.add_unit_state()
        unit_state3 = yield service_state1.add_unit_state()

        children = yield self.client.get_children("/units")
        self.assertEquals(sorted(children),
                          ["unit-0000000000", "unit-0000000001",
                           "unit-0000000002", "unit-0000000003"])

        self.assertEquals(unit_state0.service_name, "wordpress")
        self.assertEquals(unit_state0.internal_id, "unit-0000000000")
        self.assertEquals(unit_state0.unit_name, "wordpress/0")

        self.assertEquals(unit_state1.service_name, "mysql")
        self.assertEquals(unit_state1.internal_id, "unit-0000000001")
        self.assertEquals(unit_state1.unit_name, "mysql/0")

        self.assertEquals(unit_state2.service_name, "wordpress")
        self.assertEquals(unit_state2.internal_id, "unit-0000000002")
        self.assertEquals(unit_state2.unit_name, "wordpress/1")

        self.assertEquals(unit_state3.service_name, "mysql")
        self.assertEquals(unit_state3.internal_id, "unit-0000000003")
        self.assertEquals(unit_state3.unit_name, "mysql/1")

        topology = yield self.get_topology()

        self.assertTrue(
            topology.has_service_unit("service-0000000000",
                                      "unit-0000000000"))
        self.assertTrue(
            topology.has_service_unit("service-0000000001",
                                      "unit-0000000001"))
        self.assertTrue(
            topology.has_service_unit("service-0000000000",
                                      "unit-0000000002"))
        self.assertTrue(
            topology.has_service_unit("service-0000000001",
                                      "unit-0000000003"))

    @inlineCallbacks
    def test_add_service_unit_with_changing_state(self):
        """
        When adding a service unit, there's a chance that the
        service will go away mid-way through.  Rather than blowing
        up randomly, a nice error should be raised.
        """
        service_state = yield self.service_state_manager.add_service_state(
            "wordpress", self.charm_state)

        yield self.remove_service(service_state.internal_id)

        d = service_state.add_unit_state()
        yield self.assertFailure(d, StateChanged)

    @inlineCallbacks
    def test_get_unit_names(self):
        """A service's units names are retrievable."""
        service_state = yield self.service_state_manager.add_service_state(
            "wordpress", self.charm_state)

        expected_names = []
        for i in range(3):
            unit_state = yield service_state.add_unit_state()
            expected_names.append(unit_state.unit_name)

        unit_names = yield service_state.get_unit_names()
        self.assertEqual(unit_names, expected_names)

    @inlineCallbacks
    def test_remove_service_unit(self):
        """Removing a service unit removes all state associated.
        """
        service_state = yield self.service_state_manager.add_service_state(
            "wordpress", self.charm_state)

        unit_state = yield service_state.add_unit_state()

        # Assign to a machine
        machine_state = yield self.machine_state_manager.add_machine_state()
        yield unit_state.assign_to_machine(machine_state)
        # Connect a unit agent
        yield unit_state.connect_agent()

        # Now try and destroy it.
        yield service_state.remove_unit_state(unit_state)

        # Verify destruction.
        topology = yield self.get_topology()
        self.assertTrue(topology.has_service(service_state.internal_id))
        self.assertFalse(
            topology.has_service_unit(
                service_state.internal_id, unit_state.internal_id))

        exists = yield self.client.exists("/units/%s" % unit_state.internal_id)
        self.assertFalse(exists)

    def test_remove_service_unit_nonexistant(self):
        """Removing a non existant service unit, is fine."""

        service_state = yield self.service_state_manager.add_service_state(
            "wordpress", self.charm_state)
        unit_state = yield service_state.add_unit_state()
        yield service_state.remove_unit_state(unit_state)
        yield service_state.remove_unit_state(unit_state)

    @inlineCallbacks
    def test_get_all_service_states(self):
        services = yield self.service_state_manager.get_all_service_states()
        self.assertFalse(services)

        yield self.service_state_manager.add_service_state(
            "wordpress", self.charm_state)
        services = yield self.service_state_manager.get_all_service_states()
        self.assertEquals(len(services), 1)

        yield self.service_state_manager.add_service_state(
            "mysql", self.charm_state)
        services = yield self.service_state_manager.get_all_service_states()
        self.assertEquals(len(services), 2)

    @inlineCallbacks
    def test_get_service_unit(self):
        """
        Getting back service units should be possible using the
        user-oriented id.
        """
        service_state0 = yield self.service_state_manager.add_service_state(
            "wordpress", self.charm_state)
        service_state1 = yield self.service_state_manager.add_service_state(
            "mysql", self.charm_state)

        yield service_state0.add_unit_state()
        yield service_state1.add_unit_state()
        yield service_state0.add_unit_state()
        yield service_state1.add_unit_state()

        unit_state0 = yield service_state0.get_unit_state("wordpress/0")
        unit_state1 = yield service_state1.get_unit_state("mysql/0")
        unit_state2 = yield service_state0.get_unit_state("wordpress/1")
        unit_state3 = yield service_state1.get_unit_state("mysql/1")

        self.assertEquals(unit_state0.internal_id, "unit-0000000000")
        self.assertEquals(unit_state1.internal_id, "unit-0000000001")
        self.assertEquals(unit_state2.internal_id, "unit-0000000002")
        self.assertEquals(unit_state3.internal_id, "unit-0000000003")

        self.assertEquals(unit_state0.unit_name, "wordpress/0")
        self.assertEquals(unit_state1.unit_name, "mysql/0")
        self.assertEquals(unit_state2.unit_name, "wordpress/1")
        self.assertEquals(unit_state3.unit_name, "mysql/1")

    @inlineCallbacks
    def test_get_all_unit_states(self):
        service_state0 = yield self.service_state_manager.add_service_state(
            "wordpress", self.charm_state)
        service_state1 = yield self.service_state_manager.add_service_state(
            "mysql", self.charm_state)

        yield service_state0.add_unit_state()
        yield service_state1.add_unit_state()
        yield service_state0.add_unit_state()
        yield service_state1.add_unit_state()

        unit_state0 = yield service_state0.get_unit_state("wordpress/0")
        unit_state1 = yield service_state1.get_unit_state("mysql/0")
        unit_state2 = yield service_state0.get_unit_state("wordpress/1")
        unit_state3 = yield service_state1.get_unit_state("mysql/1")

        wordpress_units = yield service_state0.get_all_unit_states()
        self.assertEquals(
            set(wordpress_units), set((unit_state0, unit_state2)))

        mysql_units = yield service_state1.get_all_unit_states()
        self.assertEquals(set(mysql_units), set((unit_state1, unit_state3)))

    @inlineCallbacks
    def test_get_all_unit_states_with_changing_state(self):
        """
        When getting the service unit states, there's a chance that
        the service will go away mid-way through.  Rather than blowing
        up randomly, a nice error should be raised.
        """
        service_state = yield self.service_state_manager.add_service_state(
            "wordpress", self.charm_state)
        yield service_state.add_unit_state()
        unit_state = (yield service_state.get_all_unit_states())[0]
        self.assertEqual(unit_state.unit_name, "wordpress/0")
        yield self.remove_service(service_state.internal_id)
        yield self.assertFailure(
            service_state.get_all_unit_states(), StateChanged)

    @inlineCallbacks
    def test_set_functions(self):
        wordpress = yield self.service_state_manager.add_service_state(
            "wordpress", self.charm_state)
        mysql = yield self.service_state_manager.add_service_state(
            "mysql", self.charm_state)

        s1 = yield self.service_state_manager.get_service_state(
            "wordpress")
        s2 = yield self.service_state_manager.get_service_state(
            "mysql")
        self.assertEquals(hash(s1), hash(wordpress))
        self.assertEquals(hash(s2), hash(mysql))

        self.assertNotEqual(s1, object())
        self.assertNotEqual(s1, s2)

        self.assertEquals(s1, wordpress)
        self.assertEquals(s2, mysql)

        us0 = yield wordpress.add_unit_state()
        us1 = yield wordpress.add_unit_state()

        unit_state0 = yield wordpress.get_unit_state("wordpress/0")
        unit_state1 = yield wordpress.get_unit_state("wordpress/1")

        self.assertEquals(us0, unit_state0)
        self.assertEquals(us1, unit_state1)
        self.assertEquals(hash(us1), hash(unit_state1))

        self.assertNotEqual(us0, object())
        self.assertNotEqual(us0, us1)

    @inlineCallbacks
    def test_get_service_unit_not_found(self):
        """
        Attempting to retrieve a non-existent service unit should
        result in an errback.
        """
        service_state0 = yield self.service_state_manager.add_service_state(
            "wordpress", self.charm_state)
        service_state1 = yield self.service_state_manager.add_service_state(
            "mysql", self.charm_state)

        # Add some state in a different service to make it a little
        # bit more prone to breaking in case of errors.
        yield service_state1.add_unit_state()

        try:
            yield service_state0.get_unit_state("wordpress/0")
        except ServiceUnitStateNotFound, e:
            self.assertEquals(e.unit_name, "wordpress/0")
        else:
            self.fail("Error not raised")

    @inlineCallbacks
    def test_get_set_public_address(self):
        service_state = yield self.service_state_manager.add_service_state(
            "wordpress", self.charm_state)
        unit_state = yield service_state.add_unit_state()
        self.assertEqual((yield unit_state.get_public_address()), None)
        yield unit_state.set_public_address("example.foobar.com")
        yield self.assertEqual(
            (yield unit_state.get_public_address()),
             "example.foobar.com")

    @inlineCallbacks
    def test_get_set_private_address(self):
        service_state = yield self.service_state_manager.add_service_state(
            "wordpress", self.charm_state)
        unit_state = yield service_state.add_unit_state()
        self.assertEqual((yield unit_state.get_private_address()), None)
        yield unit_state.set_private_address("example.local")
        yield self.assertEqual(
            (yield unit_state.get_private_address()),
             "example.local")

    @inlineCallbacks
    def test_get_service_unit_with_changing_state(self):
        """
        If a service is removed during operation, get_service_unit()
        should raise a nice error.
        """
        service_state = yield self.service_state_manager.add_service_state(
            "wordpress", self.charm_state)

        yield self.remove_service(service_state.internal_id)

        d = service_state.get_unit_state("wordpress/0")
        yield self.assertFailure(d, StateChanged)

    @inlineCallbacks
    def test_get_service_unit_with_bad_service_name(self):
        """
        Service unit names contain a service name embedded into
        them.  The service name requested when calling get_unit_state()
        must match that of the object being used.
        """
        service_state0 = yield self.service_state_manager.add_service_state(
            "wordpress", self.charm_state)
        service_state1 = yield self.service_state_manager.add_service_state(
            "mysql", self.charm_state)

        # Add some state in a different service to make it a little
        # bit more prone to breaking in case of errors.
        yield service_state1.add_unit_state()

        try:
            yield service_state0.get_unit_state("mysql/0")
        except BadServiceStateName, e:
            self.assertEquals(e.expected_name, "wordpress")
            self.assertEquals(e.obtained_name, "mysql")
        else:
            self.fail("Error not raised")

    @inlineCallbacks
    def test_assign_unit_to_machine(self):
        service_state = yield self.service_state_manager.add_service_state(
            "wordpress", self.charm_state)
        unit_state = yield service_state.add_unit_state()
        machine_state = yield self.machine_state_manager.add_machine_state()

        yield unit_state.assign_to_machine(machine_state)

        topology = yield self.get_topology()

        self.assertEquals(
            topology.get_service_unit_machine(service_state.internal_id,
                                              unit_state.internal_id),
            machine_state.internal_id)

    @inlineCallbacks
    def test_assign_unit_to_machine_with_changing_state(self):
        service_state = yield self.service_state_manager.add_service_state(
            "wordpress", self.charm_state)
        unit_state = yield service_state.add_unit_state()
        machine_state = yield self.machine_state_manager.add_machine_state()

        yield self.remove_service_unit(service_state.internal_id,
                                       unit_state.internal_id)

        d = unit_state.assign_to_machine(machine_state)
        yield self.assertFailure(d, StateChanged)

        yield self.remove_service(service_state.internal_id)

        d = unit_state.assign_to_machine(machine_state)
        yield self.assertFailure(d, StateChanged)

    @inlineCallbacks
    def test_unassign_unit_from_machine(self):
        service_state = yield self.service_state_manager.add_service_state(
            "wordpress", self.charm_state)
        unit_state = yield service_state.add_unit_state()
        machine_state = yield self.machine_state_manager.add_machine_state()

        yield unit_state.assign_to_machine(machine_state)
        yield unit_state.unassign_from_machine()

        topology = yield self.get_topology()

        self.assertEquals(
            topology.get_service_unit_machine(service_state.internal_id,
                                              unit_state.internal_id),
            None)

    @inlineCallbacks
    def test_get_set_clear_resolved(self):
        """The a unit can be set to resolved to mark a future transition, with
        an optional retry flag."""

        unit_state = yield self.get_unit_state()

        self.assertIdentical((yield unit_state.get_resolved()), None)
        yield unit_state.set_resolved(NO_HOOKS)

        yield self.assertFailure(
            unit_state.set_resolved(NO_HOOKS),
            ServiceUnitResolvedAlreadyEnabled)
        yield self.assertEqual(

                (yield unit_state.get_resolved()), {"retry": NO_HOOKS})

        yield unit_state.clear_resolved()
        self.assertIdentical((yield unit_state.get_resolved()), None)
        yield unit_state.clear_resolved()

        yield self.assertFailure(unit_state.set_resolved(None), ValueError)

    @inlineCallbacks
    def test_watch_resolved(self):
        """A unit resolved watch can be instituted on a permanent basis."""
        unit_state = yield self.get_unit_state()

        results = []

        def callback(value):
            results.append(value)

        unit_state.watch_resolved(callback)
        yield unit_state.set_resolved(RETRY_HOOKS)
        yield unit_state.clear_resolved()
        yield unit_state.set_resolved(NO_HOOKS)

        yield self.poke_zk()

        self.assertEqual(len(results), 4)
        self.assertIdentical(results.pop(0), False)
        self.assertEqual(results.pop(0).type_name, "created")
        self.assertEqual(results.pop(0).type_name, "deleted")
        self.assertEqual(results.pop(0).type_name, "created")

        self.assertEqual(
            (yield unit_state.get_resolved()),
            {"retry": NO_HOOKS})

    @inlineCallbacks
    def test_watch_resolved_processes_current_state(self):
        """The watch method processes the current state before returning."""
        unit_state = yield self.get_unit_state()

        results = []

        @inlineCallbacks
        def callback(value):
            results.append((yield unit_state.get_resolved()))

        yield unit_state.watch_resolved(callback)
        self.assertTrue(results)

    @inlineCallbacks
    def test_stop_watch_resolved(self):
        """A unit resolved watch can be instituted on a permanent basis.

        However the callback can raise StopWatcher at anytime to stop the watch
        """
        unit_state = yield self.get_unit_state()

        results = []

        def callback(value):
            results.append(value)
            if len(results) == 1:
                raise StopWatcher()
            if len(results) == 3:
                raise StopWatcher()

        unit_state.watch_resolved(callback)
        yield unit_state.set_resolved(RETRY_HOOKS)
        yield unit_state.clear_resolved()
        yield self.poke_zk()

        unit_state.watch_resolved(callback)
        yield unit_state.set_resolved(NO_HOOKS)
        yield unit_state.clear_resolved()

        yield self.poke_zk()

        self.assertEqual(len(results), 3)
        self.assertIdentical(results.pop(0), False)
        self.assertIdentical(results.pop(0), False)
        self.assertEqual(results.pop(0).type_name, "created")

        self.assertEqual(
            (yield unit_state.get_resolved()), None)

    @inlineCallbacks
    def test_get_set_clear_relation_resolved(self):
        """The a unit's realtions can be set to resolved to mark a
        future transition, with an optional retry flag."""

        unit_state = yield self.get_unit_state()

        self.assertIdentical((yield unit_state.get_relation_resolved()), None)
        yield unit_state.set_relation_resolved({"0": RETRY_HOOKS})

        # Trying to set a conflicting raises an error
        yield self.assertFailure(
            unit_state.set_relation_resolved({"0": NO_HOOKS}),
            ServiceUnitRelationResolvedAlreadyEnabled)

        # Doing the same thing is fine
        yield unit_state.set_relation_resolved({"0": RETRY_HOOKS}),

        # Its fine to put in new values
        yield unit_state.set_relation_resolved({"21": RETRY_HOOKS})
        yield self.assertEqual(
            (yield unit_state.get_relation_resolved()),
            {"0": RETRY_HOOKS, "21": RETRY_HOOKS})

        yield unit_state.clear_relation_resolved()
        self.assertIdentical((yield unit_state.get_relation_resolved()), None)
        yield unit_state.clear_relation_resolved()

        yield self.assertFailure(
            unit_state.set_relation_resolved(True), ValueError)
        yield self.assertFailure(
            unit_state.set_relation_resolved(None), ValueError)

    @inlineCallbacks
    def test_watch_relation_resolved(self):
        """A unit resolved watch can be instituted on a permanent basis."""
        unit_state = yield self.get_unit_state()

        results = []

        def callback(value):
            results.append(value)

        unit_state.watch_relation_resolved(callback)
        yield unit_state.set_relation_resolved({"0": RETRY_HOOKS})
        yield unit_state.clear_relation_resolved()
        yield unit_state.set_relation_resolved({"0": NO_HOOKS})

        yield self.poke_zk()

        self.assertEqual(len(results), 4)
        self.assertIdentical(results.pop(0), False)
        self.assertEqual(results.pop(0).type_name, "created")
        self.assertEqual(results.pop(0).type_name, "deleted")
        self.assertEqual(results.pop(0).type_name, "created")

        self.assertEqual(
            (yield unit_state.get_relation_resolved()),
            {"0": NO_HOOKS})

    @inlineCallbacks
    def test_watch_relation_resolved_processes_current_state(self):
        """The watch method returns only after processing the current state."""
        unit_state = yield self.get_unit_state()

        results = []

        @inlineCallbacks
        def callback(value):
            results.append((yield unit_state.get_relation_resolved()))
        yield unit_state.watch_relation_resolved(callback)
        self.assertTrue(results)

    @inlineCallbacks
    def test_stop_watch_relation_resolved(self):
        """A unit resolved watch can be instituted on a permanent basis."""
        unit_state = yield self.get_unit_state()

        results = []

        def callback(value):
            results.append(value)

            if len(results) == 1:
                raise StopWatcher()

            if len(results) == 3:
                raise StopWatcher()

        unit_state.watch_relation_resolved(callback)
        yield unit_state.set_relation_resolved({"0": RETRY_HOOKS})
        yield unit_state.clear_relation_resolved()
        yield self.poke_zk()
        self.assertEqual(len(results), 1)

        unit_state.watch_relation_resolved(callback)
        yield unit_state.set_relation_resolved({"0": RETRY_HOOKS})
        yield unit_state.clear_relation_resolved()
        yield self.poke_zk()
        self.assertEqual(len(results), 3)
        self.assertIdentical(results.pop(0), False)
        self.assertIdentical(results.pop(0), False)
        self.assertEqual(results.pop(0).type_name, "created")

        self.assertEqual(
            (yield unit_state.get_relation_resolved()), None)

    @inlineCallbacks
    def test_watch_resolved_slow_callback(self):
        """A slow watch callback is still invoked serially."""
        unit_state = yield self.get_unit_state()

        callbacks = [Deferred() for i in range(5)]
        results = []
        contents = []

        @inlineCallbacks
        def watch(value):
            results.append(value)
            yield callbacks[len(results) - 1]
            contents.append((yield unit_state.get_resolved()))

        callbacks[0].callback(True)
        yield unit_state.watch_resolved(watch)

        # These get collapsed into a single event
        yield unit_state.set_resolved(RETRY_HOOKS)
        yield unit_state.clear_resolved()
        yield self.poke_zk()

        # Verify the callback hasn't completed
        self.assertEqual(len(results), 2)
        self.assertEqual(len(contents), 1)

        # Let it finish
        callbacks[1].callback(True)
        yield self.poke_zk()

        # Verify result counts
        self.assertEqual(len(results), 3)
        self.assertEqual(len(contents), 2)

        # Verify result values. Even though we have created event, the
        # setting retrieved shows the hook is not enabled.
        self.assertEqual(results[-1].type_name, "deleted")
        self.assertEqual(contents[-1], None)

        yield unit_state.set_resolved(NO_HOOKS)
        callbacks[2].callback(True)
        yield self.poke_zk()

        self.assertEqual(len(results), 4)
        self.assertEqual(contents[-1], {"retry": NO_HOOKS})

        # Clear out any pending activity.
        yield self.poke_zk()

    @inlineCallbacks
    def test_watch_relation_resolved_slow_callback(self):
        """A slow watch callback is still invoked serially."""
        unit_state = yield self.get_unit_state()

        callbacks = [Deferred() for i in range(5)]
        results = []
        contents = []

        @inlineCallbacks
        def watch(value):
            results.append(value)
            yield callbacks[len(results) - 1]
            contents.append((yield unit_state.get_relation_resolved()))

        callbacks[0].callback(True)
        yield unit_state.watch_relation_resolved(watch)

        # These get collapsed into a single event
        yield unit_state.set_relation_resolved({"0": RETRY_HOOKS})
        yield unit_state.clear_relation_resolved()
        yield self.poke_zk()

        # Verify the callback hasn't completed
        self.assertEqual(len(results), 2)
        self.assertEqual(len(contents), 1)

        # Let it finish
        callbacks[1].callback(True)
        yield self.poke_zk()

        # Verify result counts
        self.assertEqual(len(results), 3)
        self.assertEqual(len(contents), 2)

        # Verify result values. Even though we have created event, the
        # setting retrieved shows the hook is not enabled.
        self.assertEqual(results[-1].type_name, "deleted")
        self.assertEqual(contents[-1], None)

        yield unit_state.set_relation_resolved({"0": RETRY_HOOKS})
        callbacks[2].callback(True)
        yield self.poke_zk()

        self.assertEqual(len(results), 4)
        self.assertEqual(contents[-1], {"0": RETRY_HOOKS})

        # Clear out any pending activity.
        yield self.poke_zk()

    @inlineCallbacks
    def test_set_and_clear_upgrade_flag(self):
        """An upgrade flag can be set on a unit."""

        # Defaults to false
        unit_state = yield self.get_unit_state()
        upgrade_flag = yield unit_state.get_upgrade_flag()
        self.assertEqual(upgrade_flag, False)

        # Can be set
        yield unit_state.set_upgrade_flag()
        upgrade_flag = yield unit_state.get_upgrade_flag()
        self.assertEqual(upgrade_flag, True)

        # Can be set multiple times
        yield unit_state.set_upgrade_flag()
        upgrade_flag = yield unit_state.get_upgrade_flag()
        self.assertEqual(upgrade_flag, True)

        # Can be cleared
        yield unit_state.clear_upgrade_flag()
        upgrade_flag = yield unit_state.get_upgrade_flag()
        self.assertEqual(upgrade_flag, False)

        # Can be cleared multiple times
        yield unit_state.clear_upgrade_flag()
        upgrade_flag = yield unit_state.get_upgrade_flag()
        self.assertEqual(upgrade_flag, False)

    @inlineCallbacks
    def test_watch_upgrade_flag_once(self):
        """An upgrade watch can be set to notified of presence and changes."""
        unit_state = yield self.get_unit_state()
        yield unit_state.set_upgrade_flag()

        results = []

        def callback(value):
            results.append(value)

        unit_state.watch_upgrade_flag(callback, permanent=False)
        yield unit_state.clear_upgrade_flag()
        yield unit_state.set_upgrade_flag()
        yield self.sleep(0.1)
        yield self.poke_zk()
        self.assertEqual(len(results), 2)
        self.assertIdentical(results.pop(0), True)
        self.assertIdentical(results.pop().type_name, "deleted")

        self.assertEqual(
            (yield unit_state.get_upgrade_flag()),
            True)

    @inlineCallbacks
    def test_watch_upgrade_processes_current_state(self):
        unit_state = yield self.get_unit_state()
        results = []

        @inlineCallbacks
        def callback(value):
            results.append((yield unit_state.get_upgrade_flag()))

        yield unit_state.watch_upgrade_flag(callback)
        self.assertTrue(results)

    @inlineCallbacks
    def test_watch_upgrade_flag_permanent(self):
        """An upgrade watch can be instituted on a permanent basis."""
        unit_state = yield self.get_unit_state()

        results = []

        def callback(value):
            results.append(value)

        yield unit_state.watch_upgrade_flag(callback)
        self.assertTrue(results)
        yield unit_state.set_upgrade_flag()
        yield unit_state.clear_upgrade_flag()
        yield unit_state.set_upgrade_flag()

        yield self.poke_zk()

        self.assertEqual(len(results), 4)
        self.assertIdentical(results.pop(0), False)
        self.assertIdentical(results.pop(0).type_name, "created")
        self.assertIdentical(results.pop(0).type_name, "deleted")
        self.assertIdentical(results.pop(0).type_name, "created")

        self.assertEqual(
            (yield unit_state.get_upgrade_flag()),
            True)

    @inlineCallbacks
    def test_watch_upgrade_flag_waits_on_slow_callbacks(self):
        """A slow watch callback is still invoked serially."""
        unit_state = yield self.get_unit_state()

        callbacks = [Deferred() for i in range(5)]
        results = []
        contents = []

        @inlineCallbacks
        def watch(value):
            results.append(value)
            yield callbacks[len(results) - 1]
            contents.append((yield unit_state.get_upgrade_flag()))

        yield callbacks[0].callback(True)
        yield unit_state.watch_upgrade_flag(watch)

        # These get collapsed into a single event
        yield unit_state.set_upgrade_flag()
        yield unit_state.clear_upgrade_flag()
        yield self.poke_zk()

        # Verify the callback hasn't completed
        self.assertEqual(len(results), 2)
        self.assertEqual(len(contents), 1)

        # Let it finish
        callbacks[1].callback(True)
        yield self.poke_zk()

        # Verify result counts
        self.assertEqual(len(results), 3)
        self.assertEqual(len(contents), 2)

        # Verify result values. Even though we have created event, the
        # setting retrieved shows the hook is not enabled.
        self.assertEqual(results[-1].type_name, "deleted")
        self.assertEqual(contents[-1], False)

        yield unit_state.set_upgrade_flag()
        yield self.poke_zk()

        # Verify the callback hasn't completed
        self.assertEqual(len(contents), 2)

        callbacks[2].callback(True)
        yield self.poke_zk()

        # Verify values.
        self.assertEqual(len(contents), 3)
        self.assertEqual(results[-1].type_name, "created")
        self.assertEqual(contents[-1], True)

        # Clear out any pending activity.
        yield self.poke_zk()

    @inlineCallbacks
    def test_enable_debug_hook(self):
        """Unit hook debugging can be enabled on the unit state."""
        unit_state = yield self.get_unit_state()
        enabled = yield unit_state.enable_hook_debug(["*"])
        self.assertIdentical(enabled, True)
        content, stat = yield self.client.get(
            "/units/%s/debug" % unit_state.internal_id)
        data = yaml.load(content)
        self.assertEqual(data, {"debug_hooks": ["*"]})

    @inlineCallbacks
    def test_enable_debug_multiple_named_hooks(self):
        """Unit hook debugging can be enabled for multiple hooks."""
        unit_state = yield self.get_unit_state()
        enabled = yield unit_state.enable_hook_debug(
            ["db-relation-broken", "db-relation-changed"])

        self.assertIdentical(enabled, True)
        content, stat = yield self.client.get(
            "/units/%s/debug" % unit_state.internal_id)
        data = yaml.load(content)
        self.assertEqual(
            data,
            {"debug_hooks": ["db-relation-broken", "db-relation-changed"]})

    @inlineCallbacks
    def test_enable_debug_all_and_named_is_error(self):
        """Unit hook debugging can be enabled for multiple hooks,
        but only if they are all named hooks."""
        unit_state = yield self.get_unit_state()

        error = yield self.assertFailure(
            unit_state.enable_hook_debug(["*", "db-relation-changed"]),
            ValueError)
        self.assertEquals(
            str(error),
            "Ambigious to debug all hooks and named hooks "
            "['*', 'db-relation-changed']")

    @inlineCallbacks
    def test_enable_debug_requires_sequence(self):
        """The enable hook debug only accepts a sequences of names.
        """
        unit_state = yield self.get_unit_state()

        error = yield self.assertFailure(
            unit_state.enable_hook_debug(None),
            AssertionError)
        self.assertEquals(str(error), "Hook names must be a list: got None")

    @inlineCallbacks
    def test_enable_named_debug_hook(self):
        """Unit hook debugging can be enabled on for a named hook."""
        unit_state = yield self.get_unit_state()
        enabled = yield unit_state.enable_hook_debug(
            ["db-relation-changed"])
        self.assertIdentical(enabled, True)
        content, stat = yield self.client.get(
            "/units/%s/debug" % unit_state.internal_id)
        data = yaml.load(content)
        self.assertEqual(data, {"debug_hooks": ["db-relation-changed"]})

    @inlineCallbacks
    def test_enable_debug_hook_pre_existing(self):
        """Attempting to enable debug on a unit state already being debugged
        raises an exception.
        """
        unit_state = yield self.get_unit_state()
        yield unit_state.enable_hook_debug(["*"])
        error = yield self.assertFailure(unit_state.enable_hook_debug(["*"]),
                                         ServiceUnitDebugAlreadyEnabled)
        self.assertEquals(
            str(error), "Service unit 'wordpress/0' is already in debug mode.")

    @inlineCallbacks
    def test_enable_debug_hook_lifetime(self):
        """A debug hook setting is only active for the lifetime of the client
        that created it.
        """
        unit_state = yield self.get_unit_state()
        yield unit_state.enable_hook_debug(["*"])
        exists = yield self.client.exists(
            "/units/%s/debug" % unit_state.internal_id)
        self.assertTrue(exists)
        yield self.client.close()
        self.client = self.get_zookeeper_client()
        yield self.client.connect()
        exists = yield self.client.exists(
            "/units/%s/debug" % unit_state.internal_id)
        self.assertFalse(exists)

    @inlineCallbacks
    def test_watch_debug_hook_once(self):
        """A watch can be set to notified of presence and changes."""
        unit_state = yield self.get_unit_state()
        yield unit_state.enable_hook_debug(["*"])

        results = []

        def callback(value):
            results.append(value)

        yield unit_state.watch_hook_debug(callback, permanent=False)
        yield unit_state.clear_hook_debug()
        yield unit_state.enable_hook_debug(["*"])
        yield self.poke_zk()
        self.assertEqual(len(results), 2)
        self.assertIdentical(results.pop(0), True)
        self.assertIdentical(results.pop().type_name, "deleted")

        self.assertEqual(
            (yield unit_state.get_hook_debug()),
            {"debug_hooks": ["*"]})

    @inlineCallbacks
    def test_watch_debug_hook_processes_current_state(self):
        """A hook debug watch can be instituted on a permanent basis."""
        unit_state = yield self.get_unit_state()

        results = []

        @inlineCallbacks
        def callback(value):
            results.append((yield unit_state.get_hook_debug()))

        yield unit_state.watch_hook_debug(callback)
        self.assertTrue(results)

    @inlineCallbacks
    def test_watch_debug_hook_permanent(self):
        """A hook debug watch can be instituted on a permanent basis."""
        unit_state = yield self.get_unit_state()

        results = []

        def callback(value):
            results.append(value)

        yield unit_state.watch_hook_debug(callback)
        yield unit_state.enable_hook_debug(["*"])
        yield unit_state.clear_hook_debug()
        yield unit_state.enable_hook_debug(["*"])

        yield self.poke_zk()

        self.assertEqual(len(results), 4)
        self.assertIdentical(results.pop(0), False)
        self.assertIdentical(results.pop(0).type_name, "created")
        self.assertIdentical(results.pop(0).type_name, "deleted")
        self.assertIdentical(results.pop(0).type_name, "created")

        self.assertEqual(
            (yield unit_state.get_hook_debug()),
            {"debug_hooks": ["*"]})

    @inlineCallbacks
    def test_watch_debug_hook_waits_on_slow_callbacks(self):
        """A slow watch callback is still invoked serially."""

        unit_state = yield self.get_unit_state()

        callbacks = [Deferred() for i in range(5)]
        results = []
        contents = []

        @inlineCallbacks
        def watch(value):
            results.append(value)
            yield callbacks[len(results) - 1]
            contents.append((yield unit_state.get_hook_debug()))

        callbacks[0].callback(True)  # Finish the current state processing
        yield unit_state.watch_hook_debug(watch)

        # These get collapsed into a single event
        yield unit_state.enable_hook_debug(["*"])
        yield unit_state.clear_hook_debug()
        yield self.poke_zk()

        # Verify the callback hasn't completed
        self.assertEqual(len(results), 2)
        self.assertEqual(len(contents), 1)

        # Let it finish
        callbacks[1].callback(True)
        yield self.poke_zk()

        # Verify result counts
        self.assertEqual(len(results), 3)
        self.assertEqual(len(contents), 2)

        # Verify result values. Even though we have created event, the
        # setting retrieved shows the hook is not enabled.
        self.assertEqual(results[-1].type_name, "deleted")
        self.assertEqual(contents[-1], None)

        yield unit_state.enable_hook_debug(["*"])
        yield self.poke_zk()

        # Verify the callback hasn't completed
        self.assertEqual(len(contents), 2)

        callbacks[2].callback(True)
        yield self.poke_zk()

        # Verify values.
        self.assertEqual(len(contents), 3)
        self.assertEqual(results[-1].type_name, "created")
        self.assertEqual(contents[-1], {"debug_hooks": ["*"]})

        # Clear out any pending activity.
        yield self.poke_zk()

    @inlineCallbacks
    def test_service_unit_agent(self):
        """A service unit state has an associated unit agent."""
        service_state = yield self.service_state_manager.add_service_state(
            "wordpress", self.charm_state)
        unit_state = yield service_state.add_unit_state()
        exists_d, watch_d = unit_state.watch_agent()
        exists = yield exists_d
        self.assertFalse(exists)
        yield unit_state.connect_agent()
        event = yield watch_d
        self.assertEqual(event.type_name, "created")
        self.assertEqual(event.path,
                         "/units/%s/agent" % unit_state.internal_id)

    @inlineCallbacks
    def test_get_charm_id(self):
        """A service unit knows its charm id"""
        service_state = yield self.service_state_manager.add_service_state(
            "wordpress", self.charm_state)
        unit_state = yield service_state.add_unit_state()
        unit_charm = yield unit_state.get_charm_id()
        service_charm = yield service_state.get_charm_id()
        self.assertTrue(
            (self.charm_state.id == unit_charm == service_charm))

    @inlineCallbacks
    def test_set_charm_id(self):
        """A service unit charm can be set and is validated when set."""
        service_state = yield self.service_state_manager.add_service_state(
            "wordpress", self.charm_state)
        unit_state = yield service_state.add_unit_state()
        yield self.assertFailure(
            unit_state.set_charm_id("abc"), CharmURLError)
        yield self.assertFailure(
            unit_state.set_charm_id("abc:foobar-a"), CharmURLError)
        yield self.assertFailure(
            unit_state.set_charm_id(None), CharmURLError)
        charm_id = "local:series/name-1"
        yield unit_state.set_charm_id(charm_id)
        value = yield unit_state.get_charm_id()
        self.assertEqual(charm_id, value)

    @inlineCallbacks
    def test_unassign_unit_from_machine_without_being_assigned(self):
        """
        When unassigning a machine from a unit, it is possible that
        the machine has not been previously assigned, or that it
        was assigned but the state changed beneath us.  In either
        case, the end state is the intended state, so we simply
        move forward without any errors here, to avoid having to
        handle the extra complexity of dealing with the concurrency
        problems.
        """
        service_state = yield self.service_state_manager.add_service_state(
            "wordpress", self.charm_state)
        unit_state = yield service_state.add_unit_state()

        yield unit_state.unassign_from_machine()

        topology = yield self.get_topology()
        self.assertEquals(
            topology.get_service_unit_machine(service_state.internal_id,
                                              unit_state.internal_id),
            None)

        machine_id = yield unit_state.get_assigned_machine_id()
        self.assertEqual(machine_id, None)

    @inlineCallbacks
    def test_assign_unit_to_machine_again_fails(self):
        """
        Trying to assign a machine to an already assigned unit
        should fail, unless we're assigning to precisely the same
        machine, in which case it's no big deal.
        """
        service_state = yield self.service_state_manager.add_service_state(
            "wordpress", self.charm_state)
        unit_state = yield service_state.add_unit_state()
        machine_state0 = yield self.machine_state_manager.add_machine_state()
        machine_state1 = yield self.machine_state_manager.add_machine_state()

        yield unit_state.assign_to_machine(machine_state0)

        # Assigning again to the same machine is a NOOP, so nothing
        # terrible should happen if we let it go through.
        yield unit_state.assign_to_machine(machine_state0)

        try:
            yield unit_state.assign_to_machine(machine_state1)
        except ServiceUnitStateMachineAlreadyAssigned, e:
            self.assertEquals(e.unit_name, "wordpress/0")
        else:
            self.fail("Error not raised")

        machine_id = yield unit_state.get_assigned_machine_id()
        self.assertEqual(machine_id, 0)

    @inlineCallbacks
    def test_unassign_unit_from_machine_with_changing_state(self):
        service_state = yield self.service_state_manager.add_service_state(
            "wordpress", self.charm_state)
        unit_state = yield service_state.add_unit_state()

        yield self.remove_service_unit(service_state.internal_id,
                                       unit_state.internal_id)

        d = unit_state.unassign_from_machine()
        yield self.assertFailure(d, StateChanged)

        d = unit_state.get_assigned_machine_id()
        yield self.assertFailure(d, StateChanged)

        yield self.remove_service(service_state.internal_id)

        d = unit_state.unassign_from_machine()
        yield self.assertFailure(d, StateChanged)

        d = unit_state.get_assigned_machine_id()
        yield self.assertFailure(d, StateChanged)

    @inlineCallbacks
    def test_assign_unit_to_unused_machine(self):
        """Verify that unused machines can be assigned to."""
        yield self.machine_state_manager.add_machine_state()
        mysql_service_state = yield self.add_service_from_charm("mysql")
        mysql_unit_state = yield mysql_service_state.add_unit_state()
        mysql_machine_state = yield self.machine_state_manager.add_machine_state()
        yield mysql_unit_state.assign_to_machine(mysql_machine_state)
        yield self.service_state_manager.remove_service_state(
            mysql_service_state)
        wordpress_service_state = yield self.add_service_from_charm(
            "wordpress")
        wordpress_unit_state = yield wordpress_service_state.add_unit_state()
        yield wordpress_unit_state.assign_to_unused_machine()
        self.assertEqual(
            (yield self.get_topology()).get_machines(),
            ["machine-0000000000", "machine-0000000001"])
        yield self.assert_machine_assignments("wordpress", [1])

    @inlineCallbacks
    def test_assign_unit_to_unused_machine_with_changing_state_service(self):
        """Verify `StateChanged` raised if service is manipulated during reuse.
        """
        yield self.machine_state_manager.add_machine_state()
        mysql_service_state = yield self.add_service_from_charm("mysql")
        mysql_unit_state = yield mysql_service_state.add_unit_state()
        mysql_machine_state = yield self.machine_state_manager.add_machine_state()
        yield mysql_unit_state.assign_to_machine(mysql_machine_state)
        yield self.service_state_manager.remove_service_state(
            mysql_service_state)
        wordpress_service_state = yield self.add_service_from_charm(
            "wordpress")
        wordpress_unit_state = yield wordpress_service_state.add_unit_state()
        yield self.remove_service(wordpress_service_state.internal_id)
        yield self.assertFailure(
            wordpress_unit_state.assign_to_unused_machine(), StateChanged)

    @inlineCallbacks
    def test_assign_unit_to_unused_machine_with_changing_state_service_unit(self):
        "Verify `StateChanged` raised if unit is manipulated during reuse."""
        yield self.machine_state_manager.add_machine_state()
        mysql_service_state = yield self.add_service_from_charm("mysql")
        mysql_unit_state = yield mysql_service_state.add_unit_state()
        mysql_machine_state = yield self.machine_state_manager.add_machine_state()
        yield mysql_unit_state.assign_to_machine(mysql_machine_state)
        yield self.service_state_manager.remove_service_state(
            mysql_service_state)
        wordpress_service_state = yield self.add_service_from_charm(
            "wordpress")
        wordpress_unit_state = yield wordpress_service_state.add_unit_state()
        yield self.remove_service_unit(
            wordpress_service_state.internal_id,
            wordpress_unit_state.internal_id)
        yield self.assertFailure(
            wordpress_unit_state.assign_to_unused_machine(), StateChanged)

    @inlineCallbacks
    def test_assign_unit_to_unused_machine_only_machine_zero(self):
        """Verify when the only available machine is machine 0"""
        yield self.machine_state_manager.add_machine_state()
        wordpress_service_state = yield self.add_service_from_charm(
            "wordpress")
        wordpress_unit_state = yield wordpress_service_state.add_unit_state()
        yield self.assertFailure(
            wordpress_unit_state.assign_to_unused_machine(),
            NoUnusedMachines)

    @inlineCallbacks
    def test_assign_unit_to_unused_machine_none_available(self):
        """Verify when there are no unused machines"""
        yield self.machine_state_manager.add_machine_state()
        mysql_service_state = yield self.add_service_from_charm("mysql")
        mysql_unit_state = yield mysql_service_state.add_unit_state()
        mysql_machine_state = yield self.machine_state_manager.add_machine_state()
        yield mysql_unit_state.assign_to_machine(mysql_machine_state)
        yield self.assert_machine_assignments("mysql", [1])
        wordpress_service_state = yield self.add_service_from_charm(
            "wordpress")
        wordpress_unit_state = yield wordpress_service_state.add_unit_state()
        yield self.assertFailure(
            wordpress_unit_state.assign_to_unused_machine(),
            NoUnusedMachines)

    @inlineCallbacks
    def test_watch_relations_processes_current_state(self):
        """
        The watch method returns only after processing initial state.

        Note the callback is only invoked if there are changes
        requiring processing.
        """
        service_state = yield self.add_service("wordpress")
        yield self.add_relation(
            "rel-type", [service_state, "name", "role"])

        results = []

        def callback(*args):
            results.append(True)

        yield service_state.watch_relation_states(callback)
        self.assertTrue(results)

    @inlineCallbacks
    def test_watch_relations_when_being_created(self):
        """
        We can watch relations before we have any.
        """
        service_state = yield self.add_service("wordpress")

        wait_callback = [Deferred() for i in range(5)]
        calls = []

        def watch_relations(old_relations, new_relations):
            calls.append((old_relations, new_relations))
            wait_callback[len(calls) - 1].callback(True)

        # Start watching
        service_state.watch_relation_states(watch_relations)

        # Callback is still untouched
        self.assertEquals(calls, [])

        # add a service relation and wait for the callback
        relation_state = yield self.add_relation(
            "rel-type", [service_state, "name", "role"])
        yield wait_callback[1]

        # # verify the result
        self.assertEquals(len(calls), 2)
        old_relations, new_relations = calls[1]
        self.assertFalse(old_relations)
        self.assertEquals(new_relations[0].relation_role, "role")

        # add a new relation with the service assigned to it.
        relation_state2 = yield self.add_relation(
            "rel-type2", [service_state, "app", "server"])
        yield wait_callback[2]

        self.assertEquals(len(calls), 3)
        old_relations, new_relations = calls[2]
        self.assertEquals([r.internal_relation_id for r in old_relations],
                          [relation_state.internal_id])
        self.assertEquals([r.internal_relation_id for r in new_relations],
                          [relation_state.internal_id,
                           relation_state2.internal_id])

    @inlineCallbacks
    def test_watch_relations_may_defer(self):
        """
        The watch relations callback may return a deferred so that
        it performs some its logic asynchronously. In this case, it must
        not be called a second time before its postponed logic is finished
        completely.
        """
        wait_callback = [Deferred() for i in range(5)]
        finish_callback = [Deferred() for i in range(5)]

        calls = []

        def watch_relations(old_relations, new_relations):
            calls.append((old_relations, new_relations))
            wait_callback[len(calls) - 1].callback(True)
            return finish_callback[len(calls) - 1]

        service_state = yield self.add_service("s-1")
        service_state.watch_relation_states(watch_relations)

        # Shouldn't have any callbacks yet.
        self.assertEquals(calls, [])

        # Assign to a relation.
        yield self.add_relation("rel-type",
                                (service_state, "name", "role"))

        # Hold off until callback is started.
        yield wait_callback[0]

        # Assign to another relation.
        yield self.add_relation("rel-type",
                          (service_state, "name2", "role"))

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
    def test_get_relation_endpoints_service_name(self):
        """Test getting endpoints with descriptor ``<service name>``"""
        yield self.add_service_from_charm("wordpress")
        self.assertEqual(
            (yield self.service_state_manager.get_relation_endpoints(
            "wordpress")),
            set([RelationEndpoint("wordpress", "http", "url", "server"),
                 RelationEndpoint("wordpress", "mysql", "db", "client"),
                 RelationEndpoint(
                     "wordpress", "varnish", "cache", "client")]))
        yield self.add_service_from_charm("riak")
        self.assertEqual(
            (yield self.service_state_manager.get_relation_endpoints(
            "riak")),
            set([RelationEndpoint("riak", "http", "admin", "server"),
                 RelationEndpoint("riak", "http", "endpoint", "server"),
                 RelationEndpoint("riak", "riak", "ring", "peer")]))

    @inlineCallbacks
    def test_get_relation_endpoints_service_name_relation_name(self):
        """Test getting endpoints with ``<service name:relation name>``"""
        yield self.add_service_from_charm("wordpress")
        self.assertEqual(
            (yield self.service_state_manager.get_relation_endpoints(
            "wordpress:url")),
            set([RelationEndpoint("wordpress", "http", "url", "server")]))
        self.assertEqual(
            (yield self.service_state_manager.get_relation_endpoints(
            "wordpress:db")),
            set([RelationEndpoint("wordpress", "mysql", "db", "client")]))
        self.assertEqual(
            (yield self.service_state_manager.get_relation_endpoints(
            "wordpress:cache")),
             set([RelationEndpoint(
                "wordpress", "varnish", "cache", "client")]))
        yield self.add_service_from_charm("riak")
        self.assertEqual(
            (yield self.service_state_manager.get_relation_endpoints(
            "riak:ring")),
            set([RelationEndpoint("riak", "riak", "ring", "peer")]))

    @inlineCallbacks
    def test_descriptor_for_services_without_charms(self):
        """Test with services that have no corresponding charms defined"""
        yield self.add_service("nocharm")
        self.assertEqual(
            (yield self.service_state_manager.get_relation_endpoints(
                "nocharm")),
            set())
        self.assertEqual(
            (yield self.service_state_manager.get_relation_endpoints(
                "nocharm:nonsense")),
            set())

    @inlineCallbacks
    def test_descriptor_for_missing_service(self):
        """Test with a service that is not in the topology"""
        yield self.assertFailure(
            self.service_state_manager.get_relation_endpoints(
                "notadded"),
            ServiceStateNotFound)

    @inlineCallbacks
    def test_bad_descriptors(self):
        """Test that the descriptors meet the minimum naming standards"""
        yield self.assertFailure(
            self.service_state_manager.get_relation_endpoints("a:b:c"),
            BadDescriptor)
        yield self.assertFailure(
            self.service_state_manager.get_relation_endpoints(""),
            BadDescriptor)

    @inlineCallbacks
    def test_join_descriptors_service_name(self):
        """Test descriptor of the form ``<service name>`"""
        yield self.add_service_from_charm("wordpress")
        yield self.add_service_from_charm("mysql")
        self.assertEqual(
            (yield self.service_state_manager.join_descriptors(
                "wordpress", "mysql")),
            [(RelationEndpoint("wordpress", "mysql", "db", "client"),
              RelationEndpoint("mysql", "mysql", "server", "server"))])
        # symmetric - note the pair has rotated
        self.assertEqual(
            (yield self.service_state_manager.join_descriptors(
                "mysql", "wordpress")),
            [(RelationEndpoint("mysql", "mysql", "server", "server"),
              RelationEndpoint("wordpress", "mysql", "db", "client"))])
        yield self.add_service_from_charm("varnish")
        self.assertEqual(
            (yield self.service_state_manager.join_descriptors(
                "wordpress", "varnish")),
            [(RelationEndpoint("wordpress", "varnish", "cache", "client"),
              RelationEndpoint("varnish", "varnish", "webcache", "server"))])

    @inlineCallbacks
    def test_join_descriptors_service_name_relation_name(self):
        """Test joining descriptors ``<service name:relation name>``"""
        yield self.add_service_from_charm("wordpress")
        yield self.add_service_from_charm("mysql")
        self.assertEqual(
            (yield self.service_state_manager.join_descriptors(
                "wordpress:db", "mysql")),
            [(RelationEndpoint("wordpress", "mysql", "db", "client"),
              RelationEndpoint("mysql", "mysql", "server", "server"))])
        self.assertEqual(
            (yield self.service_state_manager.join_descriptors(
                "mysql:server", "wordpress")),
            [(RelationEndpoint("mysql", "mysql", "server", "server"),
              RelationEndpoint("wordpress", "mysql", "db", "client"))])
        self.assertEqual(
            (yield self.service_state_manager.join_descriptors(
                "mysql:server", "wordpress:db")),
            [(RelationEndpoint("mysql", "mysql", "server", "server"),
              RelationEndpoint("wordpress", "mysql", "db", "client"))])

        yield self.add_service_from_charm("varnish")
        self.assertEqual(
            (yield self.service_state_manager.join_descriptors(
                "wordpress:cache", "varnish")),
            [(RelationEndpoint("wordpress", "varnish", "cache", "client"),
              RelationEndpoint("varnish", "varnish", "webcache", "server"))])
        self.assertEqual(
            (yield self.service_state_manager.join_descriptors(
                "wordpress:cache", "varnish:webcache")),
            [(RelationEndpoint("wordpress", "varnish", "cache", "client"),
              RelationEndpoint("varnish", "varnish", "webcache", "server"))])

    @inlineCallbacks
    def test_join_peer_descriptors(self):
        """Test joining of peer relation descriptors"""
        yield self.add_service_from_charm("riak")
        self.assertEqual(
            (yield self.service_state_manager.join_descriptors(
                "riak", "riak")),
            [(RelationEndpoint("riak", "riak", "ring", "peer"),
              RelationEndpoint("riak", "riak", "ring", "peer"))])
        self.assertEqual(
            (yield self.service_state_manager.join_descriptors(
                "riak:ring", "riak")),
            [(RelationEndpoint("riak", "riak", "ring", "peer"),
              RelationEndpoint("riak", "riak", "ring", "peer"))])
        self.assertEqual(
            (yield self.service_state_manager.join_descriptors(
                "riak:ring", "riak:ring")),
            [(RelationEndpoint("riak", "riak", "ring", "peer"),
              RelationEndpoint("riak", "riak", "ring", "peer"))])
        self.assertEqual(
            (yield self.service_state_manager.join_descriptors(
                "riak:no-ring", "riak:ring")),
            [])

    @inlineCallbacks
    def test_join_descriptors_no_common_relation(self):
        """Test joining of descriptors that do not share a relation"""
        yield self.add_service_from_charm("mysql")
        yield self.add_service_from_charm("riak")
        yield self.add_service_from_charm("wordpress")
        yield self.add_service_from_charm("varnish")
        self.assertEqual(
            (yield self.service_state_manager.join_descriptors(
                "mysql", "riak")),
            [])
        self.assertEqual(
            (yield self.service_state_manager.join_descriptors(
                "mysql:server", "riak:ring")),
            [])
        self.assertEqual(
            (yield self.service_state_manager.join_descriptors(
                "varnish", "mysql")),
            [])
        self.assertEqual(
            (yield self.service_state_manager.join_descriptors(
                "riak:ring", "riak:admin")),
            [])
        self.assertEqual(
            (yield self.service_state_manager.join_descriptors(
                "riak", "wordpress")),
            [])

    @inlineCallbacks
    def test_join_descriptors_no_service_state(self):
        """Test joining of nonexistent services"""
        yield self.add_service_from_charm("wordpress")
        yield self.assertFailure(
            self.service_state_manager.join_descriptors("wordpress", "nosuch"),
            ServiceStateNotFound)
        yield self.assertFailure(
            self.service_state_manager.join_descriptors("notyet", "nosuch"),
            ServiceStateNotFound)

    @inlineCallbacks
    def test_watch_services_initial_callback(self):
        """Watch service processes initial state before returning.

        Note the callback is only executed if there is some meaningful state
        change.
        """
        results = []

        def callback(*args):
            results.append(True)

        yield self.service_state_manager.watch_service_states(callback)
        yield self.add_service("wordpress")
        yield self.poke_zk()
        self.assertTrue(results)

    @inlineCallbacks
    def test_watch_services_when_being_created(self):
        """
        It should be possible to start watching services even
        before they are created.  In this case, the callback will
        be made when it's actually introduced.
        """
        wait_callback = [Deferred() for i in range(10)]

        calls = []

        def watch_services(old_services, new_services):
            calls.append((old_services, new_services))
            wait_callback[len(calls)-1].callback(True)

        # Start watching.
        self.service_state_manager.watch_service_states(watch_services)

        # Callback is still untouched.
        self.assertEquals(calls, [])

        # Add a service, and wait for callback.
        yield self.add_service("wordpress")
        yield wait_callback[0]

        # The first callback must have been fired, and it must have None
        # as the first argument because that's the first service seen.
        self.assertEquals(len(calls), 1)
        old_services, new_services = calls[0]
        self.assertEquals(old_services, set())
        self.assertEquals(new_services, set(["wordpress"]))

        # Add a service again.
        yield self.add_service("mysql")
        yield wait_callback[1]

        # Now the watch callback must have been fired with two
        # different service sets.  The old one, and the new one.
        self.assertEquals(len(calls), 2)
        old_services, new_services = calls[1]
        self.assertEquals(old_services, set(["wordpress"]))
        self.assertEquals(new_services, set(["mysql", "wordpress"]))

    @inlineCallbacks
    def test_watch_services_may_defer(self):
        """
        The watch services callback may return a deferred so that it
        performs some of its logic asynchronously. In this case, it
        must not be called a second time before its postponed logic
        is finished completely.
        """
        wait_callback = [Deferred() for i in range(10)]
        finish_callback = [Deferred() for i in range(10)]

        calls = []

        def watch_services(old_services, new_services):
            calls.append((old_services, new_services))
            wait_callback[len(calls)-1].callback(True)
            return finish_callback[len(calls)-1]

        # Start watching.
        self.service_state_manager.watch_service_states(watch_services)

        # Create the service.
        yield self.add_service("wordpress")

        # Hold off until callback is started.
        yield wait_callback[0]

        # Add another service.
        yield self.add_service("mysql")

        # Ensure we still have a single call.
        self.assertEquals(len(calls), 1)

        # Allow the first call to be completed, and wait on the
        # next one.
        finish_callback[0].callback(None)
        yield wait_callback[1]
        finish_callback[1].callback(None)

        # We should have the second change now.
        self.assertEquals(len(calls), 2)
        old_services, new_services = calls[1]
        self.assertEquals(old_services, set(["wordpress"]))
        self.assertEquals(new_services, set(["mysql", "wordpress"]))

    @inlineCallbacks
    def test_watch_services_with_changing_topology(self):
        """
        If the topology changes in an unrelated way, the services
        watch callback should not be called with two equal
        arguments.
        """
        wait_callback = [Deferred() for i in range(10)]

        calls = []

        def watch_services(old_services, new_services):
            calls.append((old_services, new_services))
            wait_callback[len(calls)-1].callback(True)

        # Start watching.
        self.service_state_manager.watch_service_states(watch_services)

        # Callback is still untouched.
        self.assertEquals(calls, [])

        # Add a service, and wait for callback.
        yield self.add_service("wordpress")
        yield wait_callback[0]

        # Now change the topology in an unrelated way.
        yield self.machine_state_manager.add_machine_state()

        # Add a service again.
        yield self.add_service("mysql")
        yield wait_callback[1]

        # But it *shouldn't* have happened.
        self.assertEquals(len(calls), 2)

    @inlineCallbacks
    def test_watch_service_units_initial_callback(self):
        """Watch service unit processes initial state before returning.

        Note the callback is only executed if there is some meaningful state
        change.
        """
        results = []

        def callback(*args):
            results.append(True)

        service_state = yield self.add_service("wordpress")
        yield service_state.watch_service_unit_states(callback)
        yield service_state.add_unit_state()
        yield self.poke_zk()
        self.assertTrue(results)

    @inlineCallbacks
    def test_watch_service_units_when_being_created(self):
        """
        It should be possible to start watching service units even
        before they are created.  In this case, the callback will be
        made when it's actually introduced.
        """
        wait_callback = [Deferred() for i in range(10)]

        calls = []

        def watch_service_units(old_service_units, new_service_units):
            calls.append((old_service_units, new_service_units))
            wait_callback[len(calls)-1].callback(True)

        # Start watching.
        service_state = yield self.add_service("wordpress")
        service_state.watch_service_unit_states(watch_service_units)

        # Callback is still untouched.
        self.assertEquals(calls, [])

        # Add a service unit, and wait for callback.
        yield service_state.add_unit_state()

        yield wait_callback[0]

        # The first callback must have been fired, and it must have None
        # as the first argument because that's the first service seen.
        self.assertEquals(len(calls), 1)
        old_service_units, new_service_units = calls[0]
        self.assertEquals(old_service_units, set())
        self.assertEquals(new_service_units, set(["wordpress/0"]))

        # Add another service unit.
        yield service_state.add_unit_state()
        yield wait_callback[1]

        # Now the watch callback must have been fired with two
        # different service sets.  The old one, and the new one.
        self.assertEquals(len(calls), 2)
        old_service_units, new_service_units = calls[1]
        self.assertEquals(old_service_units, set(["wordpress/0"]))
        self.assertEquals(new_service_units, set(["wordpress/0", "wordpress/1"]))

    @inlineCallbacks
    def test_watch_service_units_may_defer(self):
        """
        The watch service units callback may return a deferred so that
        it performs some of its logic asynchronously.  In this case,
        it must not be called a second time before its postponed logic
        is finished completely.
        """
        wait_callback = [Deferred() for i in range(10)]
        finish_callback = [Deferred() for i in range(10)]

        calls = []

        def watch_service_units(old_service_units, new_service_units):
            calls.append((old_service_units, new_service_units))
            wait_callback[len(calls)-1].callback(True)
            return finish_callback[len(calls)-1]

        # Start watching.
        service_state = yield self.add_service("wordpress")
        service_state.watch_service_unit_states(watch_service_units)

        # Create the service unit.
        yield service_state.add_unit_state()

        # Hold off until callback is started.
        yield wait_callback[0]

        # Add another service unit.
        yield service_state.add_unit_state()

        # Ensure we still have a single call.
        self.assertEquals(len(calls), 1)

        # Allow the first call to be completed, and wait on the
        # next one.
        finish_callback[0].callback(None)
        yield wait_callback[1]
        finish_callback[1].callback(None)

        # We should have the second change now.
        self.assertEquals(len(calls), 2)
        old_service_units, new_service_units = calls[1]
        self.assertEquals(old_service_units, set(["wordpress/0"]))
        self.assertEquals(new_service_units, set(["wordpress/0", "wordpress/1"]))

    @inlineCallbacks
    def test_watch_service_units_with_changing_topology(self):
        """
        If the topology changes in an unrelated way, the services
        watch callback should not be called with two equal
        arguments.
        """
        wait_callback = [Deferred() for i in range(10)]

        calls = []

        def watch_service_units(old_service_units, new_service_units):
            calls.append((old_service_units, new_service_units))
            wait_callback[len(calls)-1].callback(True)

        # Start watching.
        service_state = yield self.add_service("wordpress")
        service_state.watch_service_unit_states(watch_service_units)

        # Callback is still untouched.
        self.assertEquals(calls, [])

        # Add a service, and wait for callback.
        yield service_state.add_unit_state()
        yield wait_callback[0]

        # Now change the topology in an unrelated way.
        yield self.machine_state_manager.add_machine_state()

        # Add a service again.
        yield service_state.add_unit_state()
        yield wait_callback[1]

        # But it *shouldn't* have happened.
        self.assertEquals(len(calls), 2)

    @inlineCallbacks
    def test_service_config_get_set(self):
        """Validate that we can set and get service config options."""
        wordpress = yield self.add_service_from_charm("wordpress")

        # attempt to get the initialized service state
        config = yield wordpress.get_config()

        # the initial state is empty
        self.assertEqual(config, {})

        # behaves as a normal dict
        self.assertRaises(KeyError, config.__getitem__, "missing")

        # various ways to set state
        config.update(dict(alpha="beta", one="two"))
        config["another"] = "value"

        # write the values
        yield config.write()

        # we should be able to read the config and see the same values
        # (in this case it would be the cached object)
        config2 = yield wordpress.get_config()
        self.assertEqual(config2, {"alpha": "beta",
                                   "one": "two",
                                   "another": "value"})

        # now set a non-string value and recover it
        config2["number"] = 1
        config2["one"] = None
        yield config2.write()

        yield config.read()
        self.assertEquals(config["number"], 1)
        self.assertEquals(config["one"], None)

    @inlineCallbacks
    def test_service_config_get_returns_new(self):
        """Validate that we can set and get service config options."""
        wordpress = yield self.add_service_from_charm("wordpress")

        # attempt to get the initialized service state
        config = yield wordpress.get_config()
        config.update({"foo": "bar"})
        self.assertEqual(config, {"foo": "bar"})

        config2 = yield wordpress.get_config()
        self.assertEqual(config2, {})

        yield config.write()
        self.assertEqual(config, {"foo": "bar"})
        # but config2 is still empty (a different YAML State)
        self.assertEqual(config2, {})

        yield config2.read()
        self.assertEqual(config2, {"foo": "bar"})

    @inlineCallbacks
    def test_get_charm_state(self):
        wordpress = yield self.add_service_from_charm("wordpress")
        charm = yield wordpress.get_charm_state()

        self.assertEqual(charm.name, "wordpress")
        metadata = yield charm.get_metadata()
        self.assertEqual(metadata.summary, "Blog engine")


class ExposedFlagTest(ServiceStateManagerTestBase):

    @inlineCallbacks
    def test_set_and_clear_exposed_flag(self):
        """An exposed flag can be set on a service."""

        # Defaults to false
        service_state = yield self.service_state_manager.add_service_state(
            "wordpress", self.charm_state)
        exposed_flag = yield service_state.get_exposed_flag()
        self.assertEqual(exposed_flag, False)

        # Can be set
        yield service_state.set_exposed_flag()
        exposed_flag = yield service_state.get_exposed_flag()
        self.assertEqual(exposed_flag, True)

        # Can be set multiple times
        yield service_state.set_exposed_flag()
        exposed_flag = yield service_state.get_exposed_flag()
        self.assertEqual(exposed_flag, True)

        # Can be cleared
        yield service_state.clear_exposed_flag()
        exposed_flag = yield service_state.get_exposed_flag()
        self.assertEqual(exposed_flag, False)

        # Can be cleared multiple times
        yield service_state.clear_exposed_flag()
        exposed_flag = yield service_state.get_exposed_flag()
        self.assertEqual(exposed_flag, False)

    @inlineCallbacks
    def test_watch_exposed_flag(self):
        """An exposed watch is setup on a permanent basis."""
        service_state = yield self.add_service("wordpress")

        results = []

        def callback(value):
            results.append(value)

        yield service_state.set_exposed_flag()

        # verify that the current state is processed before returning
        yield service_state.watch_exposed_flag(callback)
        yield service_state.clear_exposed_flag()
        yield service_state.set_exposed_flag()
        yield service_state.clear_exposed_flag()
        yield service_state.clear_exposed_flag()  # should be ignored
        yield service_state.set_exposed_flag()

        self.assertEqual(
            (yield service_state.get_exposed_flag()),
            True)
        self.assertEqual(results, [True, False, True, False, True])

    @inlineCallbacks
    def test_stop_watch_exposed_flag(self):
        """The watch is setup on a permanent basis, but can be stopped.

        The callback can raise StopWatcher at any time to stop the
        watch.
        """
        service_state = yield self.add_service("wordpress")
        results = []

        def callback(value):
            results.append(value)
            if len(results) == 2:
                raise StopWatcher()
            if len(results) == 4:
                raise StopWatcher()

        yield service_state.watch_exposed_flag(callback)
        yield service_state.set_exposed_flag()
        # two sets in a row do not retrigger callback since no change
        # in flag status
        yield service_state.set_exposed_flag()
        yield service_state.clear_exposed_flag()

        # no callback now, because StopWatcher was just raised
        yield service_state.set_exposed_flag()

        # then setup watch again
        yield service_state.watch_exposed_flag(callback)
        yield service_state.clear_exposed_flag()

        # no callbacks for these two lines, because StopWatcher was
        # already raised
        yield service_state.set_exposed_flag()
        yield service_state.clear_exposed_flag()

        self.assertEqual(
            (yield service_state.get_exposed_flag()), False)
        self.assertEqual(results, [False, True, True, False])

    @inlineCallbacks
    def test_watch_exposed_flag_waits_on_slow_callbacks(self):
        """Verify that a slow watch callback is still invoked serially."""

        service_state = yield self.add_service("wordpress")

        callbacks = [Deferred() for i in range(3)]
        before = []  # values seen before callback in `cb_watch`
        after = []   # and after

        @inlineCallbacks
        def cb_watch(value):
            before.append(value)
            yield callbacks[len(before) - 1]
            after.append((yield service_state.get_exposed_flag()))

        yield service_state.set_exposed_flag()

        # Need to let first callback be completed, otherwise will wait
        # forever in watch_exposed_flag. This is because `cb_watch` is
        # initially called in the setup of the watch
        callbacks[0].callback(True)

        yield service_state.watch_exposed_flag(cb_watch)
        self.assertEqual(before, [True])
        self.assertEqual(after, [True])

        # Go through the watch again, verifying that it is waiting on
        # `callbacks[1]`
        yield service_state.clear_exposed_flag()
        yield self.poke_zk()
        self.assertEqual(before, [True, False])
        self.assertEqual(after, [True])

        # Now let `cb_watch` finish
        callbacks[1].callback(True)
        yield self.poke_zk()

        # Go through another watch cycle
        yield service_state.set_exposed_flag()
        yield self.poke_zk()

        # Verify results, still haven't advanced through `callbacks[2]`
        self.assertEqual(before, [True, False, True])
        self.assertEqual(after, [True, False])

        # Now let it go through, verifying that `before` hasn't
        # changed, but `after` has now updated
        callbacks[2].callback(True)
        yield self.poke_zk()
        self.assertEqual(before, [True, False, True])
        self.assertEqual(after, [True, False, True])


class PortsTest(ServiceStateManagerTestBase):

    @inlineCallbacks
    def test_watch_config_options(self):
        """Verify callback trigger on config options modification"""

        service_state = yield self.service_state_manager.add_service_state(
            "wordpress", self.charm_state)
        results = []

        def callback(value):
            results.append(value)

        yield service_state.watch_config_state(callback)
        config = yield service_state.get_config()
        config["alpha"] = "beta"
        yield config.write()

        yield self.poke_zk()
        self.assertIdentical(results.pop(0), True)
        self.assertIdentical(results.pop(0).type_name, "changed")

        # and changing it again should trigger the callback again
        config["gamma"] = "delta"
        yield config.write()

        yield self.poke_zk()
        self.assertEqual(len(results), 1)
        self.assertIdentical(results.pop(0).type_name, "changed")

    @inlineCallbacks
    def test_get_open_ports(self):
        """Verify introspection and that the ports changes are immediate."""
        service_state = yield self.add_service("wordpress")
        unit_state = yield service_state.add_unit_state()

        # verify no open ports before activity
        self.assertEqual((yield unit_state.get_open_ports()), [])

        # then open_port, close_port
        yield unit_state.open_port(80, "tcp")
        self.assertEqual(
            (yield unit_state.get_open_ports()),
            [{"port": 80, "proto": "tcp"}])

        yield unit_state.open_port(53, "udp")
        self.assertEqual(
            (yield unit_state.get_open_ports()),
            [{"port": 80, "proto": "tcp"},
             {"port": 53, "proto": "udp"}])

        yield unit_state.open_port(53, "tcp")
        self.assertEqual(
            (yield unit_state.get_open_ports()),
            [{"port": 80, "proto": "tcp"},
             {"port": 53, "proto": "udp"},
             {"port": 53, "proto": "tcp"}])

        yield unit_state.open_port(443, "tcp")
        self.assertEqual(
            (yield unit_state.get_open_ports()),
            [{"port": 80, "proto": "tcp"},
             {"port": 53, "proto": "udp"},
             {"port": 53, "proto": "tcp"},
             {"port": 443, "proto": "tcp"}])

        yield unit_state.close_port(80, "tcp")
        self.assertEqual(
            (yield unit_state.get_open_ports()),
            [{"port": 53, "proto": "udp"},
             {"port": 53, "proto": "tcp"},
             {"port": 443, "proto": "tcp"}])

    @inlineCallbacks
    def test_open_ports_znode_representation(self):
        """Verify the specific representation of open ports in ZK."""
        service_state = yield self.add_service("wordpress")
        unit_state = yield service_state.add_unit_state()
        ports_path = "/units/unit-0000000000/ports"

        # verify no node exists before activity
        self.assertFailure(
            self.client.get(ports_path), zookeeper.NoNodeException)

        # verify representation format after open_port, close_port
        yield unit_state.open_port(80, "tcp")
        content, stat = yield self.client.get(ports_path)
        self.assertEquals(
            yaml.load(content),
            {"open": [{"port": 80, "proto": "tcp"}]})

        yield unit_state.open_port(53, "udp")
        content, stat = yield self.client.get(ports_path)
        self.assertEquals(
            yaml.load(content),
            {"open": [{"port": 80, "proto": "tcp"},
                      {"port": 53, "proto": "udp"}]})

        yield unit_state.open_port(443, "tcp")
        content, stat = yield self.client.get(ports_path)
        self.assertEquals(
            yaml.load(content),
            {"open": [{"port": 80, "proto": "tcp"},
                      {"port": 53, "proto": "udp"},
                      {"port": 443, "proto": "tcp"}]})

        yield unit_state.close_port(80, "tcp")
        content, stat = yield self.client.get(ports_path)
        self.assertEquals(
            yaml.load(content),
            {"open": [{"port": 53, "proto": "udp"},
                      {"port": 443, "proto": "tcp"}]})

    @inlineCallbacks
    def test_watch_ports(self):
        """An opened ports watch can be set to notified of presence and changes."""
        service_state = yield self.add_service("wordpress")
        unit_state = yield service_state.add_unit_state()
        yield unit_state.open_port(80, "tcp")
        yield unit_state.open_port(53, "udp")
        yield unit_state.open_port(443, "tcp")

        results = []

        def callback(value):
            results.append(value)

        # set up a one-time watch
        unit_state.watch_ports(callback)

        # do two actions
        yield unit_state.close_port(80, "tcp")
        yield unit_state.open_port(22, "tcp")

        # but see just the callback with one changed event, plus initial setup
        yield self.poke_zk()
        self.assertEqual(len(results), 3)
        self.assertEqual(results.pop(0), True)
        self.assertEqual(results.pop().type_name, "changed")
        self.assertEqual(results.pop().type_name, "changed")
        self.assertEqual(
            (yield unit_state.get_open_ports()),
            [{'port': 53, 'proto': 'udp'},
             {'port': 443, 'proto': 'tcp'},
             {'port': 22, 'proto': 'tcp'}])

    @inlineCallbacks
    def test_stop_watch_ports(self):
        """An exposed watch can be instituted on a permanent basis.

        However the callback can raise StopWatcher any time to stop the watch.
        """
        service_state = yield self.add_service("wordpress")
        unit_state = yield service_state.add_unit_state()
        yield unit_state.open_port(80, "tcp")

        results = []

        def callback(value):
            results.append(value)
            if len(results) == 1:
                raise StopWatcher()
            if len(results) == 4:
                raise StopWatcher()

        unit_state.watch_ports(callback)
        yield unit_state.close_port(80, "tcp")
        yield self.poke_zk()
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0], True)

        unit_state.watch_ports(callback)
        yield unit_state.open_port(53, "udp")
        yield self.poke_zk()
        yield self.sleep(0.1)
        self.assertEqual(len(results), 3)
        self.assertEqual(results[0], True)
        self.assertEqual(results[1], True)
        self.assertEqual(results[2].type_name, "changed")
        self.assertEqual(
            (yield unit_state.get_open_ports()),
            [{'port': 53, 'proto': 'udp'}])

    @inlineCallbacks
    def test_watch_ports_slow_callbacks(self):
        """A slow watch callback is still invoked serially."""
        unit_state = yield self.get_unit_state()

        callbacks = [Deferred() for i in range(5)]
        results = []
        contents = []

        @inlineCallbacks
        def watch(value):
            results.append(value)
            yield callbacks[len(results) - 1]
            contents.append((yield unit_state.get_open_ports()))

        callbacks[0].callback(True)
        yield unit_state.watch_ports(watch)

        # These get collapsed into a single event
        yield unit_state.open_port(80, "tcp")
        yield unit_state.open_port(53, "udp")
        yield unit_state.open_port(443, "tcp")
        yield unit_state.close_port(80, "tcp")
        yield self.poke_zk()

        # Verify the callback hasn't completed
        self.assertEqual(len(results), 2)
        self.assertEqual(len(contents), 1)

        # Let it finish
        callbacks[1].callback(True)
        yield self.poke_zk()

        # Verify the callback hasn't completed
        self.assertEqual(len(contents), 2)

        callbacks[2].callback(True)
        yield self.poke_zk()

        # Verify values.
        self.assertEqual(len(contents), 3)
        self.assertEqual(results[-1].type_name, "changed")
        self.assertEqual(contents[-1], [
                {'port': 53, 'proto': 'udp'}, {'port': 443, 'proto': 'tcp'}])
        yield self.poke_zk()

    def test_parse_service_name(self):
        self.assertEqual(parse_service_name("wordpress/0"), "wordpress")
        self.assertEqual(parse_service_name("myblog/1"), "myblog")
        self.assertRaises(ValueError, parse_service_name, "invalid")
        self.assertRaises(ValueError, parse_service_name, None)
