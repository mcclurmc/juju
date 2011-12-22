import functools

import yaml
import time

import zookeeper
import logging

from twisted.internet.defer import (
    inlineCallbacks, returnValue, Deferred, fail, succeed)
from txzookeeper import ZookeeperClient

from juju.charm.tests import local_charm_id
from juju.state.charm import CharmStateManager
from juju.state.endpoint import RelationEndpoint
from juju.state.errors import (
    DuplicateEndpoints, IncompatibleEndpoints, RelationAlreadyExists,
    RelationStateNotFound, StateChanged, UnitRelationStateNotFound,
    UnknownRelationRole, ServiceStateNameInUse)

from juju.state.relation import (
    RelationStateManager, ServiceRelationState, UnitRelationState)
from juju.state.service import ServiceStateManager
from juju.state.tests.common import StateTestBase


class RelationTestBase(StateTestBase):

    @inlineCallbacks
    def setUp(self):
        yield super(RelationTestBase, self).setUp()
        self.relation_manager = RelationStateManager(self.client)
        self.charm_manager = CharmStateManager(self.client)
        self.service_manager = ServiceStateManager(self.client)
        self.charm_state = None

    @inlineCallbacks
    def add_service(self, name):
        if not self.charm_state:
            self.charm_state = yield self.charm_manager.add_charm_state(
                local_charm_id(self.charm), self.charm, "")
        try:
            service_state = yield self.service_manager.add_service_state(
                name, self.charm_state)
        except ServiceStateNameInUse:
            service_state = yield self.service_manager.get_service_state(name)
        returnValue(service_state)

    @inlineCallbacks
    def add_relation(self, relation_type, *services):
        """Support older tests that don't use `RelationEndpoint`s"""
        endpoints = []
        for service_meta in services:
            service_state, relation_name, relation_role = service_meta
            endpoints.append(RelationEndpoint(
                service_state.service_name,
                relation_type,
                relation_name,
                relation_role))
        relation_state = yield self.relation_manager.add_relation_state(
            *endpoints)
        returnValue(relation_state[0])

    @inlineCallbacks
    def add_relation_service_unit_from_endpoints(self, *endpoints):
        """Build the relation and add one service unit to the first endpoint.

        This method is used to migrate older tests that would create
        the relation, assign one service, add a service unit, AND then
        assign a service. However, service assignment is now done all
        at once with the relation creation. Because we are interested
        in testing what happens with the changes to the service units,
        such tests remain valid.

        Returns a dict to collect together the various state objects
        being created. This is created from the perspective of the
        first endpoint, but the states of all of the endpoints are
        also captured, so it can be worked with from the opposite
        endpoint, as seen in :func:`add_opposite_service_unit`.
        """
        # 1. Setup all service states
        service_states = []
        for endpoint in endpoints:
            service_state = yield self.add_service(endpoint.service_name)
            service_states.append(service_state)

        # 2. And join together in a relation
        relation_state, service_relation_states = \
                        yield self.relation_manager.add_relation_state(
            *endpoints)

        # 3. Add a service unit to only the first endpoint - we need
        #    to test what happens when service units are added to the
        #    other service state (if any), so do so separately
        unit_state = yield service_states[0].add_unit_state()
        yield unit_state.set_private_address("%s.example.com" % (
            unit_state.unit_name.replace("/", "-")))
        relation_unit_state = yield service_relation_states[0].add_unit_state(
            unit_state)

        returnValue({
            "endpoints": list(endpoints),
            "service": service_states[0],
            "services": service_states,
            "unit": unit_state,
            "relation": relation_state,
            "service_relation": service_relation_states[0],
            "unit_relation": relation_unit_state,
            "service_relations": service_relation_states})

    @inlineCallbacks
    def add_opposite_service_unit(self, other_states):
        """Given `other_states`, add a service unit to the opposite endpoint.

        Like :func:`add_relation_service_unit_from_endpoints`, this is
        used to support older tests. Although it's slightly awkward to
        use because of attempt to be backwards compatible, it does
        enable the testing of a typical case: we are now bringing
        online a service unit on the opposite side of a relation
        endpoint pairing.

        TODO: there's probably a better name for this method.
        """
        assert len(other_states["services"]) == 2
        unit_state = yield other_states["services"][1].add_unit_state()
        yield unit_state.set_private_address("%s.example.com" % (
            unit_state.unit_name.replace("/", "-")))
        relation_unit_state = yield other_states["service_relations"][1].\
                              add_unit_state(unit_state)

        def rotate(X):
            rotated = X[1:]
            rotated.append(X[0])
            return rotated

        returnValue({
            "endpoints": rotate(other_states["endpoints"]),
            "service": other_states["services"][1],
            "services": rotate(other_states["services"]),
            "unit": unit_state,
            "relation": other_states["relation"],
            "service_relation": other_states["service_relations"][1],
            "unit_relation": relation_unit_state,
            "service_relations": rotate(other_states["service_relations"])})

    @inlineCallbacks
    def add_relation_service_unit_to_another_endpoint(self, states, endpoint):
        """Add a relation to `endpoint` from the first endpoint in `states`.

        This enables a scenario of creating two services and a
        relation by calling
        :func:`add_relation_service_unit_from_endpoints`, then adding
        one more endpoint to the first one. Like the other functions
        in this series, this is here to work with tests that use the
        now-deleted assign service functionality.
        """

        new_states = states.copy()
        new_states["services"][1] = (yield self.add_service(
            endpoint.service_name))
        new_states["endpoints"][1] = endpoint
        relation_state, service_relation_states = \
                        yield self.relation_manager.add_relation_state(
            *new_states["endpoints"])
        new_states["relation"] = relation_state
        new_states["service_relations"] = service_relation_states
        new_states["service_relation"] = service_relation_states[0]
        new_states["unit"] = yield new_states["services"][0].add_unit_state()
        new_states["unit_relation"] = yield service_relation_states[0].\
                                      add_unit_state(new_states["unit"])
        returnValue(new_states)

    @inlineCallbacks
    def add_relation_service_unit(self,
                                  relation_type,
                                  service_name,
                                  relation_name="name",
                                  relation_role="role",
                                  relation_state=None,
                                  client=None):
        """
        Create a relation, service, and service unit, with the
        service assigned to the relation. Optionally utilize
        existing relation state if passed in. If client is
        the service relation and the service relation state will
        utilize that as their zookeeper client.
        """
        # Add the service, relation, unit states
        service_state = yield self.add_service(service_name)
        relation_state = yield self.add_relation(
            relation_type,
            (service_state, relation_name, relation_role))
        unit_state = yield service_state.add_unit_state()

        # Get the service relation.
        relations = yield self.relation_manager.get_relations_for_service(
            service_state)
        for service_relation in relations:
            if (service_relation.internal_relation_id ==
                relation_state.internal_id):
                break

        # Utilize a separate client if requested.
        if client:
            service_relation = ServiceRelationState(
                client,
                service_relation.internal_service_id,
                service_relation.internal_relation_id,
                service_relation.relation_role,
                service_relation.relation_name)

        # Create the relation unit state
        relation_unit_state = yield service_relation.add_unit_state(
            unit_state)

        returnValue({
            "service": service_state,
            "unit": unit_state,
            "relation": relation_state,
            "service_relation": service_relation,
            "unit_relation": relation_unit_state})

    @inlineCallbacks
    def add_related_service_unit(self, state_dict):
        """
        Add a new service unit of the given service within the relation.
        """
        unit_state = yield state_dict["service"].add_unit_state()
        unit_relation = yield state_dict["service_relation"].add_unit_state(
            unit_state)
        new_state_dict = dict(state_dict)
        new_state_dict["unit"] = unit_state
        new_state_dict["unit_relation"] = unit_relation
        returnValue(new_state_dict)

    def get_unit_settings_path(self, state_dict):
        unit_relation_path = "/relations/%s/settings/%s" % (
            state_dict["relation"].internal_id,
            state_dict["unit"].internal_id)
        return unit_relation_path


class RelationStateManagerTest(RelationTestBase):

    @inlineCallbacks
    def test_add_relation_state(self):
        """Adding relation will create a relation node and update topology."""
        mysql_ep = RelationEndpoint("mysql", "mysql", "db", "server")
        yield self.add_service("mysql")
        relation_state = (yield self.relation_manager.add_relation_state(
            mysql_ep))[0]
        topology = yield self.get_topology()
        self.assertTrue(topology.has_relation(relation_state.internal_id))
        exists = yield self.client.exists(
            "/relations/%s" % relation_state.internal_id)
        self.assertTrue(exists)
        exists = yield self.client.get(
            "/relations/%s/settings" % relation_state.internal_id)
        self.assertTrue(exists)

    @inlineCallbacks
    def test_add_relation_state_to_missing_service(self):
        """Test adding a relation to a nonexistent service"""
        mysql_ep = RelationEndpoint("mysql", "mysql", "db", "server")
        blog_ep = RelationEndpoint("wordpress", "mysql", "mysql", "client")
        yield self.add_service("mysql")
        # but didn't create the service for wordpress
        yield self.assertFailure(
            self.relation_manager.add_relation_state(
                mysql_ep, blog_ep),
            StateChanged)

    @inlineCallbacks
    def test_add_relation_state_bad_relation_role(self):
        """Test adding a relation with a bad role when is one is well defined
        (client or server)"""
        blog_ep = RelationEndpoint("wordpress", "mysql", "mysql", "client")
        mysql_ep = RelationEndpoint("mysql", "mysql", "db", "server")
        bad_mysql_ep = RelationEndpoint(
            "mysql", "mysql", "db", "bad-server-role")
        bad_blog_ep = RelationEndpoint(
            "wordpress", "mysql", "mysql", "bad-client-role")
        yield self.add_service("mysql")
        yield self.add_service("wordpress")
        yield self.assertFailure(
            self.relation_manager.add_relation_state(
                bad_mysql_ep, blog_ep),
            IncompatibleEndpoints)
        yield self.assertFailure(
            self.relation_manager.add_relation_state(
                bad_blog_ep, mysql_ep),
            IncompatibleEndpoints)
        # TODO in future branch referenced in relation, also test
        # bad_blog_ep *and* bad_mysql_ep

    @inlineCallbacks
    def test_add_binary_relation_state_twice(self):
        """Test adding the same relation twice"""
        blog_ep = RelationEndpoint("wordpress", "mysql", "mysql", "client")
        mysql_ep = RelationEndpoint("mysql", "mysql", "db", "server")
        yield self.add_service("mysql")
        yield self.add_service("wordpress")
        yield self.relation_manager.add_relation_state(mysql_ep, blog_ep)
        yield self.assertFailure(
            self.relation_manager.add_relation_state(blog_ep, mysql_ep),
            RelationAlreadyExists)
        yield self.assertFailure(
            self.relation_manager.add_relation_state(mysql_ep, blog_ep),
            RelationAlreadyExists)

    @inlineCallbacks
    def test_add_peer_relation_state_twice(self):
        """Test adding the same relation twice"""
        riak_ep = RelationEndpoint("riak", "riak", "ring", "peer")
        yield self.add_service("riak")
        yield self.relation_manager.add_relation_state(riak_ep)
        yield self.assertFailure(
            self.relation_manager.add_relation_state(riak_ep),
            RelationAlreadyExists)

    @inlineCallbacks
    def test_add_relation_state_no_endpoints(self):
        """Test adding a relation with no endpoints (no longer allowed)"""
        yield self.assertFailure(
            self.relation_manager.add_relation_state(),
            TypeError)

    @inlineCallbacks
    def test_add_relation_state_relation_type_unshared(self):
        """Test adding a relation with endpoints not sharing a relation type"""
        pg_ep = RelationEndpoint("pg", "postgres", "db", "server")
        blog_ep = RelationEndpoint("wordpress", "mysql", "mysql", "client")
        yield self.assertFailure(
            self.relation_manager.add_relation_state(pg_ep, blog_ep),
            IncompatibleEndpoints)

    @inlineCallbacks
    def test_add_relation_state_too_many_endpoints(self):
        """Test adding a relation between too many endpoints (> 2)"""
        mysql_ep = RelationEndpoint("mysql", "mysql", "db", "server")
        blog_ep = RelationEndpoint("wordpress", "mysql", "mysql", "client")
        yield self.add_service("mysql")
        yield self.add_service("wordpress")
        yield self.assertFailure(
            self.relation_manager.add_relation_state(
                mysql_ep, blog_ep, mysql_ep),
            TypeError)

    @inlineCallbacks
    def test_add_relation_state_duplicate_peer_endpoints(self):
        """Test adding a relation between duplicate peer endpoints"""
        riak_ep = RelationEndpoint("riak", "riak", "ring", "peer")
        yield self.add_service("riak")
        yield self.assertFailure(
            self.relation_manager.add_relation_state(riak_ep, riak_ep),
            DuplicateEndpoints)

    @inlineCallbacks
    def test_add_relation_state_endpoints_duplicate_role(self):
        """Test adding a relation with services overlapped by duplicate role"""
        mysql_ep = RelationEndpoint("mysql", "mysql", "db", "server")
        drizzle_ep = RelationEndpoint("drizzle", "mysql", "db", "server")
        yield self.add_service("mysql")
        yield self.add_service("drizzle")
        yield self.assertFailure(
            self.relation_manager.add_relation_state(mysql_ep, drizzle_ep),
            IncompatibleEndpoints)

    @inlineCallbacks
    def test_remove_relation_state(self):
        """Removing a relation will remove it from the topology."""
        # Simulate add and remove.
        varnish_ep = RelationEndpoint("varnish", "webcache", "cache", "server")
        yield self.add_service("varnish")
        relation_state = (yield self.relation_manager.add_relation_state(
            varnish_ep))[0]
        topology = yield self.get_topology()
        self.assertTrue(topology.has_relation(relation_state.internal_id))
        yield self.relation_manager.remove_relation_state(relation_state)

        # Verify removal.
        topology = yield self.get_topology()
        self.assertFalse(topology.has_relation(relation_state.internal_id))

    @inlineCallbacks
    def test_remove_relation_state_with_service_state(self):
        """A relation can be removed using a ServiceRelationState argument."""
        # Simulate add and remove.
        varnish_endpoint = RelationEndpoint(
            "varnish", "webcache", "cache", "server")
        yield self.add_service("varnish")

        relation_state, _ = yield self.relation_manager.add_relation_state(
            varnish_endpoint)

        service_relation = _.pop()

        topology = yield self.get_topology()
        self.assertTrue(topology.has_relation(relation_state.internal_id))

        yield self.relation_manager.remove_relation_state(service_relation)

        # Verify removal.
        topology = yield self.get_topology()
        self.assertFalse(topology.has_relation(relation_state.internal_id))

    @inlineCallbacks
    def test_remove_relation_with_changing_state(self):
        # Simulate add and remove.
        varnish_ep = RelationEndpoint("varnish", "webcache", "cache", "server")
        yield self.add_service("varnish")
        relation_state = (yield self.relation_manager.add_relation_state(
            varnish_ep))[0]
        topology = yield self.get_topology()
        self.assertTrue(topology.has_relation(relation_state.internal_id))
        yield self.relation_manager.remove_relation_state(relation_state)

        topology = yield self.get_topology()
        self.assertFalse(topology.has_relation(relation_state.internal_id))

        # try to remove again, should get state change error.
        yield self.assertFailure(
            self.relation_manager.remove_relation_state(relation_state),
            StateChanged)

    @inlineCallbacks
    def test_get_relations_for_service(self):
        # Create some services and relations
        service1 = yield self.add_service("database")
        service2 = yield self.add_service("application")
        service3 = yield self.add_service("cache")

        relation1 = yield self.add_relation(
            "database",
            (service1, "client", "server"), (service2, "db", "client"))

        relation2 = yield self.add_relation(
            "cache",
            (service3, "app", "server"), (service2, "cache", "client"))

        relations = yield self.relation_manager.get_relations_for_service(
            service2)
        rel_ids = [r.internal_relation_id for r in relations]
        self.assertEqual(sorted(rel_ids),
                         [relation1.internal_id, relation2.internal_id])

        relations = yield self.relation_manager.get_relations_for_service(
            service1)
        rel_ids = [r.internal_relation_id for r in relations]
        self.assertEqual(sorted(rel_ids), [relation1.internal_id])

    @inlineCallbacks
    def test_get_relations_for_service_with_none(self):
        service1 = yield self.add_service("database")
        relations = yield self.relation_manager.get_relations_for_service(
            service1)
        self.assertFalse(relations)

    @inlineCallbacks
    def assertGetEqualRelationState(self, relation_state, *endpoints):
        get_relation_state = yield self.relation_manager.get_relation_state(
            *endpoints)
        self.assertEqual(
            relation_state.internal_id,
            get_relation_state.internal_id)

    @inlineCallbacks
    def test_get_relation_state(self):
        """Test that relation state can be retrieved from pair of endpoints."""
        mysql_ep = RelationEndpoint("mysql", "mysql", "db", "server")
        blog_mysql_ep = RelationEndpoint(
            "wordpress", "mysql", "mysql", "client")
        blog_varnish_ep = RelationEndpoint(
            "wordpress", "varnish", "webcache", "client")
        varnish_ep = RelationEndpoint("varnish", "varnish", "cache", "server")
        yield self.add_service("mysql")
        yield self.add_service("wordpress")
        yield self.add_service("varnish")
        blog_mysql = (yield self.relation_manager.add_relation_state(
            blog_mysql_ep, mysql_ep))[0]
        blog_varnish = (yield self.relation_manager.add_relation_state(
            blog_varnish_ep, varnish_ep))[0]
        yield self.assertGetEqualRelationState(
            blog_mysql, blog_mysql_ep, mysql_ep)
        yield self.assertGetEqualRelationState(
            blog_varnish, varnish_ep, blog_varnish_ep)

    @inlineCallbacks
    def test_get_relation_state_missing_relation(self):
        """Test that `RelationStateNotFound` is raised if no relation exists"""
        mysql_ep = RelationEndpoint("mysql", "mysql", "db", "server")
        blog_mysql_ep = RelationEndpoint(
            "wordpress", "mysql", "mysql", "client")
        blog_varnish_ep = RelationEndpoint(
            "wordpress", "varnish", "webcache", "client")
        varnish_ep = RelationEndpoint("varnish", "varnish", "cache", "server")
        yield self.add_service("mysql")
        yield self.add_service("wordpress")
        yield self.add_service("varnish")
        blog_varnish = (yield self.relation_manager.add_relation_state(
            blog_varnish_ep, varnish_ep))[0]
        yield self.assertFailure(
            self.relation_manager.get_relation_state(mysql_ep, blog_mysql_ep),
            RelationStateNotFound)
        yield self.assertGetEqualRelationState(
            blog_varnish, varnish_ep, blog_varnish_ep)


class ServiceRelationStateTest(RelationTestBase):

    @inlineCallbacks
    def setUp(self):
        yield super(ServiceRelationStateTest, self).setUp()

        self.service_state1 = yield self.add_service("wordpress-prod")
        self.service_state2 = yield self.add_service("wordpress-dev")
        self.relation_state = yield self.add_relation(
            "riak",
            (self.service_state1, "dev-connect", "prod"),
            (self.service_state2, "prod-connect", "dev"))

        relations = yield self.relation_manager.get_relations_for_service(
            self.service_state1)
        self.service1_relation = relations.pop()

    def get_presence_path(self, relation_state, relation_role, unit_state):
        presence_path = "/relations/%s/%s/%s" % (
            relation_state.internal_id,
            relation_role,
            unit_state.internal_id)
        return presence_path

    def test_property_internal_service_id(self):
        self.assertEqual(self.service1_relation.internal_service_id,
                         self.service_state1.internal_id)

    def test_property_internal_relation_id(self):
        self.assertEqual(self.service1_relation.internal_relation_id,
                         self.relation_state.internal_id)

    def test_property_relation_role(self):
        self.assertEqual(self.service1_relation.relation_role, "prod")

    def test_repr(self):
        id = "relation-0000000000"
        self.assertEqual(
            repr(self.service1_relation),
            "<ServiceRelationState name:dev-connect role:prod id:%s>" % id)

    def test_property_relation_name(self):
        """
        The service's name for the relation is accessible from the service
        relation state.
        """
        self.assertEqual(self.service1_relation.relation_name, "dev-connect")

    @inlineCallbacks
    def test_add_unit_state(self):
        """The service state is used to create units in the relation."""
        unit_state = yield self.service_state1.add_unit_state()

        # set some watches to verify the order things are created.
        state_created = Deferred()
        creation_order = []
        presence_path = self.get_presence_path(
            self.relation_state, "prod", unit_state)

        def append_event(name, event):
            creation_order.append((name, event))
            if len(creation_order) == 2:
                state_created.callback(creation_order)

        self.client.exists_and_watch(presence_path)[1].addCallback(
            lambda result: append_event("presence", result))

        settings_path = "/relations/%s/settings/%s" % (
            self.relation_state.internal_id,
            unit_state.internal_id)

        self.client.exists_and_watch(settings_path)[1].addCallback(
            lambda result: append_event("settings", result))

        yield unit_state.set_private_address("foobar.local")

        # add the unit agent
        yield self.service1_relation.add_unit_state(unit_state)

        # wait for the watches
        yield state_created

        # Verify order of creation, settings first, then presence.
        self.assertEqual(creation_order[0][0], "settings")
        self.assertEqual(creation_order[0][1].type_name, "created")
        self.assertEqual(creation_order[1][0], "presence")
        self.assertEqual(creation_order[1][1].type_name, "created")

        # Verify the unit mapping
        unit_map_data, stat = yield self.client.get("/relations/%s" % (
            self.relation_state.internal_id))
        unit_map = yaml.load(unit_map_data)
        self.assertEqual(
            unit_map,
            {unit_state.internal_id: unit_state.unit_name})

        content, stat = yield self.client.get(settings_path)
        self.assertEqual(
            yaml.load(content), {"private-address": "foobar.local"})

    @inlineCallbacks
    def test_presence_node_is_ephemeral(self):
        """
        A unit relation state is composed of two nodes, an ephemeral
        presence node, and a persistent settings node. Verify that
        the presence node is ephemeral.
        """
        unit_state = yield self.service_state1.add_unit_state()

        # manually construct a unit relation state using a separate
        # connection.
        client2 = ZookeeperClient(self.client.servers)
        yield client2.connect()
        service_relation = ServiceRelationState(
            client2,
            self.service_state1.internal_id,
            self.relation_state.internal_id,
            "prod",
            "name")
        yield service_relation.add_unit_state(unit_state)

        presence_path = self.get_presence_path(
            self.relation_state, "prod", unit_state)

        exists_d, watch_d = self.client.exists_and_watch(presence_path)
        exists = yield exists_d

        self.assertTrue(exists)

        yield client2.close()
        event = yield watch_d
        self.assertEquals(event.type_name, "deleted")

    @inlineCallbacks
    def test_add_unit_state_with_preexisting_presence(self):
        """
        If a unit relation presence node exists, attempting
        to add it will return a unit relation state.
        """
        unit_state = yield self.service_state1.add_unit_state()
        presence_path = self.get_presence_path(
            self.relation_state, "prod", unit_state)
        yield self.client.create(presence_path)

        # Adding it again is fine.
        unit_relation = yield self.service1_relation.add_unit_state(unit_state)
        self.assertEqual(unit_relation.internal_unit_id,
                         unit_state.internal_id)

    @inlineCallbacks
    def test_add_unit_state_with_preexisting_settings(self):
        """A unit coming backup retains its existing settings.

        With the exception of the unit address, which is always
        kept current on subsequent joinings.
        """
        unit_state = yield self.service_state1.add_unit_state()
        settings_path = "/relations/%s/settings/%s" % (
            self.relation_state.internal_id,
            unit_state.internal_id)

        data = {"hello": "world", "private-address": "foobar.local"}
        yield self.client.create(settings_path, yaml.dump(data))

        yield unit_state.set_private_address("northwest.local")
        yield self.service1_relation.add_unit_state(unit_state)

        node_data, stat = yield self.client.get(settings_path)

        # The unit address has been updated to current
        data["private-address"] = "northwest.local"
        self.assertEqual(node_data, yaml.dump(data))

        data, stat = yield self.client.get(
            "/relations/%s" % self.relation_state.internal_id)
        unit_map = yaml.load(data)
        self.assertEqual(unit_map,
                         {unit_state.internal_id: unit_state.unit_name})

    @inlineCallbacks
    def test_get_unit_state(self):
        unit_state = yield self.service_state1.add_unit_state()
        unit_relation_state = yield self.service1_relation.add_unit_state(
            unit_state)
        self.assertTrue(isinstance(unit_relation_state, UnitRelationState))
        unit_relation_state2 = yield self.service1_relation.get_unit_state(
            unit_state)
        self.assertEqual(
            (unit_relation_state.internal_unit_id,
             unit_relation_state.internal_service_id,
             unit_relation_state.internal_relation_id),
            (unit_relation_state2.internal_unit_id,
             unit_relation_state2.internal_service_id,
             unit_relation_state2.internal_relation_id))

    @inlineCallbacks
    def test_get_unit_state_nonexistant(self):
        unit_state = yield self.service_state1.add_unit_state()
        yield self.assertFailure(
            self.service1_relation.get_unit_state(unit_state),
            UnitRelationStateNotFound)

    @inlineCallbacks
    def test_get_all_service_states(self):
        services = yield self.service1_relation.get_service_states()
        self.assertEqual(set(services),
                         set((self.service_state1,
                              self.service_state2)))


class UnitRelationStateTest(RelationTestBase):

    def verify_unit_watch_result(
        self, result, old_units=None, new_units=None, modified=None):
        for value, expected in zip(result, [old_units, new_units, modified]):
            if expected is None:
                self.assertFalse(value)
                continue
            for value_unit, expected_unit in zip(value, expected):
                self.assertEqual(value_unit,
                                 expected_unit.unit_name)

    @inlineCallbacks
    def test_properties(self):
        states = yield self.add_relation_service_unit("webcache", "varnish")
        unit_relation = states["unit_relation"]

        self.assertEqual(
            unit_relation.internal_service_id,
            states["service"].internal_id)
        self.assertEqual(
            unit_relation.internal_relation_id,
            states["relation"].internal_id)
        self.assertEqual(
            unit_relation.internal_unit_id,
            states["unit"].internal_id)

    @inlineCallbacks
    def test_get_data(self):
        states = yield self.add_relation_service_unit("webcache", "varnish")
        unit_relation = states["unit_relation"]

        data = yield unit_relation.get_data()
        self.assertEqual(yaml.load(data), {"private-address": None})

        unit_relation_path = self.get_unit_settings_path(states)
        self.client.set(unit_relation_path, yaml.dump(dict(hello="world")))

        data = yield unit_relation.get_data()
        self.assertEqual(data, yaml.dump(dict(hello="world")))

    @inlineCallbacks
    def test_set_data(self):
        states = yield self.add_relation_service_unit("webcache", "varnish")
        unit_relation = states["unit_relation"]
        unit_relation_path = self.get_unit_settings_path(states)
        yield unit_relation.set_data(dict(hello="world"))
        data, stat = yield self.client.get(unit_relation_path)
        self.assertEqual(data, yaml.dump(dict(hello="world")))

    @inlineCallbacks
    def test_get_relation_role(self):
        """Retrieve the service's relation role.
        """
        states = yield self.add_relation_service_unit(
            "webcache", "varnish", "name", "server")
        role = yield states["unit_relation"].get_relation_role()
        self.assertEqual("server", role)

    @inlineCallbacks
    def test_get_related_unit_container(self):
        """Retrieve the container path of the related units."""
        states = yield self.add_relation_service_unit(
            "webcache", "varnish", "name", "server")
        container_path = "/relations/%s/%s" % (
            states["relation"].internal_id, "client")

        path = yield states["unit_relation"].get_related_unit_container()
        self.assertEqual(path, container_path)

        states = yield self.add_relation_service_unit(
            "riak", "riak", "name", "peer")
        container_path = "/relations/%s/%s" % (
            states["relation"].internal_id, "peer")

        path = yield states["unit_relation"].get_related_unit_container()
        self.assertEqual(path, container_path)

        states = yield self.add_relation_service_unit(
            "wordpress", "wordpress", "name", "client")
        container_path = "/relations/%s/%s" % (
            states["relation"].internal_id, "server")

        path = yield states["unit_relation"].get_related_unit_container()
        self.assertEqual(path, container_path)

    @inlineCallbacks
    def test_watch_start_existing_service(self):
        """Invoking watcher.start returns a deferred that only fires
        after watch on the container is in place. In the case of an
        existing service, this is after a child watch is established.
        """
        wordpress_ep = RelationEndpoint(
            "wordpress", "client-server", "", "client")
        mysql_ep = RelationEndpoint(
            "mysql", "client-server", "", "server")
        wordpress_states = yield self.add_relation_service_unit_from_endpoints(
            wordpress_ep, mysql_ep)

        yield self.add_opposite_service_unit(wordpress_states)

        results = []
        wait_callback = [Deferred() for i in range(5)]

        def watch_related(old_units=None, new_units=None, modified=None):
            results.append((old_units, new_units, modified))
            wait_callback[len(results)-1].callback(True)

        def invoked(*args, **kw):
            # sleep to make sure that things haven't fired till the watch is
            # in place.
            time.sleep(0.1)
            self.assertFalse(results)

        mock_client = self.mocker.patch(self.client)
        mock_client.get_children_and_watch("/relations/%s/server" % (
            wordpress_states["relation"].internal_id))

        self.mocker.call(invoked)
        self.mocker.passthrough()
        self.mocker.replay()

        watcher = yield wordpress_states["unit_relation"].watch_related_units(
            watch_related)
        yield watcher.start()
        yield wait_callback[0]

    @inlineCallbacks
    def test_watch_start_new_service(self):
        """Invoking watcher.start returns a deferred that only fires
        after watch on the containr is in place. In the case of a new
        service this after an existance watch is established on the
        container.
        """
        wordpress_ep = RelationEndpoint(
            "wordpress", "client-server", "", "client")
        wordpress_states = yield self.add_relation_service_unit_from_endpoints(
            wordpress_ep)

        results = []

        def watch_related(old_units=None, new_units=None, modified=None):
            results.append((old_units, new_units, modified))

        def invoked(*args, **kw):
            # sleep to make sure that things haven't fired till the watch is
            # in place.
            time.sleep(0.1)
            self.assertFalse(results)

        mock_client = self.mocker.patch(self.client)
        mock_client.exists_and_watch("/relations/%s/server" % (
            wordpress_states["relation"].internal_id))

        self.mocker.call(invoked)
        self.mocker.passthrough()
        self.mocker.replay()

        watcher = yield wordpress_states["unit_relation"].watch_related_units(
            watch_related)
        yield watcher.start().addCallback(lambda x: results.append(True))

    @inlineCallbacks
    def test_watch_client_server_with_new_service(self):
        """We simulate a scenario where the client units appear
        first within relation, and start to monitor the server service
        as it joins the relation, adds a unit, modifies, the unit.
        """
        wordpress_ep = RelationEndpoint(
            "wordpress", "client-server", "", "client")
        mysql_ep = RelationEndpoint(
            "mysql", "client-server", "", "server")
        wordpress_states = yield self.add_relation_service_unit_from_endpoints(
            wordpress_ep, mysql_ep)

        # setup watch callbacks, and start watching.
        wait_callback = [Deferred() for i in range(5)]
        results = []

        def watch_related(old_units=None, new_units=None, modified=None):
            results.append((old_units, new_units, modified))
            wait_callback[len(results) - 1].callback(True)

        watcher = yield wordpress_states["unit_relation"].watch_related_units(
            watch_related)
        yield watcher.start()

        # adding another unit of wordpress, does not cause any changes
        service1_relation = wordpress_states["service_relation"]
        service1_unit2 = yield wordpress_states["service"].add_unit_state()
        yield service1_relation.add_unit_state(service1_unit2)

        # give chance for accidental watch firing.
        yield self.sleep(0.1)

        # assert no firing of callback
        self.assertFalse(results)

        # add the server service and a unit of that
        mysql_states = yield self.add_opposite_service_unit(
            wordpress_states)

        topology = yield self.get_topology()
        # assert the relation is established correctly
        services = topology.get_relation_services(
            wordpress_states["relation"].internal_id)
        self.assertEqual(len(services), 2)

        # wait for callback
        yield wait_callback[0]
        self.verify_unit_watch_result(
            results[0], old_units=[], new_units=[mysql_states["unit"]])

        # modify the unit and get another callback
        yield mysql_states["unit_relation"].set_data(dict(hello="world"))
        yield wait_callback[1]
        self.verify_unit_watch_result(
             results[1], modified=[mysql_states["unit"]])

        # add another unit of mysql
        mysql_unit2 = yield mysql_states["service"].add_unit_state()
        yield mysql_states["service_relation"].add_unit_state(mysql_unit2)
        yield wait_callback[2]
        self.verify_unit_watch_result(
            results[2],
            old_units=[mysql_states["unit"]],
            new_units=[mysql_states["unit"], mysql_unit2])

    @inlineCallbacks
    def test_watch_client_server_with_existing_service(self):
        """We simulate a scenario where the client and server are both
        in place before the client begins observing. The server subsequently
        modifies, and remove its unit from the relation.
        """
        # add the client service and a unit of that
        wordpress_ep = RelationEndpoint(
            "wordpress", "client-server", "", "client")
        mysql_ep = RelationEndpoint(
            "mysql", "client-server", "", "server")
        wordpress_states = yield self.add_relation_service_unit_from_endpoints(
            wordpress_ep, mysql_ep)
        mysql_states = yield self.add_opposite_service_unit(
            wordpress_states)

        # setup callbacks and start observing.
        wait_callback = [Deferred() for i in range(5)]
        results = []

        def watch_related(old_units=None, new_units=None, modified=None):
            results.append((old_units, new_units, modified))
            wait_callback[len(results)-1].callback(True)

        watcher = yield wordpress_states["unit_relation"].watch_related_units(
            watch_related)
        yield watcher.start()

        # wait for initial callback
        yield wait_callback[0]
        self.verify_unit_watch_result(
            results[0], old_units=[], new_units=[mysql_states["unit"]])

        # modify the unit and get another callback, verify the result.
        yield mysql_states["unit_relation"].set_data(dict(hello="world"))
        yield wait_callback[1]
        self.verify_unit_watch_result(
            results[1], modified=[mysql_states["unit"]])

        # directly delete the presence node to trigger a deletion notification
        self.client.delete("/relations/%s/server/%s" % (
            mysql_states["relation"].internal_id,
            mysql_states["unit"].internal_id))

        # verify the deletion result.
        yield wait_callback[2]
        self.verify_unit_watch_result(
            results[2], old_units=[mysql_states["unit"]], new_units=[])

    @inlineCallbacks
    def test_watch_server_client_with_new_service(self):
        """We simulate a server watching a client.
        """
        wordpress_ep = RelationEndpoint(
            "wordpress", "client-server", "", "client")
        mysql_ep = RelationEndpoint(
            "mysql", "client-server", "", "server")
        # add the server service and a unit of that
        mysql_states = yield self.add_relation_service_unit_from_endpoints(
            mysql_ep, wordpress_ep)

        # setup callbacks and start observing.
        wait_callback = [Deferred() for i in range(5)]
        results = []

        def watch_related(old_units=None, new_units=None, modified=None):
            results.append((old_units, new_units, modified))
            wait_callback[len(results)-1].callback(True)

        watcher = yield mysql_states["unit_relation"].watch_related_units(
            watch_related)
        yield watcher.start()

        # assert no firing of callback
        self.assertFalse(results)

        # add the client service and a unit of that
        wordpress_states = yield self.add_opposite_service_unit(
            mysql_states)

        yield wait_callback[0]
        self.verify_unit_watch_result(
            results[0], old_units=[], new_units=[wordpress_states["unit"]])

    @inlineCallbacks
    def test_watch_peer(self):
        """Peer relations always watch the peer container.
        """
        # add the peer relation and two unit of the service.
        riak_ep = RelationEndpoint("riak", "peer", "riak-db", "peer")
        riak_states = yield self.add_relation_service_unit_from_endpoints(
            riak_ep)

        riak2_unit = yield riak_states["service"].add_unit_state()
        yield riak_states["service_relation"].add_unit_state(riak2_unit)

        # setup callbacks and start observing.
        wait_callback = [Deferred() for i in range(5)]
        results = []

        def watch_related(old_units=None, new_units=None, modified=None):
            results.append((old_units, new_units, modified))
            wait_callback[len(results)-1].callback(True)

        watcher = yield riak_states["unit_relation"].watch_related_units(
            watch_related)
        yield watcher.start()

        # wait for initial callback
        yield wait_callback[0]
        self.verify_unit_watch_result(
            results[0], old_units=[], new_units=[riak2_unit])

        # verify modifying self does not cause a notification.
        yield riak_states["unit_relation"].set_data(dict(hello="world"))
        yield self.sleep(0.1)
        self.assertEqual(len(results), 1)

        # add another unit
        riak3_unit = yield riak_states["service"].add_unit_state()
        riak3_relation = yield riak_states["service_relation"].add_unit_state(
            riak3_unit)
        yield wait_callback[1]

        self.verify_unit_watch_result(
            results[1],
            old_units=[riak2_unit], new_units=[riak2_unit, riak3_unit])

        # add remove one (no api atm, so directly to trigger notification)
        yield  self.client.delete(
            "/relations/%s/peer/%s" % (riak_states["relation"].internal_id,
                                       riak2_unit.internal_id))
        yield wait_callback[2]
        self.verify_unit_watch_result(
            results[2],
            old_units=[riak2_unit, riak3_unit], new_units=[riak3_unit])

        # modify one.
        yield riak3_relation.set_data(dict(later="eventually"))
        yield wait_callback[3]
        self.verify_unit_watch_result(results[3], modified=[riak3_unit])

    @inlineCallbacks
    def test_watch_role_container_created_concurrently(self):
        """If the relation role container that the unit is observing
        is created concurrent to the unit observatiohn starting, the
        created container is detected correctly and the observation
        works immediately.
        """
        # Add the relation, services, and related units.
        wordpress_ep = RelationEndpoint(
            "wordpress", "client-server", "", "client")
        mysql_ep = RelationEndpoint(
            "mysql", "client-server", "", "server")
        wordpress_states = yield self.add_relation_service_unit_from_endpoints(
            wordpress_ep, mysql_ep)
        mysql_states = yield self.add_opposite_service_unit(
            wordpress_states)

        container_path = "/relations/%s/client" % (
            mysql_states["relation"].internal_id)
        patch_client = self.mocker.patch(self.client)

        # via mocker play a scenario where the container doesn't exist
        # buts its created while the watcher is starting the observation.
        patch_client.get_children_and_watch(container_path)
        self.mocker.result((fail(zookeeper.NoNodeException()), Deferred()))

        patch_client.exists_and_watch(container_path)
        self.mocker.result((succeed({"version": 1}), Deferred()))

        patch_client.get_children_and_watch(container_path)
        self.mocker.passthrough()

        self.mocker.replay()

        # setup callbacks and start observing.
        wait_callback = [Deferred() for i in range(5)]
        results = []

        def watch_related(old_units=None, new_units=None, modified=None):
            results.append((old_units, new_units, modified))
            wait_callback[len(results)-1].callback(True)

        # Watch
        watcher = yield mysql_states["unit_relation"].watch_related_units(
            watch_related)
        yield watcher.start()

        # Verify
        yield wait_callback[0]
        self.verify_unit_watch_result(
            results[0], old_units=[], new_units=[wordpress_states["unit"]])

    @inlineCallbacks
    def test_watch_deleted_modify_notifications(self):
        """Verify modified notifications are only sent for existing nodes.

        Verify modifying a deleted unit relation settings doesn't cause a
        notification.
        """
        # Add the relation, services, and related units.
        wordpress_ep = RelationEndpoint(
            "wordpress", "client-server", "", "client")
        mysql_ep = RelationEndpoint(
            "mysql", "client-server", "", "server")
        wordpress_states = yield self.add_relation_service_unit_from_endpoints(
            wordpress_ep, mysql_ep)
        mysql_states = yield self.add_opposite_service_unit(
            wordpress_states)

        # setup callbacks and start observing.
        wait_callback = [Deferred() for i in range(5)]
        results = []

        def watch_related(old_units=None, new_units=None, modified=None):
            results.append((old_units, new_units, modified))
            wait_callback[len(results)-1].callback(True)

        # Start watching
        watcher = yield mysql_states["unit_relation"].watch_related_units(
            watch_related)
        yield watcher.start()

        yield wait_callback[0]
        self.verify_unit_watch_result(
            results[0], old_units=[], new_units=[wordpress_states["unit"]])

        # Delete the presence path
        presence_path = "/relations/%s/client/%s" % (
            wordpress_states["relation"].internal_id,
            wordpress_states["unit"].internal_id)
        yield self.client.delete(presence_path)

        # Modify the settings path
        settings_path = self.get_unit_settings_path(wordpress_states)
        yield self.client.set(settings_path, "some random string")

        # Verify only deletion callback
        yield wait_callback[1]
        self.verify_unit_watch_result(
            results[1], old_units=[wordpress_states["unit"]], new_units=[])

        # Give a moment to ensure we don't see any new callbacks
        yield self.sleep(0.1)
        self.assertEquals(len(results), 2)

    @inlineCallbacks
    def test_watch_with_settings_deleted(self):
        """If a unit relation settings are deleted, there are no callbacks.

        The agents presence node is the sole determinier of availability
        if through some unforeseen mechanism, the settings are deleted while
        the unit is being observed, the watcher will ignore the deletion.
        """
        # Add the relation, services, and related units.
        wordpress_ep = RelationEndpoint(
            "wordpress", "client-server", "", "client")
        mysql_ep = RelationEndpoint(
            "mysql", "client-server", "", "server")
        wordpress_states = yield self.add_relation_service_unit_from_endpoints(
            wordpress_ep, mysql_ep)
        mysql_states = yield self.add_opposite_service_unit(
            wordpress_states)

        # setup callbacks and start observing.
        wait_callback = [Deferred() for i in range(5)]
        results = []

        def watch_related(old_units=None, new_units=None, modified=None):
            results.append((old_units, new_units, modified))
            wait_callback[len(results)-1].callback(True)

        # Start watching
        watcher = yield mysql_states["unit_relation"].watch_related_units(
            watch_related)
        yield watcher.start()

        yield wait_callback[0]
        self.verify_unit_watch_result(
            results[0], old_units=[], new_units=[wordpress_states["unit"]])

        # Delete the settings path
        settings_path = "/relations/%s/settings/%s" % (
            wordpress_states["relation"].internal_id,
            wordpress_states["unit"].internal_id)
        yield self.client.delete(settings_path)

        # Verify no callbacks
        yield self.sleep(0.1)
        self.assertEqual(len(results), 1)

        # Recreate the settings path, we should trigger a callback
        yield self.client.create(settings_path, "abc")
        yield wait_callback[1]
        self.verify_unit_watch_result(
            results[1], modified=[wordpress_states["unit"]])

        # And modify it.
        yield self.client.set(settings_path, "123")
        yield wait_callback[2]
        self.verify_unit_watch_result(
            results[2], modified=[wordpress_states["unit"]])

    @inlineCallbacks
    def test_watch_start_stop_start_with_existing_service(self):
        """Unit relation watching can be stopped, and restarted.

        Upon restarting a watch, deltas since the watching was stopped
        are only notified regarding membership changes. Any settings
        changes to individual nodes are not captured. This capability
        mostly exists to enable agents to stop watching relations
        no longer assigned to their service in a single api call, and
        without additional callbacks.
        """
        # Add the relation, services, and related units.
        wordpress_ep = RelationEndpoint(
            "wordpress", "client-server", "", "client")
        mysql_ep = RelationEndpoint(
            "mysql", "client-server", "", "server")
        wordpress_states = yield self.add_relation_service_unit_from_endpoints(
            wordpress_ep, mysql_ep)
        mysql_states = yield self.add_opposite_service_unit(
            wordpress_states)

        # setup callbacks and start observing.
        wait_callback = [Deferred() for i in range(5)]
        results = []

        def watch_related(old_units=None, new_units=None, modified=None):
            results.append((old_units, new_units, modified))
            wait_callback[len(results)-1].callback(True)

        # Start watching
        watcher = yield mysql_states["unit_relation"].watch_related_units(
            watch_related)
        yield watcher.start()

        yield wait_callback[0]
        self.verify_unit_watch_result(
            results[0], old_units=[], new_units=[wordpress_states["unit"]])

        # Stop watching
        watcher.stop()

        # Add a new unit
        wordpress2_states = yield self.add_related_service_unit(
            wordpress_states)

        # Modify a unit
        yield wordpress_states["unit_relation"].set_data(dict(hello="world"))

        # Verify no callbacks
        yield self.sleep(0.1)
        self.assertEqual(len(results), 1)

        # Start watching
        yield watcher.start()

        # Verify we see the addition.
        yield wait_callback[1]
        self.verify_unit_watch_result(
            results[1],
            old_units=[wordpress_states["unit"]],
            new_units=[wordpress_states["unit"], wordpress2_states["unit"]])

    @inlineCallbacks
    def test_watch_start_stop_start_with_new_service(self):
        """Unit relation watching can be stopped, and restarted.

        Upon restarting a watch, deltas since the watching was stopped
        are only notified regarding membership changes. Any settings
        changes to individual nodes are not captured. This capability
        mostly exists to enable agents to stop watching relations
        no longer assigned to their service in a single api call, and
        without additional callbacks.
        """
        # Add the relation, services, and related units.
        wordpress_ep = RelationEndpoint(
            "wordpress", "client-server", "", "client")
        mysql_ep = RelationEndpoint(
            "mysql", "client-server", "", "server")
        mysql_states = yield self.add_relation_service_unit_from_endpoints(
            mysql_ep, wordpress_ep)

        # setup callbacks and start observing.
        wait_callback = [Deferred() for i in range(5)]
        results = []

        def watch_related(old_units=None, new_units=None, modified=None):
            results.append((old_units, new_units, modified))
            wait_callback[len(results)-1].callback(True)

        # Start watching
        watcher = yield mysql_states["unit_relation"].watch_related_units(
            watch_related)
        yield watcher.start()

        # Stop watching
        watcher.stop()

        # Add the new service and a unit
        wordpress_states = yield self.add_opposite_service_unit(
            mysql_states)

        # Add another unit
        wordpress2_states = yield self.add_related_service_unit(
            wordpress_states)

        # Modify a unit
        yield wordpress_states["unit_relation"].set_data(dict(hello="world"))

        # Verify no callbacks
        yield self.sleep(0.1)
        self.assertEqual(len(results), 0)

        # Start watching
        yield watcher.start()

        # Wait a moment for callback
        yield wait_callback[0]

        self.assertEqual(len(results), 1)
        self.verify_unit_watch_result(
            results[0],
            old_units=[],
            new_units=[wordpress_states["unit"], wordpress2_states["unit"]])

    @inlineCallbacks
    def test_watch_user_callback_invocation_delays_node_watch(self):
        """
        We defer on user callbacks, this effects an invariant where
        we won't receive additional notifications for the same node while
        processing an user callback for the node. We will receive the
        first modification
        """
        output = self.capture_logging("unit.relation.watch", logging.DEBUG)

        # Add the relation, services, and related units.
        riak_states = yield self.add_relation_service_unit(
            "riak", "riak", "kvstore", "peer")

        wait_callback = [Deferred() for i in range(10)]
        finish_callback = [Deferred() for i in range(10)]

        results = []

        def watch_related(old_units=None, new_units=None, modified=None):
            results.append((old_units, new_units, modified))
            wait_callback[len(results)-1].callback(True)
            return finish_callback[len(results)-1]

        watcher = yield riak_states["unit_relation"].watch_related_units(
            watch_related)
        yield watcher.start()

        # Create a new unit and add it to the relation.
        riak_unit2 = yield riak_states["service"].add_unit_state()
        riak_unit2_rel = yield riak_states["service_relation"].add_unit_state(
            riak_unit2)

        # Wait for it
        yield wait_callback[0]
        self.verify_unit_watch_result(
            results[0], old_units=[], new_units=[riak_unit2])

        # we need to allow the child callback to finish before we see
        # changes for the nodes.
        finish_callback[0].callback(True)

        # Ok modify the unit relation data
        yield riak_unit2_rel.set_data(dict(hello="world"))

        # And modify it again
        yield riak_unit2_rel.set_data(dict(hello="world 2"))

        # And modify it again and again
        yield riak_unit2_rel.set_data(dict(hello="world 3"))

        # We have 1 callback in progress, and only 1 pending notification.
        yield wait_callback[1]

        # Verify we only saw one change.
        self.assertEqual(len(results), 2)
        self.verify_unit_watch_result(results[1], modified=[riak_unit2])

        # Allow the node change user callback to finish
        finish_callback[1].callback(True)

        # The pending call should execute the callback
        yield wait_callback[2]
        self.assertEqual(len(results), 3)
        self.verify_unit_watch_result(results[1], modified=[riak_unit2])

        # Finish the callback and verify no other invocations.
        finish_callback[2].callback(True)
        yield self.sleep(0.1)
        self.assertEqual(len(results), 3)

        # Modify the node again, we should see this change, immediately.
        yield riak_unit2_rel.set_data(dict(hello="goodbye"))
        yield wait_callback[3]
        finish_callback[3].callback(True)

        self.verify_unit_watch_result(results[2], modified=[riak_unit2])
        self.assertEqual(len(results), 4)

        node_path = "/relations/relation-0000000000/settings/unit-0000000001"
        expected_output = (
            "relation watcher start",
            "relation membership change",
            "relation watcher settings change %s" % (
                "<ClientEvent changed at '%s' state: connected>" % node_path),
            "relation watcher settings change %s" % (
                "<ClientEvent changed at '%s' state: connected>" % node_path),
            "relation watcher settings change %s" % (
                "<ClientEvent changed at '%s' state: connected>\n" % node_path)
            )
        self.assertEqual(output.getvalue(), "\n".join(expected_output))

    @inlineCallbacks
    def test_watch_user_callback_invocation_delays_child_watch(self):
        """We defer on user callbacks to ensure that we don't trigger
        a callback on the same node twice in parallel. In the case of the
        container, this means we'll be processing at most one membership
        notification at a time.
        """
        # Add the relation, services, and related units.
        riak_states = yield self.add_relation_service_unit(
            "riak", "riak", "kvstore", "peer")

        wait_callback = [Deferred() for i in range(10)]
        finish_callback = [Deferred() for i in range(10)]

        results = []

        def watch_related(old_units=None, new_units=None, modified=None):
            results.append((old_units, new_units, modified))
            wait_callback[len(results)-1].callback(True)
            return finish_callback[len(results)-1]

        watcher = yield riak_states["unit_relation"].watch_related_units(
            watch_related)
        yield watcher.start()

        # Create a new unit and add it to the relation.
        riak_unit2 = yield riak_states["service"].add_unit_state()
        yield riak_states["service_relation"].add_unit_state(
            riak_unit2)

        # Wait for it
        yield wait_callback[0]
        self.verify_unit_watch_result(
            results[0], old_units=[], new_units=[riak_unit2])

        # Now add more units, we won't see it immediately, since the callback
        # is still executing, but we will have container change pending, but
        # only one pending for both the changes.
        riak_unit3 = yield riak_states["service"].add_unit_state()
        yield riak_states["service_relation"].add_unit_state(
            riak_unit3)

        riak_unit4 = yield riak_states["service"].add_unit_state()
        yield riak_states["service_relation"].add_unit_state(
            riak_unit4)

        # Now release the container callback, and verify the callback
        # for both the new nodes.
        finish_callback[0].callback(True)
        yield wait_callback[1]
        self.verify_unit_watch_result(
            results[1],
            old_units=[riak_unit2],
            new_units=[riak_unit2, riak_unit3, riak_unit4])

    @inlineCallbacks
    def test_watch_concurrent_callback_execution(self):
        """Unit relating watching invokes callbacks concurrently.

        IFF they are not synchronous and not on the same node.
        """
        #Setup parallel callbacks bookeeping and implementation
        wait_callback = dict([(i, Deferred()) for i in range(5)])
        results = {}
        success_deferrals = {
            1: Deferred(),
            3: Deferred()}

        @inlineCallbacks
        def watch_related(
            old_units=None, new_units=None, modified=None, callback_id=None):
            success_result = success_deferrals.get(callback_id)
            if success_result is not None:
                yield success_result
            results[callback_id] = (old_units, new_units, modified)
            wait_callback[callback_id].callback(True)

        class ParallelWatcherCallback(object):

            def __init__(self):
                self.callback_id_sequence = 0

            def __call__(self, *args, **kw):
                callback = functools.partial(
                    watch_related, callback_id=self.callback_id_sequence)
                self.callback_id_sequence += 1
                return callback(*args, **kw)

        watcher_callback = ParallelWatcherCallback()

        # Add the relation, services, and related units.
        wordpress_ep = RelationEndpoint(
            "wordpress", "client-server", "", "client")
        mysql_ep = RelationEndpoint(
            "mysql", "client-server", "", "server")
        wordpress_states = yield self.add_relation_service_unit_from_endpoints(
            wordpress_ep, mysql_ep)
        mysql_states = yield self.add_opposite_service_unit(
            wordpress_states)

        # Start watching, and give a moment to establish
        watcher = yield mysql_states["unit_relation"].watch_related_units(
            watcher_callback)
        yield watcher.start()

        # Modify a unit
        yield wordpress_states["unit_relation"].set_data(dict(hello="world"))

        # Add a unit.
        wordpress2_states = yield self.add_related_service_unit(
            wordpress_states)

        # Delete a unit
        presence_path = "/relations/%s/client/%s" % (
            wordpress_states["relation"].internal_id,
            wordpress_states["unit"].internal_id)
        yield self.client.delete(presence_path)

        # Verify the parallel execution, give a moment for callbacks to finish.
        yield self.sleep(0.1)
        self.assertEqual(len(results), 2)

        self.verify_unit_watch_result(
            results[0], old_units=[], new_units=[wordpress_states["unit"]])
        self.verify_unit_watch_result(
            results[2],
            old_units=[wordpress_states["unit"]],
            new_units=[wordpress_states["unit"], wordpress2_states["unit"]])

        for k, v in success_deferrals.items():
            v.callback(True)

        # Verify all callback results, give a moment for them to finish.
        yield wait_callback[3]
        self.assertEquals(len(results), 4)

        self.verify_unit_watch_result(
            results[1], modified=[wordpress_states["unit"]])
        self.verify_unit_watch_result(
            results[3],
            old_units=[wordpress_states["unit"], wordpress2_states["unit"]],
            new_units=[wordpress2_states["unit"]])

    @inlineCallbacks
    def test_watch_unknown_relation_role_error(self):
        """
        Attempting to watch a unit within an unknown relation role
        raises an error.
        """
        wordpress_states = yield self.add_relation_service_unit(
            "client-server", "wordpress", "", "zebra")

        def not_called(*kw):
            self.fail("Should not be called")

        yield self.failUnlessFailure(
            wordpress_states["unit_relation"].watch_related_units(not_called),
            UnknownRelationRole)
