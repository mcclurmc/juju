from textwrap import dedent

from juju.lib.testing import TestCase

from juju.state.endpoint import RelationEndpoint
from juju.state.errors import (
    JujuError, StateError, StateChanged, CharmStateNotFound,
    ServiceStateNotFound, ServiceUnitStateNotFound,
    MachineStateNotFound,  MachineStateInUse, NoUnusedMachines,
    ServiceUnitStateMachineAlreadyAssigned, ServiceStateNameInUse,
    BadServiceStateName, EnvironmentStateNotFound,
    RelationAlreadyExists, RelationStateNotFound, UnitRelationStateNotFound,
    UnitRelationStateAlreadyAssigned, UnknownRelationRole,
    BadDescriptor, DuplicateEndpoints, IncompatibleEndpoints,
    NoMatchingEndpoints, AmbiguousRelation,
    ServiceUnitStateMachineNotAssigned, ServiceUnitDebugAlreadyEnabled,
    ServiceUnitResolvedAlreadyEnabled,
    ServiceUnitRelationResolvedAlreadyEnabled, PrincipalNotFound)


class StateErrorsTest(TestCase):

    def assertIsStateError(self, error):
        self.assertTrue(isinstance(error, StateError))
        self.assertTrue(isinstance(error, JujuError))

    def test_state_changed(self):
        error = StateChanged()
        self.assertIsStateError(error)
        self.assertEquals(str(error),
                          "State changed while operation was in progress")

    def test_principal_not_found(self):
        error = PrincipalNotFound("joe")
        self.assertIsStateError(error)
        self.assertEquals(str(error),
                          "Principal 'joe' not found")

    def test_charm_not_found(self):
        error = CharmStateNotFound("namespace:name-123")
        self.assertIsStateError(error)
        self.assertEquals(error.charm_id, "namespace:name-123")
        self.assertEquals(str(error),
                          "Charm 'namespace:name-123' was not found")

    def test_service_not_found(self):
        error = ServiceStateNotFound("wordpress")
        self.assertIsStateError(error)
        self.assertEquals(error.service_name, "wordpress")
        self.assertEquals(str(error), "Service 'wordpress' was not found")

    def test_service_unit_not_found(self):
        error = ServiceUnitStateNotFound("wordpress/0")
        self.assertIsStateError(error)
        self.assertEquals(error.unit_name, "wordpress/0")
        self.assertEquals(str(error),
                          "Service unit 'wordpress/0' was not found")

    def test_machine_not_found(self):
        error = MachineStateNotFound(0)
        self.assertIsStateError(error)
        self.assertEquals(error.machine_id, 0)
        self.assertEquals(str(error), "Machine 0 was not found")

    def test_machine_in_use(self):
        error = MachineStateInUse(0)
        self.assertIsStateError(error)
        self.assertEquals(error.machine_id, 0)
        self.assertEquals(
            str(error), "Resources are currently assigned to machine 0")

    def test_no_unused_machines(self):
        error = NoUnusedMachines()
        self.assertIsStateError(error)
        self.assertEquals(
            str(error), "No unused machines are available for assignment")

    def test_machine_already_assigned(self):
        error = ServiceUnitStateMachineAlreadyAssigned("wordpress/0")
        self.assertIsStateError(error)
        self.assertEquals(error.unit_name, "wordpress/0")
        self.assertEquals(str(error),
                          "Service unit 'wordpress/0' is already assigned "
                          "to a machine")

    def test_unit_machine_not_assigned(self):
        error = ServiceUnitStateMachineNotAssigned("wordpress/0")
        self.assertIsStateError(error)
        self.assertEquals(error.unit_name, "wordpress/0")
        self.assertEquals(str(error),
                          "Service unit 'wordpress/0' is not assigned "
                          "to a machine")

    def test_unit_already_in_debug_mode(self):
        error = ServiceUnitDebugAlreadyEnabled("wordpress/0")
        self.assertIsStateError(error)
        self.assertEquals(error.unit_name, "wordpress/0")
        self.assertEquals(
            str(error),
            "Service unit 'wordpress/0' is already in debug mode.")

    def test_unit_already_in_resolved_mode(self):
        error = ServiceUnitResolvedAlreadyEnabled("wordpress/0")
        self.assertIsStateError(error)
        self.assertEquals(error.unit_name, "wordpress/0")
        self.assertEquals(
            str(error),
            "Service unit 'wordpress/0' is already marked as resolved.")

    def test_unit_already_in_relation_resolved_mode(self):
        error = ServiceUnitRelationResolvedAlreadyEnabled("wordpress/0")
        self.assertIsStateError(error)
        self.assertEquals(error.unit_name, "wordpress/0")
        self.assertEquals(
            str(error),
            "Service unit %r already has relations marked as resolved." % (
                "wordpress/0"))

    def test_service_name_in_use(self):
        error = ServiceStateNameInUse("wordpress")
        self.assertIsStateError(error)
        self.assertEquals(error.service_name, "wordpress")
        self.assertEquals(str(error),
                          "Service name 'wordpress' is already in use")

    def test_bad_service_name(self):
        error = BadServiceStateName("wordpress", "mysql")
        self.assertIsStateError(error)
        self.assertEquals(error.expected_name, "wordpress")
        self.assertEquals(error.obtained_name, "mysql")
        self.assertEquals(str(error),
                          "Expected service name 'wordpress' but got 'mysql'")

    def test_environment_not_found(self):
        error = EnvironmentStateNotFound()
        self.assertIsStateError(error)
        self.assertEquals(str(error),
                          "Environment state was not found")

    def test_relation_already_exists(self):
        error = RelationAlreadyExists(
            RelationEndpoint("wordpress", "mysql", "mysql", "client"),
            RelationEndpoint("mysql", "mysql", "db", "server"))
        self.assertIsStateError(error)
        self.assertTrue("wordpress" in str(error))
        self.assertTrue("mysql" in str(error))

    def test_relation_state_not_found(self):
        error = RelationStateNotFound()
        self.assertIsStateError(error)
        self.assertEqual(str(error), "Relation not found")

    def test_unit_relation_state_not_found(self):
        error = UnitRelationStateNotFound(
            "rel-1", "rel-client", "mysql/0")
        self.assertIsStateError(error)
        msg = "The relation 'rel-client' has no unit state for 'mysql/0'"
        self.assertEquals(str(error), msg)

    def test_unit_relation_state_exists(self):
        error = UnitRelationStateAlreadyAssigned(
            "rel-id", "rel-client", "mysql/0")
        self.assertIsStateError(error)
        msg = "The relation 'rel-client' already contains a unit for 'mysql/0'"
        self.assertEquals(str(error), msg)

    def test_unknown_relation_role(self):
        error = UnknownRelationRole("rel-id", "server2", "service-name")
        self.assertIsStateError(error)
        msg = "Unknown relation role 'server2' for service 'service-name'"
        self.assertEquals(str(error), msg)

    def test_bad_descriptor(self):
        error = BadDescriptor("a:b:c")
        self.assertTrue(isinstance(error, JujuError))
        msg = "Bad descriptor: 'a:b:c'"
        self.assertEquals(str(error), msg)

    def test_duplicate_endpoints(self):
        riak_ep = RelationEndpoint("riak", "riak", "ring", "peer")
        error = DuplicateEndpoints(riak_ep, riak_ep)
        self.assertIsStateError(error)
        self.assertTrue("riak" in str(error))

    def test_incompatible_endpoints(self):
        error = IncompatibleEndpoints(
            RelationEndpoint("mysql", "mysql", "db", "server"),
            RelationEndpoint("riak", "riak", "ring", "peer"))
        self.assertIsStateError(error)
        self.assertTrue("mysql" in str(error))
        self.assertTrue("riak" in str(error))

    def test_no_matching_endpoints(self):
        error = NoMatchingEndpoints()
        self.assertIsStateError(error)
        self.assertEqual("No matching endpoints", str(error))

    def test_ambiguous_relation(self):
        def endpoints(*pairs):
            return [(
                RelationEndpoint(*pair[0].split()),
                RelationEndpoint(*pair[1].split()))
                for pair in pairs]

        error = AmbiguousRelation(
            ("myblog", "mydb"), endpoints(
            ("myblog mysql db client", "mydb mysql db-admin server"),
            ("myblog mysql db client", "mydb mysql db server")))
        self.assertIsStateError(error)
        self.assertEquals(
            str(error),
            dedent("""\
                Ambiguous relation 'myblog mydb'; could refer to:
                  'myblog:db mydb:db' (mysql client / mysql server)
                  'myblog:db mydb:db-admin' (mysql client / mysql server)"""))
