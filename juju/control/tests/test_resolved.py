from twisted.internet.defer import inlineCallbacks, returnValue
from yaml import dump

from juju.control import main
from juju.control.tests.common import ControlToolTest
from juju.charm.tests.test_repository import RepositoryTestBase

from juju.state.service import RETRY_HOOKS, NO_HOOKS
from juju.state.tests.test_service import ServiceStateManagerTestBase
from juju.state.errors import ServiceStateNotFound

from juju.unit.workflow import UnitWorkflowState, RelationWorkflowState
from juju.unit.lifecycle import UnitRelationLifecycle
from juju.hooks.executor import HookExecutor


class ControlResolvedTest(
    ServiceStateManagerTestBase, ControlToolTest, RepositoryTestBase):

    @inlineCallbacks
    def setUp(self):
        yield super(ControlResolvedTest, self).setUp()
        config = {
            "environments": {"firstenv": {"type": "dummy"}}}

        self.write_config(dump(config))
        self.config.load()

        yield self.add_relation_state("wordpress", "mysql")
        yield self.add_relation_state("wordpress", "varnish")

        self.service1 = yield self.service_state_manager.get_service_state(
            "mysql")
        self.service_unit1 = yield self.service1.add_unit_state()
        self.service_unit2 = yield self.service1.add_unit_state()

        self.unit1_workflow = UnitWorkflowState(
            self.client, self.service_unit1, None, self.makeDir())
        yield self.unit1_workflow.set_state("started")

        self.environment = self.config.get_default()
        self.provider = self.environment.get_machine_provider()

        self.output = self.capture_logging()
        self.stderr = self.capture_stream("stderr")
        self.executor = HookExecutor()

    @inlineCallbacks
    def add_relation_state(self, *service_names):
        for service_name in service_names:
            try:
                yield self.service_state_manager.get_service_state(
                    service_name)
            except ServiceStateNotFound:
                yield self.add_service_from_charm(service_name)

        endpoint_pairs = yield self.service_state_manager.join_descriptors(
            *service_names)
        endpoints = endpoint_pairs[0]
        endpoints = endpoint_pairs[0]
        if endpoints[0] == endpoints[1]:
            endpoints = endpoints[0:1]
        relation_state = (yield self.relation_state_manager.add_relation_state(
            *endpoints))[0]
        returnValue(relation_state)

    @inlineCallbacks
    def get_named_service_relation(self, service_state, relation_name):
        if isinstance(service_state, str):
            service_state = yield self.service_state_manager.get_service_state(
                service_state)

        rels = yield self.relation_state_manager.get_relations_for_service(
            service_state)

        rels = [sr for sr in rels if sr.relation_name == relation_name]
        if len(rels) == 1:
            returnValue(rels[0])
        returnValue(rels)

    @inlineCallbacks
    def setup_unit_relations(self, service_relation, *units):
        """
        Given a service relation and set of unit tuples in the form
        unit_state, unit_relation_workflow_state, will add unit relations
        for these units and update their workflow state to the desired/given
        state.
        """
        for unit, state in units:
            unit_relation = yield service_relation.add_unit_state(unit)
            lifecycle = UnitRelationLifecycle(self.client,
                                              unit.unit_name, unit_relation,
                                              service_relation.relation_name,
                                              self.makeDir(), self.executor)
            workflow_state = RelationWorkflowState(
                self.client, unit_relation, lifecycle, self.makeDir())
            yield workflow_state.set_state(state)

    @inlineCallbacks
    def test_resolved(self):
        """
        'juju resolved <unit_name>' will schedule a unit for
        retrying from an error state.
        """
        # Push the unit into an error state
        yield self.unit1_workflow.set_state("start_error")
        self.setup_exit(0)
        finished = self.setup_cli_reactor()
        self.mocker.replay()

        self.assertEqual(
            (yield self.service_unit1.get_resolved()), None)

        main(["resolved", "mysql/0"])
        yield finished

        self.assertEqual(
            (yield self.service_unit1.get_resolved()), {"retry": NO_HOOKS})
        self.assertIn(
            "Marked unit 'mysql/0' as resolved",
            self.output.getvalue())

    @inlineCallbacks
    def test_resolved_retry(self):
        """
        'juju resolved --retry <unit_name>' will schedule a unit
        for retrying from an error state with a retry of hooks
        executions.
        """
        yield self.unit1_workflow.set_state("start_error")
        self.setup_exit(0)
        finished = self.setup_cli_reactor()
        self.mocker.replay()

        self.assertEqual(
            (yield self.service_unit1.get_resolved()), None)

        main(["resolved", "--retry", "mysql/0"])
        yield finished

        self.assertEqual(
            (yield self.service_unit1.get_resolved()), {"retry": RETRY_HOOKS})
        self.assertIn(
            "Marked unit 'mysql/0' as resolved",
            self.output.getvalue())

    @inlineCallbacks
    def test_relation_resolved(self):
        """
        'juju relation <unit_name> <rel_name>' will schedule
        the broken unit relations for being resolved.
        """
        service_relation = yield self.get_named_service_relation(
            self.service1, "server")

        yield self.setup_unit_relations(
            service_relation,
            (self.service_unit1, "down"),
            (self.service_unit2, "up"))

        yield self.unit1_workflow.set_state("start_error")
        self.setup_exit(0)
        finished = self.setup_cli_reactor()
        self.mocker.replay()

        self.assertEqual(
            (yield self.service_unit1.get_relation_resolved()), None)

        main(["resolved", "--retry", "mysql/0",
              service_relation.relation_name])
        yield finished

        self.assertEqual(
            (yield self.service_unit1.get_relation_resolved()),
            {service_relation.internal_relation_id: RETRY_HOOKS})
        self.assertEqual(
            (yield self.service_unit2.get_relation_resolved()),
            None)
        self.assertIn(
            "Marked unit 'mysql/0' relation 'server' as resolved",
            self.output.getvalue())

    @inlineCallbacks
    def test_resolved_relation_some_already_resolved(self):
        """
        'juju resolved <service_name> <rel_name>' will mark
        resolved all down units that are not already marked resolved.
        """

        service2 = yield self.service_state_manager.get_service_state(
            "wordpress")
        service_unit1 = yield service2.add_unit_state()

        service_relation = yield self.get_named_service_relation(
            service2, "db")
        yield self.setup_unit_relations(
            service_relation, (service_unit1, "down"))

        service_relation2 = yield self.get_named_service_relation(
            service2, "cache")
        yield self.setup_unit_relations(
            service_relation2, (service_unit1, "down"))

        yield service_unit1.set_relation_resolved(
            {service_relation.internal_relation_id: NO_HOOKS})

        self.setup_exit(0)
        finished = self.setup_cli_reactor()
        self.mocker.replay()

        main(["resolved", "--retry", "wordpress/0", "cache"])
        yield finished

        self.assertEqual(
            (yield service_unit1.get_relation_resolved()),
            {service_relation.internal_relation_id: NO_HOOKS,
             service_relation2.internal_relation_id: RETRY_HOOKS})

        self.assertIn(
            "Marked unit 'wordpress/0' relation 'cache' as resolved",
            self.output.getvalue())

    @inlineCallbacks
    def test_resolved_relation_some_already_resolved_conflict(self):
        """
        'juju resolved <service_name> <rel_name>' will mark
        resolved all down units that are not already marked resolved.
        """

        service2 = yield self.service_state_manager.get_service_state(
            "wordpress")
        service_unit1 = yield service2.add_unit_state()

        service_relation = yield self.get_named_service_relation(
            service2, "db")
        yield self.setup_unit_relations(
            service_relation, (service_unit1, "down"))

        yield service_unit1.set_relation_resolved(
            {service_relation.internal_relation_id: NO_HOOKS})

        self.setup_exit(0)
        finished = self.setup_cli_reactor()
        self.mocker.replay()

        main(["resolved", "--retry", "wordpress/0", "db"])
        yield finished

        self.assertEqual(
            (yield service_unit1.get_relation_resolved()),
            {service_relation.internal_relation_id: NO_HOOKS})

        self.assertIn(
            "Service unit 'wordpress/0' already has relations marked as resol",
            self.output.getvalue())

    @inlineCallbacks
    def test_resolved_unknown_service(self):
        """
        'juju resolved <unit_name>' will report if a service is
        invalid.
        """
        self.setup_exit(0)
        finished = self.setup_cli_reactor()
        self.mocker.replay()
        main(["resolved", "zebra/0"])
        yield finished
        self.assertIn("Service 'zebra' was not found", self.stderr.getvalue())

    @inlineCallbacks
    def test_resolved_unknown_unit(self):
        """
        'juju resolved <unit_name>' will report if a unit is
        invalid.
        """
        self.setup_exit(0)
        finished = self.setup_cli_reactor()
        self.mocker.replay()
        main(["resolved", "mysql/5"])
        yield finished
        self.assertIn(
            "Service unit 'mysql/5' was not found", self.output.getvalue())

    @inlineCallbacks
    def test_resolved_unknown_unit_relation(self):
        """
        'juju resolved <unit_name>' will report if a relation is
        invalid.
        """
        self.setup_exit(0)
        finished = self.setup_cli_reactor()
        self.mocker.replay()

        self.assertEqual(
            (yield self.service_unit1.get_resolved()), None)

        main(["resolved", "mysql/0", "magic"])
        yield finished

        self.assertIn("Relation not found", self.output.getvalue())

    @inlineCallbacks
    def test_resolved_already_running(self):
        """
        'juju resolved <unit_name>' will report if
        the unit is already running.
        """
        # Just verify we don't accidentally mark up another unit of the service
        unit2_workflow = UnitWorkflowState(
            self.client, self.service_unit2, None, self.makeDir())
        unit2_workflow.set_state("start_error")

        self.setup_exit(0)
        finished = self.setup_cli_reactor()
        self.mocker.replay()

        main(["resolved", "mysql/0"])
        yield finished

        self.assertEqual(
            (yield self.service_unit2.get_resolved()), None)
        self.assertEqual(
            (yield self.service_unit1.get_resolved()), None)

        self.assertNotIn(
            "Unit 'mysql/0 already running: started",
            self.output.getvalue())

    @inlineCallbacks
    def test_resolved_already_resolved(self):
        """
        'juju resolved <unit_name>' will report if
        the unit is already resolved.
        """
        # Mark the unit as resolved and as in an error state.
        yield self.service_unit1.set_resolved(RETRY_HOOKS)
        yield self.unit1_workflow.set_state("start_error")

        unit2_workflow = UnitWorkflowState(
            self.client, self.service_unit1, None, self.makeDir())
        unit2_workflow.set_state("start_error")

        self.assertEqual(
            (yield self.service_unit2.get_resolved()), None)

        self.setup_exit(0)
        finished = self.setup_cli_reactor()
        self.mocker.replay()

        main(["resolved", "mysql/0"])
        yield finished

        self.assertEqual(
            (yield self.service_unit1.get_resolved()),
            {"retry": RETRY_HOOKS})
        self.assertNotIn(
            "Marked unit 'mysql/0' as resolved",
            self.output.getvalue())
        self.assertIn(
            "Service unit 'mysql/0' is already marked as resolved.",
            self.stderr.getvalue(), "")

    @inlineCallbacks
    def test_resolved_relation_already_running(self):
        """
        'juju resolved <unit_name> <rel_name>' will report
        if the relation is already running.
        """
        service2 = yield self.service_state_manager.get_service_state(
            "wordpress")
        service_unit1 = yield service2.add_unit_state()

        service_relation = yield self.get_named_service_relation(
            service2, "db")
        yield self.setup_unit_relations(
            service_relation, (service_unit1, "up"))

        self.setup_exit(0)
        finished = self.setup_cli_reactor()
        self.mocker.replay()

        main(["resolved", "wordpress/0", "db"])
        yield finished

        self.assertIn("Matched relations are all running",
                      self.output.getvalue())
        self.assertEqual(
            (yield service_unit1.get_relation_resolved()),
            None)
