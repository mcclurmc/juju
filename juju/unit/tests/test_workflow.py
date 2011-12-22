import logging
import yaml
import csv
import os

from twisted.internet.defer import inlineCallbacks, returnValue

from juju.unit.tests.test_lifecycle import LifecycleTestBase
from juju.unit.lifecycle import UnitLifecycle, UnitRelationLifecycle

from juju.unit.workflow import (
    UnitWorkflowState, RelationWorkflowState, WorkflowStateClient,
    is_unit_running, is_relation_running)


class WorkflowTestBase(LifecycleTestBase):

    @inlineCallbacks
    def assertState(self, workflow, state):
        workflow_state = yield workflow.get_state()
        self.assertEqual(workflow_state, state)

    @inlineCallbacks
    def read_persistent_state(self, unit=None, history_id=None, workflow=None):
        unit = unit or self.states["unit"]
        history_id = history_id or unit.unit_name
        data, stat = yield self.client.get("/units/%s" % unit.internal_id)
        workflow = workflow or self.workflow

        state = open(workflow.state_file_path).read()
        history = open(workflow.state_history_path)
        zk_state = yaml.load(data)["workflow_state"]
        returnValue((yaml.load(state),
                    [yaml.load(r[0]) for r in csv.reader(history)],
                     yaml.load(zk_state[history_id])))


class UnitWorkflowTest(WorkflowTestBase):

    @inlineCallbacks
    def setUp(self):
        yield super(UnitWorkflowTest, self).setUp()
        yield self.setup_default_test_relation()
        self.lifecycle = UnitLifecycle(
            self.client, self.states["unit"], self.states["service"],
            self.unit_directory, self.executor)

        self.juju_directory = self.makeDir()
        self.state_directory = self.makeDir(
            path=os.path.join(self.juju_directory, "state"))

        self.workflow = UnitWorkflowState(
            self.client, self.states["unit"], self.lifecycle,
            self.state_directory)

    @inlineCallbacks
    def test_install(self):
        file_path = self.makeFile()
        self.write_hook(
            "install", "#!/bin/bash\necho installed >> %s\n" % file_path)

        result = yield self.workflow.fire_transition("install")

        self.assertTrue(result)
        current_state = yield self.workflow.get_state()
        self.assertEqual(current_state, "installed")

        f_state, history, zk_state = yield self.read_persistent_state()

        self.assertEqual(f_state, zk_state)
        self.assertEqual(f_state,
                         {"state": "installed", "state_variables": {}})
        self.assertEqual(history,
                         [{"state": "installed", "state_variables": {}}])

    @inlineCallbacks
    def test_install_with_error_and_retry(self):
        """If the install hook fails, the workflow is transition to the
        install_error state. If the install is retried, a success
        transition will take us to the started state.
        """
        self.write_hook("install", "#!/bin/bash\nexit 1")
        result = yield self.workflow.fire_transition("install")
        self.assertFalse(result)
        current_state = yield self.workflow.get_state()
        yield self.assertEqual(current_state, "install_error")
        result = yield self.workflow.fire_transition("retry_install")
        yield self.assertState(self.workflow, "started")

    @inlineCallbacks
    def test_install_error_with_retry_hook(self):
        """If the install hook fails, the workflow is transition to the
        install_error state.
        """
        self.write_hook("install", "#!/bin/bash\nexit 1")
        result = yield self.workflow.fire_transition("install")
        self.assertFalse(result)
        current_state = yield self.workflow.get_state()
        yield self.assertEqual(current_state, "install_error")

        result = yield self.workflow.fire_transition("retry_install_hook")
        yield self.assertState(self.workflow, "install_error")

        self.write_hook("install", "#!/bin/bash\necho hello\n")
        hook_deferred = self.wait_on_hook("install")
        result = yield self.workflow.fire_transition_alias("retry_hook")
        yield hook_deferred
        yield self.assertState(self.workflow, "started")

    @inlineCallbacks
    def test_start(self):
        file_path = self.makeFile()
        self.write_hook(
            "install", "#!/bin/bash\necho installed >> %s\n" % file_path)
        self.write_hook(
            "start", "#!/bin/bash\necho start >> %s\n" % file_path)
        self.write_hook(
            "stop", "#!/bin/bash\necho stop >> %s\n" % file_path)

        result = yield self.workflow.fire_transition("install")
        self.assertTrue(result)

        result = yield self.workflow.fire_transition("start")
        self.assertTrue(result)

        current_state = yield self.workflow.get_state()
        self.assertEqual(current_state, "started")

        f_state, history, zk_state = yield self.read_persistent_state()

        self.assertEqual(f_state, zk_state)
        self.assertEqual(f_state,
                         {"state": "started", "state_variables": {}})
        self.assertEqual(history,
                         [{"state": "installed", "state_variables": {}},
                          {"state": "started", "state_variables": {}}])

    @inlineCallbacks
    def test_start_with_error(self):
        """Executing the start transition with a hook error, results in the
        workflow going to the start_error state. The start can be retried.
        """
        self.write_hook("install", "#!/bin/bash\necho hello\n")
        result = yield self.workflow.fire_transition("install")
        self.assertTrue(result)
        self.write_hook("start", "#!/bin/bash\nexit 1")
        result = yield self.workflow.fire_transition("start")
        self.assertFalse(result)
        current_state = yield self.workflow.get_state()
        self.assertEqual(current_state, "start_error")

        result = yield self.workflow.fire_transition("retry_start")
        yield self.assertState(self.workflow, "started")

    @inlineCallbacks
    def test_start_error_with_retry_hook(self):
        """Executing the start transition with a hook error, results in the
        workflow going to the start_error state. The start can be retried.
        """
        self.write_hook("install", "#!/bin/bash\necho hello\n")
        result = yield self.workflow.fire_transition("install")
        self.assertTrue(result)
        self.write_hook("start", "#!/bin/bash\nexit 1")
        result = yield self.workflow.fire_transition("start")
        self.assertFalse(result)
        current_state = yield self.workflow.get_state()
        self.assertEqual(current_state, "start_error")

        hook_deferred = self.wait_on_hook("start")
        result = yield self.workflow.fire_transition("retry_start_hook")
        yield hook_deferred
        yield self.assertState(self.workflow, "start_error")

        self.write_hook("start", "#!/bin/bash\nexit 0")
        hook_deferred = self.wait_on_hook("start")
        result = yield self.workflow.fire_transition_alias("retry_hook")
        yield hook_deferred
        yield self.assertState(self.workflow, "started")

    @inlineCallbacks
    def test_is_unit_running(self):
        running, state = yield is_unit_running(
            self.client, self.states["unit"])
        self.assertIdentical(running, False)
        self.assertIdentical(state, None)
        yield self.workflow.fire_transition("install")
        yield self.workflow.fire_transition("start")
        running, state = yield is_unit_running(
            self.client, self.states["unit"])
        self.assertIdentical(running, True)
        self.assertEqual(state, "started")

    @inlineCallbacks
    def test_configure(self):
        """Configuring a unit results in the config-changed hook
        being run.
        """
        yield self.workflow.fire_transition("install")
        result = yield self.workflow.fire_transition("start")
        self.assertTrue(result)
        self.assertState(self.workflow, "started")

        hook_deferred = self.wait_on_hook("config-changed")
        file_path = self.makeFile()
        self.write_hook("config-changed",
                        "#!/bin/bash\necho hello >> %s" % file_path)
        result = yield self.workflow.fire_transition("reconfigure")
        self.assertTrue(result)
        yield hook_deferred
        yield self.assertState(self.workflow, "started")
        self.assertEqual(open(file_path).read().strip(), "hello")

    @inlineCallbacks
    def test_configure_error_and_retry(self):
        """An error while configuring, transitions the unit and
        stops the lifecycle."""

        yield self.workflow.fire_transition("install")
        result = yield self.workflow.fire_transition("start")
        self.assertTrue(result)
        self.assertState(self.workflow, "started")

        # Verify transition to error state
        hook_deferred = self.wait_on_hook("config-changed")
        self.write_hook("config-changed", "#!/bin/bash\nexit 1")
        result = yield self.workflow.fire_transition("reconfigure")
        yield hook_deferred
        self.assertFalse(result)
        yield self.assertState(self.workflow, "configure_error")

        # Verify recovery from error state
        result = yield self.workflow.fire_transition_alias("retry")
        self.assertTrue(result)
        yield self.assertState(self.workflow, "started")

    @inlineCallbacks
    def test_configure_error_and_retry_hook(self):
        """An error while configuring, transitions the unit and
        stops the lifecycle."""
        #self.capture_output()
        yield self.workflow.fire_transition("install")
        result = yield self.workflow.fire_transition("start")
        self.assertTrue(result)
        self.assertState(self.workflow, "started")

        # Verify transition to error state
        hook_deferred = self.wait_on_hook("config-changed")
        self.write_hook("config-changed", "#!/bin/bash\nexit 1")
        result = yield self.workflow.fire_transition("reconfigure")
        yield hook_deferred
        self.assertFalse(result)
        yield self.assertState(self.workflow, "configure_error")

        # Verify retry hook with hook error stays in error state
        hook_deferred = self.wait_on_hook("config-changed")
        result = yield self.workflow.fire_transition("retry_configure_hook")

        self.assertFalse(result)
        yield hook_deferred
        yield self.assertState(self.workflow, "configure_error")

        hook_deferred = self.wait_on_hook("config-changed")
        self.write_hook("config-changed", "#!/bin/bash\nexit 0")
        result = yield self.workflow.fire_transition_alias("retry_hook")
        yield hook_deferred
        yield self.assertState(self.workflow, "started")

    @inlineCallbacks
    def test_upgrade(self):
        """Upgrading a workflow results in the upgrade hook being
        executed.
        """
        self.makeFile()
        yield self.workflow.fire_transition("install")
        yield self.workflow.fire_transition("start")
        current_state = yield self.workflow.get_state()
        self.assertEqual(current_state, "started")
        file_path = self.makeFile()
        self.write_hook("upgrade-charm",
                        ("#!/bin/bash\n"
                         "echo upgraded >> %s") % file_path)
        self.executor.stop()
        yield self.workflow.fire_transition("upgrade_charm")
        current_state = yield self.workflow.get_state()
        self.assertEqual(current_state, "started")

    @inlineCallbacks
    def test_upgrade_without_stopping_hooks_errors(self):
        """Attempting to execute an upgrade without stopping the
        executor is an error.
        """
        yield self.workflow.fire_transition("install")
        yield self.workflow.fire_transition("start")
        current_state = yield self.workflow.get_state()
        self.assertEqual(current_state, "started")
        yield self.assertFailure(
            self.workflow.fire_transition("upgrade_charm"),
            AssertionError)

    @inlineCallbacks
    def test_upgrade_error_retry(self):
        """A hook error during an upgrade transitions to
        upgrade_error.
        """
        self.write_hook("upgrade-charm", "#!/bin/bash\nexit 1")
        yield self.workflow.fire_transition("install")
        yield self.workflow.fire_transition("start")
        current_state = yield self.workflow.get_state()
        self.assertEqual(current_state, "started")
        self.executor.stop()
        yield self.workflow.fire_transition("upgrade_charm")

        current_state = yield self.workflow.get_state()
        self.assertEqual(current_state, "charm_upgrade_error")
        file_path = self.makeFile()
        self.write_hook("upgrade-charm",
                        ("#!/bin/bash\n"
                         "echo upgraded >> %s") % file_path)

        # The upgrade error hook should ensure that the executor is stoppped.
        self.assertFalse(self.executor.running)
        yield self.workflow.fire_transition("retry_upgrade_charm")
        current_state = yield self.workflow.get_state()
        self.assertEqual(current_state, "started")

    @inlineCallbacks
    def test_upgrade_error_retry_hook(self):
        """A hook error during an upgrade transitions to
        upgrade_error, and can be re-tried with hook execution.
        """
        yield self.workflow.fire_transition("install")
        yield self.workflow.fire_transition("start")
        current_state = yield self.workflow.get_state()
        self.assertEqual(current_state, "started")

        # Agent prepares this.
        self.executor.stop()

        self.write_hook("upgrade-charm", "#!/bin/bash\nexit 1")
        hook_deferred = self.wait_on_hook("upgrade-charm")
        yield self.workflow.fire_transition("upgrade_charm")
        yield hook_deferred
        current_state = yield self.workflow.get_state()
        self.assertEqual(current_state, "charm_upgrade_error")

        hook_deferred = self.wait_on_hook("upgrade-charm")
        self.write_hook("upgrade-charm", "#!/bin/bash\nexit 0")
        # The upgrade error hook should ensure that the executor is stoppped.
        self.assertFalse(self.executor.running)
        yield self.workflow.fire_transition_alias("retry_hook")
        yield hook_deferred
        current_state = yield self.workflow.get_state()
        self.assertEqual(current_state, "started")
        self.assertTrue(self.executor.running)

    @inlineCallbacks
    def test_stop(self):
        """Executing the stop transition, results in the workflow going
        to the down state.
        """
        file_path = self.makeFile()
        self.write_hook(
            "install", "#!/bin/bash\necho installed >> %s\n" % file_path)
        self.write_hook(
            "start", "#!/bin/bash\necho start >> %s\n" % file_path)
        self.write_hook(
            "stop", "#!/bin/bash\necho stop >> %s\n" % file_path)
        result = yield self.workflow.fire_transition("install")
        result = yield self.workflow.fire_transition("start")
        result = yield self.workflow.fire_transition("stop")
        self.assertTrue(result)
        current_state = yield self.workflow.get_state()
        self.assertEqual(current_state, "stopped")
        f_state, history, zk_state = yield self.read_persistent_state()
        self.assertEqual(f_state, zk_state)
        self.assertEqual(f_state,
                         {"state": "stopped", "state_variables": {}})

        workflow_client = WorkflowStateClient(self.client, self.states["unit"])
        value = yield workflow_client.get_state()
        self.assertEqual(value, "stopped")

        self.assertEqual(history,
                         [{"state": "installed", "state_variables": {}},
                          {"state": "started", "state_variables": {}},
                          {"state": "stopped", "state_variables": {}}])

    @inlineCallbacks
    def test_stop_with_error(self):
        self.write_hook("install", "#!/bin/bash\necho hello\n")
        self.write_hook("start", "#!/bin/bash\necho hello\n")
        result = yield self.workflow.fire_transition("install")
        self.assertTrue(result)
        result = yield self.workflow.fire_transition("start")
        self.assertTrue(result)

        self.write_hook("stop", "#!/bin/bash\nexit 1")
        result = yield self.workflow.fire_transition("stop")
        self.assertFalse(result)

        yield self.assertState(self.workflow, "stop_error")
        self.write_hook("stop", "#!/bin/bash\necho hello\n")
        result = yield self.workflow.fire_transition("retry_stop")

        yield self.assertState(self.workflow, "stopped")

    @inlineCallbacks
    def test_stop_error_with_retry_hook(self):
        self.write_hook("install", "#!/bin/bash\necho hello\n")
        self.write_hook("start", "#!/bin/bash\necho hello\n")
        result = yield self.workflow.fire_transition("install")
        self.assertTrue(result)
        result = yield self.workflow.fire_transition("start")
        self.assertTrue(result)

        self.write_hook("stop", "#!/bin/bash\nexit 1")
        result = yield self.workflow.fire_transition("stop")
        self.assertFalse(result)
        yield self.assertState(self.workflow, "stop_error")

        result = yield self.workflow.fire_transition_alias("retry_hook")
        yield self.assertState(self.workflow, "stop_error")

        self.write_hook("stop", "#!/bin/bash\nexit 0")
        result = yield self.workflow.fire_transition_alias("retry_hook")
        yield self.assertState(self.workflow, "stopped")

    @inlineCallbacks
    def test_client_with_no_state(self):
        workflow_client = WorkflowStateClient(self.client, self.states["unit"])
        state = yield workflow_client.get_state()
        self.assertEqual(state, None)

    @inlineCallbacks
    def test_client_with_state(self):
        yield self.workflow.fire_transition("install")
        workflow_client = WorkflowStateClient(self.client, self.states["unit"])
        self.assertEqual(
            (yield workflow_client.get_state()),
            "installed")

    @inlineCallbacks
    def test_client_readonly(self):
        yield self.workflow.fire_transition("install")
        workflow_client = WorkflowStateClient(
            self.client, self.states["unit"])

        self.assertEqual(
            (yield workflow_client.get_state()),
            "installed")
        yield self.assertFailure(
            workflow_client.set_state("started"),
            NotImplementedError)
        self.assertEqual(
            (yield workflow_client.get_state()),
            "installed")


class UnitRelationWorkflowTest(WorkflowTestBase):

    @inlineCallbacks
    def setUp(self):
        yield super(UnitRelationWorkflowTest, self).setUp()
        yield self.setup_default_test_relation()
        self.relation_name = self.states["service_relation"].relation_name
        self.juju_directory = self.makeDir()
        self.log_stream = self.capture_logging(
            "unit.relation.lifecycle", logging.DEBUG)

        self.lifecycle = UnitRelationLifecycle(
            self.client,
            self.states["unit"].unit_name,
            self.states["unit_relation"],
            self.relation_name,
            self.unit_directory,
            self.executor)

        self.state_directory = self.makeDir(
            path=os.path.join(self.juju_directory, "state"))

        self.workflow = RelationWorkflowState(
            self.client, self.states["unit_relation"], self.lifecycle,
            self.state_directory)

    @inlineCallbacks
    def test_is_relation_running(self):
        """The unit relation's workflow state can be categorized as a
        boolean.
        """
        running, state = yield is_relation_running(
            self.client, self.states["unit_relation"])
        self.assertIdentical(running, False)
        self.assertIdentical(state, None)
        yield self.workflow.fire_transition("start")
        running, state = yield is_relation_running(
            self.client, self.states["unit_relation"])
        self.assertIdentical(running, True)
        self.assertEqual(state, "up")
        yield self.workflow.fire_transition("stop")
        running, state = yield is_relation_running(
            self.client, self.states["unit_relation"])
        self.assertIdentical(running, False)
        self.assertEqual(state, "down")

    @inlineCallbacks
    def test_up_down_cycle(self):
        """The workflow can be transition from up to down, and back.
        """
        self.write_hook("%s-relation-changed" % self.relation_name,
                        "#!/bin/bash\nexit 0\n")

        yield self.workflow.fire_transition("start")
        yield self.assertState(self.workflow, "up")

        hook_executed = self.wait_on_hook("app-relation-changed")

        # Add a new unit, and this will be scheduled by the time
        # we finish stopping.
        yield self.add_opposite_service_unit(self.states)
        yield self.workflow.fire_transition("stop")
        yield self.assertState(self.workflow, "down")
        self.assertFalse(hook_executed.called)

        # Currently if we restart, we will only see the previously
        # queued event, as the last watch active when a lifecycle is
        # stopped, may already be in flight and will be scheduled, and
        # will be executed when the lifecycle is started. However any
        # events that may have occured after the lifecycle is stopped
        # are currently ignored and un-notified.
        yield self.workflow.fire_transition("restart")
        yield self.assertState(self.workflow, "up")
        yield hook_executed

        f_state, history, zk_state = yield self.read_persistent_state(
            history_id=self.workflow.zk_state_id)

        self.assertEqual(f_state, zk_state)
        self.assertEqual(f_state,
                         {"state": "up", "state_variables": {}})

        self.assertEqual(history,
                         [{"state": "up", "state_variables": {}},
                          {"state": "down", "state_variables": {}},
                          {"state": "up", "state_variables": {}}])

    @inlineCallbacks
    def test_change_hook_with_error(self):
        """An error while processing a change hook, results
        in the workflow transitioning to the down state.
        """
        self.capture_logging("unit.relation.lifecycle", logging.DEBUG)
        self.write_hook("%s-relation-joined" % self.relation_name,
                        "#!/bin/bash\nexit 0\n")
        self.write_hook("%s-relation-changed" % self.relation_name,
                        "#!/bin/bash\nexit 1\n")

        current_state = yield self.workflow.get_state()
        self.assertEqual(current_state, None)
        yield self.workflow.fire_transition("start")
        yield self.assertState(self.workflow, "up")
        current_state = yield self.workflow.get_state()

        # Add a new unit, and wait for the broken hook to result in
        # the transition to the down state.
        yield self.add_opposite_service_unit(self.states)
        yield self.wait_on_state(self.workflow, "error")

        f_state, history, zk_state = yield self.read_persistent_state(
            history_id=self.workflow.zk_state_id)

        self.assertEqual(f_state, zk_state)
        error = "Error processing '%s': exit code 1." % (
            os.path.join(self.unit_directory,
                         "charm", "hooks", "app-relation-changed"))

        self.assertEqual(f_state,
                         {"state": "error",
                          "state_variables": {
                              "change_type": "joined",
                              "error_message": error}})

    @inlineCallbacks
    def test_depart(self):
        """When the workflow is transition to the down state, a relation
        broken hook is executed, and the unit stops responding to relation
        changes.
        """
        self.write_hook("%s-relation-joined" % self.relation_name,
                        "#!/bin/bash\necho hello\n")
        self.write_hook("%s-relation-changed" % self.relation_name,
                        "#!/bin/bash\necho hello\n")
        self.write_hook("%s-relation-broken" % self.relation_name,
                        "#!/bin/bash\necho hello\n")

        results = []

        def collect_executions(*args):
            results.append(args)

        yield self.workflow.fire_transition("start")
        yield self.assertState(self.workflow, "up")

        wait_on_hook = self.wait_on_hook("app-relation-changed")
        states = yield self.add_opposite_service_unit(self.states)
        yield wait_on_hook

        wait_on_hook = self.wait_on_hook("app-relation-broken")
        wait_on_state = self.wait_on_state(self.workflow, "departed")
        yield self.workflow.fire_transition("depart")
        yield wait_on_hook
        yield wait_on_state

        # verify further changes to the related unit, don't result in
        # hook executions.
        self.executor.set_observer(collect_executions)
        yield states["unit_relation"].set_data(dict(a=1))
        # Sleep to give errors a chance.
        yield self.sleep(0.1)
        self.assertFalse(results)

    def test_lifecycle_attribute(self):
        """The workflow lifecycle is accessible from the workflow."""
        self.assertIdentical(self.workflow.lifecycle, self.lifecycle)

    @inlineCallbacks
    def test_client_read_none(self):
        workflow = WorkflowStateClient(
            self.client, self.states["unit_relation"])
        self.assertEqual(None, (yield workflow.get_state()))

    @inlineCallbacks
    def test_client_read_state(self):
        """The relation workflow client can read the state of a unit
        relation."""
        yield self.workflow.fire_transition("start")
        yield self.assertState(self.workflow, "up")

        self.write_hook("%s-relation-changed" % self.relation_name,
                        "#!/bin/bash\necho hello\n")
        wait_on_hook = self.wait_on_hook("app-relation-changed")
        yield self.add_opposite_service_unit(self.states)
        yield wait_on_hook

        workflow = WorkflowStateClient(
            self.client, self.states["unit_relation"])
        self.assertEqual("up", (yield workflow.get_state()))

    @inlineCallbacks
    def test_client_read_only(self):
        workflow_client = WorkflowStateClient(
            self.client, self.states["unit_relation"])
        yield self.assertFailure(
            workflow_client.set_state("up"),
            NotImplementedError)

    @inlineCallbacks
    def test_depart_hook_error(self):
        """A depart hook error, still results in a transition to the
        departed state with a state variable noting the error."""

        self.write_hook("%s-relation-changed" % self.relation_name,
                        "#!/bin/bash\necho hello\n")
        self.write_hook("%s-relation-broken" % self.relation_name,
                        "#!/bin/bash\nexit 1\n")

        error_output = self.capture_logging("unit.relation.workflow")

        results = []

        def collect_executions(*args):
            results.append(args)

        yield self.workflow.fire_transition("start")
        yield self.assertState(self.workflow, "up")

        wait_on_hook = self.wait_on_hook("app-relation-changed")
        states = yield self.add_opposite_service_unit(self.states)
        yield wait_on_hook

        wait_on_hook = self.wait_on_hook("app-relation-broken")
        wait_on_state = self.wait_on_state(self.workflow, "departed")
        yield self.workflow.fire_transition("depart")
        yield wait_on_hook
        yield wait_on_state

        # verify further changes to the related unit, don't result in
        # hook executions.
        self.executor.set_observer(collect_executions)
        yield states["unit_relation"].set_data(dict(a=1))
        # Sleep to give errors a chance.
        yield self.sleep(0.1)
        self.assertFalse(results)

        # Verify final state and log output.
        msg = "Depart hook error, ignoring: "
        error_msg = "Error processing "
        error_msg += repr(os.path.join(
            self.unit_directory, "charm", "hooks",
            "app-relation-broken"))
        error_msg += ": exit code 1."

        self.assertEqual(
            error_output.getvalue(), (msg + error_msg + "\n"))
        current_state = yield self.workflow.get_state()
        self.assertEqual(current_state, "departed")

        f_state, history, zk_state = yield self.read_persistent_state(
            history_id=self.workflow.zk_state_id)

        self.assertEqual(f_state, zk_state)
        self.assertEqual(f_state,
                         {"state": "departed",
                          "state_variables": {
                              "change_type": "depart",
                              "error_message": error_msg}})

    def test_depart_down(self):
        """When the workflow is transition to the down state, a relation
        broken hook is executed, and the unit stops responding to relation
        changes.
        """
        self.write_hook("%s-relation-changed" % self.relation_name,
                        "#!/bin/bash\necho hello\n")
        self.write_hook("%s-relation-broken" % self.relation_name,
                        "#!/bin/bash\necho hello\n")

        results = []

        def collect_executions(*args):
            results.append(args)

        yield self.workflow.fire_transition("start")
        yield self.assertState(self.workflow, "up")

        yield self.workflow.fire_transition("stop")
        yield self.assertState(self.workflow, "down")

        states = yield self.add_opposite_service_unit(self.states)

        wait_on_hook = self.wait_on_hook("app-relation-broken")
        wait_on_state = self.wait_on_state(self.workflow, "departed")

        yield self.workflow.fire_transition("depart")
        yield wait_on_hook
        yield wait_on_state

        # Verify further changes to the related unit, don't result in
        # hook executions.
        self.executor.set_observer(collect_executions)
        yield states["unit_relation"].set_data(dict(a=1))
        # Sleep to give errors a chance.
        yield self.sleep(0.1)
        self.assertFalse(results)
