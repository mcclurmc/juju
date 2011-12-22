import logging
import os
import sys
import StringIO
import stat

import yaml
import zookeeper

from twisted.internet.defer import inlineCallbacks, Deferred, fail, returnValue

from juju.unit.lifecycle import (
    UnitLifecycle, UnitRelationLifecycle, RelationInvoker)

from juju.unit.workflow import RelationWorkflowState

from juju.hooks.invoker import Invoker
from juju.hooks.executor import HookExecutor

from juju.errors import CharmInvocationError, CharmError

from juju.state.endpoint import RelationEndpoint
from juju.state.relation import ClientServerUnitWatcher
from juju.state.service import NO_HOOKS
from juju.state.tests.test_relation import RelationTestBase
from juju.state.hook import RelationChange


from juju.lib.testing import TestCase
from juju.lib.mocker import MATCH


class LifecycleTestBase(RelationTestBase):

    juju_directory = None

    @inlineCallbacks
    def setUp(self):
        yield super(LifecycleTestBase, self).setUp()

        if self.juju_directory is None:
            self.juju_directory = self.makeDir()

        self.hook_log = self.capture_logging("hook.output",
                                             level=logging.DEBUG)
        self.agent_log = self.capture_logging("unit-agent",
                                              level=logging.DEBUG)
        self.executor = HookExecutor()
        self.executor.start()
        self.change_environment(
            PATH=os.environ["PATH"],
            JUJU_UNIT_NAME="service-unit/0")

    @inlineCallbacks
    def setup_default_test_relation(self):
        mysql_ep = RelationEndpoint(
            "mysql", "client-server", "app", "server")
        wordpress_ep = RelationEndpoint(
            "wordpress", "client-server", "db", "client")
        self.states = yield self.add_relation_service_unit_from_endpoints(
            mysql_ep, wordpress_ep)
        self.unit_directory = os.path.join(self.juju_directory,
            "units",
            self.states["unit"].unit_name.replace("/", "-"))
        os.makedirs(os.path.join(self.unit_directory, "charm", "hooks"))
        os.makedirs(os.path.join(self.juju_directory, "state"))

    def write_hook(self, name, text, no_exec=False):
        hook_path = os.path.join(self.unit_directory, "charm", "hooks", name)
        hook_file = open(hook_path, "w")
        hook_file.write(text.strip())
        hook_file.flush()
        hook_file.close()
        if not no_exec:
            os.chmod(hook_path, stat.S_IRWXU)
        return hook_path

    def wait_on_hook(self, name=None, count=None, sequence=(), debug=False,
                     executor=None):
        """Wait on the given named hook to be executed.

        @param: name: if specified only one hook name can be waited on
        at a given time.

        @param: count: Multiples of the same name can be captured by specifying
        the count parameter.

        @param: sequence: A list of hook names executed in sequence to
        be waited on

        @param: debug: This parameter enables debug stdout loogging.

        @param: executor: A HookExecutor instance to use instead of the default
        """
        d = Deferred()
        results = []
        assert name is not None or sequence, "Hook match must be specified"

        def observer(hook_path):
            hook_name = os.path.basename(hook_path)
            results.append(hook_name)
            if debug:
                print "-> exec hook", hook_name
            if d.called:
                return
            if results == sequence:
                d.callback(True)
            if hook_name == name and count is None:
                d.callback(True)
            if hook_name == name and results.count(hook_name) == count:
                d.callback(True)

        executor = executor or self.executor
        executor.set_observer(observer)
        return d

    def wait_on_state(self, workflow, state, debug=False):
        state_changed = Deferred()

        def observer(workflow_state, state_variables):
            if debug:
                print " workflow state", state, workflow
            if workflow_state == state:
                state_changed.callback(True)

        workflow.set_observer(observer)
        return state_changed

    def capture_output(self, stdout=True):
        """Convience method to capture log output.

        Useful tool for observing interaction between components.
        """
        if stdout:
            output = sys.stdout
        else:
            output = StringIO.StringIO()
        for log_name, indent in (
            ("statemachine", 0),
            ("hook.executor", 2),
            ("hook.scheduler", 1),
            ("unit.lifecycle", 1),
            ("unit.relation.watch", 1),
            ("unit.relation.lifecycle", 1)):
            formatter = logging.Formatter(
                (" " * indent) + "%(name)s: %(message)s")
            self.capture_logging(
                log_name, level=logging.DEBUG,
                log_file=output, formatter=formatter)
        print
        return output


class LifecycleResolvedTest(LifecycleTestBase):

    @inlineCallbacks
    def setUp(self):
        yield super(LifecycleResolvedTest, self).setUp()
        yield self.setup_default_test_relation()
        self.lifecycle = UnitLifecycle(
            self.client, self.states["unit"], self.states["service"],
            self.unit_directory, self.executor)

    def get_unit_relation_workflow(self, states):
        state_dir = os.path.join(self.juju_directory, "state")
        lifecycle = UnitRelationLifecycle(
            self.client,
            states["unit_relation"],
            states["service_relation"].relation_name,
            self.unit_directory,
            self.executor)

        workflow = RelationWorkflowState(
            self.client,
            states["unit_relation"],
            lifecycle,
            state_dir)

        return (workflow, lifecycle)

    @inlineCallbacks
    def wb_test_start_with_relation_errors(self):
        """
        White box testing to ensure that an error when starting the
        lifecycle is propogated appropriately, and that we collect
        all results before returning.
        """
        mock_service = self.mocker.patch(self.lifecycle._service)
        mock_service.watch_relation_states(MATCH(lambda x: callable(x)))
        self.mocker.result(fail(SyntaxError()))

        mock_unit = self.mocker.patch(self.lifecycle._unit)
        mock_unit.watch_relation_resolved(MATCH(lambda x: callable(x)))
        results = []
        wait = Deferred()

        @inlineCallbacks
        def complete(*args):
            yield wait
            results.append(True)
            returnValue(True)

        self.mocker.call(complete)
        self.mocker.replay()

        # Start the unit, assert a failure, and capture the deferred
        wait_failure = self.assertFailure(self.lifecycle.start(), SyntaxError)

        # Verify we have no results for the second callback or the start call
        self.assertFalse(results)
        self.assertFalse(wait_failure.called)

        # Let the second callback complete
        wait.callback(True)

        # Wait for the start error to bubble up.
        yield wait_failure

        # Verify the second deferred was waited on.
        self.assertTrue(results)

    @inlineCallbacks
    def test_resolved_relation_watch_unit_lifecycle_not_running(self):
        """If the unit is not running then no relation resolving is performed.
        However the resolution value remains the same.
        """
        # Start the unit.
        yield self.lifecycle.start()

        # Simulate relation down on an individual unit relation
        workflow = self.lifecycle.get_relation_workflow(
            self.states["unit_relation"].internal_relation_id)
        self.assertEqual("up", (yield workflow.get_state()))

        yield workflow.transition_state("down")
        resolved = self.wait_on_state(workflow, "up")

        # Stop the unit lifecycle
        yield self.lifecycle.stop()

        # Set the relation to resolved
        yield self.states["unit"].set_relation_resolved(
            {self.states["unit_relation"].internal_relation_id: NO_HOOKS})

        # Give a moment for the watch to fire erroneously
        yield self.sleep(0.2)

        # Ensure we didn't attempt a transition.
        self.assertFalse(resolved.called)
        self.assertEqual(
            {self.states["unit_relation"].internal_relation_id: NO_HOOKS},
            (yield self.states["unit"].get_relation_resolved()))

        # If the unit is restarted start, we currently have the
        # behavior that the unit relation workflow will automatically
        # be transitioned back to running, as part of the normal state
        # transition. Sigh.. we should have a separate error
        # state for relation hooks then down with state variable usage.
        # The current end behavior though seems like the best outcome, ie.
        # automatically restart relations.

    @inlineCallbacks
    def test_resolved_relation_watch_relation_up(self):
        """If a relation marked as to be resolved is already running,
        then no work is performed.
        """
        # Start the unit.
        yield self.lifecycle.start()

        # get a hold of the unit relation and verify state
        workflow = self.lifecycle.get_relation_workflow(
            self.states["unit_relation"].internal_relation_id)
        self.assertEqual("up", (yield workflow.get_state()))

        # Set the relation to resolved
        yield self.states["unit"].set_relation_resolved(
            {self.states["unit_relation"].internal_relation_id: NO_HOOKS})

        # Give a moment for the watch to fire, invoke callback, and reset.
        yield self.sleep(0.1)

        # Ensure we're still up and the relation resolved setting has been
        # cleared.
        self.assertEqual(
            None, (yield self.states["unit"].get_relation_resolved()))
        self.assertEqual("up", (yield workflow.get_state()))

    @inlineCallbacks
    def test_resolved_relation_watch_from_error(self):
        """Unit lifecycle's will process a unit relation resolved
        setting, and transition a down relation back to a running
        state.
        """
        log_output = self.capture_logging(
            "unit.lifecycle", level=logging.DEBUG)

        # Start the unit.
        yield self.lifecycle.start()

        # Simulate an error condition
        workflow = self.lifecycle.get_relation_workflow(
            self.states["unit_relation"].internal_relation_id)
        self.assertEqual("up", (yield workflow.get_state()))
        yield workflow.fire_transition("error")

        resolved = self.wait_on_state(workflow, "up")

        # Set the relation to resolved
        yield self.states["unit"].set_relation_resolved(
            {self.states["unit_relation"].internal_relation_id: NO_HOOKS})

        # Wait for the relation to come back up
        value = yield self.states["unit"].get_relation_resolved()

        yield resolved

        # Verify state
        value = yield workflow.get_state()
        self.assertEqual(value, "up")

        self.assertIn(
            "processing relation resolved changed", log_output.getvalue())

    @inlineCallbacks
    def test_resolved_relation_watch(self):
        """Unit lifecycle's will process a unit relation resolved
        setting, and transition a down relation back to a running
        state.
        """
        log_output = self.capture_logging(
            "unit.lifecycle", level=logging.DEBUG)

        # Start the unit.
        yield self.lifecycle.start()

        # Simulate an error condition
        workflow = self.lifecycle.get_relation_workflow(
            self.states["unit_relation"].internal_relation_id)
        self.assertEqual("up", (yield workflow.get_state()))
        yield workflow.transition_state("down")

        resolved = self.wait_on_state(workflow, "up")

        # Set the relation to resolved
        yield self.states["unit"].set_relation_resolved(
            {self.states["unit_relation"].internal_relation_id: NO_HOOKS})

        # Wait for the relation to come back up
        value = yield self.states["unit"].get_relation_resolved()

        yield resolved

        # Verify state
        value = yield workflow.get_state()
        self.assertEqual(value, "up")

        self.assertIn(
            "processing relation resolved changed", log_output.getvalue())


class UnitLifecycleTest(LifecycleTestBase):

    @inlineCallbacks
    def setUp(self):
        yield super(UnitLifecycleTest, self).setUp()
        yield self.setup_default_test_relation()
        self.lifecycle = UnitLifecycle(
            self.client, self.states["unit"], self.states["service"],
            self.unit_directory, self.executor)

    @inlineCallbacks
    def test_hook_invocation(self):
        """Verify lifecycle methods invoke corresponding charm hooks.
        """
        # install hook
        file_path = self.makeFile()
        self.write_hook(
            "install",
            '#!/bin/sh\n echo "hello world" > %s' % file_path)

        yield self.lifecycle.install()
        self.assertEqual(open(file_path).read().strip(), "hello world")

        # Start hook
        file_path = self.makeFile()
        self.write_hook(
            "start",
            '#!/bin/sh\n echo "sugarcane" > %s' % file_path)
        yield self.lifecycle.start()
        self.assertEqual(open(file_path).read().strip(), "sugarcane")

        # Stop hook
        file_path = self.makeFile()
        self.write_hook(
            "stop",
            '#!/bin/sh\n echo "siesta" > %s' % file_path)
        yield self.lifecycle.stop()
        self.assertEqual(open(file_path).read().strip(), "siesta")

        # verify the sockets are cleaned up.
        self.assertEqual(os.listdir(self.unit_directory), ["charm"])

    @inlineCallbacks
    def test_start_sans_hook(self):
        """The lifecycle start can be invoked without firing hooks."""
        self.write_hook("start", "#!/bin/sh\n exit 1")
        start_executed = self.wait_on_hook("start")
        yield self.lifecycle.start(fire_hooks=False)
        self.assertFalse(start_executed.called)

    @inlineCallbacks
    def test_stop_sans_hook(self):
        """The lifecycle stop can be invoked without firing hooks."""
        self.write_hook("stop", "#!/bin/sh\n exit 1")
        stop_executed = self.wait_on_hook("stop")
        yield self.lifecycle.start()
        yield self.lifecycle.stop(fire_hooks=False)
        self.assertFalse(stop_executed.called)

    @inlineCallbacks
    def test_install_sans_hook(self):
        """The lifecycle install can be invoked without firing hooks."""
        self.write_hook("install", "#!/bin/sh\n exit 1")
        install_executed = self.wait_on_hook("install")
        yield self.lifecycle.install(fire_hooks=False)
        self.assertFalse(install_executed.called)

    @inlineCallbacks
    def test_upgrade_sans_hook(self):
        """The lifecycle upgrade can be invoked without firing hooks."""
        self.executor.stop()
        self.write_hook("upgrade-charm", "#!/bin/sh\n exit 1")
        upgrade_executed = self.wait_on_hook("upgrade-charm")
        yield self.lifecycle.upgrade_charm(fire_hooks=False)
        self.assertFalse(upgrade_executed.called)
        self.assertTrue(self.executor.running)

    def test_hook_error(self):
        """Verify hook execution error, raises an exception."""
        self.write_hook("install", '#!/bin/sh\n exit 1')
        d = self.lifecycle.install()
        return self.failUnlessFailure(d, CharmInvocationError)

    def test_hook_not_executable(self):
        """A hook not executable, raises an exception."""
        self.write_hook("install", '#!/bin/sh\n exit 0', no_exec=True)
        return self.failUnlessFailure(
            self.lifecycle.install(), CharmError)

    def test_hook_not_formatted_correctly(self):
        """Hook execution error, raises an exception."""
        self.write_hook("install", '!/bin/sh\n exit 0')
        return self.failUnlessFailure(
            self.lifecycle.install(), CharmInvocationError)

    def write_start_and_relation_hooks(self, relation_name=None):
        """Write some minimal start, and relation-changed hooks.

        Returns the output file of the relation hook.
        """
        file_path = self.makeFile()
        if relation_name is None:
            relation_name = self.states["service_relation"].relation_name
        self.write_hook("start", ("#!/bin/bash\n" "echo hello"))
        self.write_hook("config-changed", ("#!/bin/bash\n" "echo configure"))
        self.write_hook("stop", ("#!/bin/bash\n" "echo goodbye"))
        self.write_hook(
            "%s-relation-joined" % relation_name,
            ("#!/bin/bash\n" "echo joined >> %s\n" % file_path))
        self.write_hook(
            "%s-relation-changed" % relation_name,
            ("#!/bin/bash\n" "echo changed >> %s\n" % file_path))
        self.write_hook(
            "%s-relation-departed" % relation_name,
            ("#!/bin/bash\n" "echo departed >> %s\n" % file_path))

        self.assertFalse(os.path.exists(file_path))
        return file_path

    @inlineCallbacks
    def test_upgrade_hook_invoked_on_upgrade_charm(self):
        """Invoking the upgrade_charm lifecycle method executes the
        upgrade-charm hook.
        """
        file_path = self.makeFile("")
        self.write_hook(
            "upgrade-charm",
            ("#!/bin/bash\n" "echo upgraded >> %s\n" % file_path))

        # upgrade requires the external actor that extracts the charm
        # to stop the hook executor, prior to extraction so the
        # upgrade is the first hook run.
        yield self.executor.stop()
        yield self.lifecycle.upgrade_charm()
        self.assertEqual(open(file_path).read().strip(), "upgraded")

    @inlineCallbacks
    def test_config_hook_invoked_on_configure(self):
        """Invoke the configure lifecycle method will execute the
        config-changed hook.
        """
        output = self.capture_logging("unit.lifecycle", level=logging.DEBUG)
        # configure hook requires a running unit lifecycle..
        yield self.assertFailure(self.lifecycle.configure(), AssertionError)

        # Config hook
        file_path = self.makeFile()
        self.write_hook(
            "config-changed",
            '#!/bin/sh\n echo "palladium" > %s' % file_path)

        yield self.lifecycle.start()
        yield self.lifecycle.configure()

        self.assertEqual(open(file_path).read().strip(), "palladium")
        self.assertIn("configured unit", output.getvalue())

    @inlineCallbacks
    def test_service_relation_watching(self):
        """When the unit lifecycle is started, the assigned relations
        of the service are watched, with unit relation lifecycles
        created for each.

        Relation hook invocation do not maintain global order or determinism
        across relations. They only maintain ordering and determinism within
        a relation. A shared scheduler across relations would be needed to
        maintain such behavior.
        """
        file_path = self.write_start_and_relation_hooks()
        wordpress1_states = yield self.add_opposite_service_unit(self.states)
        yield self.lifecycle.start()
        yield self.wait_on_hook("app-relation-changed")

        self.assertTrue(os.path.exists(file_path))
        self.assertEqual([x.strip() for x in open(file_path).readlines()],
                         ["joined", "changed"])

        # Queue up our wait condition, of 4 hooks firing
        hooks_complete = self.wait_on_hook(
            sequence=[
                "app-relation-joined",   # joined event fires join hook,
                "app-relation-changed",  # followed by changed hook
                "app-relation-changed",
                "app-relation-departed"])

        # add another.
        wordpress2_states = yield self.add_opposite_service_unit(
            (yield self.add_relation_service_unit_to_another_endpoint(
                self.states,
                RelationEndpoint(
                    "wordpress-2", "client-server", "db", "client"))))

        # modify one.
        wordpress1_states["unit_relation"].set_data(
            {"hello": "world"})

        # delete one.
        self.client.delete(
            "/relations/%s/client/%s" % (
                wordpress2_states["relation"].internal_id,
                wordpress2_states["unit"].internal_id))

        # verify results, waiting for hooks to complete
        yield hooks_complete
        self.assertEqual(
            set([x.strip() for x in open(file_path).readlines()]),
            set(["joined", "changed", "joined", "changed", "departed"]))

    @inlineCallbacks
    def test_removed_relation_depart(self):
        """
        If a removed relation is detected, the unit relation lifecycle is
        stopped.
        """
        file_path = self.write_start_and_relation_hooks()
        self.write_hook("app-relation-broken", "#!/bin/bash\n echo broken")

        yield self.lifecycle.start()
        wordpress_states = yield self.add_opposite_service_unit(self.states)

        # Wait for the watch and hook to fire.
        yield self.wait_on_hook("app-relation-changed")

        self.assertTrue(os.path.exists(file_path))
        self.assertEqual([x.strip() for x in open(file_path).readlines()],
                         ["joined", "changed"])

        self.assertTrue(self.lifecycle.get_relation_workflow(
            self.states["relation"].internal_id))

        # Remove the relation between mysql and wordpress
        yield self.relation_manager.remove_relation_state(
            self.states["relation"])

        # Wait till the unit relation workflow has been processed the event.
        yield self.wait_on_state(
            self.lifecycle.get_relation_workflow(
                self.states["relation"].internal_id),
            "departed")

        # Modify the unit relation settings, to generate a spurious event.
        yield wordpress_states["unit_relation"].set_data(
            {"hello": "world"})

        # Verify no notice was recieved for the modify before we where stopped.
        self.assertEqual([x.strip() for x in open(file_path).readlines()],
                         ["joined", "changed"])

        # Verify the unit relation lifecycle has been disposed of.
        self.assertRaises(KeyError,
                          self.lifecycle.get_relation_workflow,
                          self.states["relation"].internal_id)

    @inlineCallbacks
    def test_lifecycle_start_stop_starts_relations(self):
        """Starting a stopped lifecycle, restarts relation events.
        """
        wordpress1_states = yield self.add_opposite_service_unit(self.states)
        wordpress2_states = yield self.add_opposite_service_unit(
            (yield self.add_relation_service_unit_to_another_endpoint(
                self.states,
                RelationEndpoint(
                    "wordpress-2", "client-server", "db", "client"))))

        # Start and stop lifecycle
        file_path = self.write_start_and_relation_hooks()
        yield self.lifecycle.start()
        yield self.wait_on_hook("app-relation-changed")
        self.assertTrue(os.path.exists(file_path))
        yield self.lifecycle.stop()

        ########################################################
        # Add, remove relations, and modify related unit settings.

        # The following isn't enough to trigger a hook notification.
        # yield wordpress1_states["relation"].unassign_service(
        #    wordpress1_states["service"])
        #
        # The removal of the external relation, means we stop getting notifies
        # of it, but the underlying unit agents of the service are responsible
        # for removing their presence nodes within the relationship, which
        # triggers a hook invocation.
        yield self.client.delete("/relations/%s/client/%s" % (
            wordpress1_states["relation"].internal_id,
            wordpress1_states["unit"].internal_id))

        yield wordpress2_states["unit_relation"].set_data(
            {"hello": "world"})

        yield self.add_opposite_service_unit(
            (yield self.add_relation_service_unit_to_another_endpoint(
                self.states,
                RelationEndpoint(
                    "wordpress-3", "client-server", "db", "client"))))

        # Verify no hooks are executed.
        yield self.sleep(0.1)

        res = [x.strip() for x in open(file_path)]
        if ((res != ["joined", "changed", "joined", "changed"])
            and
            (res != ["joined", "joined", "changed", "changed"])):
            self.fail("Invalid join sequence %s" % res)

        # XXX - With scheduler local state recovery, we should get the modify.

        # Start and verify events.
        hooks_executed = self.wait_on_hook(
            sequence=[
                "config-changed",
                "start",
                "app-relation-departed",
                "app-relation-joined",   # joined event fires joined hook,
                "app-relation-changed"   # followed by changed hook
                ])
        yield self.lifecycle.start()
        yield hooks_executed
        res.extend(["departed", "joined", "changed"])
        self.assertEqual([x.strip() for x in open(file_path)],
                         res)

    @inlineCallbacks
    def test_lock_start_stop_watch(self):
        """The lifecycle, internally employs lock to prevent simulatenous
        execution of methods which modify internal state. This allows
        for a long running hook to be called safely, even if the other
        invocations of the lifecycle, the subsequent invocations will
        block till they can acquire the lock.
        """
        self.write_hook("start", "#!/bin/bash\necho start\n")
        self.write_hook("stop", "#!/bin/bash\necho stop\n")
        results = []
        finish_callback = [Deferred() for i in range(4)]

        # Control the speed of hook execution
        original_invoker = Invoker.__call__
        invoker = self.mocker.patch(Invoker)

        @inlineCallbacks
        def long_hook(ctx, hook_path):
            results.append(os.path.basename(hook_path))
            yield finish_callback[len(results) - 1]
            yield original_invoker(ctx, hook_path)

        for i in range(4):
            invoker(
                MATCH(lambda x: x.endswith("start") or x.endswith("stop")))
            self.mocker.call(long_hook, with_object=True)

        self.mocker.replay()

        # Hook execution sequence to match on.
        test_complete = self.wait_on_hook(sequence=["config-changed",
                                                    "start",
                                                    "stop",
                                                    "config-changed",
                                                    "start"])

        # Fire off the lifecycle methods
        execution_callbacks = [self.lifecycle.start(),
                               self.lifecycle.stop(),
                               self.lifecycle.start(),
                               self.lifecycle.stop()]

        self.assertEqual([0, 0, 0, 0],
                         [x.called for x in execution_callbacks])

        # kill the delay on the second
        finish_callback[1].callback(True)
        finish_callback[2].callback(True)

        self.assertEqual([0, 0, 0, 0],
                         [x.called for x in execution_callbacks])

        # let them pass, kill the delay on the first
        finish_callback[0].callback(True)
        yield test_complete

        self.assertEqual([False, True, True, False],
                         [x.called for x in execution_callbacks])

        # Finish the last hook
        finish_callback[3].callback(True)
        yield self.wait_on_hook("stop")

        self.assertEqual([True, True, True, True],
                         [x.called for x in execution_callbacks])


class RelationInvokerTest(TestCase):

    def test_relation_invoker_environment(self):
        """Verify the relation invoker has populated the environment as per
        charm specification of hook invocation."""
        self.change_environment(
            PATH=os.environ["PATH"],
            JUJU_UNIT_NAME="service-unit/0")
        change = RelationChange("clients", "joined", "s/2")
        unit_hook_path = self.makeDir()
        invoker = RelationInvoker(None, change, "", "", unit_hook_path, None)
        environ = invoker.get_environment()
        self.assertEqual(environ["JUJU_RELATION"], "clients")
        self.assertEqual(environ["JUJU_REMOTE_UNIT"], "s/2")
        self.assertEqual(environ["CHARM_DIR"],
                         os.path.join(unit_hook_path, "charm"))


class UnitRelationLifecycleTest(LifecycleTestBase):

    hook_template = (
            "#!/bin/bash\n"
            "echo %(change_type)s >> %(file_path)s\n"
            "echo JUJU_RELATION=$JUJU_RELATION >> %(file_path)s\n"
            "echo JUJU_REMOTE_UNIT=$JUJU_REMOTE_UNIT >> %(file_path)s")

    @inlineCallbacks
    def setUp(self):
        yield super(UnitRelationLifecycleTest, self).setUp()
        yield self.setup_default_test_relation()
        self.relation_name = self.states["service_relation"].relation_name
        self.lifecycle = UnitRelationLifecycle(
            self.client,
            self.states["unit"].unit_name,
            self.states["unit_relation"],
            self.states["service_relation"].relation_name,
            self.unit_directory,
            self.executor)
        self.log_stream = self.capture_logging("unit.relation.lifecycle",
                                               logging.DEBUG)

    @inlineCallbacks
    def test_initial_start_lifecycle_no_related_no_exec(self):
        """
        If there are no related units on startup, the relation joined hook
        is not invoked.
        """
        file_path = self.makeFile()
        self.write_hook(
            "%s-relation-changed" % self.relation_name,
            ("/bin/bash\n" "echo executed >> %s\n" % file_path))
        yield self.lifecycle.start()
        self.assertFalse(os.path.exists(file_path))

    @inlineCallbacks
    def test_stop_can_continue_watching(self):
        """
        """
        file_path = self.makeFile()
        self.write_hook(
            "%s-relation-changed" % self.relation_name,
            ("#!/bin/bash\n" "echo executed >> %s\n" % file_path))
        rel_states = yield self.add_opposite_service_unit(self.states)
        yield self.lifecycle.start()
        yield self.wait_on_hook(
            sequence=["app-relation-joined", "app-relation-changed"])
        changed_executed = self.wait_on_hook("app-relation-changed")
        yield self.lifecycle.stop(watches=False)
        rel_states["unit_relation"].set_data(yaml.dump(dict(hello="world")))
        # Sleep to give an error a chance.
        yield self.sleep(0.1)
        self.assertFalse(changed_executed.called)
        yield self.lifecycle.start(watches=False)
        yield changed_executed

    @inlineCallbacks
    def test_initial_start_lifecycle_with_related(self):
        """
        If there are related units on startup, the relation changed hook
        is invoked.
        """
        yield self.add_opposite_service_unit(self.states)

        file_path = self.makeFile()
        self.write_hook("%s-relation-joined" % self.relation_name,
                        self.hook_template % dict(change_type="joined",
                                                  file_path=file_path))
        self.write_hook("%s-relation-changed" % self.relation_name,
                        self.hook_template % dict(change_type="changed",
                                                  file_path=file_path))

        yield self.lifecycle.start()
        yield self.wait_on_hook(
            sequence=["app-relation-joined", "app-relation-changed"])
        self.assertTrue(os.path.exists(file_path))

        contents = open(file_path).read()
        self.assertEqual(contents,
                         ("joined\n"
                          "JUJU_RELATION=app\n"
                          "JUJU_REMOTE_UNIT=wordpress/0\n"
                          "changed\n"
                          "JUJU_RELATION=app\n"
                          "JUJU_REMOTE_UNIT=wordpress/0\n"))

    @inlineCallbacks
    def test_hooks_executed_during_lifecycle_start_stop_start(self):
        """If the unit relation lifecycle is stopped, hooks will no longer
        be executed."""
        file_path = self.makeFile()
        self.write_hook("%s-relation-joined" % self.relation_name,
                        self.hook_template % dict(change_type="joined",
                                                  file_path=file_path))
        self.write_hook("%s-relation-changed" % self.relation_name,
                        self.hook_template % dict(change_type="changed",
                                                  file_path=file_path))

        # starting is async
        yield self.lifecycle.start()

        # stopping is sync.
        self.lifecycle.stop()

        # Add a related unit.
        yield self.add_opposite_service_unit(self.states)

        # Give a chance for things to go bad
        yield self.sleep(0.1)

        self.assertFalse(os.path.exists(file_path))

        # Now start again
        yield self.lifecycle.start()

        # Verify we get our join event.
        yield self.wait_on_hook("app-relation-changed")

        self.assertTrue(os.path.exists(file_path))

    @inlineCallbacks
    def test_hook_error_handler(self):
        # use an error handler that completes async.
        self.write_hook("app-relation-joined", "#!/bin/bash\nexit 0\n")
        self.write_hook("app-relation-changed", "#!/bin/bash\nexit 1\n")

        results = []
        finish_callback = Deferred()

        @inlineCallbacks
        def error_handler(change, e):
            yield self.client.create(
                "/errors", str(e),
                flags=zookeeper.EPHEMERAL | zookeeper.SEQUENCE)
            results.append((change.change_type, e))
            yield self.lifecycle.stop()
            finish_callback.callback(True)

        self.lifecycle.set_hook_error_handler(error_handler)

        # Add a related unit.
        yield self.add_opposite_service_unit(self.states)

        yield self.lifecycle.start()
        yield finish_callback
        self.assertEqual(len(results), 1)
        self.assertTrue(results[0][0], "joined")
        self.assertTrue(isinstance(results[0][1], CharmInvocationError))

        hook_relative_path = "charm/hooks/app-relation-changed"
        output = (
            "started relation:app lifecycle",
            "Executing hook app-relation-joined",
            "Executing hook app-relation-changed",
            "Error in app-relation-changed hook: %s '%s/%s': exit code 1." % (
                "Error processing",
                self.unit_directory, hook_relative_path),
            "Invoked error handler for app-relation-changed hook",
            "stopped relation:app lifecycle\n")
        self.assertEqual(self.log_stream.getvalue(), "\n".join(output))

    @inlineCallbacks
    def test_depart(self):
        """If a relation is departed, the depart hook is executed.
        """
        file_path = self.makeFile()
        self.write_hook("%s-relation-joined" % self.relation_name,
                        "#!/bin/bash\n echo joined")
        self.write_hook("%s-relation-changed" % self.relation_name,
                        "#!/bin/bash\n echo hello")
        self.write_hook("%s-relation-broken" % self.relation_name,
                        self.hook_template % dict(change_type="broken",
                                                  file_path=file_path))

        yield self.lifecycle.start()
        wordpress_states = yield self.add_opposite_service_unit(self.states)
        yield self.wait_on_hook(
            sequence=["app-relation-joined", "app-relation-changed"])
        yield self.lifecycle.stop()
        yield self.relation_manager.remove_relation_state(
            wordpress_states["relation"])
        hook_complete = self.wait_on_hook("app-relation-broken")
        yield self.lifecycle.depart()
        yield hook_complete
        self.assertTrue(os.path.exists(file_path))

    @inlineCallbacks
    def test_lock_start_stop(self):
        """
        The relation lifecycle, internally uses a lock when its interacting
        with zk, and acquires the lock to protct its internal data structures.
        """

        original_method = ClientServerUnitWatcher.start
        watcher = self.mocker.patch(ClientServerUnitWatcher)

        finish_callback = Deferred()

        @inlineCallbacks
        def long_op(*args):
            yield finish_callback
            yield original_method(*args)

        watcher.start()
        self.mocker.call(long_op, with_object=True)
        self.mocker.replay()

        start_complete = self.lifecycle.start()
        stop_complete = self.lifecycle.stop()

        # Sadly this sleeping is the easiest way to verify that
        # the stop hasn't procesed prior to the start.
        yield self.sleep(0.1)
        self.assertFalse(start_complete.called)
        self.assertFalse(stop_complete.called)
        finish_callback.callback(True)

        yield start_complete
        self.assertTrue(stop_complete.called)
