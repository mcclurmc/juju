import os
import logging

from twisted.internet.defer import (
    inlineCallbacks, DeferredLock, DeferredList, returnValue)


from juju.hooks.invoker import Invoker
from juju.hooks.scheduler import HookScheduler
from juju.state.hook import RelationChange, HookContext
from juju.state.errors import StopWatcher, UnitRelationStateNotFound

from juju.unit.workflow import RelationWorkflowState


HOOK_SOCKET_FILE = ".juju.hookcli.sock"

hook_log = logging.getLogger("hook.output")


class UnitLifecycle(object):
    """Manager for a unit lifecycle.

    Primarily used by the workflow interaction, to modify unit behavior
    according to the current unit workflow state and transitions.
    """

    def __init__(self, client, unit, service, unit_path, executor):
        self._client = client
        self._unit = unit
        self._service = service
        self._executor = executor
        self._unit_path = unit_path
        self._relations = {}
        self._running = False
        self._watching_relation_memberships = False
        self._watching_relation_resolved = False
        self._run_lock = DeferredLock()
        self._log = logging.getLogger("unit.lifecycle")

    def get_relation_workflow(self, relation_id):
        """Accessor to a unit relation workflow, by relation id.

        Primarily intended for and used by unit tests. Raises
        a KeyError if the relation workflow does not exist.
        """
        return self._relations[relation_id]

    @inlineCallbacks
    def install(self, fire_hooks=True):
        """Invoke the unit's install hook.
        """
        if fire_hooks:
            yield self._execute_hook("install")

    @inlineCallbacks
    def upgrade_charm(self, fire_hooks=True):
        """Invoke the unit's upgrade-charm hook.
        """
        if fire_hooks:
            yield self._execute_hook("upgrade-charm", now=True)
        # Restart hook queued hook execution.
        self._executor.start()

    @inlineCallbacks
    def start(self, fire_hooks=True):
        """Invoke the start hook, and setup relation watching.
        """
        self._log.debug("pre-start acquire, running:%s", self._running)
        yield self._run_lock.acquire()
        self._log.debug("start running, unit lifecycle")
        watches = []

        try:
            # Verify current state
            assert not self._running, "Already started"

            # Execute the start hook
            if fire_hooks:
                yield self._execute_hook("config-changed")
                yield self._execute_hook("start")

            # If we have any existing relations in memory, start them.
            if self._relations:
                self._log.debug("starting relation lifecycles")

            for workflow in self._relations.values():
                yield workflow.transition_state("up")

            # Establish a watch on the existing relations.
            if not self._watching_relation_memberships:
                self._log.debug("starting service relation watch")
                watches.append(self._service.watch_relation_states(
                    self._on_service_relation_changes))
                self._watching_relation_memberships = True

            # Establish a watch for resolved relations
            if not self._watching_relation_resolved:
                self._log.debug("starting unit relation resolved watch")
                watches.append(self._unit.watch_relation_resolved(
                    self._on_relation_resolved_changes))
                self._watching_relation_resolved = True

            # Set current status
            self._running = True
        finally:
            self._run_lock.release()

        # Give up the run lock before waiting on initial watch invocations.
        results = yield DeferredList(watches, consumeErrors=True)

        # If there's an error reraise the first one found.
        errors = [e[1] for e in results if not e[0]]
        if errors:
            returnValue(errors[0])

        self._log.debug("started unit lifecycle")

    @inlineCallbacks
    def stop(self, fire_hooks=True):
        """Stop the unit, executes the stop hook, and stops relation watching.
        """
        self._log.debug("pre-stop acquire, running:%s", self._running)
        yield self._run_lock.acquire()
        try:
            # Verify state
            assert self._running, "Already Stopped"

            # Stop relation lifecycles
            if self._relations:
                self._log.debug("stopping relation lifecycles")

            for workflow in self._relations.values():
                yield workflow.transition_state("down")

            if fire_hooks:
                yield self._execute_hook("stop")

            # Set current status
            self._running = False
        finally:
            self._run_lock.release()
        self._log.debug("stopped unit lifecycle")

    @inlineCallbacks
    def configure(self, fire_hooks=True):
        """Inform the unit that its service config has changed.
        """
        if not fire_hooks:
            returnValue(None)
        yield self._run_lock.acquire()
        try:
            # Verify State
            assert self._running, "Needs to be running."

            # Execute hook
            yield self._execute_hook("config-changed")
        finally:
            self._run_lock.release()
        self._log.debug("configured unit")

    @inlineCallbacks
    def _on_relation_resolved_changes(self, event):
        """Callback for unit relation resolved watching.

        The callback is invoked whenever the relation resolved
        settings change.
        """
        self._log.debug("relation resolved changed")
        # Acquire the run lock, and process the changes.
        yield self._run_lock.acquire()

        try:
            # If the unit lifecycle isn't running we shouldn't process
            # any relation resolutions.
            if not self._running:
                self._log.debug("stop watch relation resolved changes")
                self._watching_relation_resolved = False
                raise StopWatcher()

            self._log.info("processing relation resolved changed")
            if self._client.connected:
                yield self._process_relation_resolved_changes()
        finally:
            yield self._run_lock.release()

    @inlineCallbacks
    def _process_relation_resolved_changes(self):
        """Invoke retry transitions on relations if their not running.
        """
        relation_resolved = yield self._unit.get_relation_resolved()
        if relation_resolved is None:
            returnValue(None)
        else:
            yield self._unit.clear_relation_resolved()

        keys = set(relation_resolved).intersection(self._relations)
        for rel_id in keys:
            relation_workflow = self._relations[rel_id]
            relation_state = yield relation_workflow.get_state()
            if relation_state == "up":
                continue
            yield relation_workflow.transition_state("up")

    @inlineCallbacks
    def _on_service_relation_changes(self, old_relations, new_relations):
        """Callback for service relation watching.

        The callback is used to manage the unit relation lifecycle in
        accordance with the current relations of the service.

        @param old_relations: Previous service relations for a service. On the
               initial execution, this value is None.
        @param new_relations: Current service relations for a service.
        """
        self._log.debug(
            "services changed old:%s new:%s", old_relations, new_relations)

        # Acquire the run lock, and process the changes.
        yield self._run_lock.acquire()
        try:
            # If the lifecycle is not running, then stop the watcher
            if not self._running:
                self._log.debug("stop service-rel watcher, discarding changes")
                self._watching_relation_memberships = False
                raise StopWatcher()

            self._log.debug("processing relations changed")
            yield self._process_service_changes(old_relations, new_relations)
        finally:
            self._run_lock.release()

    @inlineCallbacks
    def _process_service_changes(self, old_relations, new_relations):
        """Add and remove unit lifecycles per the service relations Determine.
        """
        # changes relation delta of global zk state with our memory state.
        new_relations = dict([(service_relation.internal_relation_id,
                               service_relation) for
                              service_relation in new_relations])
        added = set(new_relations.keys()) - set(self._relations.keys())
        removed = set(self._relations.keys()) - set(new_relations.keys())

        # Stop and remove, old ones.

        # Trying to directly transition this causes additional yielding
        # operations, which means that concurrent events for subsequent
        # watch firings will be executed. ie. if the relation
        # is broken, but a subsequent modify comes in for a related unit,
        # it will cause the modify to have a hook execution. To prevent
        # this we stop the lifecycle immediately before executing the
        # transition. see UnitLifecycleTest.test_removed_relation_depart
        for relation_id in removed:
            yield self._relations[relation_id].lifecycle.stop()

        for relation_id in removed:
            workflow = self._relations.pop(relation_id)
            yield workflow.transition_state("departed")

        # Process new relations.
        for relation_id in added:
            service_relation = new_relations[relation_id]
            try:
                unit_relation = yield service_relation.get_unit_state(
                    self._unit)
            except UnitRelationStateNotFound:
                # This unit has not yet been assigned a unit relation state,
                # Go ahead and add one.
                unit_relation = yield service_relation.add_unit_state(
                    self._unit)

            self._log.debug(
                "Starting new relation: %s", service_relation.relation_name)

            workflow = self._get_unit_relation_workflow(unit_relation,
                                                        service_relation)
            # Start it before storing it.
            yield workflow.fire_transition("start")
            self._relations[service_relation.internal_relation_id] = workflow

    def _get_unit_path(self):
        """Retrieve the root path of the unit.
        """
        return self._unit_path

    def _get_unit_relation_workflow(self, unit_relation, service_relation):

        lifecycle = UnitRelationLifecycle(self._client,
                                          self._unit.unit_name,
                                          unit_relation,
                                          service_relation.relation_name,
                                          self._get_unit_path(),
                                          self._executor)

        state_directory = os.path.abspath(os.path.join(
            self._unit_path, "../../state"))

        workflow = RelationWorkflowState(
            self._client, unit_relation, lifecycle, state_directory)

        return workflow

    @inlineCallbacks
    def _execute_hook(self, hook_name, now=False):
        """Execute the hook with the given name.

        For priority hooks, the hook is scheduled and then the
        executioner started, before wait on the result.
        """
        unit_path = self._get_unit_path()
        hook_path = os.path.join(unit_path, "charm", "hooks", hook_name)
        socket_path = os.path.join(unit_path, HOOK_SOCKET_FILE)

        invoker = Invoker(HookContext(self._client, self._unit.unit_name),
                          None, "constant", socket_path,
                          self._unit_path, hook_log)
        if now:
            yield self._executor.run_priority_hook(invoker, hook_path)
        else:
            yield self._executor(invoker, hook_path)


class RelationInvoker(Invoker):
    """A relation hook invoker, that populates the environment.
    """

    def get_environment_from_change(self, env, change):
        """Populate environment with relation change information."""
        env["JUJU_RELATION"] = change.relation_name
        env["JUJU_REMOTE_UNIT"] = change.unit_name
        return env


class UnitRelationLifecycle(object):
    """Unit Relation Lifcycle management.

    Provides for watching related units in a relation, and executing hooks
    in response to changes. The lifecycle is driven by the workflow.

    The Unit relation lifecycle glues together a number of components.
    It controls a watcher that recieves watch events from zookeeper,
    and it controls a hook scheduler which gets fed those events. When
    the scheduler wants to execute a hook, the executor is called with
    the hook path and the hook invoker.

    **Relation hook invocation do not maintain global order or
    determinism across relations**. They only maintain ordering and
    determinism within a relation. A shared scheduler across relations
    would be needed to maintain such behavior.
    """

    def __init__(self, client, unit_name, unit_relation, relation_name, unit_path, executor):
        self._client = client
        self._unit_path = unit_path
        self._relation_name = relation_name
        self._unit_relation = unit_relation
        self._executor = executor
        self._run_lock = DeferredLock()
        self._log = logging.getLogger("unit.relation.lifecycle")
        self._error_handler = None

        self._scheduler = HookScheduler(client,
                                        self._execute_change_hook,
                                        self._unit_relation,
                                        self._relation_name,
                                        unit_name=unit_name)
        self._watcher = None

    @inlineCallbacks
    def _execute_change_hook(self, context, change, hook_name=None):
        """Invoked by the contained HookScheduler, to execute a hook.

        We utilize the HookExecutor to execute the hook, if an
        error occurs, it will be reraised, unless an error handler
        is specified see ``set_hook_error_handler``.
        """
        socket_path = os.path.join(self._unit_path, HOOK_SOCKET_FILE)
        if hook_name is None:
            if change.change_type == "departed":
                hook_names = [
                    "%s-relation-departed" % self._relation_name]
            elif change.change_type == "joined":
                hook_names = [
                    "%s-relation-joined" % self._relation_name,
                    "%s-relation-changed" % self._relation_name]
            else:
                hook_names = ["%s-relation-changed" % self._relation_name]
        else:
            hook_names = [hook_name]

        invoker = RelationInvoker(
            context, change, "constant", socket_path, self._unit_path,
            hook_log)

        for hook_name in hook_names:
            hook_path = os.path.join(
                self._unit_path, "charm", "hooks", hook_name)
            yield self._run_lock.acquire()
            self._log.debug("Executing hook %s", hook_name)
            try:
                yield self._executor(invoker, hook_path)
            except Exception, e:
                yield self._run_lock.release()
                self._log.warn("Error in %s hook: %s", hook_name, e)

                if not self._error_handler:
                    raise
                self._log.info(
                    "Invoked error handler for %s hook", hook_name)
                # We can't hold the run lock, when we invoke the error
                # handler, or we get a deadlock if the handler
                # manipulates the lifecycle.
                yield self._error_handler(change, e)
            else:
                yield self._run_lock.release()

    def set_hook_error_handler(self, handler):
        """Set an error handler to be invoked if a hook errors.

        The handler should accept one parameter, the exception instance.
        """
        self._error_handler = handler

    @inlineCallbacks
    def start(self, watches=True):
        """Start watching related units and executing change hooks.

        @param watches: boolean parameter denoting if relation watches
               should be started.
        """
        yield self._run_lock.acquire()
        try:
            # Start the hook execution scheduler.
            self._scheduler.run()
            # Create a watcher if we don't have one yet.
            if self._watcher is None:
                self._watcher = yield self._unit_relation.watch_related_units(
                    self._scheduler.notify_change)
            # And start the watcher.
            if watches:
                yield self._watcher.start()
        finally:
            self._run_lock.release()
        self._log.debug(
            "started relation:%s lifecycle", self._relation_name)

    @inlineCallbacks
    def stop(self, watches=True):
        """Stop watching changes and stop executing relation change hooks.

        @param watches: boolean parameter denoting if relation watches
               should be stopped.
        """
        yield self._run_lock.acquire()
        try:
            if watches and self._watcher:
                self._watcher.stop()
            self._scheduler.stop()
        finally:
            yield self._run_lock.release()
        self._log.debug("stopped relation:%s lifecycle", self._relation_name)

    @inlineCallbacks
    def depart(self):
        """Inform the charm that the service has departed the relation.
        """
        self._log.debug("depart relation lifecycle")
        change = RelationChange(self._relation_name, "departed", "")
        context = self._scheduler.get_hook_context(change)
        hook_name = "%s-relation-broken" % self._relation_name
        yield self._execute_change_hook(context, change, hook_name)
