import yaml
import csv
import os
import logging

from zookeeper import NoNodeException
from twisted.internet.defer import inlineCallbacks, returnValue

from txzookeeper.utils import retry_change

from juju.errors import CharmInvocationError, CharmError, FileNotFound
from juju.lib.statemachine import (
    WorkflowState, Workflow, Transition, TransitionError)


UnitWorkflow = Workflow(
    # Install transitions
    Transition("install", "Install", None, "installed",
               error_transition_id="error_install"),
    Transition("error_install", "Install error", None, "install_error"),
    Transition("retry_install", "Retry install", "install_error", "installed",
               alias="retry", success_transition_id="start"),
    Transition("retry_install_hook", "Retry install with hook",
               "install_error", "installed", alias="retry_hook",
               success_transition_id="start"),

    # Start transitions
    Transition("start", "Start", "installed", "started",
               error_transition_id="error_start"),
    Transition("error_start", "Start error", "installed", "start_error"),
    Transition("retry_start", "Retry start", "start_error", "started",
               alias="retry"),
    Transition("retry_start_hook", "Retry start with hook",
              "start_error", "started",  alias="retry_hook"),

    # Stop transitions
    Transition("stop", "Stop", "started", "stopped",
               error_transition_id="error_stop"),
    Transition("error_stop", "Stop error", "started", "stop_error"),
    Transition("retry_stop", "Retry stop", "stop_error", "stopped",
               alias="retry"),
    Transition("retry_stop_hook", "Retry stop with hook",
               "stop_error", "stopped", alias="retry_hook"),

    # Restart transitions
    Transition("restart", "Restart", "stop", "start",
               error_transition_id="error_start", alias="retry"),
    Transition("restart_with_hook", "Restart with hook",
               "stop", "start", alias="retry_hook",
               error_transition_id="error_start"),

    # Upgrade transitions
    Transition(
        "upgrade_charm", "Upgrade", "started", "started",
        error_transition_id="upgrade_charm_error"),
    Transition(
        "upgrade_charm_error", "Upgrade from stop error",
        "started", "charm_upgrade_error"),
    Transition(
        "retry_upgrade_charm", "Upgrade from stop error",
        "charm_upgrade_error", "started", alias="retry"),
    Transition(
        "retry_upgrade_charm_hook", "Upgrade from stop error with hook",
        "charm_upgrade_error", "started", alias="retry_hook"),

    # Configuration Transitions
    Transition(
        "reconfigure", "Reconfigure", "started", "started",
        error_transition_id="error_configure"),
    Transition(
        "error_configure", "On configure error",
        "started", "configure_error"),
    Transition(
        "retry_error", "On retry configure error",
        "configure_error", "configure_error"),
    Transition(
        "retry_configure", "Retry configure",
        "configure_error", "started", alias="retry",
        error_transition_id="retry_error"),
    Transition(
        "retry_configure_hook", "Retry configure with hooks",
        "configure_error", "started", alias="retry_hook",
        error_transition_id="retry_error")
    )


# Unit relation error states
#
# There's been some discussion, if we should have per change type
# error states here, corresponding to the different changes that the
# relation-changed hook is invoked for. The important aspects to
# capture are both observability of error type locally and globally
# (zk), and per error type and instance recovery of the same. To
# provide for this functionality without additional states, the error
# information (change type, and error message) are captured in state
# variables which are locally and globally observable. Future
# extension of the restart transition action, will allow for
# customized recovery based on the change type state
# variable. Effectively this differs from the unit definition, in that
# it collapses three possible error states, into a behavior off
# switch. A separate state will be needed to denote departing.


# Process recovery using on disk workflow state
#
# Another interesting issue, process recovery using the on disk state,
# is complicated by consistency to the the in memory state, which
# won't be directly recoverable anymore without some state specific
# semantics to recovering from on disk state, ie a restarted unit
# agent, with a relation in an error state would require special
# semantics around loading from disk to ensure that the in-memory
# process state (watching and scheduling but not executing) matches
# the recovery transition actions (which just restart hook execution,
# but assume the watch continues).. this functionality added to better
# allow for the behavior that while down due to a hook error, the
# relation would continues to schedule pending hooks

RelationWorkflow = Workflow(
    Transition("start", "Start", None, "up"),
    Transition("stop", "Stop", "up", "down"),
    Transition("restart", "Restart", "down", "up", alias="retry"),
    Transition("error", "Relation hook error", "up", "error"),
    Transition("reset", "Recover from hook error", "error", "up"),
    Transition("depart", "Relation broken", "up", "departed"),
    Transition("down_depart", "Relation broken", "down", "departed"),
    )


@inlineCallbacks
def is_unit_running(client, unit):
    """Is the service unit in a running state.

    Returns a boolean which is true if the unit is running, and
    the unit workflow state in a two element tuple.
    """
    workflow_state = yield WorkflowStateClient(client, unit).get_state()
    if not workflow_state:
        returnValue((False, None))
    running = workflow_state == "started"
    returnValue((running, workflow_state))


@inlineCallbacks
def is_relation_running(client, relation):
    """Is the unit relation in a running state.

    Returns a boolean which is true if the relation is running, and
    the unit relation workflow state in a two element tuple.
    """
    workflow_state = yield WorkflowStateClient(client, relation).get_state()
    if not workflow_state:
        returnValue((False, None))
    running = workflow_state == "up"
    returnValue((running, workflow_state))


def zk_workflow_identity(domain_state):
    """Return workflow storage path and key for zookeeper.

    Returns back the path to the zk workflow state node,
    and this domain object's key into the workflow data.
    """
    from juju.state.service import ServiceUnitState
    from juju.state.relation import UnitRelationState

    if isinstance(domain_state, ServiceUnitState):
        return (
            "/units/%s" % domain_state.internal_id,
            domain_state.unit_name)

    elif isinstance(domain_state, UnitRelationState):
        return (
            "/units/%s" % domain_state.internal_unit_id,
            domain_state.internal_relation_id)
    else:
        raise ValueError("Unknown domain object %r" % domain_state)


def fs_workflow_paths(state_directory, domain_state):
    """Returns back the file paths where state should be stored.

    Return value is a two element tuple (state_file, history_file).
    """
    from juju.state.service import ServiceUnitState
    from juju.state.relation import UnitRelationState

    if isinstance(domain_state, ServiceUnitState):
        return (
            "%s/%s-%s" % (
                state_directory,
                domain_state.unit_name.replace("/", "-"),
                "state.txt"),
            "%s/%s-%s" % (
                state_directory,
                domain_state.unit_name.replace("/", "-"),
                "history.txt"))

    elif isinstance(domain_state, UnitRelationState):
        return (
            "%s/%s-%s-%s" % (
                state_directory,
                domain_state.internal_unit_id,
                domain_state.internal_relation_id,
                "state.txt"),
            "%s/%s-%s-%s" % (
                state_directory,
                domain_state.internal_unit_id,
                domain_state.internal_relation_id,
                "history.txt"))
    else:
        raise ValueError("Unknown domain object %r" % domain_state)


class ZookeeperWorkflowState(WorkflowState):
    """Workflow state persisted in zookeeper.
    """

    def __init__(self, client, domain_state):
        self._client = client
        self._state = domain_state
        self.zk_state_path, self.zk_state_id = zk_workflow_identity(
            domain_state)
        super(ZookeeperWorkflowState, self).__init__()

    @inlineCallbacks
    def _store(self, state_dict):
        """Store the workflow state dictionary in zookeeper."""
        state_serialized = yaml.safe_dump(state_dict)

        def update_state(content, stat):
            unit_data = yaml.load(content)
            if not unit_data:
                unit_data = {}

            persistent_workflow = unit_data.setdefault("workflow_state", {})
            persistent_workflow[self.zk_state_id] = state_serialized
            return yaml.dump(unit_data)

        yield retry_change(self._client, self.zk_state_path, update_state)
        yield super(ZookeeperWorkflowState, self)._store(
            state_dict)

    @inlineCallbacks
    def _load(self):
        """Load the workflow state dictionary from zookeeper."""
        try:
            data, stat = yield self._client.get(self.zk_state_path)
        except NoNodeException:
            returnValue({"state": None})
        unit_data = yaml.load(data)
        data = yaml.load(unit_data.get("workflow_state", {}).get(
                self.zk_state_id, ""))
        returnValue(data)


class DiskWorkflowState(ZookeeperWorkflowState):
    """Stores the workflow state and history on disk.

    Also stores state to zookeeper, but always reads state
    from disk only.
    """

    def __init__(self, client, domain_state, state_directory):
        super(DiskWorkflowState, self).__init__(
            client, domain_state)
        self.state_file_path, self.state_history_path = fs_workflow_paths(
            state_directory, domain_state)

    def _store(self, state_dict):
        """Persist the workflow state.

        Stores the state as the sole contents of the state file.
        For history, append workflow state to history file.

        Internally the history file is stored a csv, with a new
        row per entry with CSV escaping.
        """
        state_serialized = yaml.safe_dump(state_dict)
        # State File
        with open(self.state_file_path, "w") as handle:
            handle.write(state_serialized)

        # History File
        with open(self.state_history_path, "a") as handle:
            writer = csv.writer(handle)
            writer.writerow((state_serialized,))
            handle.flush()

        return super(DiskWorkflowState, self)._store(state_dict)

    def _load(self):
        """Load the on-disk workflow state.
        """
        if not os.path.exists(self.state_file_path):
            return {"state": None}
        with open(self.state_file_path, "r") as handle:
            content = handle.read()

        return yaml.load(content)


class WorkflowStateClient(ZookeeperWorkflowState):
    """A remote accessor to a unit or unit relation workflow state
    in zookeeper.

    Meant for out of process usage to examine the client's state. Currently
    read-only.

    For example to get the workflow state of a unit::

       >> from juju.unit.workflow import WorkflowStateClient
       >> state_dict = yield WorkflowStateClient(unit_state).get_state()
       >> print state_dict["state"]
       "started"

    This client can also be used with unit relations::

       >> from juju.unit.workflow import WorkflowStateClient
       >> state_dict = yield WorkflowStateClient(unit_relation).get_state()
       >> print state_dict["state"]
       "up"
    """

    def _store(self, state_dict):
        raise NotImplementedError("Read only client")


class UnitWorkflowState(DiskWorkflowState):

    _workflow = UnitWorkflow

    def __init__(self, client, unit, lifecycle, state_directory):
        super(UnitWorkflowState, self).__init__(
            client, unit, state_directory)
        self._lifecycle = lifecycle

    @inlineCallbacks
    def _invoke_lifecycle(self, method, *args, **kw):
        try:
            result = yield method(*args, **kw)
        except (FileNotFound, CharmError, CharmInvocationError), e:
            raise TransitionError(e)
        returnValue(result)

    # Install transitions
    def do_install(self):
        return self._invoke_lifecycle(self._lifecycle.install)

    def do_retry_install(self):
        return self._invoke_lifecycle(self._lifecycle.install,
                                      fire_hooks=False)

    def do_retry_install_hook(self):
        return self._invoke_lifecycle(self._lifecycle.install)

    # Start transitions
    def do_start(self):
        return self._invoke_lifecycle(self._lifecycle.start)

    def do_retry_start(self):
        return self._invoke_lifecycle(self._lifecycle.start,
                                      fire_hooks=False)

    def do_retry_start_hook(self):
        return self._invoke_lifecycle(self._lifecycle.start)

    # Stop transitions
    def do_stop(self):
        return self._invoke_lifecycle(self._lifecycle.stop)

    def do_retry_stop(self):
        return self._invoke_lifecycle(self._lifecycle.stop,
                                      fire_hooks=False)

    def do_retry_stop_hook(self):
        return self._invoke_lifecycle(self._lifecycle.stop)

    # Upgrade transititions
    def do_upgrade_charm(self):
        return self._invoke_lifecycle(self._lifecycle.upgrade_charm)

    def do_retry_upgrade_charm(self):
        return self._invoke_lifecycle(self._lifecycle.upgrade_charm,
                                      fire_hooks=False)

    def do_retry_upgrade_charm_hook(self):
        return self._invoke_lifecycle(self._lifecycle.upgrade_charm)

    # Config transitions
    def do_error_configure(self):
        return self._invoke_lifecycle(self._lifecycle.stop, fire_hooks=False)

    def do_reconfigure(self):
        return self._invoke_lifecycle(self._lifecycle.configure)

    def do_retry_error(self):
        return self._invoke_lifecycle(self._lifecycle.stop, fire_hooks=False)

    @inlineCallbacks
    def do_retry_configure(self):
        yield self._invoke_lifecycle(self._lifecycle.start, fire_hooks=False)
        yield self._invoke_lifecycle(self._lifecycle.configure,
                                     fire_hooks=False)

    @inlineCallbacks
    def do_retry_configure_hook(self):
        yield self._invoke_lifecycle(self._lifecycle.start, fire_hooks=False)
        yield self._invoke_lifecycle(self._lifecycle.configure)


class RelationWorkflowState(DiskWorkflowState):

    _workflow = RelationWorkflow

    def __init__(self, client, unit_relation, lifecycle, state_directory):
        super(RelationWorkflowState, self).__init__(
            client, unit_relation, state_directory)
        self._lifecycle = lifecycle
        # Catch any related-change hook errors
        self._lifecycle.set_hook_error_handler(self.on_hook_error)
        self._log = logging.getLogger("unit.relation.workflow")

    @property
    def lifecycle(self):
        return self._lifecycle

    @inlineCallbacks
    def on_hook_error(self, relation_change, error):
        """Handle relation-change hook errors.

        Invoked by the hook scheduler on error. The relation-change
        hooks are executed out of band, as a result of watch
        invocations. We have the relation lifecycle accept this method
        as an error handler, so we can drive workflow changes as a
        result of hook errors.

        @param: relation_change: describes the change for which the
        hook is being invoked.

        @param: error: The error from hook invocation.
        """
        yield self.fire_transition("error",
                                   change_type=relation_change.change_type,
                                   error_message=str(error))

    @inlineCallbacks
    def do_stop(self):
        """Transition the workflow to the 'down' state.

        Turns off the unit-relation lifecycle monitoring and hook execution.

        :param error_info: If called on relation hook error, contains
        error variables.
        """
        yield self._lifecycle.stop()

    @inlineCallbacks
    def do_reset(self):
        """Transition the workflow to the 'up' state from an error state.

        Turns on the unit-relation lifecycle monitoring and hook execution.
        """
        yield self._lifecycle.start(watches=False)

    @inlineCallbacks
    def do_error(self, **error_info):
        """A relation hook error, stops further execution hooks but
        continues to watch for changes.
        """
        yield self._lifecycle.stop(watches=False)

    @inlineCallbacks
    def do_restart(self):
        """Transition the workflow to the 'up' state from the down state.

        Turns on the unit-relation lifecycle monitoring and hook execution.
        """
        yield self._lifecycle.start()

    @inlineCallbacks
    def do_start(self):
        """Transition the workflow to the 'up' state.

        Turns on the unit-relation lifecycle monitoring and hook execution.
        """
        yield self._lifecycle.start()

    @inlineCallbacks
    def do_depart(self):
        """Transition a relation to the departed state, from the up state.
        """
        # Stop related unit watches and change hook execution.
        yield self._lifecycle.stop()
        result = yield self._do_depart()
        returnValue(result)

    def do_down_depart(self):
        """Transition a relation to the departed state, from the down state.
        """
        return self._do_depart()

    @inlineCallbacks
    def _do_depart(self):
        """Execute the depart hook.

        We ignore hook errors, as we won't logically process any additional
        events for the relation once it doesn't exist. However we do
        note the error in the log.
        """
        # To avoid the relation-changed hook error handler being used,
        # set the handler to None, so the exception is raised.
        self._lifecycle.set_hook_error_handler(None)

        try:
            yield self._lifecycle.depart()
        except Exception, e:
            self._log.error("Depart hook error, ignoring: %s", str(e))
            returnValue({"change_type": "depart",
                         "error_message": str(e)})
