import logging

from twisted.internet.defer import DeferredQueue, inlineCallbacks
from juju.state.hook import RelationHookContext, RelationChange

ADDED = 0
REMOVED = 1
MODIFIED = 2

CHANGE_LABELS = {
    ADDED: "joined",
    MODIFIED: "modified",
    REMOVED: "departed"}

log = logging.getLogger("hook.scheduler")


class HookScheduler(object):
    """Schedules hooks for execution in response to changes observed.

    Performs merging of redunant events, and maintains a membership
    list for hooks, guaranteeing only nodes that have previously been
    notified of joining are present in th membership.

    Internally this utilizes a change clock, which is incremented for
    every change seen. All hook operations that would result from a
    change are indexed by change clock, and the clock is placed into
    the run queue.
    """

    def __init__(self, client, executor, unit_relation, relation_name, unit_name):
        self._running = None

        # The thing that will actually run the hook for us
        self._executor = executor

        # For hook context construction.
        self._client = client
        self._unit_relation = unit_relation
        self._relation_name = relation_name
        self._members = None
        self._unit_name = unit_name

        # Track next operation by node
        self._node_queue = {}

        # Track node operations by clock tick
        self._clock_queue = {}

        # Run queue (clock)
        self._run_queue = DeferredQueue()

        # Artifical clock sequence
        self._clock_sequence = 0

    @inlineCallbacks
    def run(self):
        """Run the hook scheduler and execution."""
        assert not self._running, "Scheduler is already running"
        self._running = True
        log.debug("start")

        while self._running:
            clock = yield self._run_queue.get()

            # Check for a stop now marker value.
            if clock is None:
                break

            # Get all the units with changes in this clock tick.
            for unit_name in self._clock_queue.pop(clock):

                # Get the change for the unit.
                change_clock, change_type = self._node_queue.pop(unit_name)

                log.debug("executing hook for %s:%s",
                          unit_name, CHANGE_LABELS[change_type])

                # Execute the hook
                yield self._execute(unit_name, change_type)

    def stop(self):
        """Stop the hook execution.

        Note this does not stop the scheduling, the relation watcher
        that feeds changes to the scheduler needs to be stopped to
        achieve that effect.
        """
        log.debug("stop")
        if not self._running:
            return
        self._running = False
        # Put a marker value onto the queue to designate, stop now.
        # This is in case we're waiting on the queue, when the stop
        # occurs.
        self._run_queue.put(None)

    def notify_change(self, old_units=(), new_units=(), modified=()):
        """Receive changes regarding related units and schedule hook execution.
        """
        log.debug("relation change old:%s, new:%s, modified:%s",
                  old_units, new_units, modified)

        self._clock_sequence += 1

        # keep track if we've scheduled changes during this clock
        scheduled = 0

        # Handle membership changes

        # If we don't have a cached membership yet, use the old units
        # as a baseline.
        if self._members is None:
            self._members = list(old_units)

        added = set(new_units) - set(old_units)
        removed = set(old_units) - set(new_units)

        for unit_name in sorted(added):
            scheduled += self._queue_change(
                unit_name, ADDED, self._clock_sequence)

        for unit_name in sorted(removed):
            scheduled += self._queue_change(
                unit_name, REMOVED, self._clock_sequence)

        # Handle modified change
        for unit_name in modified:
            scheduled += self._queue_change(
                unit_name, MODIFIED, self._clock_sequence)

        if scheduled:
            self._run_queue.put(self._clock_sequence)

    def get_hook_context(self, change):
        """
        Return a hook context, corresponding to the current state of the
        system.
        """
        members = self._members or ()
        context = RelationHookContext(
            self._client, self._unit_relation, change,
            sorted(members), unit_name=self._unit_name)
        return context

    def _queue_change(self, unit_name, change_type, clock):
        """Queue up the node change for execution.
        """
        # If its a new change for the unit store it, and return.
        if not unit_name in self._node_queue:
            self._node_queue[unit_name] = (clock, change_type)
            self._clock_queue.setdefault(clock, []).append(unit_name)
            return True

        # Else merge/reduce with the previous operation.
        previous_clock, previous_change = self._node_queue[unit_name]
        change_clock, change_type = self._reduce(
            (previous_clock, previous_change),
            (self._clock_sequence, change_type))

        # If they've cancelled, remove from node and clock queues
        if change_type is None:
            del self._node_queue[unit_name]
            self._clock_queue[previous_clock].remove(unit_name)
            return False

        # Update the node queue with the merged change.
        self._node_queue[unit_name] = (change_clock, change_type)

        # If the clock has changed, remove the old entry.
        if change_clock != previous_clock:
            self._clock_queue[previous_clock].remove(unit_name)

        # If the old entry has precedence, we didn't schedule anything for
        # this clock cycle.
        if change_clock != clock:
            return False

        self._clock_queue.setdefault(clock, []).append(unit_name)
        return True

    def _reduce(self, previous, new):
        """Given two change operations for a node, reduce to one operation.

        We depend on zookeeper's total ordering behavior as we don't
        attempt to handle nonsensical operation sequences like
        removed followed by a modified, or modified followed by an
        add.
        """
        previous_clock, previous_change = previous
        new_clock, new_change = new

        if previous_change == REMOVED and new_change == ADDED:
            return (new_clock, MODIFIED)

        elif previous_change == ADDED and new_change == MODIFIED:
            return (previous_clock, previous_change)

        elif previous_change == ADDED and new_change == REMOVED:
            return (None, None)

        elif previous_change == MODIFIED and new_change == REMOVED:
            return (new_clock, new_change)

        elif previous_change == MODIFIED and new_change == MODIFIED:
            return (previous_clock, previous_change)

    def _execute(self, unit_name, change_type):
        """Execute a hook script for a change.
        """
        # Determine the current members as of the change.
        if change_type == ADDED:
            self._members.append(unit_name)
        elif change_type == REMOVED:
            self._members.remove(unit_name)

        # Assemble the change and hook execution context
        change = RelationChange(
            self._relation_name, CHANGE_LABELS[change_type], unit_name)
        context = self.get_hook_context(change)

        # Execute the change.
        return self._executor(context, change)
