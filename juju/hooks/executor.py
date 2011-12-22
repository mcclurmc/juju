""" Hook Execution.
"""
import os
import fnmatch
import logging
import tempfile

from twisted.internet.defer import (
    inlineCallbacks, DeferredQueue, Deferred, DeferredLock, returnValue)
from twisted.internet.error import ProcessExitedAlready

DEBUG_HOOK_TEMPLATE = r"""#!/bin/bash
set -e
export JUJU_DEBUG=$(mktemp -d)
exec > $JUJU_DEBUG/debug.log >&1

# Save environment variables and export them for sourcing.
FILTER='^\(LS_COLORS\|LESSOPEN\|LESSCLOSE\|PWD\)='
env | grep -v $FILTER > $JUJU_DEBUG/env.sh
sed -i 's/^/export /' $JUJU_DEBUG/env.sh

# Create an internal script which will load the hook environment.
cat > $JUJU_DEBUG/hook.sh <<END
#!/bin/bash
. $JUJU_DEBUG/env.sh
echo \$\$ > $JUJU_DEBUG/hook.pid
exec /bin/bash
END
chmod +x $JUJU_DEBUG/hook.sh

# If the session already exists, the ssh command won the race, so just use it.
# The beauty below is a workaround for a bug in tmux (1.5 in Oneiric) or
# epoll that doesn't support /dev/null or whatever.  Without it the
# command hangs.
tmux new-session -d -s $JUJU_UNIT_NAME 2>&1 | cat > /dev/null || true
tmux new-window -t $JUJU_UNIT_NAME -n {hook_name} "$JUJU_DEBUG/hook.sh"

# If we exit for whatever reason, kill the hook shell.
exit_handler() {
    if [ -f $JUJU_DEBUG/hook.pid ]; then
        kill -9 $(cat $JUJU_DEBUG/hook.pid) || true
    fi
}
trap exit_handler EXIT

# Wait for the hook shell to start, and then wait for it to exit.
while [ ! -f $JUJU_DEBUG/hook.pid ]; do
    sleep 1
done
HOOK_PID=$(cat $JUJU_DEBUG/hook.pid)
while kill -0 "$HOOK_PID" 2> /dev/null; do
    sleep 1
done
"""


class HookExecutor(object):
    """Executes scheduled hooks.

    A typical unit agent is subscribed to multiple event streams
    across unit and relation lifecycles. All of which will attempt to
    execute hooks in response to events. In order to serialize hook
    execution and bring observability, a hook executor is utilized
    across the different components that want to execute hooks.
    """

    STOP = object()

    def __init__(self):
        self._running = False
        self._executions = DeferredQueue()
        self._observer = None
        self._log = logging.getLogger("hook.executor")
        self._run_lock = DeferredLock()

        # The currently executing hook invoker. None if no hook is executing.
        self._invoker = None
        # The currently executing hook's context. None if no hook is executing.
        self._hook_context = None
        # The current names of hooks that should be debugged.
        self._debug_hook_names = None
        # The path to the last utilized tempfile debug hook.
        self._debug_hook_file_path = None

    @property
    def running(self):
        """Returns a boolean, denoting if the executor is running."""
        return self._running

    @inlineCallbacks
    def start(self):
        """Start the hook executor.

        After the executor is started, it will continue to serially execute
        any queued hook executions.
        """
        assert self._running is False, "Already running"
        self._running = True

        self._log.debug("started")
        while self._running:

            next = yield self._executions.get()

            # The stop logic here is to allow for two different
            # scenarios. One is if the executor is currently waiting on
            # the queue, putting a stop value there will, immediately
            # wake it up and cause it to stop.

            # The other distinction is more subtle, if we invoke
            # start/stop/start on the executioner, and it was
            # currently executing a slow hook, then when the
            # executioner finishes with the hook it may now be in the
            # running state, resulting in two executioners closures
            # executing hooks. We track stops to ensure that only one
            # executioner closure is running at a time.

            if next is self.STOP:
                continue

            yield self._run_lock.acquire()

            if not self._running:
                self._run_lock.release()
                continue

            yield self._run_one(*next)
            self._run_lock.release()

    @inlineCallbacks
    def _run_one(self, invoker, path, exec_deferred):
        """Run a hook.
        """
        hook_path = self.get_hook_path(path)

        if not os.path.exists(hook_path):
            self._log.info(
                "Hook does not exist, skipping %s", hook_path)
            exec_deferred.callback(False)

            if self._observer:
                self._observer(path)

            returnValue(None)

        self._log.debug("Running hook: %s", path)

        # Store for context for callbacks, execution is serialized.
        self._invoker = invoker
        self._hook_context = invoker.get_context()

        try:
            yield invoker(hook_path)
        except Exception, e:
            self._invoker = self._hook_context = None
            self._log.debug("Hook error: %s %s", path, e)
            exec_deferred.errback(e)
        else:
            self._invoker = self._hook_context = None
            self._log.debug("Hook complete: %s", path)
            exec_deferred.callback(True)

        if self._observer:
            self._observer(path)

    @inlineCallbacks
    def stop(self):
        """Stop hook executions.

        Returns a deferred that fires when the executor has stopped.
        """
        assert self._running, "Already stopped"
        yield self._run_lock.acquire()
        self._running = False
        self._executions.put(self.STOP)
        self._run_lock.release()
        self._log.debug("stopped")

    @inlineCallbacks
    def run_priority_hook(self, invoker, hook_path):
        """Execute a hook while the executor is stopped.

        Executes a hook immediately, ignoring the existing queued
        hook executions, requires the hook executor to be stopped.
        """
        yield self._run_lock.acquire()
        try:
            assert not self._running, "Executor must not be running"
            exec_deferred = Deferred()
            yield self._run_one(invoker, hook_path, exec_deferred)
        finally:
            self._run_lock.release()
        yield exec_deferred

    def set_observer(self, observer):
        """Set a callback hook execution observer.

        The callback receives a single parameter, the path to the hook,
        and is invoked after the hook has been executed.
        """
        self._observer = observer

    def get_hook_context(self, client_id):
        """Retrieve the context of the currently executing hook.

        This serves as the integration point with the hook api server,
        which utilizes this function to retrieve a hook context for
        a given client. Since we're serializing execution its effectively
        a constant lookup to the currently executing hook's context.
        """
        return self._hook_context

    def get_hook_path(self, hook_path):
        """Retrieve a hook path. We use this to enable debugging.

        :param hook_path:  The requested hook path to execute.

        If the executor is in debug mode, a path to a debug hook
        is returned.
        """
        hook_name = os.path.basename(hook_path)

        # Cleanup/Release any previous hook debug scripts.
        if self._debug_hook_file_path:
            os.unlink(self._debug_hook_file_path)
            self._debug_hook_file_path = None

        # Check if debug is active, if not use the requested hook.
        if not self._debug_hook_names:
            return hook_path

        # Check if its a hook we want to debug
        found = False
        for debug_name in self._debug_hook_names:
            if fnmatch.fnmatch(hook_name, debug_name):
                found = True

        if not found:
            return hook_path

        # Setup a debug hook script.
        self._debug_hook_file_path = self._write_debug_hook(hook_name)
        return self._debug_hook_file_path

    def _write_debug_hook(self, hook_name):
        debug_hook = DEBUG_HOOK_TEMPLATE.replace("{hook_name}", hook_name)
        debug_hook_file = tempfile.NamedTemporaryFile(
            suffix="-%s" % hook_name, delete=False)
        debug_hook_file.write(debug_hook)
        debug_hook_file.flush()
        # We have to close the hook file, else linux throws a Text
        # File busy on exec because the file is open for write.
        debug_hook_file.close()
        os.chmod(debug_hook_file.name, 0700)

        return debug_hook_file.name

    def set_debug(self, hook_names):
        """Set some hooks to be debugged. Also used to clear debug.

        :param hook_names: A list of hook names to debug, None values
               means disable debugging, and end current debugging
               underway.
        """
        if hook_names is not None and not isinstance(hook_names, list):
            raise AssertionError("Invalid hook names %r" % (hook_names))

        # Terminate an existing debug session when the debug ends.
        if hook_names is None and self._invoker:
            try:
                self._invoker.send_signal("HUP")
            except (ProcessExitedAlready, ValueError):
                pass
        self._debug_hook_names = hook_names

    def __call__(self, invoker, hook_path):
        """Schedule a hook for execution.

        Returns a deferred that fires when the hook has been executed.
        """
        exec_deferred = Deferred()
        self._executions.put(
            (invoker, hook_path, exec_deferred))
        return exec_deferred
