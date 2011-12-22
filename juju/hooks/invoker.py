import os
import sys

from twisted.internet import protocol
from twisted.internet.defer import Deferred, inlineCallbacks, returnValue
from twisted.python.failure import Failure

from juju import errors


class HookProtocol(protocol.ProcessProtocol):
    """Protocol used to communicate between the unit agent and hook process.

    This class manages events associated with the hook process,
    including its exit and status of file descriptors.
    """
    def __init__(self, hook_name, context, log=None):
        self._context = context
        self._hook_name = hook_name
        self._log = log

        # The process has exited. The value of this Deferred is
        # the exit code, and only if it is 0. Otherwise a
        # `CharmInvocationError` is communicated through this
        # Deferred.
        self.exited = Deferred()

        # The process has ended, that is, its file descriptors
        # are closed. Output can now be fully read. The deferred
        # semantics are the same as `exited` above.
        self.ended = Deferred()

    def outReceived(self, data):
        """Log `data` from stduout until the child process has ended."""
        self._log.info(data)

    def errReceived(self, data):
        """Log `data` from stderr until the child process has ended."""
        self._log.error(data)

    def _process_reason(self, reason, deferred):
        """Common code for working with both processEnded and processExited.

        The semantics of `exited` and `ended` are the same with
        respect to how they process the status code; the difference is
        when these events occur. For more, see :class:`Invoker`.
        """
        exit_code = reason.value.exitCode
        if exit_code in (0, None):
            deferred.callback(exit_code)
        else:
            error = errors.CharmInvocationError(self._hook_name, exit_code)
            deferred.errback(error)

    def processExited(self, reason):
        """Called when the process has exited."""
        self._process_reason(reason, self.exited)

    def processEnded(self, reason):
        """Called when file descriptors for the process are closed."""
        self._process_reason(reason, self.ended)


class FormatItems(object):
    """Wrapper to delay executing __str_ of items until formatted in log."""
    def __init__(self, items):
        self.items = items

    def __str__(self):
        items = sorted(self.items, key=lambda item: item.key)
        return "\n".join("    %s" % str(item) for item in items)


class Invoker(object):
    """Responsible for the execution and management of hook invocation.

    In a nutshell, *how* hooks are invoked, not *when* or *why*.

    Responsible for the following:

      * Manages socket connection with the unit agent.

      * Connects the child process stdout/stderr file descriptors to
        logging.

      * Handles the exit of the hook process, including reporting its
        exit code.

      * Cleans up resources of the hook process upon its exit.

    It's important to understand the difference between a process
    exiting and the process ending (using the terminology established
    by Twisted). Process exit is simple - this is the first event and
    occurs by the process returning its status code through the exit
    process. Normally process ending occurs very shortly thereafter,
    however, it may be briefly delayed because of pending writes to
    its file descriptors.

    In certain cases, however, hook scripts may invoke poorly written
    commands that fork child processes in the background that will
    wait around indefinitely, but do not close their file
    descriptors. In this case, it is the responsibility of the Invoker
    to wait briefly (for now hardcoded to 5 seconds), then reap such
    processes.
    """
    def __init__(self, context, change, client_id, socket_path,
                 unit_path, logger):
        """Takes the following arguments:

        `context`: an `juju.state.hook.HookContext`

        `change`: an `juju.state.hook.RelationChange`

        `client_id`: a string uniquely identifying a client session

        `socket_path`: the path to the UNIX Domain socket used by
           clients to communicate with the Unit Agent

        `logger`: instance of a `logging.Logger` object used to capture
           hook output
        """
        self.environment = {}
        self._context = context
        self._change = change
        self._client_id = client_id
        self._socket_path = socket_path
        self._unit_path = unit_path
        self._log = logger

        # The twisted.internet.process.Process instance.
        self._process = None

        # The hook executable path
        self._process_executable = None

        # Deferred tracking whether the process HookProtocol is ended
        self._ended = None

        # When set, a delayed call that ensures the process is
        # properly terminated with loseConnection
        self._reaper = None

    @property
    def ended(self):
        return self._ended

    @property
    def unit_path(self):
        return self._unit_path

    def get_environment(self):
        """
        Returns the environment used to run the hook as a dict.
        Defaults are provided based on information passed to __init__.
        By setting keys inside Invoker.environment you can override
        the defaults or provide additional variables.
        """
        base = dict(JUJU_AGENT_SOCKET=self._socket_path,
                    JUJU_CLIENT_ID=self._client_id,
                    CHARM_DIR=os.path.join(self._unit_path, "charm"),
                    JUJU_UNIT_NAME=os.environ["JUJU_UNIT_NAME"],
                    DEBIAN_FRONTEND="noninteractive",
                    APT_LISTCHANGES_FRONTEND="none",
                    PATH=os.environ["PATH"],
                    JUJU_PYTHONPATH=":".join(sys.path))
        base.update(self.environment)

        return self.get_environment_from_change(base, self._change)

    def get_environment_from_change(self, env, change):
        """Supplement the default environment with dict with variables
        originating from the `change` argument to __init__.
        """
        return env

    def get_context(self):
        """Returns the hook context for the invocation."""
        return self._context

    def validate_hook(self, hook_filename):
        """Verify that the hook_filename exists and is executable. """
        if not os.path.exists(hook_filename):
            raise errors.FileNotFound(hook_filename)

        if not os.access(hook_filename, os.X_OK):
            raise errors.CharmError(hook_filename,
                                      "hook is not executable")

    def send_signal(self, signal_id):
        """Send a signal of the given signal_id.

        `signal_id`: limited value interpretation, numeric signals
        ids are used as given, some values for symbolic string
        interpretation are available see
        ``twisted.internet.process._BaseProcess.signalProcess`` for
        additional details.

        Raises a `ValueError` if the process doesn't exist or
        `ProcessExitedAlready` if the process has already ended.
        """

        if not self._process:
            raise ValueError("No Process")
        return self._process.signalProcess(signal_id)

    def _ensure_process_termination(self, ignored):
        """Cancels any scheduled reaper and terminates hook process, if around.

        Canceling the reaper itself is necessary to ensure that
        deferreds like this are not left in the reactor. This would
        otherwise be the case for test that are awaiting the log, by
        using the `Invoker.end` deferred.
        """
        if self._reaper:
            if not self._reaper.called:
                self._reaper.cancel()
        self._process.loseConnection()

    @inlineCallbacks
    def _cleanup_process(self, hook, result):
        """Performs process cleanup:

           * Flushes any changes (eg relation settings maded by the
             hook)

           * Ensures that the result will be the exit code of the
             process (if 0), or the `CharmInvocationError` from the
             underlying `HookProtocol`, with cleaned up traceback.

           * Also schedules a reaper to be called later that ensures
             process termination.
        """
        message = result
        if isinstance(message, Failure):
            message = message.getTraceback(elideFrameworkCode=True)
        self._log.debug("hook %s exited, exit code %s." % (
                os.path.basename(hook), message))

        # Ensure that the process is terminated (via loseConnection)
        # no more than 5 seconds (arbitrary) after it exits, unless it
        # normally ends. If ended, the reaper is cancelled to ensure
        # it is not left in the reactor.
        #
        # The 5 seconds was chosen to make it vanishly small that
        # there would be any lost output (as might be *occasionally*
        # seen with a 50ms threshold in actual testing).
        from twisted.internet import reactor
        self._reaper = reactor.callLater(5, self._process.loseConnection)

        # Flush context changes back to zookeeper if hook was successful.
        if result == 0 and self._context:
            relation_setting_changes = yield self._context.flush()
            if relation_setting_changes:
                self._log.debug(
                    "Flushed values for hook %r\n%s",
                    os.path.basename(hook),
                    FormatItems(relation_setting_changes))

        returnValue(result)

    def __call__(self, hook):
        """Execute `hook` in a runtime context and returns status code.

        The `hook` parameter should be a complete path to the desired
        executable. The returned value is a `Deferred` that is called
        when the hook exits.
        """
        # Sanity check the hook.
        self.validate_hook(hook)

        # Setup for actual invocation
        env = self.get_environment()
        hook_proto = HookProtocol(hook, self._context, self._log)
        exited = hook_proto.exited
        self._ended = ended = hook_proto.ended

        from twisted.internet import reactor
        self._process = reactor.spawnProcess(
            hook_proto, hook, [hook], env,
            os.path.join(self._unit_path, "charm"))

        # Manage cleanup after hook exits
        def cb_cleanup_process(result):
            return self._cleanup_process(hook, result)

        exited.addBoth(cb_cleanup_process)
        ended.addBoth(self._ensure_process_termination)
        return exited
