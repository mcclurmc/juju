import logging
import os
import subprocess

from twisted.internet.defer import inlineCallbacks, Deferred
from twisted.internet.error import ProcessExitedAlready

import juju.hooks.executor
from juju.hooks.executor import HookExecutor

from juju.hooks.invoker import Invoker
from juju.lib.testing import TestCase
from juju.lib.twistutils import gather_results


class HookExecutorTest(TestCase):

    def setUp(self):
        self._executor = HookExecutor()
        self.output = self.capture_logging("hook.executor", logging.DEBUG)

    @inlineCallbacks
    def test_observer(self):
        """An observer can be registered against the executor
        to recieve callbacks when hooks are executed."""
        results = []
        d = Deferred()

        def observer(hook_path):
            results.append(hook_path)
            if len(results) == 3:
                d.callback(True)

        self._executor.set_observer(observer)
        self._executor.start()

        class _Invoker(object):

            def get_context(self):
                return None

            def __call__(self, hook_path):
                results.append(hook_path)

        hook_path = self.makeFile("hook content")
        yield self._executor(_Invoker(), hook_path)
        # Also observes non existant hooks
        yield self._executor(_Invoker(), self.makeFile())
        self.assertEqual(len(results), 3)

    @inlineCallbacks
    def test_start_deferred_ends_on_stop(self):
        """The executor start method returns a deferred that
        fires when the executor has been stopped."""

        stopped = []

        def on_start_finish(result):
            self.assertTrue(stopped)

        d = self._executor.start()
        d.addCallback(on_start_finish)
        stopped.append(True)
        yield self._executor.stop()
        self._executor.debug = True
        yield d

    def test_start_start(self):
        """Attempting to start twice raises an exception."""
        self._executor.start()
        return self.assertFailure(self._executor.start(), AssertionError)

    def test_stop_stop(self):
        """Attempt to stop twice raises an exception."""
        self._executor.start()
        self._executor.stop()
        return self.assertFailure(self._executor.stop(), AssertionError)

    @inlineCallbacks
    def test_debug_hook(self):
        """A debug hook is executed if a debug hook name is found.
        """
        self.output = self.capture_logging(
            "hook.executor", level=logging.DEBUG)
        results = []

        class _Invoker(object):

            def get_context(self):
                return None

            def __call__(self, hook_path):
                results.append(hook_path)

        self._executor.set_debug(["*"])
        self._executor.start()

        yield self._executor(_Invoker(), "abc")
        self.assertNotEqual(results, ["abc"])
        self.assertIn("abc", self.output.getvalue())

    def test_get_debug_hook_path_executable(self):
        """The debug hook path return from the executor should be executable.
        """
        self.patch(
            juju.hooks.executor, "DEBUG_HOOK_TEMPLATE",
            "#!/bin/bash\n echo {hook_name}\n exit 0")
        self._executor.set_debug(["*"])

        debug_hook = self._executor.get_hook_path("something/good")
        stdout = open(self.makeFile(), "w+")
        p = subprocess.Popen(debug_hook, stdout=stdout.fileno())
        self.assertEqual(p.wait(), 0)
        stdout.seek(0)
        self.assertEqual(stdout.read(), "good\n")

    @inlineCallbacks
    def test_end_debug_with_exited_process(self):
        """Ending debug with a process that has already ended is a noop."""

        results = []

        class _Invoker(object):

            process_ended = Deferred()

            def get_context(self):
                return None

            def __call__(self, hook_path):
                results.append(hook_path)
                return self.process_ended

            def send_signal(self, signal_id):
                if results:
                    results.append(1)
                    raise ProcessExitedAlready()
                results.append(2)
                raise ValueError("No such process")

        self._executor.start()
        self._executor.set_debug(["abc"])
        hook_done = self._executor(_Invoker(), "abc")
        self._executor.set_debug(None)

        _Invoker.process_ended.callback(True)

        yield hook_done
        self.assertEqual(len(results), 2)
        self.assertNotEqual(results[0], "abc")
        self.assertEqual(results[1], 1)

    @inlineCallbacks
    def test_end_debug_with_hook_not_started(self):
        results = []

        class _Invoker(object):

            process_ended = Deferred()

            def get_context(self):
                return None

            def __call__(self, hook_path):
                results.append(hook_path)
                return self.process_ended

            def send_signal(self, signal_id):
                if len(results) == 1:
                    results.append(1)
                    raise ValueError()
                results.append(2)
                raise ProcessExitedAlready()

        self._executor.start()
        self._executor.set_debug(["abc"])
        hook_done = self._executor(_Invoker(), "abc")
        self._executor.set_debug(None)

        _Invoker.process_ended.callback(True)
        yield hook_done
        self.assertEqual(len(results), 2)
        self.assertNotEqual(results[0], "abc")
        self.assertEqual(results[1], 1)

    @inlineCallbacks
    def test_end_debug_with_debug_running(self):
        """If a debug hook is running, it is signaled if the debug is disabled.
        """
        self.patch(
            juju.hooks.executor, "DEBUG_HOOK_TEMPLATE",
            "\n".join(("#!/bin/bash",
             "exit_handler() {",
             "  echo clean exit",
             "  exit 0",
             "}",
             'trap "exit_handler" HUP',
             "sleep 0.2",
             "exit 1")))

        unit_dir = self.makeDir()

        charm_dir = os.path.join(unit_dir, "charm")
        self.makeDir(path=charm_dir)

        self._executor.set_debug(["*"])
        log = logging.getLogger("invoker")
        # Populate environment variables for default invoker.
        self.change_environment(
            JUJU_UNIT_NAME="dummy/1", PATH="/bin/:/usr/bin")
        output = self.capture_logging("invoker", level=logging.DEBUG)
        invoker = Invoker(
            None, None, "constant", self.makeFile(), unit_dir, log)

        self._executor.start()
        hook_done = self._executor(invoker, "abc")

        # Give a moment for execution to start.
        yield self.sleep(0.1)
        self._executor.set_debug(None)
        yield hook_done
        self.assertIn("clean exit", output.getvalue())

    def test_get_debug_hook_path(self):
        """A debug hook file path is returned if a debug hook name is found.
        """
        # Default is to return the file path.
        file_path = self.makeFile()
        hook_name = os.path.basename(file_path)
        self.assertEquals(self._executor.get_hook_path(file_path), file_path)

        # Hook names can be specified as globs.
        self._executor.set_debug(["*"])
        debug_hook_path = self._executor.get_hook_path(file_path)
        self.assertNotEquals(file_path, debug_hook_path)
        # The hook base name is suffixed onto the debug hook file
        self.assertIn(os.path.basename(file_path),
                      os.path.basename(debug_hook_path))

        # Verify the debug hook contents.
        debug_hook_file = open(debug_hook_path)
        debug_contents = debug_hook_file.read()
        debug_hook_file.close()

        self.assertIn("hook.sh", debug_contents)
        self.assertIn("-n %s" % hook_name, debug_contents)
        self.assertTrue(os.access(debug_hook_path, os.X_OK))

        # The hook debug can be set back to none.
        self._executor.set_debug(None)
        self.assertEquals(self._executor.get_hook_path(file_path), file_path)

        # the executor can debug only selected hooks.
        self._executor.set_debug(["abc"])
        self.assertEquals(self._executor.get_hook_path(file_path), file_path)

        # The debug hook file is removed on the next hook path access.
        self.assertFalse(os.path.exists(debug_hook_path))

    def test_hook_exception_propgates(self):
        """An error in a hook is propogated to the execution deferred."""

        class _Invoker:
            def get_context(self):
                return None

            def __call__(self, hook_path):
                raise AttributeError("Foo")

        hook_path = self.makeFile("never got here")
        self._executor.start()
        return self.assertFailure(
            self._executor(_Invoker(), hook_path), AttributeError)

    @inlineCallbacks
    def test_executor_running_property(self):
        self._executor.start()
        self.assertTrue(self._executor.running)
        yield self._executor.stop()
        self.assertFalse(self._executor.running)

    @inlineCallbacks
    def test_nonexistant_hook_skipped(self):
        """If a hook does not exist a warning is logged and the hook skipped.
        """

        class _Invoker:
            def get_context(self):
                return None

        self._executor.start()
        hook_path = self.makeFile()
        value = yield self._executor(_Invoker(), hook_path)
        self.assertEqual(value, False)
        self.assertIn("Hook does not exist, skipping %s" % hook_path,
                      self.output.getvalue())

    def test_start_stop_start(self):
        """The executor can be stopped and restarted."""
        results = []

        def invoke(hook_path):
            results.append(hook_path)

        self._executor(invoke, "1")
        start_complete = self._executor.start()
        self._executor.stop()
        yield start_complete

        self.assertEqual(len(results), 1)
        self._executor(invoke, "1")
        self._executor(invoke, "2")
        start_complete = self._executor.start()
        self._executor.stop()
        yield start_complete
        self.assertEqual(len(results), 3)

    @inlineCallbacks
    def test_run_priority_hook_while_already_running(self):
        """Attempting to run a priority hook while running is an error.
        """
        def invoke(hook_path):
            pass

        self._executor.start()
        error = yield self.assertFailure(
            self._executor.run_priority_hook(invoke, "foobar"),
            AssertionError)
        self.assertEquals(str(error), "Executor must not be running")

    @inlineCallbacks
    def test_prioritize_with_queued(self):
        """A prioritized hook will execute before queued hooks.
        """
        results = []
        execs = []
        hooks = [self.makeFile(str(i)) for i in range(5)]

        class _Invoker(object):

            def get_context(self):
                return None

            def __call__(self, hook_path):
                results.append(hook_path)

        invoke = _Invoker()
        for i in hooks:
            execs.append(self._executor(invoke, i))

        priority_hook = self.makeFile(str("me first"))
        yield self._executor.run_priority_hook(invoke, priority_hook)
        self._executor.start()
        yield gather_results(execs)
        hooks.insert(0, priority_hook)
        self.assertEqual(results, hooks)

    def test_serialized_execution(self):
        """Hook execution is serialized via the HookExecution api.
        """

        wait_callback = [Deferred() for i in range(5)]
        finish_callback = [Deferred() for i in range(5)]
        results = []

        @inlineCallbacks
        def invoker(hook_path):
            results.append(hook_path)
            yield finish_callback[len(results) - 1]
            wait_callback[len(results) - 1].callback(True)

        start_complete = self._executor.start()
        for i in range(5):
            self._executor(invoker, "hook-%s" % i)

        self.assertEqual(len(results), 1)
        finish_callback[1].callback(True)
        self.assertEqual(len(results), 1)

        # Verify stop behavior
        stop_complete = yield self._executor.stop()

        # Finish the running execution.
        finish_callback[0].callback(True)

        # Verify we've stopped executing.
        yield stop_complete
        self.assertTrue(start_complete.called)
        self.assertEqual(len(results), 1)

        # Start the executioner again.
        self._executor.start()

        for finish in finish_callback[2:]:
            finish.callback(True)

        self.assertEqual(len(results), 5)
