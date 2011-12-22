import os
import time

from twisted.internet.defer import (
    succeed, fail, Deferred, DeferredList, inlineCallbacks, returnValue)
from twisted.internet import reactor

import juju

from juju.lib.testing import TestCase
from juju.lib.twistutils import (
    concurrent_execution_guard, gather_results, get_module_directory, sleep)


class Bar(object):

    def __init__(self):
        self._count = 0

    @concurrent_execution_guard("guard")
    def my_function(self, a, b=0):
        """zebra"""
        return succeed(a / b)

    @concurrent_execution_guard("other_guard")
    def other_function(self, a):
        return fail(OSError("Bad"))

    @concurrent_execution_guard("increment_guard")
    def slow_increment(self, delay=0.1):
        deferred = Deferred()

        def _increment():
            self._count += 1
            return deferred.callback(self._count)

        reactor.callLater(delay, _increment)
        return deferred

    @concurrent_execution_guard("inline_guard")
    @inlineCallbacks
    def inline_increment(self):
        result = yield self.slow_increment()
        returnValue(result * 100)


class ExecutionGuardTest(TestCase):

    def test_guarded_function_metadata(self):
        self.assertEqual(Bar().my_function.__name__, "my_function")
        self.assertEqual(Bar().my_function.__doc__, "zebra")

    def test_guarded_function_failure(self):
        foo = Bar()
        return self.assertFailure(foo.other_function("1"), OSError)

    def test_guarded_function_sync_exception(self):
        foo = Bar()
        try:
            result = foo.my_function(1)
        except:
            self.fail("Should not raise exception")

        self.assertFailure(result, ZeroDivisionError)
        self.assertFailure(foo.my_function(1), ZeroDivisionError)
        self.assertFalse(foo.guard, False)

    def test_guard_multiple_execution(self):
        foo = Bar()

        d1 = foo.slow_increment()
        d2 = foo.slow_increment()

        def validate_results(results):
            success, value = results[0]
            self.assertTrue(success)
            self.assertEqual(value, 1)

            success, value = results[1]
            self.assertTrue(success)
            self.assertEqual(value, False)
            return foo.slow_increment()

        def validate_value(results):
            # if the guard had not prevent execution the value
            # would be 3.
            self.assertEqual(results, 2)

        dlist = DeferredList([d1, d2])
        dlist.addCallback(validate_results)
        dlist.addCallback(validate_value)
        return dlist

    def test_guard_w_inline_callbacks(self):
        foo = Bar()

        def validate_result(result):
            self.assertEqual(result, 100)

        d = foo.inline_increment()
        d.addCallback(validate_result)
        return d


class GatherResultsTest(TestCase):

    def test_empty(self):
        d = gather_results([])

        def check_result(result):
            self.assertEqual(result, [])
        d.addCallback(check_result)
        return d

    def test_success(self):
        d1 = succeed(1)
        d2 = succeed(2)
        d = gather_results([d1, d2])

        def check_result(result):
            self.assertEqual(result, [1, 2])
        d.addCallback(check_result)
        return d

    def test_failure_consuming_errors(self):
        d1 = succeed(1)
        d2 = fail(AssertionError("Expected failure"))
        d = gather_results([d1, d2])
        self.assertFailure(d, AssertionError)
        return d

    def test_failure_without_consuming_errors(self):
        d1 = succeed(1)
        d2 = fail(AssertionError("Expected failure"))
        d = gather_results([d1, d2], consume_errors=False)
        self.assertFailure(d, AssertionError)
        self.assertFailure(d2, AssertionError)
        return d


class ModuleDirectoryTest(TestCase):

    def test_get_module_directory(self):
        directory = get_module_directory(juju)
        self.assertIn("juju", directory)
        self.assertNotIn("_trial_temp", directory)
        self.assertTrue(os.path.isdir(directory))


class SleepTest(TestCase):

    @inlineCallbacks
    def test_sleep(self):
        """Directly test deferred sleep."""
        start = time.time()
        yield sleep(0.1)
        self.assertGreaterEqual(time.time() - start, 0.1)
