import inspect
import os

from twisted.internet import reactor
from twisted.internet.defer import (
    Deferred, maybeDeferred, succeed, DeferredList)
from twisted.python.util import mergeFunctionMetadata


def concurrent_execution_guard(attribute):
    """Sets attribute to True/False during execution of the decorated method.

    Used to ensure non concurrent execution of the decorated function via
    an instance attribute. *The underlying function must return a defered*.
    """

    def guard(f):

        def guard_execute(self, *args, **kw):
            value = getattr(self, attribute, None)
            if value:
                return succeed(False)
            else:
                setattr(self, attribute, True)

            d = maybeDeferred(f, self, *args, **kw)

            def post_execute(result):
                setattr(self, attribute, False)
                return result
            d.addBoth(post_execute)
            return d

        return mergeFunctionMetadata(f, guard_execute)

    return guard


def gather_results(deferreds, consume_errors=True):
    d = DeferredList(deferreds, fireOnOneErrback=1,
                     consumeErrors=consume_errors)
    d.addCallback(lambda r: [x[1] for x in r])
    d.addErrback(lambda f: f.value.subFailure)
    return d


def get_module_directory(module):
    """Determine the directory of a module.

    Trial rearranges the working directory such that the module
    paths are relative to a modified current working directory,
    which results in failing tests when run under coverage, we
    manually remove the trial locations to ensure correct
    directories are utilized.
    """
    return os.path.abspath(os.path.dirname(inspect.getabsfile(module)).replace(
        "/_trial_temp", ""))


def sleep(delay):
    """Non-blocking sleep.

    :param int delay: time in seconds to sleep.
    :return: a Deferred that fires after the desired delay.
    :rtype: :class:`twisted.internet.defer.Deferred`
    """
    deferred = Deferred()
    reactor.callLater(delay, deferred.callback, None)
    return deferred
