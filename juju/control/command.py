from twisted.internet import defer
from twisted.python.failure import Failure

from argparse import Namespace
from StringIO import StringIO
import sys


class Commander(object):
    """Command container.

    Command objects are constructed in the argument parser in package
    __init__ and used to control the execution of juju command
    line activities.

    Keyword Arguments:
    callback -- a callable object which will be triggered in the
                reactor loop when Commander.__call__ is invoked.

    """

    def __init__(self, callback, passthrough=False):
        if not callable(callback):
            raise ValueError(
                "Commander callback argument must be a callable")

        self.callback = callback
        self.passthrough = passthrough
        self.options = None
        self.exit_code = 0

    def __call__(self, options):
        from twisted.internet import reactor

        if not options or not isinstance(options, Namespace):
            raise ValueError(
                "%s.__call__ must be passed a valid argparse.Namespace" %
                self.__class__.__name__)
        self.options = options
        options.log.debug("Initializing %s runtime" %
                          options.parser.prog)

        reactor.callWhenRunning(self._run)
        reactor.run()

        sys.exit(self.exit_code)

    def _run(self):
        d = defer.maybeDeferred(self.callback, self.options)
        d.addBoth(self._handle_exit)
        return d

    def _handle_exit(self, result, stream=None):
        from twisted.internet import reactor

        if stream is None:
            stream = sys.stderr
        if isinstance(result, Failure):
            if self.options.verbose:
                tracebackIO = StringIO()
                result.printTraceback(file=tracebackIO)
                stream.write(tracebackIO.getvalue())
                self.options.log.error(tracebackIO.getvalue())

            # be a bit noisy on failure
            print >>stream, result.getErrorMessage()
            self.options.log.error(result.getErrorMessage())

            if self.exit_code == 0:
                self.exit_code = 1
        else:
            command_name = self.callback.__module__.rsplit('.', 1)[-1]
            self.options.log.info("%r command finished successfully" %
                                  command_name)

        if reactor.running:
            reactor.stop()
