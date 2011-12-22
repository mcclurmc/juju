import argparse
import os
import sys
import logging

import zookeeper

from twisted.application import service
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.scripts._twistd_unix import UnixApplicationRunner, UnixAppLogger
from twisted.python.log import PythonLoggingObserver

from txzookeeper import ZookeeperClient

from juju.control.options import setup_twistd_options
from juju.errors import NoConnection, JujuError
from juju.lib.zklog import ZookeeperHandler
from juju.state.environment import GlobalSettingsStateManager


class TwistedOptionNamespace(object):
    """
    An argparse namespace implementation that is compatible with twisted
    config dictionary usage.
    """

    def __getitem__(self, key):
        return self.__dict__[key]

    def __setitem__(self, key, value):
        self.__dict__[key] = value

    def get(self, key, default=None):
        return self.__dict__.get(key, default)

    def has_key(self, key):
        return key in self.__dict__


class AgentLogger(UnixAppLogger):

    def __init__(self, options):
        super(AgentLogger, self).__init__(options)
        self._loglevel = options.get("loglevel", logging.DEBUG)

    def _getLogObserver(self):

        if self._logfilename == "-":
            log_file = sys.stdout
        else:
            log_file = open(self._logfilename, "a")

        # Setup file logger
        log_handler = logging.StreamHandler(log_file)
        formatter = logging.Formatter(
            "%(asctime)s: %(name)s@%(levelname)s: %(message)s")
        log_handler.setFormatter(formatter)

        # Also capture zookeeper logs (XXX not compatible with rotation)
        zookeeper.set_log_stream(log_file)

        # Configure logging.
        root = logging.getLogger()
        root.addHandler(log_handler)
        root.setLevel(logging.getLevelName(self._loglevel))

        # Twisted logging is painfully verbose on twisted.web, and
        # there isn't a good way to distinguish different channels
        # within twisted, so just utlize error level logging only for
        # all of twisted.
        twisted_log = logging.getLogger("twisted")
        twisted_log.setLevel(logging.ERROR)

        observer = PythonLoggingObserver()
        return observer.emit


class AgentRunner(UnixApplicationRunner):

    application = None
    loggerFactory = AgentLogger

    def createOrGetApplication(self):
        return self.application


class BaseAgent(object, service.Service):

    name = "juju-agent-unknown"
    client = None

    # Flag when enabling persistent topology watches, testing aid.
    _watch_enabled = True

    # Distributed debug log handler
    _debug_log_handler = None

    @classmethod
    def run(cls):
        """Runs the agent as a unix daemon.

        Main entry point for starting an agent, parses cli options, and setups
        a daemon using twistd as per options.
        """
        parser = argparse.ArgumentParser()
        cls.setup_options(parser)
        config = parser.parse_args(namespace=TwistedOptionNamespace())
        runner = AgentRunner(config)
        agent = cls()
        agent.configure(config)
        runner.application = agent.as_app()
        runner.run()

    @classmethod
    def setup_options(cls, parser):
        """Configure the argparse cli parser for the agent."""
        return cls.setup_default_options(parser)

    @classmethod
    def setup_default_options(cls, parser):
        """Setup default twistd daemon and agent options.

        This method is intended as a utility for subclasses.

        @param parser an argparse instance.
        @type C{argparse.ArgumentParser}
        """
        setup_twistd_options(parser, cls)
        setup_default_agent_options(parser, cls)

    def as_app(self):
        """
        Return the agent as a C{twisted.application.service.Application}
        """
        app = service.Application(self.name)
        self.setServiceParent(app)
        return app

    def configure(self, options):
        """Configure the agent to handle its cli options.

        Invoked called before the service is started.

        @param options
        @type C{TwistedOptionNamespace} an argparse namespace corresponding
              to a dict.
        """
        if not options.get("zookeeper_servers"):
            raise NoConnection("No zookeeper connection configured.")

        if not os.path.exists(options.get("juju_directory", "")):
            raise JujuError(
                "Invalid juju-directory %r, does not exist." % (
                    options.get("juju_directory")))

        self.config = options

    @inlineCallbacks
    def connect(self):
        """Return an authenticated connection to the juju zookeeper."""
        hosts = self.config["zookeeper_servers"]
        self.client = yield ZookeeperClient().connect(hosts)

        principals = self.config.get("principals", ())
        for principal in principals:
            self.client.add_auth("digest", principal)

        # bug work around to keep auth fast
        if principals:
            yield self.client.exists("/")

        returnValue(self.client)

    def start(self):
        """Callback invoked on the agent's startup.

        The agent will already be connected to zookeeper. Subclasses are
        responsible for implementing.
        """
        raise NotImplementedError

    def stop(self):
        """Callback invoked on when the agent is shutting down."""
        pass

    # Twisted IService implementation, used for delegates to maintain naming
    # conventions.
    @inlineCallbacks
    def startService(self):
        yield self.connect()
        yield self.start()

        if self.get_watch_enabled():
            yield self.start_global_settings_watch()

    @inlineCallbacks
    def stopService(self):
        try:
            yield self.stop()
        finally:
            if self.client and self.client.connected:
                self.client.close()

    def set_watch_enabled(self, flag):
        """Set boolean flag for whether this agent should watching zookeeper.

        This is mainly used for testing, to allow for setting up the
        various data scenarios, before enabling an agent watch which will
        be observing state.
        """
        self._watch_enabled = bool(flag)

    def get_watch_enabled(self):
        """Returns a boolean if the agent should be settings state watches.

        The meaning of this flag is typically agent specific, as each
        agent has separate watches they'd like to establish on agent specific
        state within zookeeper. In general if this flag is False, the agent
        should refrain from establishing a watch on startup. This flag is
        typically used by tests to isolate and test the watch behavior
        independent of the agent startup, via construction of test data.
        """
        return self._watch_enabled

    def start_global_settings_watch(self):
        """Start watching the runtime state for configuration changes."""
        self.global_settings_state = GlobalSettingsStateManager(self.client)
        return self.global_settings_state.watch_settings_changes(
            self.on_global_settings_change)

    @inlineCallbacks
    def on_global_settings_change(self, change):
        """On global settings change, take action.
        """
        if (yield self.global_settings_state.is_debug_log_enabled()):
            yield self.start_debug_log()
        else:
            self.stop_debug_log()

    @inlineCallbacks
    def start_debug_log(self):
        """Enable the distributed debug log handler.
        """
        if self._debug_log_handler is not None:
            returnValue(None)
        context_name = self.get_agent_name()
        self._debug_log_handler = ZookeeperHandler(
            self.client, context_name)
        yield self._debug_log_handler.open()
        log_root = logging.getLogger()
        log_root.addHandler(self._debug_log_handler)

    def stop_debug_log(self):
        """Disable any configured debug log handler.
        """
        if self._debug_log_handler is None:
            return
        handler, self._debug_log_handler = self._debug_log_handler, None
        log_root = logging.getLogger()
        log_root.removeHandler(handler)

    def get_agent_name(self):
        """Return the agent's name and context such that it can be identified.

        Subclasses should override this to provide additional context and
        unique naming.
        """
        return self.__class__.__name__


def setup_default_agent_options(parser, cls):

    principals_default = os.environ.get("JUJU_PRINCIPALS", "").split()
    parser.add_argument(
        "--principal", "-e",
        action="append", dest="principals", default=principals_default,
        help="Agent principals to utilize for the zookeeper connection")

    servers_default = os.environ.get("JUJU_ZOOKEEPER", "")
    parser.add_argument(
        "--zookeeper-servers", "-z", default=servers_default,
        help="juju Zookeeper servers to connect to ($JUJU_ZOOKEEPER)")

    juju_home = os.environ.get("JUJU_HOME", "/var/lib/juju")
    parser.add_argument(
        "--juju-directory", default=juju_home, type=os.path.abspath,
        help="juju working directory ($JUJU_HOME)")
