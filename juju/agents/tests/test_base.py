import argparse
import json
import logging
import os
import sys


from twisted.application.app import AppLogger
from twisted.application.service import IService, IServiceCollection
from twisted.internet.defer import (
    succeed, Deferred, inlineCallbacks, returnValue)
from twisted.python.components import Componentized
from twisted.python import log

import zookeeper
from txzookeeper import ZookeeperClient
from juju.lib.testing import TestCase
from juju.lib.mocker import MATCH
from juju.tests.common import get_test_zookeeper_address

from juju.agents.base import (
    BaseAgent, TwistedOptionNamespace, AgentRunner, AgentLogger)
from juju.errors import NoConnection, JujuError
from juju.lib.zklog import ZookeeperHandler

from juju.agents.tests.common import AgentTestBase

MATCH_APP = MATCH(lambda x: isinstance(x, Componentized))
MATCH_HANDLER = MATCH(lambda x: isinstance(x, ZookeeperHandler))


class BaseAgentTest(TestCase):

    @inlineCallbacks
    def setUp(self):
        yield super(BaseAgentTest, self).setUp()
        self.change_environment(
            JUJU_HOME=self.makeDir())

    def test_as_app(self):
        """The agent class can be accessed as an application."""
        app = BaseAgent().as_app()
        multi_service = IService(app, None)
        self.assertTrue(IServiceCollection.providedBy(multi_service))
        services = list(multi_service)
        self.assertEqual(len(services), 1)

    def test_twistd_default_options(self):
        """The agent cli parsing, populates standard twistd options."""
        parser = argparse.ArgumentParser()
        BaseAgent.setup_options(parser)

        # Daemon group
        self.assertEqual(
            parser.get_default("logfile"), "%s.log" % BaseAgent.name)
        self.assertEqual(
            parser.get_default("pidfile"), "%s.pid" % BaseAgent.name)

        self.assertEqual(parser.get_default("loglevel"), "DEBUG")
        self.assertTrue(parser.get_default("nodaemon"))
        self.assertEqual(parser.get_default("rundir"), ".")
        self.assertEqual(parser.get_default("chroot"), None)
        self.assertEqual(parser.get_default("umask"), None)
        self.assertEqual(parser.get_default("uid"), None)
        self.assertEqual(parser.get_default("gid"), None)
        self.assertEqual(parser.get_default("euid"), None)
        self.assertEqual(parser.get_default("prefix"), BaseAgent.name)
        self.assertEqual(parser.get_default("syslog"), False)

        # Development Group
        self.assertFalse(parser.get_default("debug"))
        self.assertFalse(parser.get_default("profile"))
        self.assertFalse(parser.get_default("savestats"))
        self.assertEqual(parser.get_default("profiler"), "cprofile")

        # Hidden defaults
        self.assertEqual(parser.get_default("reactor"), "epoll")
        self.assertEqual(parser.get_default("originalname"), None)

        # Agent options
        self.assertEqual(parser.get_default("principals"), [])
        self.assertEqual(parser.get_default("zookeeper_servers"), "")

    def test_twistd_flags_correspond(self):
        parser = argparse.ArgumentParser()
        BaseAgent.setup_options(parser)
        args = [
            "--profile",
            "--savestats",
            "--daemon"]

        options = parser.parse_args(args, namespace=TwistedOptionNamespace())
        self.assertEqual(options.get("savestats"), True)
        self.assertEqual(options.get("nodaemon"), False)
        self.assertEqual(options.get("profile"), True)

    def test_agent_logger(self):
        parser = argparse.ArgumentParser()
        BaseAgent.setup_options(parser)
        log_file_path = self.makeFile()

        options = parser.parse_args(
            ["--logfile", log_file_path], namespace=TwistedOptionNamespace())

        def match_observer(observer):
            return isinstance(observer.im_self, log.PythonLoggingObserver)

        def cleanup(observer):
            # post test cleanup of global state.
            log.removeObserver(observer)
            logging.getLogger().handlers = []

        original_log_with_observer = log.startLoggingWithObserver

        def _start_log_with_observer(observer):
            self.addCleanup(cleanup, observer)
            # by default logging will replace stdout/stderr
            return original_log_with_observer(observer, 0)

        app = self.mocker.mock()
        app.getComponent(log.ILogObserver, None)
        self.mocker.result(None)

        start_log_with_observer = self.mocker.replace(
            log.startLoggingWithObserver)
        start_log_with_observer(MATCH(match_observer))
        self.mocker.call(_start_log_with_observer)
        self.mocker.replay()

        agent_logger = AgentLogger(options)
        agent_logger.start(app)

        # We suppress twisted messages below the error level.
        output = open(log_file_path).read()
        self.assertFalse(output)

        # also verify we didn't mess with the app logging.
        app_log = logging.getLogger()
        app_log.info("Good")

        # and that twisted errors still go through.
        log.err("Something bad happened")
        output = open(log_file_path).read()

        self.assertIn("Good", output)
        self.assertIn("Something bad happened", output)

    def test_custom_log_level(self):
        parser = argparse.ArgumentParser()
        BaseAgent.setup_options(parser)
        options = parser.parse_args(
            ["--loglevel", "INFO"], namespace=TwistedOptionNamespace())
        self.assertEqual(options.loglevel, "INFO")

    def test_twistd_option_namespace(self):
        """
        The twisted option namespace bridges argparse attribute access,
        to twisted dictionary access for cli options.
        """
        options = TwistedOptionNamespace()
        options.x = 1
        self.assertEqual(options['x'], 1)
        self.assertEqual(options.get('x'), 1)
        self.assertEqual(options.get('y'), None)
        self.assertRaises(KeyError, options.__getitem__, 'y')
        options['y'] = 2
        self.assertEqual(options.y, 2)
        self.assertTrue(options.has_key('y'))
        self.assertFalse(options.has_key('z'))

    def test_runner_attribute_application(self):
        """The agent runner retrieve the application as an attribute."""
        runner = AgentRunner({})
        self.assertEqual(runner.createOrGetApplication(), None)
        runner.application = 21
        self.assertEqual(runner.createOrGetApplication(), 21)

    def test_run(self):
        """Invokes the run class method on an agent.

        This will create an agent instance,  parse the cli args, passes them to
        the agent, and starts the agent runner.
        """
        self.change_args("es-agent", "--zookeeper-servers",
                         get_test_zookeeper_address())
        runner = self.mocker.patch(AgentRunner)
        runner.run()
        mock_agent = self.mocker.patch(BaseAgent)

        def match_args(config):
            self.assertEqual(config["zookeeper_servers"],
                             get_test_zookeeper_address())
            return True

        mock_agent.configure(MATCH(match_args))
        self.mocker.passthrough()

        self.mocker.replay()
        BaseAgent.run()

    def test_full_run(self):
        """Verify a functional agent start via the 'run' method.

        This test requires Zookeeper running on the default port of localhost.
        The mocked portions are to prevent the daemon start from altering the
        test environment (sys.stdout/sys.stderr, and reactor start).
        """
        zookeeper.set_debug_level(0)
        started = Deferred()

        class DummyAgent(BaseAgent):
            started = False

            def start(self):
                started.callback(self)

        def validate_started(agent):
            self.assertTrue(agent.client.connected)

        started.addCallback(validate_started)

        pid_file = self.makeFile()
        self.change_args(
            "es-agent",
            "--zookeeper-servers", get_test_zookeeper_address(),
            "--pidfile", pid_file)
        runner = self.mocker.patch(AgentRunner)
        logger = self.mocker.patch(AppLogger)
        logger.start(MATCH_APP)
        runner.startReactor(None, sys.stdout, sys.stderr)
        logger.stop()
        self.mocker.replay()
        DummyAgent.run()
        return started

    def test_stop_service_stub_closes_agent(self):
        """The base class agent, stopService will the stop method.

        Additionally it will close the agent's zookeeper client if
        the client is still connected.
        """
        mock_agent = self.mocker.patch(BaseAgent)
        mock_client = self.mocker.mock(ZookeeperClient)

        # connection is closed after agent.stop invoked.
        with self.mocker.order():
            mock_agent.stop()
            self.mocker.passthrough()

            # client existence check
            mock_agent.client
            self.mocker.result(mock_client)

            # client connected check
            mock_agent.client
            self.mocker.result(mock_client)
            mock_client.connected
            self.mocker.result(True)

            # client close
            mock_agent.client
            self.mocker.result(mock_client)
            mock_client.close()

        self.mocker.replay()

        agent = BaseAgent()
        return agent.stopService()

    def test_stop_service_stub_ignores_disconnected_agent(self):
        """The base class agent, stopService will the stop method.

        If the client is not connected then no attempt is made to close it.
        """
        mock_agent = self.mocker.patch(BaseAgent)
        mock_client = self.mocker.mock(ZookeeperClient)

        # connection is closed after agent.stop invoked.
        with self.mocker.order():
            mock_agent.stop()

            # client existence check
            mock_agent.client
            self.mocker.result(mock_client)

            # client connected check
            mock_agent.client
            self.mocker.result(mock_client)
            mock_client.connected
            self.mocker.result(False)

        self.mocker.replay()

        agent = BaseAgent()
        return agent.stopService()

    def test_run_base_raises_error(self):
        """The base class agent, raises a notimplemented error when started."""
        client = self.mocker.patch(ZookeeperClient)
        client.connect(get_test_zookeeper_address())
        client_mock = self.mocker.mock()
        self.mocker.result(succeed(client_mock))
        self.mocker.replay()

        agent = BaseAgent()
        agent.configure({
            "zookeeper_servers": get_test_zookeeper_address(),
            "juju_directory": self.makeDir()})
        d = agent.startService()
        self.failUnlessFailure(d, NotImplementedError)
        return d

    def test_connect_cli_option(self):
        """The zookeeper server can be passed via cli argument."""
        mock_client = self.mocker.mock()
        client = self.mocker.patch(ZookeeperClient)
        client.connect("x2.example.com")
        self.mocker.result(succeed(mock_client))
        self.mocker.replay()

        agent = BaseAgent()
        agent.configure({"zookeeper_servers": "x2.example.com",
                         "juju_directory": self.makeDir()})
        result = agent.connect()
        self.assertEqual(result.result, mock_client)
        self.assertEqual(agent.client, mock_client)

    def test_non_existant_directory(self):
        """If the juju directory does not exist an error should be raised.
        """
        juju_directory = self.makeDir()
        os.rmdir(juju_directory)
        data = {"zookeeper_servers": get_test_zookeeper_address(),
                "juju_directory": juju_directory}

        agent = BaseAgent()
        self.assertRaises(
            JujuError,
            agent.configure,
            data)

    def test_directory_cli_option(self):
        """The juju directory can be configured on the cli."""
        juju_directory = self.makeDir()
        self.change_args(
            "es-agent", "--zookeeper-servers", get_test_zookeeper_address(),
            "--juju-directory", juju_directory)

        agent = BaseAgent()
        parser = argparse.ArgumentParser()
        agent.setup_options(parser)
        options = parser.parse_args(namespace=TwistedOptionNamespace())
        agent.configure(options)
        self.assertEqual(
            agent.config["juju_directory"], juju_directory)

    def test_directory_env(self):
        """The juju directory passed via environment."""
        self.change_args("es-agent")

        juju_directory = self.makeDir()
        self.change_environment(
            JUJU_HOME=juju_directory,
            JUJU_ZOOKEEPER=get_test_zookeeper_address())

        agent = BaseAgent()
        parser = argparse.ArgumentParser()
        agent.setup_options(parser)
        options = parser.parse_args(namespace=TwistedOptionNamespace())
        agent.configure(options)
        self.assertEqual(
            agent.config["juju_directory"], juju_directory)

    def test_connect_env(self):
        """Zookeeper connection information can be passed via environment."""
        self.change_args("es-agent")
        self.change_environment(
            JUJU_HOME=self.makeDir(),
            JUJU_ZOOKEEPER="x1.example.com",
            JUJU_PRINCIPALS="admin:abc agent:xyz")

        client = self.mocker.patch(ZookeeperClient)
        client.connect("x1.example.com")
        self.mocker.result(succeed(client))
        client.add_auth("digest", "admin:abc")
        client.add_auth("digest", "agent:xyz")
        client.exists("/")
        self.mocker.replay()

        agent = BaseAgent()
        parser = argparse.ArgumentParser()
        agent.setup_options(parser)
        options = parser.parse_args(namespace=TwistedOptionNamespace())
        agent.configure(options)
        d = agent.startService()
        self.failUnlessFailure(d, NotImplementedError)
        return d

    def test_zookeeper_hosts_not_configured(self):
        """a NoConnection error is raised if no zookeeper host is specified."""
        agent = BaseAgent()
        self.assertRaises(
            NoConnection, agent.configure, {"zookeeper_servers": None})

    def test_watch_enabled_accessors(self):
        agent = BaseAgent()
        self.assertTrue(agent.get_watch_enabled())
        agent.set_watch_enabled(False)
        self.assertFalse(agent.get_watch_enabled())


class AgentDebugLogSettingsWatch(AgentTestBase):

    agent_class = BaseAgent

    @inlineCallbacks
    def get_log_entry(self, number, wait=True):
        entry_path = "/logs/log-%010d" % number
        exists_d, watch_d = self.client.exists_and_watch(entry_path)

        exists = yield exists_d
        if not exists and wait:
            yield watch_d
        elif not exists:
            returnValue(False)

        data, stat = yield self.client.get(entry_path)
        returnValue(json.loads(data))

    def test_get_agent_name(self):
        self.assertEqual(self.agent.get_agent_name(), "BaseAgent")

    @inlineCallbacks
    def test_runtime_watching_toggles_log(self):
        """Redundant changes with regard to the current configuration
        are ignored."""
        yield self.agent.connect()
        root_log = logging.getLogger()
        mock_log = self.mocker.replace(root_log)

        mock_log.addHandler(MATCH_HANDLER)
        self.mocker.result(True)

        mock_log.removeHandler(MATCH_HANDLER)
        self.mocker.result(True)

        mock_log.addHandler(MATCH_HANDLER)
        self.mocker.result(True)
        self.mocker.replay()

        yield self.agent.start_global_settings_watch()
        yield self.agent.global_settings_state.set_debug_log(True)
        yield self.agent.global_settings_state.set_debug_log(True)
        yield self.agent.global_settings_state.set_debug_log(False)
        yield self.agent.global_settings_state.set_debug_log(False)
        yield self.agent.global_settings_state.set_debug_log(True)

        # Give a moment for watches to fire.
        yield self.sleep(0.1)

    @inlineCallbacks
    def test_log_enable_disable(self):
        """The log can be enabled and disabled."""
        root_log = logging.getLogger()
        root_log.setLevel(logging.DEBUG)
        self.capture_logging(None, level=logging.DEBUG)
        yield self.agent.connect()
        self.assertFalse((yield self.client.exists("/logs")))

        yield self.agent.start_debug_log()
        root_log.debug("hello world")
        yield self.agent.stop_debug_log()
        root_log.info("goodbye")
        root_log.info("world")

        entry = yield self.get_log_entry(0)
        self.assertTrue(entry)
        self.assertEqual(entry["levelname"], "DEBUG")
        entry = yield self.get_log_entry(1, wait=False)
        self.assertFalse(entry)

        # Else zookeeper is closing on occassion in teardown
        yield self.sleep(0.1)
