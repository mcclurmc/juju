from StringIO import StringIO
import json
import itertools
import logging
import os
import stat
import sys
import yaml

from twisted.internet import defer
from twisted.internet.process import Process

import juju
from juju import errors
from juju.control.tests.test_status import StatusTestBase
from juju.hooks import invoker
from juju.hooks import commands
from juju.hooks.protocol import UnitSettingsFactory
from juju.lib.mocker import MATCH
from juju.lib.testing import TestCase
from juju.lib.twistutils import get_module_directory
from juju.state import hook
from juju.state.endpoint import RelationEndpoint
from juju.state.tests.test_relation import RelationTestBase


class MockUnitAgent(object):
    """Pretends to implement the client state cache, and the UA hook socket.
    """
    def __init__(self, client, socket_path, charm_dir):
        self.client = client
        self.socket_path = socket_path
        self.charm_dir = charm_dir
        self._clients = {}  # client_id -> HookContext

        self._agent_log = logging.getLogger("unit-agent")
        self._agent_io = StringIO()
        handler = logging.StreamHandler(self._agent_io)
        self._agent_log.addHandler(handler)

        self.server_listen()

    def make_context(self, relation_name, change_type, unit_name,
                     unit_relation, client_id):
        """Create, record and return a HookContext object for a change."""
        change = hook.RelationChange(relation_name, change_type, unit_name)
        context = hook.RelationHookContext(self.client, unit_relation, change,
                                           unit_name=unit_name)
        self._clients[client_id] = context
        return context, change

    def get_logger(self):
        """Build a logger to be used for a hook."""
        logger = logging.getLogger("hook")
        log_file = StringIO()
        handler = logging.StreamHandler(log_file)
        logger.addHandler(handler)
        return logger

    def get_invoker(self, relation_name, change_type, unit_name,
                    unit_relation,
                    client_id="client_id"):
        """Build an Invoker for the execution of a hook.

        `relation_name`: the name of the relation the Invoker is for.
        `change_type`: the string name of the type of change the hook
                       is invoked for.
        `unit_name`: the name of the local unit of the change.
        `unit_relation`: a UnitRelationState instance for the hook.
        `client_id`: unique client identifier.
        `service`: The local service of the executing hook.
        """
        context, change = self.make_context(relation_name, change_type,
                                            unit_name, unit_relation,
                                            client_id)
        logger = self.get_logger()

        exe = invoker.Invoker(context, change,
                              self.get_client_id(),
                              self.socket_path,
                              self.charm_dir,
                              logger)
        return exe

    def get_client_id(self):
        # simulate associating a client_id with a client connection
        # for later context look up. In reality this would be a mapping.
        return "client_id"

    def get_context(self, client_id):
        return self._clients[client_id]

    def stop(self):
        """Stop the process invocation.

        Trigger any registered cleanup functions.
        """
        self.server_socket.stopListening()

    def server_listen(self):
        from twisted.internet import reactor

        # hook context and a logger to the settings factory
        logger = logging.getLogger("unit-agent")
        self.log_file = StringIO()
        handler = logging.StreamHandler(self.log_file)
        logger.addHandler(handler)
        self.server_factory = UnitSettingsFactory(self.get_context, logger)

        self.server_socket = reactor.listenUNIX(
            self.socket_path, self.server_factory)


def get_cli_environ_path(*search_path):
    """Construct a path environment variable.

    This path will contain the juju bin directory and any paths
    passed as *search_path.

    @param search_path: additional directories to put on PATH
    """
    search_path = list(search_path)

    # Look for the top level juju bin directory and make sure
    # that is available for the client utilities.
    bin_path = os.path.normpath(
        os.path.join(get_module_directory(juju), "..", "bin"))

    search_path.append(bin_path)
    search_path.extend(os.environ.get("PATH", "").split(":"))

    return ":".join(search_path)


class InvokerTestBase(TestCase):

    def update_invoker_env(self, local_unit, remote_unit):
        """Update os.env for a hook invocation.

        Update the invoker (and hence the hook) environment with the
        path to the juju cli utils, and the local unit name.
        """
        test_hook_path = os.path.join(
            os.path.abspath(
                os.path.dirname(__file__)).replace("/_trial_temp", ""),
            "hooks")
        self.change_environment(
            PATH=get_cli_environ_path(test_hook_path, "/usr/bin", "/bin"),
            JUJU_UNIT_NAME=local_unit,
            JUJU_REMOTE_UNIT=remote_unit)

    def get_test_hook(self, hook):
        """Search for the test hook under the testing directory.

        Returns the full path name of the hook to be invoked from its
        basename.
        """
        dirname = os.path.dirname(__file__)
        abspath = os.path.abspath(dirname)
        hook_file = os.path.join(abspath, "hooks", hook)

        if not os.path.exists(hook_file):
            # attempt to find it via sys_path
            for p in sys.path:
                hook_file = os.path.normpath(
                    os.path.join(p, dirname, "hooks", hook))
                if os.path.exists(hook_file):
                    return hook_file
            raise IOError("%s doesn't exist" % hook_file)
        return hook_file

    def get_cli_hook(self, hook):
        bin_path = os.path.normpath(
            os.path.join(get_module_directory(juju), "..", "bin"))
        return os.path.join(bin_path, hook)

    def create_hook(self, hook, arguments):
        bin_path = self.get_cli_hook(hook)
        fn = self.makeFile("#!/bin/sh\n%s %s" % (bin_path, arguments))
        # make the hook executable
        os.chmod(fn, stat.S_IEXEC | stat.S_IREAD)
        return fn


class TestCompleteInvoker(InvokerTestBase, StatusTestBase):

    @defer.inlineCallbacks
    def setUp(self):
        yield super(TestCompleteInvoker, self).setUp()

        self.update_invoker_env("mysql/0", "wordpress/0")
        self.socket_path = self.makeFile()
        unit_dir = self.makeDir()
        self.makeDir(path=os.path.join(unit_dir, "charm"))
        self.ua = MockUnitAgent(
            self.client,
            self.socket_path,
            unit_dir)

    @defer.inlineCallbacks
    def tearDown(self):
        self.ua.stop()
        yield super(TestCompleteInvoker, self).tearDown()

    @defer.inlineCallbacks
    def build_default_relationships(self):
        state = yield self.build_topology(skip_unit_agents=("*",))
        myr = yield self.relation_state_manager.get_relations_for_service(
            state["services"]["mysql"])
        self.mysql_relation = yield myr[0].add_unit_state(
            state["relations"]["mysql"][0])
        wpr = yield self.relation_state_manager.get_relations_for_service(
            state["services"]["wordpress"])
        wpr = [r for r in wpr if r.internal_relation_id == \
                 self.mysql_relation.internal_relation_id][0]
        self.wordpress_relation = yield wpr.add_unit_state(
            state["relations"]["wordpress"][0])

        defer.returnValue(state)

    @defer.inlineCallbacks
    def test_get_from_different_unit(self):
        """Verify that relation-get works with a remote unit.

        This test will run the logic of relation-get and will ensure
        that, even though we're running the hook within the context of
        unit A, a hook can obtain the data from unit B using
        relation-get. To do this a more complete simulation of the
        runtime is needed than with the local test cases below.
        """
        yield self.build_default_relationships()
        yield self.wordpress_relation.set_data({"hello": "world"})

        hook_log = self.capture_logging("hook")
        exe = self.ua.get_invoker("db", "add", "mysql/0",
                                  self.mysql_relation,
                                  client_id="client_id")

        yield exe(self.create_hook(
            "relation-get", "--format=json - wordpress/0"))
        self.assertEqual({"hello": "world"},
                         json.loads(hook_log.getvalue()))

    @defer.inlineCallbacks
    def test_spawn_cli_get_hook_no_args(self):
        """Validate the get hook works with no (or all default) args.

        This should default to the remote unit. We do pass a format
        arg so we can marshall the data.
        """
        yield self.build_default_relationships()
        yield self.wordpress_relation.set_data({"hello": "world"})

        hook_log = self.capture_logging("hook")
        exe = self.ua.get_invoker("db", "add", "mysql/0", self.mysql_relation,
                                  client_id="client_id")
        exe.environment["JUJU_REMOTE_UNIT"] = "wordpress/0"

        result = yield exe(self.create_hook("relation-get", "--format=json"))
        self.assertEqual(result, 0)
        # verify that its the wordpress data
        self.assertEqual({"hello": "world"},
                         json.loads(hook_log.getvalue()))

    @defer.inlineCallbacks
    def test_spawn_cli_get_implied_unit(self):
        """Validate the get hook can transmit values to the hook."""
        yield self.build_default_relationships()

        hook_log = self.capture_logging("hook")

        # Populate and verify some data we will
        # later extract with the hook
        expected = {"name": "rabbit",
                    "forgotten": "lyrics",
                    "nottobe": "requested"}
        yield self.wordpress_relation.set_data(expected)

        exe = self.ua.get_invoker("db", "add", "mysql/0", self.mysql_relation,
                                  client_id="client_id")
        exe.environment["JUJU_REMOTE_UNIT"] = "wordpress/0"

        # invoke relation-get and verify the result
        result = yield exe(self.create_hook("relation-get", "--format=json -"))
        self.assertEqual(result, 0)
        data = json.loads(hook_log.getvalue())
        self.assertEqual(data["name"], "rabbit")
        self.assertEqual(data["forgotten"], "lyrics")

    @defer.inlineCallbacks
    def test_spawn_cli_get_format_shell(self):
        """Validate the get hook can transmit values to the hook."""
        yield self.build_default_relationships()

        hook_log = self.capture_logging("hook")

        # Populate and verify some data we will
        # later extract with the hook
        expected = {"name": "rabbit",
                    "forgotten": "lyrics"}
        yield self.wordpress_relation.set_data(expected)

        exe = self.ua.get_invoker("db", "add", "mysql/0", self.mysql_relation,
                                  client_id="client_id")
        exe.environment["JUJU_REMOTE_UNIT"] = "wordpress/0"

        # invoke relation-get and verify the result
        result = yield exe(
            self.create_hook("relation-get", "--format=shell -"))
        self.assertEqual(result, 0)
        data = hook_log.getvalue()

        self.assertEqual('VAR_FORGOTTEN=lyrics\nVAR_NAME=rabbit\n\n', data)

        # and with a single value request
        hook_log.truncate(0)
        result = yield exe(
            self.create_hook("relation-get", "--format=shell name"))
        self.assertEqual(result, 0)
        data = hook_log.getvalue()
        self.assertEqual('VAR_NAME=rabbit\n\n', data)

    @defer.inlineCallbacks
    def test_relation_get_format_shell_bad_vars(self):
        """If illegal values are make somehow available warn."""
        yield self.build_default_relationships()
        hook_log = self.capture_logging("hook")

        # Populate and verify some data we will
        # later extract with the hook
        expected = {"BAR": "none", "funny-chars*":  "should work"}
        yield self.wordpress_relation.set_data(expected)

        exe = self.ua.get_invoker("db", "add", "mysql/0", self.mysql_relation,
                                  client_id="client_id")
        exe.environment["JUJU_REMOTE_UNIT"] = "wordpress/0"
        exe.environment["VAR_FOO"] = "jungle"

        result = yield exe(
            self.create_hook("relation-get", "--format=shell -"))
        self.assertEqual(result, 0)

        yield exe.ended
        data = hook_log.getvalue()
        self.assertIn('VAR_BAR=none', data)
        # Verify that illegal shell variable names get converted
        # in an expected way
        self.assertIn("VAR_FUNNY_CHARS_='should work'", data)

        # Verify that it sets VAR_FOO to null because it shouldn't
        # exist in the environment
        self.assertIn("VAR_FOO=", data)
        self.assertIn("The following were omitted from", data)

    @defer.inlineCallbacks
    def test_hook_exec_in_charm_directory(self):
        """Hooks are executed in the charm directory."""
        yield self.build_default_relationships()
        hook_log = self.capture_logging("hook")
        exe = self.ua.get_invoker("db", "add", "mysql/0", self.mysql_relation,
                                  client_id="client_id")
        self.assertTrue(os.path.isdir(exe.unit_path))
        exe.environment["JUJU_REMOTE_UNIT"] = "wordpress/0"

        # verify the hook's execution directory
        hook_path = self.makeFile("#!/bin/bash\necho $PWD")
        os.chmod(hook_path, stat.S_IEXEC | stat.S_IREAD)
        result = yield exe(hook_path)
        self.assertEqual(hook_log.getvalue().strip(),
                         os.path.join(exe.unit_path, "charm"))
        self.assertEqual(result, 0)

        # Reset the output capture
        hook_log.seek(0)
        hook_log.truncate()

        # Verify the environment variable is set.
        hook_path = self.makeFile("#!/bin/bash\necho $CHARM_DIR")
        os.chmod(hook_path, stat.S_IEXEC | stat.S_IREAD)
        result = yield exe(hook_path)
        self.assertEqual(hook_log.getvalue().strip(),
                         os.path.join(exe.unit_path, "charm"))

    @defer.inlineCallbacks
    def test_spawn_cli_config_get(self):
        """Validate that config-get returns expected values."""
        yield self.build_default_relationships()

        hook_log = self.capture_logging("hook")

        # Populate and verify some data we will
        # later extract with the hook
        expected = {"name": "rabbit",
                    "forgotten": "lyrics",
                    "nottobe": "requested"}

        exe = self.ua.get_invoker("db", "add", "mysql/0", self.mysql_relation,
                                  client_id="client_id")

        context = yield self.ua.get_context("client_id")
        config = yield context.get_config()
        config.update(expected)
        yield config.write()

        # invoke relation-get and verify the result

        result = yield exe(self.create_hook("config-get", "--format=json"))
        self.assertEqual(result, 0)

        data = json.loads(hook_log.getvalue())
        self.assertEqual(data["name"], "rabbit")
        self.assertEqual(data["forgotten"], "lyrics")


class RelationInvokerTestBase(InvokerTestBase, RelationTestBase):

    @defer.inlineCallbacks
    def setUp(self):
        yield super(RelationInvokerTestBase, self).setUp()
        yield self._default_relations()
        self.update_invoker_env("mysql/0", "wordpress/0")
        self.socket_path = self.makeFile()
        unit_dir = self.makeDir()
        self.makeDir(path=os.path.join(unit_dir, "charm"))
        self.ua = MockUnitAgent(
            self.client,
            self.socket_path,
            unit_dir)
        self.log = self.capture_logging(
            formatter=logging.Formatter(
                "%(name)s:%(levelname)s:: %(message)s"),
            level=logging.DEBUG)

    @defer.inlineCallbacks
    def tearDown(self):
        self.ua.stop()
        yield super(RelationInvokerTestBase, self).tearDown()

    @defer.inlineCallbacks
    def _default_relations(self):
        wordpress_ep = RelationEndpoint(
            "wordpress", "client-server", "app", "client")
        mysql_ep = RelationEndpoint(
            "mysql", "client-server", "db", "server")
        self.wordpress_states = yield self.\
            add_relation_service_unit_from_endpoints(wordpress_ep, mysql_ep)
        self.mysql_states = yield self.add_opposite_service_unit(
            self.wordpress_states)
        self.relation = self.mysql_states["unit_relation"]


class InvokerTest(RelationInvokerTestBase):

    def test_environment(self):
        """Test various way to manipulate the calling environment.
        """
        exe = self.ua.get_invoker("database", "add", "mysql/0", self.relation)
        exe.environment.update(dict(FOO="bar"))
        env = exe.get_environment()

        # these come from the init argument
        self.assertEqual(env["JUJU_AGENT_SOCKET"], self.socket_path)
        self.assertEqual(env["JUJU_CLIENT_ID"], "client_id")

        # this comes from updating the Invoker.environment
        self.assertEqual(env["FOO"], "bar")

        # and this comes from the unit agent passing through its environment
        self.assertTrue(env["PATH"])
        self.assertEqual(env["JUJU_UNIT_NAME"], "mysql/0")

        # Set for all hooks
        self.assertEqual(env["DEBIAN_FRONTEND"], "noninteractive")
        self.assertEqual(env["APT_LISTCHANGES_FRONTEND"], "none")

    def test_missing_hook(self):
        exe = self.ua.get_invoker("database", "add", "mysql/0", self.relation)
        self.failUnlessRaises(errors.FileNotFound, exe, "hook-missing")

    def test_noexec_hook(self):
        exe = self.ua.get_invoker("database", "add", "mysql/0",
                                  self.relation)
        hook = self.get_test_hook("noexec-hook")
        error = self.failUnlessRaises(errors.CharmError, exe, hook)

        self.assertEqual(error.path, hook)
        self.assertEqual(error.message, "hook is not executable")

    @defer.inlineCallbacks
    def test_spawn_success(self):
        """Validate hook with success from exit."""
        exe = self.ua.get_invoker("database", "add", "mysql/0", self.relation)
        result = yield exe(self.get_test_hook("success-hook"))
        self.assertEqual(result, 0)
        yield exe.ended
        self.assertIn("WIN", self.log.getvalue())
        self.assertIn("exit code 0", self.log.getvalue())

    @defer.inlineCallbacks
    def test_spawn_fail(self):
        """Validate hook with fail from exit."""
        exe = self.ua.get_invoker("database", "add", "mysql/0", self.relation)
        d = exe(self.get_test_hook("fail-hook"))
        result = yield self.assertFailure(d, errors.CharmInvocationError)
        self.assertEqual(result.exit_code, 1)
        # ERROR indicate the level name, we are checking that the
        # proper level was logged here
        yield exe.ended
        self.assertIn("ERROR", self.log.getvalue())
        # and the message
        self.assertIn("FAIL", self.log.getvalue())
        self.assertIn("exit code 1", self.log.getvalue())

    @defer.inlineCallbacks
    def test_hanging_hook(self):
        """Verify that a hook that's slow to end is terminated.

        Test this by having the hook fork a process that hangs around
        for a while, necessitating reaping. This happens because the
        child process does not close the parent's file descriptors (as
        expected with daemonization, for example).

        http://www.snailbook.com/faq/background-jobs.auto.html
        provides some insight into what can happen.
        """
        from twisted.internet import reactor

        # Ordinarily the reaper for any such hanging hooks will run in
        # 5s, but we are impatient. Force it to end much sooner by
        # intercepting the reaper setup.
        mock_reactor = self.mocker.patch(reactor)

        # Although we can match precisely on the
        # Process.loseConnection, Mocker gets confused with also
        # trying to match the delay time, using something like
        # `MATCH(lambda x: isinstance(x, (int, float)))`. So instead
        # we hardcode it here as just 5.
        mock_reactor.callLater(
            5, MATCH(lambda x: isinstance(x.im_self, Process)))

        def intercept_reaper_setup(delay, reaper):
            # Given this is an external process, let's sleep for a
            # short period of time
            return reactor.callLater(0.2, reaper)

        self.mocker.call(intercept_reaper_setup)
        self.mocker.replay()

        # The hook script will immediately exit with a status code of
        # 0, but it created a child process (via shell backgrounding)
        # that is running (and will sleep for >10s)
        exe = self.ua.get_invoker("database", "add", "mysql/0", self.relation)
        result = yield exe(self.get_test_hook("hanging-hook"))
        self.assertEqual(result, 0)

        # Verify after waiting for the process to close (which means
        # the reaper ran!), we get output for the first phase of the
        # hanging hook, but not after its second, more extended sleep.
        yield exe.ended
        self.assertIn("Slept for 50ms", self.log.getvalue())
        self.assertNotIn("Slept for 1s", self.log.getvalue())

        # Lastly there's a nice long sleep that would occur after the
        # default timeout of this test. Successful completion of this
        # test without a timeout means this sleep was never executed.

    def test_path_setup(self):
        """Validate that the path allows finding the executable."""
        from twisted.python.procutils import which
        exe = which("relation-get")
        self.assertTrue(exe)
        self.assertTrue(exe[0].endswith("relation-get"))

    @defer.inlineCallbacks
    def test_spawn_cli_get_hook(self):
        """Validate the get hook can transmit values to the hook"""
        hook_log = self.capture_logging("hook")
        exe = self.ua.get_invoker("database", "add", "mysql/0",
                                  self.relation,
                                  client_id="client_id")

        # Populate and verify some data we will
        # later extract with the hook
        context = self.ua.get_context("client_id")
        expected = {"a": "b", "c": "d",
                    "private-address": "mysql-0.example.com"}
        yield context.set(expected)
        data = yield context.get("mysql/0")

        self.assertEqual(expected, data)

        # invoke the hook and process the results
        # verifying they are expected
        result = yield exe(self.create_hook("relation-get",
                                             "--format=json - mysql/0"))
        self.assertEqual(result, 0)
        data = hook_log.getvalue()
        self.assertEqual(json.loads(data), expected)

    @defer.inlineCallbacks
    def test_spawn_cli_get_value_hook(self):
        """Validate the get hook can transmit values to the hook."""
        hook_log = self.capture_logging("hook")
        exe = self.ua.get_invoker("database", "add", "mysql/0",
                                  self.relation,
                                  client_id="client_id")

        # Populate and verify some data we will
        # later extract with the hook
        context = self.ua.get_context("client_id")
        expected = {"name": "rabbit", "private-address": "mysql-0.example.com"}
        yield context.set(expected)
        data = yield context.get("mysql/0")

        self.assertEqual(expected, data)

        # invoke the hook and process the results
        # verifying they are expected
        result = yield exe(self.create_hook("relation-get",
                                            "--format=json name mysql/0"))
        self.assertEqual(result, 0)
        data = hook_log.getvalue()
        self.assertEqual("rabbit", json.loads(data))

    @defer.inlineCallbacks
    def test_spawn_cli_get_unit_private_address(self):
        """Private addresses can be retrieved."""
        hook_log = self.capture_logging("hook")
        exe = self.ua.get_invoker("database", "add", "mysql/0",
                                  self.relation,
                                  client_id="client_id")
        result = yield exe(self.create_hook("unit-get", "private-address"))
        self.assertEqual(result, 0)
        data = hook_log.getvalue()
        self.assertEqual("mysql-0.example.com", data.strip())

    @defer.inlineCallbacks
    def test_spawn_cli_get_unit_unknown_public_address(self):
        """If for some hysterical raison, the public address hasn't been set.

        We shouldn't error. This should never happen, the unit agent is sets
        it on startup.
        """
        hook_log = self.capture_logging("hook")
        exe = self.ua.get_invoker("database", "add", "mysql/0",
                                  self.relation,
                                  client_id="client_id")

        result = yield exe(self.create_hook("unit-get", "public-address"))
        self.assertEqual(result, 0)
        data = hook_log.getvalue()
        self.assertEqual("", data.strip())

    def test_get_remote_unit_arg(self):
        """Simple test around remote arg parsing."""
        self.change_environment(JUJU_UNIT_NAME="mysql/0",
                                JUJU_CLIENT_ID="client_id",
                                JUJU_AGENT_SOCKET=self.socket_path)
        client = commands.RelationGetCli()
        client.setup_parser()
        options = client.parse_args(["-", "mysql/1"])
        self.assertEqual(options.unit_name, "mysql/1")

    @defer.inlineCallbacks
    def test_spawn_cli_set_hook(self):
        """Validate the set hook can set values in zookeeper."""
        output = self.capture_logging("hook", level=logging.DEBUG)
        exe = self.ua.get_invoker("database", "add", "mysql/0",
                                  self.relation,
                                  client_id="client_id")

        # Invoke the hook and process the results verifying they are expected
        hook = self.create_hook("relation-set", "a=b c=d")
        result = yield exe(hook)
        self.assertEqual(result, 0)

        # Verify the context was flushed to zk
        zk_data = yield self.relation.get_data()
        self.assertEqual(
            {"a": "b", "c": "d", "private-address": "mysql-0.example.com"},
            yaml.load(zk_data))
        yield exe.ended
        self.assertIn(
            "Flushed values for hook %r\n"
            "    Setting changed: u'a'=u'b' (was unset)\n"
            "    Setting changed: u'c'=u'd' (was unset)" % (
                os.path.basename(hook)),
            output.getvalue())

    @defer.inlineCallbacks
    def test_spawn_cli_set_can_delete_and_modify(self):
        """Validate the set hook can delete values in zookeeper."""
        output = self.capture_logging("hook", level=logging.DEBUG)
        hook_directory = self.makeDir()
        hook_file_path = self.makeFile(
            content=("#!/bin/bash\n"
                     "relation-set existing= absent= new-value=2 "
                     "changed=abc changed2=xyz\n"
                     "exit 0\n"),
            basename=os.path.join(hook_directory, "set-delete-test"))
        os.chmod(hook_file_path, stat.S_IRWXU)
        exe = self.ua.get_invoker("database", "add", "mysql/1",
                                  self.relation,
                                  client_id="client_id")

        # Populate with data that will be deleted
        context = self.ua.get_context("client_id")
        yield context.set(
            {u"existing": u"42",
             u"changed": u"a" * 101,
             u"changed2": u"a" * 100})
        yield context.flush()

        # Invoke the hook and process the results verifying they are expected
        self.assertTrue(os.path.exists(hook_file_path))
        result = yield exe(hook_file_path)
        self.assertEqual(result, 0)

        # Verify the context was flushed to zk
        zk_data = yield self.relation.get_data()
        self.assertEqual(
            {"new-value": "2", "changed": "abc", "changed2": "xyz",
             "private-address": "mysql-0.example.com"},
            yaml.load(zk_data))

        # Verify that unicode/strings longer than 100 characters in
        # representation (including quotes and the u marker) are cut
        # off; 100 is the default cutoff used in the change items
        # __str__ method
        yield exe.ended
        self.assertIn(
            "Flushed values for hook 'set-delete-test'\n"
            "    Setting changed: u'changed'=u'abc' (was u'%s)\n"
            "    Setting changed: u'changed2'=u'xyz' (was u'%s)\n"
            "    Setting deleted: u'existing' (was u'42')\n"
            "    Setting changed: u'new-value'=u'2' (was unset)" % (
                "a" * 98, "a" * 98),
            output.getvalue())

    @defer.inlineCallbacks
    def test_spawn_cli_set_noop_only_logs_on_change(self):
        """Validate the set hook only logs flushes when there are changes."""
        output = self.capture_logging("hook", level=logging.DEBUG)
        hook_directory = self.makeDir()
        hook_file_path = self.makeFile(
            content=("#!/bin/bash\n"
                     "relation-set no-change=42 absent=\n"
                     "exit 0\n"),
            basename=os.path.join(hook_directory, "set-does-nothing"))
        os.chmod(hook_file_path, stat.S_IRWXU)
        exe = self.ua.get_invoker("database", "add", "mysql/1",
                                  self.relation,
                                  client_id="client_id")

        # Populate with data that will be *not* be modified
        context = self.ua.get_context("client_id")
        yield context.set({"no-change": "42", "untouched": "xyz"})
        yield context.flush()

        # Invoke the hook and process the results verifying they are expected
        self.assertTrue(os.path.exists(hook_file_path))
        result = yield exe(hook_file_path)
        self.assertEqual(result, 0)

        # Verify the context was flushedto zk
        zk_data = yield self.relation.get_data()
        self.assertEqual({"no-change": "42", "untouched": "xyz",
                          "private-address": "mysql-0.example.com"},
                         yaml.load(zk_data))
        self.assertNotIn(
            "Flushed values for hook 'set-does-nothing'",
            output.getvalue())

    @defer.inlineCallbacks
    def test_logging(self):
        exe = self.ua.get_invoker("database", "add", "mysql/0", self.relation)

        # The echo hook will echo out the value
        # it will also echo to stderr the ERROR variable
        message = "This is only a test"
        error = "All is full of fail"
        default = "Default level"

        exe.environment["MESSAGE"] = message
        exe.environment["ERROR"] = error
        exe.environment["DEFAULT"] = default
        # of the MESSAGE variable
        result = yield exe(self.get_test_hook("echo-hook"))
        self.assertEqual(result, 0)

        yield exe.ended
        self.assertIn(message, self.log.getvalue())
        # The 'error' was sent via juju-log
        # to the UA. Our test UA has a fake log stream
        # which we can check now
        output = self.ua.log_file.getvalue()
        self.assertIn("ERROR:: " + error, self.log.getvalue())
        self.assertIn("INFO:: " + default, self.log.getvalue())

        assert message not in output, """Log includes unintended messages"""

    @defer.inlineCallbacks
    def test_spawn_cli_list_hook_smart(self):
        """Validate the get hook can transmit values to the hook."""
        exe = self.ua.get_invoker("database", "add", "mysql/0",
                                  self.relation,
                                  client_id="client_id")

        # Populate and verify some data we will
        # later extract with the hook
        context = self.ua.get_context("client_id")

        # directly manipulate the context to the expected list of
        # members
        expected = ["alpha/0", "beta/0"]
        context._members = expected

        # invoke the hook and process the results
        # verifying they are expected
        exe.environment["FORMAT"] = "smart"
        result = yield exe(self.create_hook("relation-list",
                                            "--format=smart"))

        self.assertEqual(result, 0)
        yield exe.ended
        self.assertIn("alpha/0\nbeta/0\n", self.log.getvalue())

    @defer.inlineCallbacks
    def test_spawn_cli_list_hook_eval(self):
        """Validate the get hook can transmit values to the hook."""
        exe = self.ua.get_invoker("database", "add", "mysql/0",
                                  self.relation,
                                  client_id="client_id")

        # Populate and verify some data we will
        # later extract with the hook
        context = self.ua.get_context("client_id")

        # directly manipulate the context to the expected list of
        # members
        expected = ["alpha/0", "beta/0"]
        context._members = expected

        # invoke the hook and process the results
        # verifying they are expected
        result = yield exe(self.create_hook("relation-list",
                                            "--format=eval"))

        self.assertEqual(result, 0)
        yield exe.ended
        self.assertIn("alpha/0 beta/0", self.log.getvalue())

    @defer.inlineCallbacks
    def test_spawn_cli_list_hook_json(self):
        """Validate the get hook can transmit values to the hook."""
        exe = self.ua.get_invoker("database", "add", "mysql/0",
                                  self.relation,
                                  client_id="client_id")

        # Populate and verify some data we will
        # later extract with the hook
        context = self.ua.get_context("client_id")

        # directly manipulate the context to the expected list of
        # members
        expected = ["alpha/0", "beta/0"]
        context._members = expected

        # invoke the hook and process the results
        # verifying they are expected
        result = yield exe(self.create_hook("relation-list", "--format json"))

        self.assertEqual(result, 0)
        yield exe.ended
        self.assertIn('["alpha/0", "beta/0"]', self.log.getvalue())


class PortCommandsTest(RelationInvokerTestBase):

    def assertLogLines(self, expected):
        """Asserts that the lines of `expected` exist in order in the log."""
        logged = self.log.getvalue().split("\n")
        expected = iter(expected)
        for line in logged:
            expected, peekat = itertools.tee(expected)
            peeked = next(peekat)
            if peeked in line:
                next(expected)  # then consume this line and move on

        self.assertFalse(expected,
                         "Did not see all expected lines in log, in order")

    def test_path_setup(self):
        """Validate that the path allows finding the executable."""
        from twisted.python.procutils import which
        open_port_exe = which("open-port")
        self.assertTrue(open_port_exe)
        self.assertTrue(open_port_exe[0].endswith("open-port"))

        close_port_exe = which("close-port")
        self.assertTrue(close_port_exe)
        self.assertTrue(close_port_exe[0].endswith("close-port"))

    @defer.inlineCallbacks
    def test_open_and_close_ports(self):
        """Verify that port hook commands run and changes are immediate."""
        unit_state = self.mysql_states["unit"]
        self.assertEqual((yield unit_state.get_open_ports()), [])

        exe = self.ua.get_invoker("database", "add", "mysql/0", self.relation)
        result = yield exe(self.create_hook("open-port", "80"))
        self.assertEqual(result, 0)
        self.assertEqual(
            (yield unit_state.get_open_ports()),
            [{"port": 80, "proto": "tcp"}])

        result = yield exe(self.create_hook("open-port", "53/udp"))
        self.assertEqual(result, 0)
        self.assertEqual(
            (yield unit_state.get_open_ports()),
            [{"port": 80, "proto": "tcp"},
             {"port": 53, "proto": "udp"}])

        result = yield exe(self.create_hook("open-port", "53/tcp"))
        self.assertEqual(result, 0)
        self.assertEqual(
            (yield unit_state.get_open_ports()),
            [{"port": 80, "proto": "tcp"},
             {"port": 53, "proto": "udp"},
             {"port": 53, "proto": "tcp"}])

        result = yield exe(self.create_hook("open-port", "443/tcp"))
        self.assertEqual(result, 0)
        self.assertEqual(
            (yield unit_state.get_open_ports()),
            [{"port": 80, "proto": "tcp"},
             {"port": 53, "proto": "udp"},
             {"port": 53, "proto": "tcp"},
             {"port": 443, "proto": "tcp"}])

        result = yield exe(self.create_hook("close-port", "80/tcp"))
        self.assertEqual(result, 0)
        self.assertEqual(
            (yield unit_state.get_open_ports()),
            [{"port": 53, "proto": "udp"},
             {"port": 53, "proto": "tcp"},
             {"port": 443, "proto": "tcp"}])

        yield exe.ended
        self.assertLogLines([
                "opened 80/tcp",
                "opened 53/udp",
                "opened 443/tcp",
                "closed 80/tcp"])

    @defer.inlineCallbacks
    def test_open_port_args(self):
        """Verify that open-port properly reports arg parse errors."""
        exe = self.ua.get_invoker("database", "add", "mysql/0", self.relation)

        result = yield self.assertFailure(
            exe(self.create_hook("open-port", "80/invalid-protocol")),
            errors.CharmInvocationError)
        self.assertEqual(result.exit_code, 2)
        yield exe.ended
        self.assertIn(
            "open-port: error: argument PORT[/PROTOCOL]: "
            "Invalid protocol, must be 'tcp' or 'udp', got 'invalid-protocol'",
            self.log.getvalue())

        result = yield self.assertFailure(
            exe(self.create_hook("open-port", "0/tcp")),
            errors.CharmInvocationError)
        self.assertEqual(result.exit_code, 2)
        yield exe.ended
        self.assertIn(
            "open-port: error: argument PORT[/PROTOCOL]: "
            "Invalid port, must be from 1 to 65535, got 0",
            self.log.getvalue())

        result = yield self.assertFailure(
            exe(self.create_hook("open-port", "80/udp/extra-info")),
            errors.CharmInvocationError)
        self.assertEqual(result.exit_code, 2)
        yield exe.ended
        self.assertIn(
            "open-port: error: argument PORT[/PROTOCOL]: "
            "Invalid format for port/protocol, got '80/udp/extra-info",
            self.log.getvalue())

    @defer.inlineCallbacks
    def test_close_port_args(self):
        """Verify that close-port properly reports arg parse errors."""
        exe = self.ua.get_invoker("database", "add", "mysql/0", self.relation)

        result = yield self.assertFailure(
            exe(self.create_hook("close-port", "80/invalid-protocol")),
            errors.CharmInvocationError)
        self.assertEqual(result.exit_code, 2)
        yield exe.ended
        self.assertIn(
            "close-port: error: argument PORT[/PROTOCOL]: "
            "Invalid protocol, must be 'tcp' or 'udp', got 'invalid-protocol'",
            self.log.getvalue())

        result = yield self.assertFailure(
            exe(self.create_hook("close-port", "0/tcp")),
            errors.CharmInvocationError)
        self.assertEqual(result.exit_code, 2)
        yield exe.ended
        self.assertIn(
            "close-port: error: argument PORT[/PROTOCOL]: "
            "Invalid port, must be from 1 to 65535, got 0",
            self.log.getvalue())

        result = yield self.assertFailure(
            exe(self.create_hook("close-port", "80/udp/extra-info")),
            errors.CharmInvocationError)
        self.assertEqual(result.exit_code, 2)
        yield exe.ended
        self.assertIn(
            "close-port: error: argument PORT[/PROTOCOL]: "
            "Invalid format for port/protocol, got '80/udp/extra-info",
            self.log.getvalue())
