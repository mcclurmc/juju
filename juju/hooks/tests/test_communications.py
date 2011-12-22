from StringIO import StringIO
import logging

from twisted.internet import protocol, defer, error

from juju.hooks.protocol import (UnitAgentClient, UnitAgentServer,
                                 UnitSettingsFactory, NoSuchUnit,
                                 NoSuchKey)

from juju.lib.mocker import ANY
from juju.lib.testing import TestCase
from juju.lib.twistutils import gather_results
from juju.state.errors import UnitRelationStateNotFound


def _loseAndPass(err, proto):
    # be specific, pass on the error to the client.
    err.trap(error.ConnectionLost, error.ConnectionDone)
    del proto.connectionLost
    proto.connectionLost(err)


class UnitAgentServerMock(UnitAgentServer):
    def _get_data(self):
        return self.factory.data

    def _set_data(self, dictlike):
        """
        protected method used in testing to rewrite internal data state
        """
        self.factory.data = dictlike

    data = property(_get_data, _set_data)

    def _set_members(self, members):
        # replace the content of the current list
        self.factory.members = members

    @property
    def config(self):
        return self.factory.config

    def config_set(self, dictlike):
        """Write service state directly. """
        self.factory.config_set(dictlike)


class MockServiceUnitState(object):

    def __init__(self):
        self.ports = set()
        self.config = {}

    def open_port(self, port, proto):
        self.ports.add((port, proto))

    def close_port(self, port, proto):
        self.ports.discard((port, proto))

    def get_public_address(self):
        return self.config.get("public-address", "")

    def get_private_address(self):
        return self.config.get("private-address", "")


MockServiceUnitState = MockServiceUnitState()


class MockServiceState(object):

    def get_unit_state(self, unit_name):
        return MockServiceUnitState


class UnitSettingsFactoryLocal(UnitSettingsFactory):
    """
    For testing a UnitSettingsFactory with local storage. Loosely
    mimics a HookContext
    """
    protocol = UnitAgentServerMock

    def __init__(self):
        super(UnitSettingsFactoryLocal, self).__init__(self.context_provider)
        self.data = {}  # relation data
        self.config = {}  # service options
        self.members = []
        self._agent_io = StringIO()

        # hook context and a logger to the settings factory
        logger = logging.getLogger("unit-settings-fact-test")
        handler = logging.StreamHandler(self._agent_io)
        handler.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
        logger.addHandler(handler)
        base = super(UnitSettingsFactoryLocal, self)
        base.__init__(self.context_provider, logger)

    def context_provider(self, client_id):
        return self

    def get_value(self, unit_name, setting_name):
        return self.data[unit_name][setting_name]

    def get(self, unit_name):
        # Currently this is cheating as the real impl can
        if unit_name not in self.data:
            # raise it with fake data
            raise UnitRelationStateNotFound("mysql/1",
                                            "server",
                                            unit_name)
        return self.data[unit_name]

    def get_members(self):
        return self.members

    def set_value(self, key, value):
        self.data.setdefault(self._unit_name, {})[key] = value

    def set(self, blob):
        self.data[self._unit_name] = blob

    def _set_unit_name(self, unit_name):
        self._unit_name = unit_name

    def config_set(self, data):
        """Directly update service options for testing."""
        self.config.update(data)

    def get_config(self, option_name=None):
        d = self.config.copy()
        if option_name:
            d = d[option_name]
        return d

    def get_local_unit_state(self):
        return MockServiceUnitState


class LiveFireBase(TestCase):
    """
    Utility for connected reactor-using tests.
    """
    def _listen_server(self, addr):
        from twisted.internet import reactor

        self.server_factory = UnitSettingsFactoryLocal()
        self.server_socket = reactor.listenUNIX(addr, self.server_factory)
        self.addCleanup(self.server_socket.stopListening)
        return self.server_socket

    def _connect_client(self, addr):
        from twisted.internet import reactor
        d = protocol.ClientCreator(
            reactor, self.client_protocol).connectUNIX(addr)
        return d

    def setUp(self):
        """
        Create an amp server and connect a client to it.
        """
        super(LiveFireBase, self).setUp()
        sock = self.makeFile()

        self._listen_server(sock)
        on_client_connect = self._connect_client(sock)

        def getProtocols(results):
            [(_, client), (_, server)] = results
            self.client = client
            self.server = server

        dl = defer.DeferredList([on_client_connect,
                                 self.server_factory.onMade])
        return dl.addCallback(getProtocols)

    def tearDown(self):
        """
        Cleanup client and server connections, and check the error got at
        C{connectionLost}.
        """
        L = []

        for conn in self.client, self.server:
            if conn.transport is not None:
                # depend on amp's function connection-dropping behavior
                d = defer.Deferred().addErrback(_loseAndPass, conn)
                conn.connectionLost = d.errback
                conn.transport.loseConnection()
                L.append(d)

        super(LiveFireBase, self).tearDown()
        return gather_results(L)


class TestCommunications(LiveFireBase):
    """
   Verify that client and server can communicate with the proper
    protocol.
    """
    client_protocol = UnitAgentClient
    server_protocol = UnitAgentServer

    @defer.inlineCallbacks
    def setUp(self):
        yield super(TestCommunications, self).setUp()
        self.log = self.capture_logging(
            level=logging.DEBUG,
            formatter=logging.Formatter("%(levelname)s %(message)s"))

    @defer.inlineCallbacks
    def test_relation_get_command(self):
        # Allow our testing class to pass the usual guard
        require_test_context = self.mocker.replace(
            "juju.hooks.protocol.require_relation_context")
        require_test_context(ANY)
        self.mocker.result(True)
        self.mocker.count(3)
        self.mocker.replay()

        # provide fake data to the server so the client can test it for
        # verification
        self.server.data = dict(test_node=dict(a="b", foo="bar"))
        self.assertIn("test_node", self.server.factory.data)

        data = yield self.client.relation_get("client_id", "test_node", "a")
        self.assertEquals(data, "b")

        data = yield self.client.relation_get("client_id", "test_node", "foo")
        self.assertEquals(data, "bar")

        # A request for <empty_string> asks for all the settings
        data = yield self.client.relation_get("client_id", "test_node", "")
        self.assertEquals(data["a"], "b")
        self.assertEquals(data["foo"], "bar")

    @defer.inlineCallbacks
    def test_get_no_such_unit(self):
        """
        An attempt to retrieve a value for a nonexistant unit raises
        an appropriate error.
        """
        # Allow our testing class to pass the usual guard
        require_test_context = self.mocker.replace(
            "juju.hooks.protocol.require_relation_context")
        require_test_context(ANY)
        self.mocker.result(True)
        self.mocker.replay()

        yield self.assertFailure(
            self.client.relation_get("client_id", "missing_unit/99", ""),
            NoSuchUnit)

    @defer.inlineCallbacks
    def test_relation_with_nonrelation_context(self):
        """
        Verify that using a non-relation context doesn't allow for the
        calling of relation commands and that an appropriate error is
        available.
        """
        # Allow our testing class to pass the usual guard
        from juju.hooks.protocol import NotRelationContext

        failure = self.client.relation_get("client_id", "missing_unit/99", "")
        yield self.assertFailure(failure, NotRelationContext)

    @defer.inlineCallbacks
    def test_relation_set_command(self):
        # Allow our testing class to pass the usual guard
        require_test_context = self.mocker.replace(
            "juju.hooks.protocol.require_relation_context")
        require_test_context(ANY)
        self.mocker.result(True)
        self.mocker.replay()

        self.assertEquals(self.server.data, {})

        # for testing mock the context being stored in the factory
        self.server_factory._set_unit_name("test_node")

        result = yield self.client.relation_set("client_id",
                                       dict(a="b", foo="bar"))

        # set returns nothing
        self.assertEqual(result, None)

        # verify the data exists in the server now
        self.assertTrue(self.server.data)
        self.assertEquals(self.server.data["test_node"]["a"], "b")
        self.assertEquals(self.server.data["test_node"]["foo"], "bar")

    @defer.inlineCallbacks
    def test_list_relations(self):
        # Allow our testing class to pass the usual guard
        require_test_context = self.mocker.replace(
            "juju.hooks.protocol.require_relation_context")
        require_test_context(ANY)
        self.mocker.result(True)
        self.mocker.replay()

        self.server.factory.relation_type = "peer"
        self.server._set_members(["riak/1", "riak/2"])

        members = yield self.client.list_relations("client_id")
        self.assertIn("riak/1", members)
        self.assertIn("riak/2", members)

    @defer.inlineCallbacks
    def test_log_command(self):
        # This is the default calling convention from clients
        yield self.client.log(logging.WARNING, ["This", "is", "a", "WARNING"])
        yield self.client.log(logging.INFO, "This is INFO")
        yield self.client.log(logging.CRITICAL, ["This is CRITICAL"])

        self.assertIn("WARNING This is a WARNING", self.log.getvalue())
        self.assertIn("INFO This is INFO", self.log.getvalue())
        self.assertIn("CRITICAL This is CRITICAL", self.log.getvalue())

    @defer.inlineCallbacks
    def test_config_get_command(self):
        """Verify ConfigGetCommand.

        Test that the communication between the client and server side
        of the protocol is marshalling data as expected. Using mock
        data and services this exists only to test that
        self.client.config_get is returning expected data.
        """
        self.server.config_set(dict(a="b", foo="bar"))
        data = yield self.client.config_get("client_id", "a")
        self.assertEquals(data, "b")

        data = yield self.client.config_get("client_id", "foo")
        self.assertEquals(data, "bar")

        # A request for <empty_string> asks for all the settings
        data = yield self.client.config_get("client_id", "")
        self.assertEquals(data["a"], "b")
        self.assertEquals(data["foo"], "bar")

        # test with valid option names
        data = yield self.client.config_get("client_id", "a")
        self.assertEquals(data, "b")

        data = yield self.client.config_get("client_id", "foo")
        self.assertEquals(data, "bar")

        # test with invalid option name
        data = yield self.client.config_get("client_id", "missing")
        self.assertEquals(data, None)

    @defer.inlineCallbacks
    def test_port_commands(self):
        mock_service_unit_state = MockServiceState().get_unit_state("mock/0")
        yield self.client.open_port("client-id", 80, "tcp")
        self.assertEqual(mock_service_unit_state.ports, set([(80, "tcp")]))
        yield self.client.open_port("client-id", 53, "udp")
        yield self.client.close_port("client-id", 80, "tcp")
        self.assertEqual(mock_service_unit_state.ports, set([(53, "udp")]))
        yield self.client.close_port("client-id", 53, "udp")
        self.assertEqual(mock_service_unit_state.ports, set())
        self.assertIn(
            "DEBUG opened 80/tcp\n"
            "DEBUG opened 53/udp\n"
            "DEBUG closed 80/tcp\n"
            "DEBUG closed 53/udp\n",
            self.log.getvalue())

    @defer.inlineCallbacks
    def test_unit_get_commands(self):
        mock_service_unit_state = MockServiceState().get_unit_state("mock/0")
        mock_service_unit_state.config["public-address"] = "foobar.example.com"
        value = yield self.client.get_unit_info("client-id", "public-address")
        self.assertEqual(value, {"data": "foobar.example.com"})
        yield self.assertFailure(
            self.client.get_unit_info("client-id", "garbage"), NoSuchKey)
        # Shouldn't ever happen in practice (unit agent inits on startup)
        value = yield self.client.get_unit_info("client-id", "private-address")
        self.assertEqual(value, {"data": ""})
