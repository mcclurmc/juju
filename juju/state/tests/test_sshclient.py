import collections
import socket
import time

from twisted.internet.process import Process
from twisted.internet.protocol import ProcessProtocol
from twisted.internet.defer import succeed, fail, Deferred, inlineCallbacks

from txzookeeper import ZookeeperClient
from txzookeeper.client import ConnectionTimeoutException

from juju.errors import NoConnection, InvalidHost, InvalidUser
from juju.lib.mocker import MATCH, match_params
from juju.lib.testing import TestCase
from juju.state.sshclient import SSHClient


MATCH_PROTOCOL = MATCH(lambda x: isinstance(x, ProcessProtocol))
MATCH_TIMEOUT = MATCH(lambda x: isinstance(x, (int, float)))
MATCH_FUNC = MATCH(lambda x: callable(x))
MATCH_DEFERRED = MATCH(lambda x: isinstance(x, Deferred))
MATCH_PORT = MATCH(lambda x: isinstance(x, int))


def _match_localhost_port(value):
    if not ":" in value:
        return False
    host, port = value.split(":")
    if not port.isdigit():
        return False
    return True

MATCH_LOCALHOST_PORT = MATCH(_match_localhost_port)


class TestDeferredSequence(object):
    """Helper class for testing sequence of Deferred values w/ mocker."""

    def __init__(self, sequence, *args, **kwargs):
        self.sequence = collections.deque(sequence)
        self.args = args
        self.kwargs = kwargs

    def __call__(self, func):

        def f(*args, **kwargs):
            if not match_params(self.args, self.kwargs, args, kwargs):
                raise AssertionError(
                    "Unmet expectation %r %r %r %r",
                    func, self.args, self.kwargs, args, kwargs)
            obj = self.sequence.popleft()
            if isinstance(obj, BaseException):
                return fail(obj)
            else:
                return succeed(obj)
        return f


class SSHClientTest(TestCase):

    def setUp(self):
        self.sshclient = SSHClient()
        self.log = self.capture_logging("juju.state.sshforward")

    def test_is_zookeeper_client(self):
        self.assertTrue(isinstance(self.sshclient, ZookeeperClient))

    def test_close_while_not_connected_does_nothing(self):
        # Hah! Nothing!  But it doesn't blow up either.
        self.sshclient.close()

    def test_internal_connect_behavior(self):
        """Verify the order of operations for sshclient._internal_connect."""

        zkconnect = self.mocker.replace(ZookeeperClient.connect)
        zkclose = self.mocker.replace(ZookeeperClient.close)
        forward = self.mocker.replace("juju.state.sshforward.forward_port")
        thread = self.mocker.replace("twisted.internet.threads.deferToThread")
        process = self.mocker.mock(Process)

        with self.mocker.order():
            # First, get the forwarding going, targetting the remote
            # address provided.
            forward("ubuntu", MATCH_PORT, "remote", 2181,
                    process_protocol=MATCH_PROTOCOL, share=False)
            self.mocker.result(process)

            # Next ensure the port check succeeds
            thread(MATCH_FUNC)
            self.mocker.result(succeed(True))

            # Then, connect to localhost, though the set up proxy.
            zkconnect(MATCH_LOCALHOST_PORT, MATCH_TIMEOUT)

            # Just a marker to ensure the following happens as a
            # side effect of actually closing the SSHClient.
            process.pre_close_marker

            zkclose()
            process.signalProcess("TERM")
            process.loseConnection()

        # There we go!
        self.mocker.replay()
        self.sshclient.connect(
            "remote:2181", timeout=123)

        # Trick to ensure process.close() didn't happen
        # before this point.  This only works because we're
        # asking mocker to order events here.
        process.pre_close_marker
        self.sshclient.close()

    def test_new_timeout_after_port_probe(self):
        forward = self.mocker.replace("juju.state.sshforward.forward_port")
        thread = self.mocker.replace("twisted.internet.threads.deferToThread")

        original_time = time.time
        times = [220, 200, 200]

        def get_time():
            if times:
                return times.pop()
            return original_time()

        self.patch(time, "time", get_time)

        protocol = self.mocker.mock()
        forward("ubuntu", MATCH_PORT, "remote", 2181,
                process_protocol=MATCH_PROTOCOL, share=False)
        self.mocker.result(protocol)

        thread(MATCH_FUNC)
        self.mocker.result(succeed(True))

        protocol.signalProcess("TERM")
        protocol.loseConnection()
        self.mocker.replay()

        d = self.sshclient.connect("remote:2181", timeout=20)
        self.failUnlessFailure(d, ConnectionTimeoutException)
        return d

    def test_tunnel_port_open_error(self):
        """Errors when probing the port are reported on the connect deferred.

        Port probing errors are converted to connectiontimeout exceptions.
        """
        forward = self.mocker.replace("juju.state.sshforward.forward_port")
        thread = self.mocker.replace("twisted.internet.threads.deferToThread")

        protocol = self.mocker.mock()
        forward("ubuntu", MATCH_PORT, "remote", 2181,
                process_protocol=MATCH_PROTOCOL, share=False)
        self.mocker.result(protocol)

        thread(MATCH_FUNC)
        self.mocker.result(fail(socket.error("a",)))

        protocol.signalProcess("TERM")
        protocol.loseConnection()
        self.mocker.replay()

        d = self.sshclient.connect("remote:2181", timeout=20)
        self.failUnlessFailure(d, ConnectionTimeoutException)
        return d

    def test_tunnel_client_error(self):
        """A zkclient connect error is reported on the sshclient deferred.

        Client connection errors are propogated as is.
        """
        forward = self.mocker.replace("juju.state.sshforward.forward_port")
        thread = self.mocker.replace("twisted.internet.threads.deferToThread")

        protocol = self.mocker.mock()
        forward("ubuntu", MATCH_PORT, "remote", 2181,
                process_protocol=MATCH_PROTOCOL, share=False)
        self.mocker.result(protocol)

        thread(MATCH_FUNC)

        def wait_result(func):
            return succeed(True)
        self.mocker.call(wait_result)

        zkconnect = self.mocker.replace(ZookeeperClient.connect)
        zkconnect(MATCH_LOCALHOST_PORT, MATCH_TIMEOUT)
        self.mocker.result(fail(OSError()))

        protocol.signalProcess("TERM")
        protocol.loseConnection()
        self.mocker.replay()

        d = self.sshclient.connect(
            "remote:2181", timeout=20)
        self.failUnlessFailure(d, OSError)
        return d

    def test_share_connection(self):
        """Connection sharing requests are passed to port_forward().
        """
        forward = self.mocker.replace("juju.state.sshforward.forward_port")
        thread = self.mocker.replace("twisted.internet.threads.deferToThread")

        protocol = self.mocker.mock()
        forward("ubuntu", MATCH_PORT, "remote", 2181,
                process_protocol=MATCH_PROTOCOL, share=True)
        self.mocker.result(protocol)

        thread(MATCH_FUNC)

        def wait_result(func):
            return succeed(True)
        self.mocker.call(wait_result)

        zkconnect = self.mocker.replace(ZookeeperClient.connect)
        zkconnect(MATCH_LOCALHOST_PORT, MATCH_TIMEOUT)
        self.mocker.result(True)

        protocol.signalProcess("TERM")
        protocol.loseConnection()
        self.mocker.replay()

        yield self.sshclient.connect("remote:2181", timeout=20, share=True)

    @inlineCallbacks
    def test_connect(self):
        """Test normal connection w/o retry loop."""
        mock_client = self.mocker.patch(self.sshclient)
        mock_client._internal_connect(
            "remote:2181", timeout=MATCH_TIMEOUT, share=False)
        self.mocker.result(succeed(True))
        self.mocker.replay()

        yield self.sshclient.connect("remote:2181", timeout=123)

    @inlineCallbacks
    def test_connect_no_connection(self):
        """Test sequence of NoConnection failures, followed by success."""
        mock_client = self.mocker.patch(self.sshclient)
        mock_client._internal_connect
        self.mocker.call(TestDeferredSequence(
            [NoConnection(), NoConnection(), True],
            "remote:2181", timeout=MATCH_TIMEOUT, share=False))
        self.mocker.count(3, 3)
        self.mocker.replay()

        yield self.sshclient.connect("remote:2181", timeout=123)

    @inlineCallbacks
    def test_connect_invalid_host(self):
        """Test connect to invalid host will raise exception asap."""
        mock_client = self.mocker.patch(self.sshclient)
        mock_client._internal_connect
        self.mocker.call(TestDeferredSequence(
            [NoConnection(), InvalidHost(), succeed(True)],
            "remote:2181", timeout=MATCH_TIMEOUT, share=False))
        self.mocker.count(2, 2)
        self.mocker.replay()

        yield self.assertFailure(
            self.sshclient.connect("remote:2181", timeout=123),
            InvalidHost)

    @inlineCallbacks
    def test_connect_invalid_user(self):
        """Test connect with invalid user will raise exception asap."""
        mock_client = self.mocker.patch(self.sshclient)
        mock_client._internal_connect
        self.mocker.call(TestDeferredSequence(
            [NoConnection(), InvalidUser(), succeed(True)],
            "remote:2181", timeout=MATCH_TIMEOUT, share=False))
        self.mocker.count(2, 2)
        self.mocker.replay()

        yield self.assertFailure(
            self.sshclient.connect("remote:2181", timeout=123),
            InvalidUser)

    @inlineCallbacks
    def test_connect_timeout(self):
        """Test that retry fails after timeout in retry loop."""
        mock_client = self.mocker.patch(self.sshclient)
        mock_client._internal_connect
        self.mocker.call(TestDeferredSequence(
            [NoConnection(), NoConnection(), True],
            "remote:2181", timeout=MATCH_TIMEOUT, share=False))
        self.mocker.count(2, 2)

        original_time = time.time
        times = [220, 215, 210, 205, 200]

        def get_time():
            if times:
                return times.pop()
            return original_time()

        self.patch(time, "time", get_time)
        self.mocker.replay()

        ex = yield self.assertFailure(
            self.sshclient.connect("remote:2181", timeout=123),
            ConnectionTimeoutException)
        self.assertEqual(
            str(ex),
            "could not connect before timeout after 2 retries")

    @inlineCallbacks
    def test_connect_tunnel_portwatcher_timeout(self):
        """Test that retry fails after timeout seen in tunnel portwatcher."""
        mock_client = self.mocker.patch(self.sshclient)
        mock_client._internal_connect
        self.mocker.call(TestDeferredSequence(
            [NoConnection(), NoConnection(),
             ConnectionTimeoutException(), True],
            "remote:2181", timeout=MATCH_TIMEOUT, share=False))
        self.mocker.count(3, 3)
        self.mocker.replay()

        ex = yield self.assertFailure(
            self.sshclient.connect("remote:2181", timeout=123),
            ConnectionTimeoutException)
        self.assertEqual(
            str(ex),
            "could not connect before timeout after 3 retries")
