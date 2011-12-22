import logging
import random

from twisted.internet.defer import fail, inlineCallbacks, succeed

import zookeeper

from txzookeeper import ZookeeperClient
from txzookeeper.client import ConnectionTimeoutException
from txzookeeper.tests.utils import deleteTree

from juju.errors import EnvironmentNotFound, NoConnection
from juju.lib.testing import TestCase
from juju.machine import ProviderMachine
from juju.providers.common.base import MachineProviderBase
from juju.state.sshclient import SSHClient
from juju.tests.common import get_test_zookeeper_address


class DummyProvider(MachineProviderBase):

    def __init__(self, *zookeepers):
        self._zookeepers = zookeepers

    def get_zookeeper_machines(self):
        """
        Return a pair of possible zookeepers, the first of which is invalid
        """
        machines = [ProviderMachine("i-havenodns")]
        machines.extend(self._zookeepers)
        return succeed(machines)


class NotBootstrappedProvider(MachineProviderBase):
    """Pretend to be an environment that has not been bootstrapped."""

    def __init__(self):
        pass

    def get_zookeeper_machines(self):
        return fail(EnvironmentNotFound("is the environment bootstrapped?"))


class ConnectTest(TestCase):

    def mock_connect(self, share, result):
        client = self.mocker.patch(SSHClient)
        client.connect("foo.example.com:2181", timeout=30, share=share)
        self.mocker.result(result)

    @inlineCallbacks
    def test_none_have_dns_at_first(self):
        """Verify retry of ZK machine that at first doesn't have a DNS name."""
        log = self.capture_logging(level=logging.DEBUG)
        mocked_machine = self.mocker.proxy(ProviderMachine(
                "i-have-no-dns-at-first"))
        mocked_machine.dns_name
        self.mocker.result(None)
        mocked_machine.dns_name
        self.mocker.count(0, None)
        self.mocker.result("foo.example.com")  # name assumed by mock_connect

        # Provide for a valid mocked client once connected
        client = self.mocker.mock(type=SSHClient)
        self.mock_connect(True, succeed(client))
        client.exists_and_watch("/initialized")
        self.mocker.result((succeed(True), None))

        self.mocker.replay()

        provider = DummyProvider(mocked_machine)
        client = yield provider.connect(share=True)

        self.assertIn(
            "Retrying connection: No machines have assigned addresses",
            log.getvalue())

    def test_share_kwarg(self):
        """The `share` kwarg should be passed through to `SSHClient.connect`"""
        client = self.mocker.mock(type=SSHClient)
        self.mock_connect(True, succeed(client))
        client.exists_and_watch("/initialized")
        self.mocker.result((succeed(True), None))
        self.mocker.replay()

        provider = DummyProvider(ProviderMachine("i-amok", "foo.example.com"))
        d = provider.connect(share=True)

        def verify(result):
            self.assertIdentical(result, client)
        d.addCallback(verify)
        return d

    @inlineCallbacks
    def test_no_connection(self):
        """Verify retry of `NoConnection` errors."""
        log = self.capture_logging(level=logging.DEBUG)

        # First connection attempts fails
        mock_client = self.mocker.patch(SSHClient)
        mock_client.connect("foo.example.com:2181", timeout=30, share=False)
        self.mocker.result(fail(NoConnection("KABOOM!")))

        # Subsequent connection attempt then succeeds with a valid
        # (mocked) client that can be waited on.
        mock_client.connect("foo.example.com:2181", timeout=30, share=False)
        self.mocker.result(succeed(mock_client))
        mock_client.exists_and_watch("/initialized")
        self.mocker.result((succeed(True), None))

        self.mocker.replay()

        provider = DummyProvider(ProviderMachine("i-exist", "foo.example.com"))
        yield provider.connect()
        self.assertIn(
            "Retrying connection: Cannot connect to environment "
            "using foo.example.com (perhaps still initializing): KABOOM!",
            log.getvalue())

    @inlineCallbacks
    def test_not_bootstrapped(self):
        """Verify failure when the provider has not been bootstrapped."""
        provider = NotBootstrappedProvider()
        e = yield self.assertFailure(provider.connect(), EnvironmentNotFound)
        self.assertEqual(
            str(e),
            "juju environment not found: is the environment bootstrapped?")

    @inlineCallbacks
    def test_txzookeeper_error(self):
        """Verify retry of `ConnectionTimeoutException` errors."""
        log = self.capture_logging(level=logging.DEBUG)

        # First connection attempts fails
        mock_client = self.mocker.patch(SSHClient)
        mock_client.connect("foo.example.com:2181", timeout=30, share=False)
        self.mocker.result(fail(ConnectionTimeoutException("SPLAT!")))

        # Subsequent connection attempt then succeeds with a valid
        # (mocked) client that can be waited on.
        mock_client.connect("foo.example.com:2181", timeout=30, share=False)
        self.mocker.result(succeed(mock_client))
        mock_client.exists_and_watch("/initialized")
        self.mocker.result((succeed(True), None))

        self.mocker.replay()

        provider = DummyProvider(ProviderMachine("i-exist", "foo.example.com"))
        yield provider.connect()
        self.assertIn(
            "Retrying connection: Cannot connect to environment "
            "using foo.example.com (perhaps still initializing): SPLAT!",
            log.getvalue())

    @inlineCallbacks
    def test_other_error(self):
        """Other errors should propagate"""
        log = self.capture_logging()
        self.mock_connect(False, fail(TypeError("THUD!")))
        self.mocker.replay()

        provider = DummyProvider(ProviderMachine("i-exist", "foo.example.com"))
        ex = yield self.assertFailure(provider.connect(), TypeError)
        self.assertEqual(str(ex), "THUD!")
        self.assertIn(
            "Cannot connect to environment: THUD!",
            log.getvalue())

    @inlineCallbacks
    def test_wait_for_initialize(self):
        """
        A connection to a zookeeper that is running, but whose juju state
        is not ready, should wait until that state is ready.
        """
        client = ZookeeperClient()
        self.client = client  # for poke_zk
        self.mock_connect(False, succeed(client))
        self.mocker.replay()

        zookeeper.set_debug_level(0)
        yield client.connect(get_test_zookeeper_address())

        provider = DummyProvider(ProviderMachine("i-amok", "foo.example.com"))
        d = provider.connect()
        client_result = []
        d.addCallback(client_result.append)

        # Give it a chance to do it incorrectly.
        yield self.poke_zk()

        try:
            self.assertEquals(client_result, [])
            yield client.create("/initialized")
            yield d
            self.assertTrue(client_result, client_result)
            self.assertIdentical(client_result[0], client)
        finally:
            deleteTree("/", client.handle)
            client.close()

    @inlineCallbacks
    def test_fast_connection(self):
        """Verify connection when requirements are available at time of conn.

        This includes /initialized is already set. In addition, also
        verifies that if multiple ZKs are available, one is selected
        via random.choice.
        """
        log = self.capture_logging(level=logging.DEBUG)
        client = ZookeeperClient()
        self.mock_connect(False, succeed(client))

        def get_choice(lst):
            for item in lst:
                if item.dns_name == "foo.example.com":
                    return item
            raise AssertionError("expected choice not seen")

        self.patch(random, "choice", get_choice)
        self.mocker.replay()

        provider = DummyProvider(
            ProviderMachine("i-am-picked", "foo.example.com"),
            ProviderMachine("i-was-not", "bar.example.com"))

        zookeeper.set_debug_level(0)
        yield client.connect(get_test_zookeeper_address())
        try:
            yield client.create("/initialized")
            yield provider.connect()
            self.assertEqual(
                "Connecting to environment...\n"
                "Connecting to environment using foo.example.com...\n"
                "Environment is initialized.\n"
                "Connected to environment.\n",
                log.getvalue())
        finally:
            deleteTree("/", client.handle)
            client.close()
