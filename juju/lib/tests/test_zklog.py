import json
import logging
import time

import zookeeper

from twisted.internet.defer import inlineCallbacks, returnValue, fail
from txzookeeper.tests.utils import deleteTree

from juju.lib.mocker import MATCH
from juju.lib.testing import TestCase
from juju.lib.zklog import ZookeeperHandler, LogIterator


class LogTestBase(TestCase):

    @inlineCallbacks
    def setUp(self):
        yield super(LogTestBase, self).setUp()
        zookeeper.set_debug_level(0)
        self.client = yield self.get_zookeeper_client().connect()

    @inlineCallbacks
    def tearDown(self):
        yield super(LogTestBase, self).tearDown()
        if self.client.connected:
            self.client.close()
        client = yield self.get_zookeeper_client().connect()
        deleteTree(handle=client.handle)
        yield client.close()

    @inlineCallbacks
    def get_configured_log(
        self, channel="test-zk-log", context_name="unit:mysql/0"):
        """Get a log channel configured with a zk handler.
        """
        log = logging.getLogger(channel)
        log.setLevel(logging.DEBUG)
        handler = ZookeeperHandler(self.client, context_name)
        yield handler.open()
        log.addHandler(handler)
        returnValue(log)

    @inlineCallbacks
    def poke_zk(self):
        """Do a roundtrip to zookeeper."""
        yield self.client.exists("/zookeeper")


class ZookeeperLogTest(LogTestBase):

    def test_log_container_path(self):
        handler = ZookeeperHandler(self.client, "unit:mysql/0")
        self.assertEqual(handler.log_container_path, "/logs")

    def test_invalid_log_path(self):
        """The handler raises ValueError on invalid log paths."""
        self.assertRaises(
            ValueError,
            ZookeeperHandler,
            self.client, "something",
            log_path="//")

        self.assertRaises(
            ValueError,
            ZookeeperHandler,
            self.client, "something",
            log_path="/abc/def/gef")

    def test_log_path_specs_path(self):
        """The handler can specify either the container or full log path."""

        # Verify default log path location
        handler = ZookeeperHandler(self.client, "something")
        self.assertEqual(handler.log_path, "/logs/log-")

        # Specify a log path
        handler = ZookeeperHandler(
            self.client, "something", log_path="/logs/msg-")
        self.assertEqual(handler.log_path, "/logs/msg-")

    @inlineCallbacks
    def test_open_with_existing_container_path(self):
        yield self.client.create("/logs")
        handler = ZookeeperHandler(self.client, "unit:mysql/0")
        # should not raise exception
        yield handler.open()

    @inlineCallbacks
    def test_open_with_nonexisting_container_path(self):
        handler = ZookeeperHandler(self.client, "unit:mysql/0")
        # should not raise exception
        yield handler.open()
        exists = yield self.client.exists("/logs")
        self.assertTrue(exists)

    @inlineCallbacks
    def test_log_after_client_close_does_not_emit(self):
        """After a client is closed no log messages are sent."""
        log = yield self.get_configured_log()
        self.client.close()
        log.info("bad stuff")
        yield self.client.connect()
        entries = yield self.client.get_children("/logs")
        self.assertFalse(entries)

    @inlineCallbacks
    def test_log_message(self):
        """Log messages include standard log record information."""
        log = yield self.get_configured_log()
        log.info("hello world")

        # Retrieve the log record
        content, stat = yield self.client.get("/logs/log-%010d" % 0)

        # Verify the stored record
        now = time.time()
        data = json.loads(content)
        self.assertEqual(data["msg"], "hello world")
        self.assertEqual(data["context"], "unit:mysql/0")
        self.assertEqual(data["levelname"], "INFO")
        self.assertEqual(data["funcName"], "test_log_message")
        self.assertTrue(data["created"] + 5 > now)

    @inlineCallbacks
    def test_log_error(self):
        """Exceptions, and errors are formatted w/ tracebacks."""
        log = yield self.get_configured_log()
        try:
            1 / 0
        except Exception:
            log.exception("something bad")

        # Retrieve the log record
        content, stat = yield self.client.get("/logs/log-%010d" % 0)

        data = json.loads(content)
        self.assertIn("something bad", data["msg"])
        self.assertIn("Traceback", data["message"])
        self.assertIn("ZeroDivisionError", data["message"])
        self.assertEqual(data["context"], "unit:mysql/0")
        self.assertEqual(data["levelname"], "ERROR")
        self.assertEqual(data["funcName"], "test_log_error")

    @inlineCallbacks
    def test_handler_error(self):
        """An error in the handler gets reported to stderr."""
        self.error_stream = self.capture_stream("stderr")
        mock_client = self.mocker.patch(self.client)
        mock_client.create("/logs/log-",
                           MATCH(lambda x: isinstance(x, str)),
                           flags=zookeeper.SEQUENCE)
        self.mocker.result(fail(zookeeper.NoNodeException()))
        self.mocker.replay()

        log = yield self.get_configured_log()
        log.info("something interesting")

        # assert the log entry doesn't exist
        exists = yield self.client.exists("/logs/log-%010d" % 0)
        self.assertFalse(exists)
        # assert the error made it to stderr
        self.assertIn("NoNodeException", self.error_stream.getvalue())

    @inlineCallbacks
    def test_log_object(self):
        """Log message interpolation using objects is supported."""

        class Foobar(object):
            def __init__(self, v):
                self._v = v

            def __str__(self):
                return str("Foobar:%s" % self._v)

        log = yield self.get_configured_log()
        log.info("world of %s", Foobar("cloud"))

        content, stat = yield self.client.get("/logs/log-%010d" % 0)
        record = json.loads(content)
        self.assertEqual(
            record["message"], "world of Foobar:cloud")
        self.assertEqual(
            record["args"], [])


class LogIteratorTest(LogTestBase):

    @inlineCallbacks
    def setUp(self):
        yield super(LogIteratorTest, self).setUp()
        self.log = yield self.get_configured_log()
        self.iter = LogIterator(self.client)

    @inlineCallbacks
    def test_get_next_log(self):
        self.log.info("hello world")
        yield self.poke_zk()
        entry = yield self.iter.next()
        self.assertEqual(entry["levelname"], "INFO")

    @inlineCallbacks
    def test_flush_log_last_seen(self):

        for i in ("a", "b", "c"):
            self.log.info(i)

        iter = LogIterator(self.client, seen_block_size=1)
        yield iter.next()
        yield iter.next()

        # Now if we pick up again, we should continue with the last message
        iter = LogIterator(self.client, seen_block_size=1)
        entry = yield iter.next()

        data, stat = yield self.client.get("/logs")
        self.assertTrue(data)
        self.assertEqual(json.loads(data), {"next-log-index": 3})
        self.assertEqual(entry["msg"], "c")
        self.assertEqual(stat["version"], 3)

    @inlineCallbacks
    def test_iter_sans_container(self):
        iter = LogIterator(self.client)
        entry_d = iter.next()
        # make sure it doesn't blow up
        yield self.poke_zk()
        self.log.info("apple")
        entry = yield entry_d
        self.assertEqual(entry["msg"], "apple")

    @inlineCallbacks
    def test_replay_log(self):
        for i in ("a", "b", "c", "d", "e", "f", "z"):
            self.log.info(i)

        yield self.client.set("/logs", json.dumps({"next-log-index": 3}))
        iter = LogIterator(self.client, replay=True, seen_block_size=1)

        entry = yield iter.next()
        self.assertEqual(entry["msg"], "a")

        entry = yield iter.next()
        self.assertEqual(entry["msg"], "b")

        # make sure we haven't updated the last seen index.
        data, stat = yield self.client.get("/logs")
        self.assertEqual(json.loads(data), {"next-log-index": 3})

        # now if we advance past the last seen index, we'll start
        # updating the counter.
        for i in range(4):
            entry = yield iter.next()

        self.assertEqual(entry["msg"], "f")

        # make sure we updated the last seen index.
        data, stat = yield self.client.get("/logs")
        self.assertEqual(json.loads(data), {"next-log-index": 6})
