import errno
import socket
import time
import zookeeper

from Queue import Queue

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from twisted.internet.threads import deferToThread
from txzookeeper import ZookeeperClient
import yaml

from juju.lib.testing import TestCase
from juju.state.errors import StateChanged, StateNotFound
from juju.state.utils import (
    PortWatcher, remove_tree, dict_merge,
    get_open_port, YAMLState, AddedItem, ModifiedItem, DeletedItem)

from juju.tests.common import get_test_zookeeper_address


class PortWait(TestCase):

    def bind_port(self, delay, port=0):
        """Create a fake server for testing.

        Binds socket to `port` for `delay` seconds, then listens. If
        `port` is 0, then it is assigned by the system.  Returns a
        pair of deferreds: completed, port

        The `completed` deferred is called back when the socket is
        closed. The port deferred has the port number, which is useful
        if a system-assigned port is requested.
        """
        # used to communicate the port number
        port_queue = Queue()

        def bind_port_sync():
            sock = socket.socket()
            sock.bind(("127.0.0.1", port))
            sock.listen(1)
            port_queue.put(sock.getsockname()[1])
            time.sleep(delay)
            sock.close()

        return deferToThread(bind_port_sync), deferToThread(port_queue.get)

    @inlineCallbacks
    def test_existing_connect(self):
        """Test watch fires asap if a real server is then available."""
        server_deferred, port_deferred = self.bind_port(0.2)
        port = yield port_deferred
        yield self.sleep(0.1)
        yield PortWatcher("127.0.0.1", port, 1).async_wait()
        yield server_deferred

    @inlineCallbacks
    def test_connect(self):
        """Test watch fires soon after a real server becomes available.

        0.5 second for the approximation is based on the polling
        interval of PortWatcher."""
        now = time.time()
        reactor.callLater(0.7, self.bind_port, 1, 22181)
        yield PortWatcher("127.0.0.1", 22181, 1.5).async_wait()
        self.failUnlessApproximates(time.time() - now, 0.7, 0.5)

    def test_wait_until_timeout_raises_timeout(self):
        """If the timeout is exceeded, a socket.timeout error is raised."""
        self.assertRaises(
            socket.timeout, PortWatcher("localhost", 800, 0).sync_wait)

    def test_wait_stops_when_watcher_stopped(self):
        """
        If the watcher is stopped, no more attempts are made to attempt
        connect to the socket.
        """
        watcher = PortWatcher("127.0.0.1", 800, 30)

        mock_socket = self.mocker.patch(socket.socket)
        sleep = self.mocker.replace("time.sleep")

        mock_socket.connect(("127.0.0.1", 800))
        self.mocker.throw(socket.error(errno.ECONNREFUSED))
        sleep(0.5)
        self.mocker.call(watcher.stop)
        self.mocker.replay()
        self.assertEquals(watcher.sync_wait(), None)

    def test_unknown_socket_error_raises(self):
        """Unknown socket errors are thrown to the caller."""
        mock_socket = self.mocker.patch(socket.socket)
        mock_socket.connect(("127.0.0.1", 465))
        self.mocker.throw(socket.error(2000))
        self.mocker.replay()
        watcher = PortWatcher("127.0.0.1", 465, 5)
        self.assertRaises(socket.error, watcher.sync_wait)

    def test_unknown_error_raises(self):
        """Unknown errors are thrown to the caller."""
        mock_socket = self.mocker.patch(socket.socket)
        mock_socket.connect(("127.0.0.1", 465))
        self.mocker.throw(SyntaxError())
        self.mocker.replay()
        watcher = PortWatcher("127.0.0.1", 465, 5)
        self.assertRaises(SyntaxError, watcher.sync_wait)

    def test_on_connect_returns(self):
        """On a successful connect, the function returns None."""
        mock_socket = self.mocker.patch(socket.socket)
        mock_socket.connect(("127.0.0.1", 800))
        self.mocker.result(True)
        mock_socket.close()

        self.mocker.replay()
        watcher = PortWatcher("127.0.0.1", 800, 30)
        self.assertEqual(watcher.sync_wait(), True)

    @inlineCallbacks
    def test_listen(self):
        """Test with a real socket server watching for port availability."""
        bind_port_time = 0.7
        now = time.time()
        server_deferred, port_deferred = self.bind_port(bind_port_time)
        port = yield port_deferred
        yield PortWatcher("127.0.0.1", port, 10, True).async_wait()
        self.failUnlessApproximates(time.time() - now, bind_port_time, 0.5)
        yield server_deferred

    @inlineCallbacks
    def test_listen_nothing_there(self):
        """Test with a real socket server watching for port availability.

        This should result in the watch returning almost immediately,
        since nothing is (or should be - dangers of real testing)
        holding this port."""
        now = time.time()
        yield PortWatcher("127.0.0.1", 22181, 10, True).async_wait()
        self.failUnlessApproximates(time.time() - now, 0, 0.1)

    def test_listen_wait_stops_when_watcher_stopped(self):
        """
        If the watcher is stopped, no more attempts are made to attempt
        to listen to the socket.
        """
        watcher = PortWatcher("127.0.0.1", 800, 30, True)

        mock_socket = self.mocker.patch(socket.socket)
        sleep = self.mocker.replace("time.sleep")

        mock_socket.bind(("127.0.0.1", 800))
        self.mocker.throw(socket.error(errno.ECONNREFUSED))
        sleep(0.5)
        self.mocker.call(watcher.stop)
        self.mocker.replay()
        self.assertEquals(watcher.sync_wait(), None)

    def test_listen_unknown_socket_error_raises(self):
        """Unknown socket errors are thrown to the caller."""
        mock_socket = self.mocker.patch(socket.socket)
        mock_socket.bind(("127.0.0.1", 465))
        self.mocker.throw(socket.error(2000))
        self.mocker.replay()
        watcher = PortWatcher("127.0.0.1", 465, 5, True)
        self.assertRaises(socket.error, watcher.sync_wait)

    def test_listen_unknown_error_raises(self):
        """Unknown errors are thrown to the caller."""
        mock_socket = self.mocker.patch(socket.socket)
        mock_socket.bind(("127.0.0.1", 465))
        self.mocker.throw(SyntaxError())
        self.mocker.replay()
        watcher = PortWatcher("127.0.0.1", 465, 5, True)
        self.assertRaises(SyntaxError, watcher.sync_wait)

    def test_listen_on_connect_returns(self):
        """On a successful connect, the function returns None."""
        mock_socket = self.mocker.patch(socket.socket)
        mock_socket.bind(("127.0.0.1", 800))
        self.mocker.result(True)
        mock_socket.close()

        self.mocker.replay()
        watcher = PortWatcher("127.0.0.1", 800, 30, True)
        self.assertEqual(watcher.sync_wait(), True)


class RemoveTreeTest(TestCase):

    @inlineCallbacks
    def setUp(self):
        yield super(RemoveTreeTest, self).setUp()
        zookeeper.set_debug_level(0)
        self.client = ZookeeperClient(get_test_zookeeper_address())
        yield self.client.connect()

    @inlineCallbacks
    def test_remove_tree(self):
        yield self.client.create("/zoo")
        yield self.client.create("/zoo/mammals")
        yield self.client.create("/zoo/mammals/elephant")
        yield self.client.create("/zoo/reptiles")
        yield self.client.create("/zoo/reptiles/snake")

        yield remove_tree(self.client, "/zoo")

        children = yield self.client.get_children("/")
        self.assertNotIn("zoo", children)


class DictMergeTest(TestCase):

    def test_merge_no_match(self):
        self.assertEqual(
            dict_merge(dict(a=1), dict(b=2)),
            dict(a=1, b=2))

    def test_merge_matching_keys_same_value(self):
        self.assertEqual(
            dict_merge(dict(a=1, b=2), dict(b=2, c=1)),
            dict(a=1, b=2, c=1))

    def test_merge_conflict(self):
        self.assertRaises(
            StateChanged,
            dict_merge,
            dict(a=1, b=3),
            dict(b=2, c=1))


class OpenPortTest(TestCase):

    def test_get_open_port(self):
        port = get_open_port()
        self.assertTrue(isinstance(port, int))

        sock = socket.socket(
            socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP)

        # would raise an error if we got it wrong.
        sock.bind(("127.0.0.1", port))
        sock.listen(1)
        sock.close()
        del sock

    def test_get_open_port_with_host(self):
        port = get_open_port("localhost")
        self.assertTrue(isinstance(port, int))

        sock = socket.socket(
            socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP)

        # would raise an error if we got it wrong.
        sock.bind(("127.0.0.1", port))
        sock.listen(1)
        sock.close()
        del sock


class ChangeItemsTest(TestCase):
    """Tests the formatting of change items.

    Note that values always are stored in Unicode in the current
    scheme, but this testing ensures that other types can also be used
    if/when desired.
    """

    def test_deleted_item(self):
        self.assertEqual(str(DeletedItem("my-int", 42)),
                         "Setting deleted: 'my-int' (was 42)")
        self.assertEqual(str(DeletedItem("my-uni", u"a")),
                         "Setting deleted: 'my-uni' (was u'a')")
        self.assertEqual(str(DeletedItem("my-str", "x")),
                         "Setting deleted: 'my-str' (was 'x')")
        self.assertEqual(
            str(DeletedItem("my-long-uni", u"a" * 101)),
            "Setting deleted: 'my-long-uni' (was u'%s)" % (
                "a" * 98,))

    def test_modified_item(self):
        self.assertEqual(str(ModifiedItem("formerly-int", 42, u"x")),
                         "Setting changed: 'formerly-int'=u'x' (was 42)")
        self.assertEqual(str(ModifiedItem("formerly-uni", u"a", "b")),
                         "Setting changed: 'formerly-uni'='b' (was u'a')")
        self.assertEqual(str(ModifiedItem("changed-str", "x", "y")),
                         "Setting changed: 'changed-str'='y' (was 'x')")
        self.assertEqual(
            str(ModifiedItem("my-long-uni", u"a" * 101, u"x" * 200)),
            "Setting changed: 'my-long-uni'=u'%s (was u'%s)" % (
                "x" * 98, "a" * 98,))

    def test_added_item(self):
        self.assertEqual(str(AddedItem("new-int", 42)),
                         "Setting changed: 'new-int'=42 (was unset)")
        self.assertEqual(str(AddedItem("new-uni", u"a")),
                         "Setting changed: 'new-uni'=u'a' (was unset)")
        self.assertEqual(str(AddedItem("new-str", "x")),
                         "Setting changed: 'new-str'='x' (was unset)")
        self.assertEqual(
            str(AddedItem("my-long-uni", u"a" * 101)),
            "Setting changed: 'my-long-uni'=u'%s (was unset)" % (
                "a" * 98,))


class YAMLStateTest(TestCase):

    @inlineCallbacks
    def setUp(self):
        zookeeper.set_debug_level(0)
        self.client = ZookeeperClient(get_test_zookeeper_address())
        yield self.client.connect()
        self.path = "/zoo"

    @inlineCallbacks
    def tearDown(self):
        exists = yield self.client.exists(self.path)
        if exists:
            yield remove_tree(self.client, self.path)

    @inlineCallbacks
    def test_get_empty(self):
        """Verify getting an empty node works as expected."""
        path = yield self.client.create(self.path)
        node = YAMLState(self.client, path)
        self.assertEqual(node, {})

    @inlineCallbacks
    def test_access_wo_create(self):
        """Verify accessing data for a non-existant node works as expected."""
        node = YAMLState(self.client, self.path)
        yield node.read()
        self.assertEqual(node, {})

    def test_set_wo_read(self):
        """Verify that not calling read before mutation raises."""
        node = YAMLState(self.client, self.path)

        self.assertRaises(ValueError, node.__setitem__, "alpha", "beta")
        self.assertRaises(ValueError, node.update, {"alpha": "beta"})

    @inlineCallbacks
    def test_set_wo_write(self):
        """Check that get resolves from the internal write buffer.

        set/get pairs w/o write should present a view of the state
        reflecting local change. Verify that w/o write local data
        appears on subsequent calls but that zk state hasn't been
        changed.
        """
        path = yield self.client.create(self.path)
        node = YAMLState(self.client, path)
        yield node.read()

        options = dict(alpha="beta", one=1)
        node.update(options)
        self.assertEqual(node, options)

        zk_data, stat = yield self.client.get(self.path)
        # the node isn't created yet in zk
        self.assertEqual(zk_data, "")

    @inlineCallbacks
    def test_set_w_write(self):
        """Verify that write updates the local and zk state.

        When write is called we expect that zk state reflects this. We
        also expect calls to get to expect the reflected state.
        """
        node = YAMLState(self.client, self.path)
        yield node.read()

        options = dict(alpha="beta", one=1)
        node.update(options)

        changes = yield node.write()
        self.assertEqual(
            set(changes),
            set([AddedItem(key='alpha', new='beta'),
                 AddedItem(key='one', new=1)]))

        # a local get should reflect proper data
        self.assertEqual(node, options)

        # and a direct look at zk should work as well
        zk_data, stat = yield self.client.get(self.path)
        zk_data = yaml.load(zk_data)
        self.assertEqual(zk_data, options)

    @inlineCallbacks
    def test_conflict_on_set(self):
        """Version conflict error tests.

        Test that two YAMLState objects writing to the same path can
        and will throw version errors when elements become out of
        read.
        """
        node = YAMLState(self.client, self.path)
        node2 = YAMLState(self.client, self.path)

        yield node.read()
        yield node2.read()

        options = dict(alpha="beta", one=1)
        node.update(options)
        yield node.write()

        node2.update(options)
        changes = yield node2.write()
        self.assertEqual(
            set(changes),
            set([AddedItem("alpha", "beta"), AddedItem("one", 1)]))

        # first read node2
        self.assertEqual(node, options)

        # write on node 1
        options2 = dict(alpha="gamma", one="two")
        node.update(options2)
        changes = yield node.write()
        self.assertEqual(
            set(changes),
            set([ModifiedItem("alpha", "beta", "gamma"),
                 ModifiedItem("one", 1, "two")]))

        # verify that node 1 reports as expected
        self.assertEqual(node, options2)

        # verify that node2 has the older data still
        self.assertEqual(node2, options)

        # now issue a set/write from node2
        # this will merge the data deleting 'one'
        # and updating other values
        options3 = dict(alpha="cappa", new="next")
        node2.update(options3)
        del node2["one"]

        expected = dict(alpha="cappa", new="next")
        changes = yield node2.write()
        self.assertEqual(
            set(changes),
            set([DeletedItem("one", 1),
                 ModifiedItem("alpha", "beta", "cappa"),
                 AddedItem("new", "next")]))
        self.assertEqual(expected, node2)

        # but node still reflects the old data
        self.assertEqual(node, options2)

    @inlineCallbacks
    def test_setitem(self):
        node = YAMLState(self.client, self.path)
        yield node.read()

        options = dict(alpha="beta", one=1)
        node["alpha"] = "beta"
        node["one"] = 1
        changes = yield node.write()
        self.assertEqual(
            set(changes),
            set([AddedItem("alpha", "beta"),
                 AddedItem("one", 1)]))

        # a local get should reflect proper data
        self.assertEqual(node, options)

        # and a direct look at zk should work as well
        zk_data, stat = yield self.client.get(self.path)
        zk_data = yaml.load(zk_data)
        self.assertEqual(zk_data, options)

    @inlineCallbacks
    def test_multiple_reads(self):
        """Calling read resets state to ZK after multiple round-trips."""
        node = YAMLState(self.client, self.path)
        yield node.read()

        node.update({"alpha": "beta", "foo": "bar"})

        self.assertEqual(node["alpha"], "beta")
        self.assertEqual(node["foo"], "bar")

        yield node.read()

        # A read resets the data to the empty state
        self.assertEqual(node, {})

        node.update({"alpha": "beta", "foo": "bar"})
        changes = yield node.write()
        self.assertEqual(
            set(changes),
            set([AddedItem("alpha", "beta"), AddedItem("foo", "bar")]))

        # A write retains the newly set values
        self.assertEqual(node["alpha"], "beta")
        self.assertEqual(node["foo"], "bar")

        # now get another state instance and change zk state
        node2 = YAMLState(self.client, self.path)
        yield node2.read()
        node2.update({"foo": "different"})
        changes = yield node2.write()
        self.assertEqual(
            changes, [ModifiedItem("foo", "bar", "different")])

        # This should pull in the new state (and still have the merged old.
        yield node.read()

        self.assertEqual(node["alpha"], "beta")
        self.assertEqual(node["foo"], "different")

    def test_dictmixin_usage(self):
        """Verify that the majority of dict operation function."""
        node = YAMLState(self.client, self.path)
        yield node.read()

        node.update({"alpha": "beta", "foo": "bar"})
        self.assertEqual(node, {"alpha": "beta", "foo": "bar"})

        result = node.pop("foo")
        self.assertEqual(result, "bar")
        self.assertEqual(node, {"alpha": "beta"})

        node["delta"] = "gamma"
        self.assertEqual(set(node.keys()), set(("alpha", "delta")))

        result = list(node.iteritems())
        self.assertIn(("alpha", "beta"), result)
        self.assertIn(("delta", "gamma"), result)

    @inlineCallbacks
    def test_del_empties_state(self):
        d = YAMLState(self.client, self.path)
        yield d.read()

        d["a"] = "foo"
        changes = yield d.write()
        self.assertEqual(changes, [AddedItem("a", "foo")])
        del d["a"]
        changes = yield d.write()
        self.assertEqual(changes, [DeletedItem("a", "foo")])
        self.assertEqual(d, {})

    @inlineCallbacks
    def test_read_resync(self):
        d1 = YAMLState(self.client, self.path)
        yield d1.read()
        d1["a"] = "foo"
        changes = yield d1.write()
        self.assertEqual(changes, [AddedItem("a", "foo")])

        d2 = YAMLState(self.client, self.path)
        yield d2.read()
        del d2["a"]
        changes = yield d2.write()
        self.assertEqual(changes, [DeletedItem("a", "foo")])
        d2["a"] = "bar"
        changes = yield d2.write()
        self.assertEqual(changes, [AddedItem("a", "bar")])
        zk_data, stat = yield self.client.get(self.path)
        yield d1.read()
        # d1 should pick up the new value (from d2) on a read
        zk_data, stat = yield self.client.get(self.path)
        self.assertEqual(d1["a"], "bar")

    @inlineCallbacks
    def test_multiple_writes(self):
        d1 = YAMLState(self.client, self.path)
        yield d1.read()

        d1.update(dict(foo="bar", this="that"))
        changes = yield d1.write()
        self.assertEqual(
            set(changes),
            set([AddedItem("foo", "bar"), AddedItem("this", "that")]))

        del d1["this"]
        d1["another"] = "value"

        changes = yield d1.write()
        self.assertEqual(
            set(changes),
            set([DeletedItem("this", "that"), AddedItem("another", "value")]))

        expected = {"foo": "bar", "another": "value"}
        self.assertEqual(d1, expected)

        changes = yield d1.write()
        self.assertEqual(changes, [])
        self.assertEqual(d1, expected)

        yield d1.read()
        self.assertEqual(d1, expected)

        # This shouldn't write any changes
        changes = yield d1.write()
        self.assertEqual(changes, [])
        self.assertEqual(d1, expected)

    @inlineCallbacks
    def test_write_twice(self):
        d1 = YAMLState(self.client, self.path)
        yield d1.read()
        d1["a"] = "foo"
        changes = yield d1.write()
        self.assertEqual(changes, [AddedItem("a", "foo")])

        d2 = YAMLState(self.client, self.path)
        yield d2.read()
        d2["a"] = "bar"
        changes = yield d2.write()
        self.assertEqual(changes, [ModifiedItem("a", "foo", "bar")])

        # Shouldn't write again. Changes were already
        # flushed and acted upon by other parties.
        changes = yield d1.write()
        self.assertEqual(changes, [])

        yield d1.read()
        self.assertEquals(d1, d2)

    @inlineCallbacks
    def test_read_requires_node(self):
        """Validate that read raises when required=True."""
        d1 = YAMLState(self.client, self.path)
        yield self.assertFailure(d1.read(True), StateNotFound)
