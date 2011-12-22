from twisted.internet.defer import inlineCallbacks, Deferred
from txzookeeper import ZookeeperClient

from juju.state.tests.common import StateTestBase

from juju.state.base import StateBase
from juju.state.errors import StopWatcher
from juju.state.topology import InternalTopology
from juju.tests.common import get_test_zookeeper_address


class StateBaseTest(StateTestBase):
    """
    Test the StateBase class, which provides helpers to other
    state management classes.

    Note that some underscored methods in this class are like this with
    the intention of implementing "protected" semantics, not "private".
    """

    @inlineCallbacks
    def setUp(self):
        yield super(StateBaseTest, self).setUp()
        self.base = StateBase(self.client)

    def parse_topology(self, content):
        topology = InternalTopology()
        topology.parse(content)
        return topology

    @inlineCallbacks
    def test_read_empty_topology(self):
        """
        When the state is empty (no /topology) file, reading the
        topology should return an empty one.
        """
        topology = yield self.base._read_topology()
        empty_topology = InternalTopology()
        self.assertEquals(topology.dump(), empty_topology.dump())

    @inlineCallbacks
    def test_non_empty_topology(self):
        """
        When there's something in /topology already, it should of
        course be read and parsed when we read the topology.
        """
        test_topology = InternalTopology()
        test_topology.add_machine("m-0")
        topology_dump = test_topology.dump()
        yield self.client.create("/topology", topology_dump)
        topology = yield self.base._read_topology()
        self.assertEquals(topology.dump(), topology_dump)

    @inlineCallbacks
    def test_change_empty_topology(self):
        """
        Attempting to change a non-existing topology should create
        it with the initial content in place.
        """

        def change_topology(topology):
            topology.add_machine("m-0")

        yield self.base._retry_topology_change(change_topology)

        content, stat = yield self.client.get("/topology")
        topology = self.parse_topology(content)
        self.assertTrue(topology.has_machine("m-0"))

    @inlineCallbacks
    def test_change_non_empty_topology(self):
        """
        Attempting to change a pre-existing topology should modify
        it accordingly.
        """
        test_topology = InternalTopology()
        test_topology.add_machine("m-0")
        topology_dump = test_topology.dump()
        yield self.client.create("/topology", topology_dump)

        def change_topology(topology):
            topology.add_machine("m-1")
        yield self.base._retry_topology_change(change_topology)

        content, stat = yield self.client.get("/topology")
        topology = self.parse_topology(content)
        self.assertTrue(topology.has_machine("m-0"))
        self.assertTrue(topology.has_machine("m-1"))

    @inlineCallbacks
    def test_watch_topology_when_being_created(self):
        """
        It should be possible to start watching the topology even
        before it is created.  In this case, the callback will be
        made when it's actually introduced.
        """
        wait_callback = [Deferred() for i in range(10)]

        calls = []

        def watch_topology(old_topology, new_topology):
            calls.append((old_topology, new_topology))
            wait_callback[len(calls)-1].callback(True)

        # Start watching.
        self.base._watch_topology(watch_topology)

        # Callback is still untouched.
        self.assertEquals(calls, [])

        # Create the topology, and wait for callback.
        topology = InternalTopology()
        topology.add_machine("m-0")
        yield self.set_topology(topology)
        yield wait_callback[0]

        # The first callback must have been fired, and it must have None
        # as the first argument because that's the first topology seen.
        self.assertEquals(len(calls), 1)
        old_topology, new_topology = calls[0]
        self.assertEquals(old_topology, None)
        self.assertEquals(new_topology.has_machine("m-0"), True)
        self.assertEquals(new_topology.has_machine("m-1"), False)

        # Change the topology again.
        topology.add_machine("m-1")
        yield self.set_topology(topology)
        yield wait_callback[1]

        # Now the watch callback must have been fired with two
        # different topologies.  The old one, and the new one.
        self.assertEquals(len(calls), 2)
        old_topology, new_topology = calls[1]
        self.assertEquals(old_topology.has_machine("m-0"), True)
        self.assertEquals(old_topology.has_machine("m-1"), False)
        self.assertEquals(new_topology.has_machine("m-0"), True)
        self.assertEquals(new_topology.has_machine("m-1"), True)

    @inlineCallbacks
    def test_watch_topology_when_it_already_exists(self):
        """
        It should also be possible to start watching the topology when
        the topology already exists, of course. In this case, the callback
        should fire immediately, and should have an old_topology of None
        so that the callback has a chance to understand that it's the
        first time a topology is being processed, even if it already
        existed before.
        """
        # Create the topology ahead of time.
        topology = InternalTopology()
        topology.add_machine("m-0")
        yield self.set_topology(topology)

        wait_callback = [Deferred() for i in range(10)]

        calls = []

        def watch_topology(old_topology, new_topology):
            calls.append((old_topology, new_topology))
            wait_callback[len(calls)-1].callback(True)

        # Start watching, and wait on callback immediately.
        self.base._watch_topology(watch_topology)
        yield wait_callback[0]

        # The first callback must have been fired, and it must have None
        # as the first argument because that's the first topology seen.
        self.assertEquals(len(calls), 1)
        old_topology, new_topology = calls[0]
        self.assertEquals(old_topology, None)
        self.assertEquals(new_topology.has_machine("m-0"), True)
        self.assertEquals(new_topology.has_machine("m-1"), False)

        # Change the topology again.
        topology.add_machine("m-1")
        yield self.set_topology(topology)
        yield wait_callback[1]

        # Give a chance for something bad to happen.
        yield self.poke_zk()

        # Now the watch callback must have been fired with two
        # different topologies.  The old one, and the new one.
        self.assertEquals(len(calls), 2)
        old_topology, new_topology = calls[1]
        self.assertEquals(old_topology.has_machine("m-0"), True)
        self.assertEquals(old_topology.has_machine("m-1"), False)
        self.assertEquals(new_topology.has_machine("m-0"), True)
        self.assertEquals(new_topology.has_machine("m-1"), True)

    @inlineCallbacks
    def test_watch_topology_when_it_goes_missing(self):
        """
        We consider a deleted /topology to be an error, and will not
        warn the callback about it. Instead, we'll wait until a new
        topology is brought up, and then will fire the callback with
        the full delta.
        """
        # Create the topology ahead of time.
        topology = InternalTopology()
        topology.add_machine("m-0")
        yield self.set_topology(topology)

        wait_callback = [Deferred() for i in range(10)]

        calls = []

        def watch_topology(old_topology, new_topology):
            calls.append((old_topology, new_topology))
            wait_callback[len(calls)-1].callback(True)

        # Start watching, and wait on callback immediately.
        self.base._watch_topology(watch_topology)

        # Ignore the first callback with the initial state, as
        # this is already tested above.
        yield wait_callback[0]

        log = self.capture_logging()

        # Kill the /topology node entirely.
        yield self.client.delete("/topology")

        # Create a new topology from the ground up.
        topology.add_machine("m-1")
        yield self.set_topology(topology)
        yield wait_callback[1]

        # The issue should have been logged.
        self.assertIn("The /topology node went missing!",
                      log.getvalue())

        # Check that we've only perceived the delta.
        self.assertEquals(len(calls), 2)
        old_topology, new_topology = calls[1]
        self.assertEquals(old_topology.has_machine("m-0"), True)
        self.assertEquals(old_topology.has_machine("m-1"), False)
        self.assertEquals(new_topology.has_machine("m-0"), True)
        self.assertEquals(new_topology.has_machine("m-1"), True)

    @inlineCallbacks
    def test_watch_topology_may_defer(self):
        """
        The watch topology may return a deferred so that it performs
        some of its logic asynchronously.  In this case, it must not
        be called a second time before its postponed logic is finished
        completely.
        """
        wait_callback = [Deferred() for i in range(10)]
        finish_callback = [Deferred() for i in range(10)]

        calls = []

        def watch_topology(old_topology, new_topology):
            calls.append((old_topology, new_topology))
            wait_callback[len(calls)-1].callback(True)
            return finish_callback[len(calls)-1]

        # Start watching.
        self.base._watch_topology(watch_topology)

        # Create the topology.
        topology = InternalTopology()
        topology.add_machine("m-0")
        yield self.set_topology(topology)

        # Hold off until callback is started.
        yield wait_callback[0]

        # Change the topology again.
        topology.add_machine("m-1")
        yield self.set_topology(topology)

        # Give a chance for something bad to happen.
        yield self.poke_zk()

        # Ensure we still have a single call.
        self.assertEquals(len(calls), 1)

        # Allow the first call to be completed, and wait on the
        # next one.
        finish_callback[0].callback(None)
        yield wait_callback[1]
        finish_callback[1].callback(None)

        # We should have the second change now.
        self.assertEquals(len(calls), 2)
        old_topology, new_topology = calls[1]
        self.assertEquals(old_topology.has_machine("m-0"), True)
        self.assertEquals(old_topology.has_machine("m-1"), False)
        self.assertEquals(new_topology.has_machine("m-0"), True)
        self.assertEquals(new_topology.has_machine("m-1"), True)

    @inlineCallbacks
    def test_stop_watch(self):
        """
        A watch that fires a `StopWatcher` exception will end the
        watch."""
        wait_callback = [Deferred() for i in range(5)]
        calls = []

        def watcher(old_topology, new_topology):
            calls.append((old_topology, new_topology))
            wait_callback[len(calls)-1].callback(True)
            if len(calls) == 2:
                raise StopWatcher()

        # Start watching.
        self.base._watch_topology(watcher)

        # Create the topology.
        topology = InternalTopology()
        topology.add_machine("m-0")
        yield self.set_topology(topology)

        # Hold off until callback is started.
        yield wait_callback[0]

        # Change the topology again.
        topology.add_machine("m-1")
        yield self.set_topology(topology)

        yield wait_callback[1]
        self.assertEqual(len(calls), 2)

        # Change the topology again, we shouldn't see this.
        topology.add_machine("m-2")
        yield self.set_topology(topology)

        # Give a chance for something bad to happen.
        yield self.poke_zk()

        # Ensure we still have a single call.
        self.assertEquals(len(calls), 2)

    @inlineCallbacks
    def test_watch_stops_on_closed_connection(self):
        """Verify watches stops when the connection is closed."""

        # Use a separate client connection for watching so it can be
        # disconnected.
        watch_client = ZookeeperClient(get_test_zookeeper_address())
        yield watch_client.connect()
        watch_base = StateBase(watch_client)

        wait_callback = Deferred()
        finish_callback = Deferred()
        calls = []

        def watcher(old_topology, new_topology):
            calls.append((old_topology, new_topology))
            wait_callback.callback(True)
            return finish_callback

        # Start watching.
        yield watch_base._watch_topology(watcher)

        # Create the topology.
        topology = InternalTopology()
        topology.add_machine("m-0")
        yield self.set_topology(topology)
        
        # Hold off until callback is started.
        yield wait_callback

        # Change the topology.
        topology.add_machine("m-1")
        yield self.set_topology(topology)

        # Ensure that the watch has been called just once so far
        # (although still pending due to the finish_callback).
        self.assertEquals(len(calls), 1)

        # Now disconnect the client.
        watch_client.close()
        self.assertFalse(watch_client.connected)
        self.assertTrue(self.client.connected)

        # Change the topology again.
        topology.add_machine("m-2")
        yield self.set_topology(topology)

        # Allow the first call to be completed, starting a process of
        # watching for the next change. At this point, the watch will
        # encounter that the client is disconnected.
        finish_callback.callback(True)

        # Give a chance for something bad to happen.
        yield self.poke_zk()

        # Ensure the watch was still not called.
        self.assertEquals(len(calls), 1)

    @inlineCallbacks
    def test_watch_stops_on_early_closed_connection(self):
        """Verify watches stops when the connection is closed early.

        _watch_topology chains from an exists_and_watch to a
        get_and_watch. This test ensures that this chaining will fail
        gracefully if the connection is closed before this chaining
        can occur.
        """
        # Use a separate client connection for watching so it can be
        # disconnected.
        watch_client = ZookeeperClient(get_test_zookeeper_address())
        yield watch_client.connect()
        watch_base = StateBase(watch_client)

        calls = []

        @inlineCallbacks
        def watcher(old_topology, new_topology):
            calls.append((old_topology, new_topology))

        # Create the topology.
        topology = InternalTopology()
        topology.add_machine("m-0")
        yield self.set_topology(topology)
        
        # Now disconnect the client.
        watch_client.close()
        self.assertFalse(watch_client.connected)
        self.assertTrue(self.client.connected)

        # Start watching.
        yield watch_base._watch_topology(watcher)

        # Change the topology, this will trigger the watch.
        topology.add_machine("m-1")
        yield self.set_topology(topology)

        # Give a chance for something bad to happen.
        yield self.poke_zk()

        # Ensure the watcher was never called, because its client was
        # disconnected.
        self.assertEquals(len(calls), 0)
