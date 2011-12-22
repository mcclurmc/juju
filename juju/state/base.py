import logging

from twisted.internet.defer import inlineCallbacks, returnValue

from zookeeper import NoNodeException

from txzookeeper.utils import retry_change

from juju.state.errors import StopWatcher
from juju.state.topology import InternalTopology


log = logging.getLogger("juju.state")


class StateBase(object):
    """Base class for state handling subclasses.

    At the moment, this class provides a useful constructor, and a couple of
    methods to deal with reading and changing the /topology node in a
    sensible way.
    """

    def __init__(self, client):
        """Constructor.

        @param client: ZookeeperClient instance.
        """
        self._client = client
        self._old_topology = None

    @inlineCallbacks
    def _read_topology(self):
        """Read the /topology node and return an InternalTopology object.

        This object should be used with read-only semantics. For changing the
        topology, check out the _retry_topology_change() method.

        Note that this method name is underlined to mean "protected", not
        "private", since the only purpose of this method is to be used by
        subclasses.
        """
        topology = InternalTopology()
        try:
            content, stat = yield self._client.get("/topology")
            topology.parse(content)
        except NoNodeException:
            pass
        returnValue(topology)

    def _retry_topology_change(self, change_topology_function):
        """Change the current /topology node in a reliable way.

        @param change_topology_function: A function/method which accepts a
            InternalTopology instance as an argument.  This function can read
            and modify the topology instance, and after it returns (or after
            the returned deferred fires) the modified topology will be
            persisted into the /topology node.  Note that this function must
            have no side-effects, since it may be called multiple times
            depending on conflict situations.

        Note that this method name is underlined to mean "protected", not
        "private", since the only purpose of this method is to be used by
        subclasses.
        """

        def change_content_function(content, stat):
            topology = InternalTopology()
            if content:
                topology.parse(content)
            change_topology_function(topology)
            return topology.dump()
        return retry_change(self._client, "/topology",
                            change_content_function)

    @inlineCallbacks
    def _watch_topology(self, watch_topology_function):
        """Changes in the /topology node will fire the given callback.

        @param watch_topology_function: A function/method which accepts two
            InternalTopology parameters: the old topology, and the new one.
            The old topology will be None the first time this function is
            called.

        Note that there are no guarantees that this function will be
        called once for *every* change in the topology, which means
        that multiple modifications may be observed as a single call.

        This method currently sets a pretty much perpetual watch (errors
        will make it bail out).  In order to cleanly stop the watcher, a
        StopWatch exception can be raised by the callback.

        Note that this method name is underlined to mean "protected", not
        "private", since the only purpose of this method is to be used by
        subclasses.
        """
        # Need to guard on the client being connected in the case
        # 1) a watch is waiting to run (in the reactor);
        # 2) and the connection is closed.
        # Because _watch_topology always chains to __watch_topology,
        # the other guarding seen with `StopWatcher` is done there.
        if not self._client.connected:
            return
        exists, watch = self._client.exists_and_watch("/topology")
        stat = yield exists

        if stat is not None:
            yield self.__topology_changed(None, watch_topology_function)
        else:
            watch.addCallback(self.__topology_changed,
                              watch_topology_function)

    @inlineCallbacks
    def __topology_changed(self, ignored, watch_topology_function):
        """Internal callback used by _watch_topology()."""
        # Need to guard on the client being connected in the case
        # 1) a watch is waiting to run (in the reactor);
        # 2) and the connection is closed.
        #
        # It remains the reponsibility of `watch_topology_function` to
        # raise `StopWatcher`, per the doc of `_topology_changed`.
        if not self._client.connected:
            return
        try:
            get, watch = self._client.get_and_watch("/topology")
            content, stat = yield get
        except NoNodeException:
            # WTF? The node went away! This is an unexpected bug
            # which we try to hide from the callback to simplify
            # things.  We'll set the watch back, and once the new
            # content comes up, we'll present the delta as usual.
            log.warning("The /topology node went missing!")
            self._watch_topology(watch_topology_function)
        else:
            new_topology = InternalTopology()
            new_topology.parse(content)
            try:
                yield watch_topology_function(self._old_topology, new_topology)
            except StopWatcher:
                return
            self._old_topology = new_topology
            watch.addCallback(self.__topology_changed, watch_topology_function)
