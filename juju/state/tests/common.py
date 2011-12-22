from twisted.internet.defer import inlineCallbacks, returnValue

import zookeeper

from txzookeeper import ZookeeperClient
from txzookeeper.tests.utils import deleteTree

from juju.lib.testing import TestCase
from juju.charm.tests.test_directory import sample_directory
from juju.charm.directory import CharmDirectory
from juju.state.topology import InternalTopology
from juju.tests.common import get_test_zookeeper_address


class StateTestBase(TestCase):

    @inlineCallbacks
    def setUp(self):
        yield super(StateTestBase, self).setUp()
        zookeeper.set_debug_level(0)
        self.charm = CharmDirectory(sample_directory)
        self.client = self.get_zookeeper_client()

        yield self.client.connect()
        yield self.client.create("/charms")
        yield self.client.create("/machines")
        yield self.client.create("/services")
        yield self.client.create("/units")
        yield self.client.create("/relations")

    @inlineCallbacks
    def tearDown(self):
        # Close and reopen connection, so that watches set during
        # testing are not affected by the cleaning up.
        self.client.close()
        client = ZookeeperClient(get_test_zookeeper_address())
        yield client.connect()
        deleteTree(handle=client.handle)
        client.close()
        yield super(StateTestBase, self).tearDown()

    @inlineCallbacks
    def get_topology(self):
        """Read /topology and return InternalTopology instance with it."""
        content, stat = yield self.client.get("/topology")
        topology = InternalTopology()
        topology.parse(content)
        returnValue(topology)

    @inlineCallbacks
    def set_topology(self, topology):
        """Dump the given InternalTopology into /topology."""
        content = topology.dump()
        try:
            yield self.client.set("/topology", content)
        except zookeeper.NoNodeException:
            yield self.client.create("/topology", content)
