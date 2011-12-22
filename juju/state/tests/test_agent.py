import zookeeper

from twisted.internet.defer import inlineCallbacks
from txzookeeper.tests.utils import deleteTree

from juju.lib.testing import TestCase
from juju.state.base import StateBase
from juju.state.agent import AgentStateMixin


class DomainObject(StateBase, AgentStateMixin):

    def _get_agent_path(self):
        return "/agent"


class AgentDomainTest(TestCase):

    @inlineCallbacks
    def setUp(self):
        zookeeper.set_debug_level(0)
        yield super(TestCase, self).setUp()
        self.client = self.get_zookeeper_client()
        yield self.client.connect()

    @inlineCallbacks
    def tearDown(self):
        yield self.client.close()
        client = self.get_zookeeper_client()
        yield client.connect()
        deleteTree("/", client.handle)
        yield client.close()

    @inlineCallbacks
    def test_has_agent(self):
        domain = DomainObject(self.client)
        exists = yield domain.has_agent()
        self.assertIs(exists, False)
        yield domain.connect_agent()
        exists = yield domain.has_agent()
        self.assertIs(exists, True)

    @inlineCallbacks
    def test_watch_agent(self):
        domain = DomainObject(self.client)
        results = []

        def on_change(event):
            results.append(event)

        exists_d, watch_d = domain.watch_agent()
        exists = yield exists_d
        self.assertIs(exists, False)
        watch_d.addCallback(on_change)
        self.assertFalse(results)

        # connect the agent, and ensure its observed.
        yield domain.connect_agent()
        yield self.sleep(0.1)
        self.assertEqual(len(results), 1)

        # restablish the watch and manually delete.
        exists_d, watch_d = domain.watch_agent()
        exists = yield exists_d
        self.assertIs(exists, True)
        watch_d.addCallback(on_change)
        yield self.client.delete("/agent")

        self.assertEqual(results[0].type_name, "created")
        self.assertEqual(results[1].type_name, "deleted")

    @inlineCallbacks
    def test_connect_agent(self):
        client = self.get_zookeeper_client()
        yield client.connect()

        exists_d, watch_d = self.client.exists_and_watch("/agent")
        exists = yield exists_d
        self.assertFalse(exists)

        domain = DomainObject(client)
        yield domain.connect_agent()

        event = yield watch_d
        self.assertEqual(event.type_name, "created")

        exists_d, watch_d = self.client.exists_and_watch("/agent")
        self.assertTrue((yield exists_d))

        # Force the connection of the domain object to disappear
        yield client.close()

        event = yield watch_d
        self.assertEqual(event.type_name, "deleted")
