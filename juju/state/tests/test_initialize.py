import zookeeper

from twisted.internet.defer import inlineCallbacks

from txzookeeper import ZookeeperClient
from txzookeeper.tests.utils import deleteTree

from juju.state.auth import  make_identity
from juju.state.environment import GlobalSettingsStateManager
from juju.state.initialize import StateHierarchy
from juju.state.machine import MachineStateManager

from juju.lib.testing import TestCase
from juju.tests.common import get_test_zookeeper_address


class LayoutTest(TestCase):

    def setUp(self):
        self.log = self.capture_logging("juju.state.init")
        zookeeper.set_debug_level(0)
        self.client = ZookeeperClient(get_test_zookeeper_address())
        self.identity = make_identity("admin:genie")
        self.layout = StateHierarchy(
            self.client, self.identity, "i-abcdef", "dummy")
        return self.client.connect()

    def tearDown(self):
        deleteTree(handle=self.client.handle)
        self.client.close()

    @inlineCallbacks
    def assert_existence_and_acl(self, path):
        exists = yield self.client.exists(path)
        self.assertTrue(exists)
        acls, stat = yield self.client.get_acl(path)

        found_admin_acl = False
        for acl in acls:
            if acl["id"] == self.identity \
                   and acl["perms"] == zookeeper.PERM_ALL:
                found_admin_acl = True
                break
        self.assertTrue(found_admin_acl)

    @inlineCallbacks
    def test_initialize(self):
        yield self.layout.initialize()

        yield self.assert_existence_and_acl("/charms")
        yield self.assert_existence_and_acl("/services")
        yield self.assert_existence_and_acl("/units")
        yield self.assert_existence_and_acl("/machines")
        yield self.assert_existence_and_acl("/relations")
        yield self.assert_existence_and_acl("/initialized")

        machine_state_manager = MachineStateManager(self.client)
        machine_state = yield machine_state_manager.get_machine_state(0)
        self.assertTrue(machine_state)
        instance_id = yield machine_state.get_instance_id()
        self.assertEqual(instance_id, "i-abcdef")

        settings_manager = GlobalSettingsStateManager(self.client)
        self.assertEqual((yield settings_manager.get_provider_type()), "dummy")
        self.assertEqual(
            self.log.getvalue().strip(),
            "Initializing zookeeper hierarchy")
