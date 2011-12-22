import logging
import os
import pwd
from StringIO import StringIO
import zookeeper

from twisted.internet.defer import succeed, inlineCallbacks

from txzookeeper.tests.utils import deleteTree

from juju.errors import ProviderError, EnvironmentNotFound
from juju.lib.lxc import LXCContainer
from juju.lib.zk import Zookeeper
from juju.providers.local import MachineProvider
from juju.providers import local
from juju.providers.local.agent import ManagedMachineAgent
from juju.providers.local.files import StorageServer
from juju.providers.local.network import Network
from juju.lib import lxc as lxc_lib

from juju.lib.testing import TestCase
from juju.tests.common import get_test_zookeeper_address


class LocalProviderTest(TestCase):

    @inlineCallbacks
    def setUp(self):
        self.provider = MachineProvider(
            "local-test", {
                "admin-secret": "admin:abc", "data-dir": self.makeDir(),
                "authorized-keys": "fooabc123"})
        self.output = self.capture_logging(
            "juju.local-dev", level=logging.DEBUG)
        zookeeper.set_debug_level(0)
        self.client = yield self.get_zookeeper_client().connect()

    def tearDown(self):
        deleteTree("/", self.client.handle)
        self.client.close()

    @property
    def qualified_name(self):
        user_name = pwd.getpwuid(os.getuid()).pw_name
        return "%s-%s" % (user_name, self.provider.environment_name)

    def test_get_placement_policy(self):
        """Lxc provider only supports local placement."""
        self.assertEqual(self.provider.get_placement_policy(), "local")
        provider = MachineProvider(
            "test", {"placement": "unassigned",
                     "data-dir": self.makeDir()})
        self.assertRaises(
            ProviderError, provider.get_placement_policy)

    def assertDir(self, *path_parts):
        path = os.path.join(*path_parts)
        self.assertTrue(os.path.isdir(path))

    def bootstrap_mock(self):
        self.patch(local, "REQUIRED_PACKAGES", [])
        mock_network = self.mocker.patch(Network)
        mock_network.start()
        self.mocker.result(succeed(True))
        mock_network.get_attributes()
        self.mocker.result({"ip": {"address": "127.0.0.1"}})

        mock_storage = self.mocker.patch(StorageServer)
        mock_storage.start()
        self.mocker.result(succeed(True))

        mock_zookeeper = self.mocker.patch(Zookeeper)
        mock_zookeeper.start()
        self.mocker.result(succeed(True))

        mock_zookeeper.address
        self.mocker.result(get_test_zookeeper_address())
        self.mocker.count(3)

        mock_agent = self.mocker.patch(ManagedMachineAgent)
        mock_agent.start()
        self.mocker.result(succeed(True))

    def test_provider_type(self):
        self.assertEqual(self.provider.provider_type, "local")

    @inlineCallbacks
    def test_bootstrap(self):
        self.bootstrap_mock()
        self.mocker.replay()

        yield self.provider.bootstrap()

        children = yield self.client.get_children("/")
        self.assertEqual(
            sorted(['services', 'settings', 'charms', 'relations', 'zookeeper',
                    'initialized', 'topology', 'machines', 'units']),
            sorted(children))
        output = self.output.getvalue()
        self.assertIn("Starting networking...", output)
        self.assertIn("Starting zookeeper...", output)
        self.assertIn("Initializing state...", output)
        self.assertIn("Starting storage server", output)
        self.assertIn("Starting machine agent", output)
        self.assertIn("Environment bootstrapped", output)

        self.assertDir(
            self.provider.config["data-dir"], self.qualified_name, "files")
        self.assertDir(
            self.provider.config["data-dir"], self.qualified_name, "zookeeper")

        self.assertEqual((yield self.provider.load_state()),
                         {"zookeeper-address": get_test_zookeeper_address(),
                          "zookeeper-instances": ["local"]})

    @inlineCallbacks
    def test_boostrap_previously_bootstrapped(self):
        """Any local state is a sign that we had a previous bootstrap.
        """
        yield self.provider.save_state({"xyz": 1})
        error = yield self.assertFailure(
            self.provider.bootstrap(), ProviderError)
        self.assertEqual(str(error), "Environment already bootstrapped")

    @inlineCallbacks
    def test_destroy_environment(self):
        """Destroying a local environment kills units, zk, and machine agent.
        """
        user_name = pwd.getpwuid(os.getuid()).pw_name
        # Mock container destruction, including stopping running contianers.
        lxc_ls_mock = self.mocker.mock()
        self.patch(lxc_lib, "_cmd", lxc_ls_mock)
        lxc_ls_mock(["lxc-ls"])
        self.mocker.result(
            (0,
             "%(name)s-local-test-unit-1\n%(name)s-local-test-unit-2\n"
             "%(name)s-local-test-unit-3\n%(name)s-local-test-unit-1\n"
             "%(name)s-local-test-unit-2\n" % dict(name=user_name)))

        mock_container = self.mocker.patch(LXCContainer)
        mock_container.stop()
        self.mocker.count(2)
        mock_container.destroy()
        self.mocker.count(3)

        mock_agent = self.mocker.patch(ManagedMachineAgent)
        mock_agent.stop()

        mock_server = self.mocker.patch(StorageServer)
        mock_server.stop()

        mock_zookeeper = self.mocker.patch(Zookeeper)
        mock_zookeeper.stop()
        yield self.provider.save_state({"i-exist": 1})
        self.mocker.replay()

        yield self.provider.destroy_environment()

        output = self.output.getvalue()
        self.assertIn("Environment destroyed", output)

        self.assertEqual((yield self.provider.load_state()), False)

    @inlineCallbacks
    def test_connect(self):
        self.bootstrap_mock()
        self.mocker.replay()
        yield self.provider.bootstrap()
        client = yield self.provider.connect()
        self.assertTrue((yield client.exists("/initialized")))

    def test_connect_sans_environment(self):
        return self.assertFailure(self.provider.connect(), EnvironmentNotFound)

    def test_shutdown_machine(self):
        return self.assertFailure(
            self.provider.shutdown_machine("local"), ProviderError)

    def test_start_machine(self):
        return self.assertFailure(
            self.provider.start_machine({}), ProviderError)

    @inlineCallbacks
    def test_get_machines(self):
        machines = yield self.provider.get_machines()
        self.assertEqual(len(machines), 1)
        self.assertEqual(machines[0].instance_id, "local")
        self.assertEqual(machines[0].dns_name, "localhost")

    @inlineCallbacks
    def test_get_file_storage(self):
        storage = self.provider.get_file_storage()
        content = StringIO("put")
        yield storage.put("abc", content)
        data_dir = self.provider.config["data-dir"]
        self.assertTrue("abc" in os.listdir(
            os.path.join(data_dir, self.qualified_name, "files")))
