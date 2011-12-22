from yaml import dump

from twisted.internet.defer import succeed

from juju.errors import EnvironmentNotFound
from juju.lib.testing import TestCase
from juju.providers.orchestra import MachineProvider
from juju.providers.orchestra.machine import OrchestraMachine

from .common import OrchestraTestMixin

CONFIG = {"orchestra-server": "somewhe.re",
          "storage-url": "http://somewhe.re/webdav",
          "orchestra-user": "user",
          "orchestra-pass": "pass",
          "acquired-mgmt-class": "acquired",
          "available-mgmt-class": "available"}


class FindZookeepersTest(TestCase, OrchestraTestMixin):

    def get_provider(self):
        return MachineProvider("tetrascape", CONFIG)

    def mock_load_state(self, code, content):
        self.mock_fs_get(
            "http://somewhe.re/webdav/provider-state", code, content)

    def assert_no_environment(self):
        provider = self.get_provider()
        d = provider.get_zookeeper_machines()
        self.assertFailure(d, EnvironmentNotFound)
        return d

    def verify_no_environment(self, code, content):
        self.mock_load_state(code, content)
        self.mocker.replay()
        return self.assert_no_environment()

    def test_no_state(self):
        self.setup_mocks()
        self.verify_no_environment(404, None)

    def test_empty_state(self):
        self.setup_mocks()
        self.verify_no_environment(200, dump([]))

    def test_no_hosts(self):
        self.setup_mocks()
        self.verify_no_environment(200, dump({"abc": 123}))

    def test_bad_instance(self):
        self.setup_mocks()
        self.mock_load_state(200, dump({"zookeeper-instances": ["foo"]}))
        self.proxy_m.callRemote("get_systems")
        self.mocker.result(succeed([]))
        self.mocker.replay()

        return self.assert_no_environment()

    def _system(self, uid, name, mgmt_classes):
        return {"uid": uid,
                "name": name,
                "mgmt_classes": mgmt_classes,
                "netboot_enabled": True}

    def test_eventual_success(self):
        self.setup_mocks()
        self.mock_load_state(200, dump({
            "zookeeper-instances": ["bad", "foo", "missing", "bar"]}))
        for _ in range(4):
            self.proxy_m.callRemote("get_systems")
            self.mocker.result(succeed([
                self._system("bad", "whatever", ["whatever"]),
                self._system("foo", "foo", ["acquired"]),
                self._system("bar", "bar", ["acquired"])]))
        self.mocker.replay()

        provider = self.get_provider()
        d = provider.get_zookeeper_machines()

        def verify_machine(machines):
            (foo, bar) = machines
            self.assertTrue(isinstance(foo, OrchestraMachine))
            self.assertEquals(foo.instance_id, "foo")
            self.assertTrue(isinstance(bar, OrchestraMachine))
            self.assertEquals(bar.instance_id, "bar")
        d.addCallback(verify_machine)
        return d
