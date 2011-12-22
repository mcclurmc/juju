from twisted.internet.defer import succeed

from juju.errors import MachinesNotFound
from juju.lib.testing import TestCase
from juju.providers.orchestra.machine import OrchestraMachine

from .common import OrchestraTestMixin


class SomeError(Exception):
    pass


class ShutdownTest(OrchestraTestMixin, TestCase):

    def assert_machine(self, machine, uid, name):
        self.assertTrue(isinstance(machine, OrchestraMachine))
        self.assertEquals(machine.instance_id, uid)
        self.assertEquals(machine.dns_name, name)

    def assert_missing(self, d, instance_ids):
        self.assertFailure(d, MachinesNotFound)

        def verify(error):
            self.assertEquals(error.instance_ids, instance_ids)
        d.addCallback(verify)
        return d

    def mock_get_systems(self, *systems):
        self.proxy_m.callRemote("get_systems")
        # skip an expected ks_meta key, to verify we can handle situations
        # where we never managed to start it properly in the first place
        self.mocker.result(succeed([
            {"uid": uid, "name": name, "mgmt_classes": ["keep", cls],
            "netboot_enabled": True,
            "ks_meta": {"MACHINE_ID": "123", "PRESERVE": "blob"}}
            for (uid, name, cls) in systems]))

    def mock_shutdown_release(self, uid, name):
        self.mock_get_systems((uid, name, "acquired"))
        self.proxy_m.callRemote("find_system", {"uid": uid})
        self.mocker.result(succeed([name]))
        self.proxy_m.callRemote("get_system_handle", name, "")
        self.mocker.result(succeed("some-handle"))
        self.proxy_m.callRemote(
            "modify_system", "some-handle", "ks_meta", {"PRESERVE": "blob"},
            "")
        self.mocker.result(succeed(True))
        self.proxy_m.callRemote(
            "modify_system", "some-handle",
            "mgmt_classes", ["keep", "available"], "")
        self.mocker.result(succeed(True))
        self.proxy_m.callRemote(
            "modify_system", "some-handle", "netboot_enabled", True, "")
        self.mocker.result(succeed(True))
        self.proxy_m.callRemote("save_system", "some-handle", "")
        self.mocker.result(succeed(True))
        self.proxy_m.callRemote(
            "background_power_system", {"power": "off", "systems": [name]}, "")
        self.mocker.result(succeed("ignored"))

    def check_shutdown_all(self, method_name):
        self.mock_get_systems(
            ("i-amok", "ok", "acquired"),
            ("i-amalien", "alien", "available"),
            ("i-amoktoo", "oktoo", "acquired"))
        self.mock_get_systems(
            ("i-amok", "ok", "acquired"),
            ("i-amalien", "alien", "available"),
            ("i-amoktoo", "oktoo", "acquired"))
        self.mock_shutdown_release("i-amok", "ok")
        self.mock_shutdown_release("i-amoktoo", "oktoo")
        self.mocker.replay()

        provider = self.get_provider()
        method = getattr(provider, method_name)
        d = method()

        def verify(machines):
            (ok, oktoo) = machines
            self.assert_machine(ok, "i-amok", "ok")
            self.assert_machine(oktoo, "i-amoktoo", "oktoo")
        d.addCallback(verify)
        return d

    def test_shutdown_machine(self):
        self.setup_mocks()
        self.mock_get_systems(("i-amhere", "blah", "acquired"))
        self.mock_shutdown_release("i-amhere", "blah")
        self.mocker.replay()

        provider = self.get_provider()
        d = provider.shutdown_machine(OrchestraMachine("i-amhere"))
        d.addCallback(self.assert_machine, "i-amhere", "blah")
        return d

    def test_shutdown_machine_missing(self):
        self.setup_mocks()
        self.mock_get_systems(("i-amirrelevant", "blah", "acquired"))
        self.mocker.replay()

        provider = self.get_provider()
        d = provider.shutdown_machine(OrchestraMachine("i-ammissing"))
        return self.assert_missing(d, ["i-ammissing"])

    def test_shutdown_machine_unowned(self):
        self.setup_mocks()
        self.mock_get_systems(("i-amalien", "blah", "available"))
        self.mocker.replay()

        provider = self.get_provider()
        d = provider.shutdown_machine(OrchestraMachine("i-amalien"))
        return self.assert_missing(d, ["i-amalien"])

    def test_shutdown_machines_none(self):
        self.mocker.replay()
        provider = self.get_provider()
        d = provider.shutdown_machines([])
        d.addCallback(self.assertEquals, [])
        return d

    def test_shutdown_machines_some_good(self):
        self.setup_mocks()
        self.mock_get_systems(
            ("i-amok", "ok", "acquired"),
            ("i-amalien", "alien", "available"),
            ("i-amoktoo", "oktoo", "acquired"))
        self.mock_shutdown_release("i-amok", "ok")
        self.mock_shutdown_release("i-amoktoo", "oktoo")
        self.mocker.replay()

        provider = self.get_provider()
        d = provider.shutdown_machines([
            OrchestraMachine("i-amok"),
            OrchestraMachine("i-amoktoo")])

        def verify(machines):
            (ok, oktoo) = machines
            self.assert_machine(ok, "i-amok", "ok")
            self.assert_machine(oktoo, "i-amoktoo", "oktoo")
        d.addCallback(verify)
        return d

    def test_shutdown_machines_some_missing(self):
        self.setup_mocks()
        self.mock_get_systems(
            ("i-amok", "ok", "acquired"),
            ("i-amalien", "alien", "available"),
            ("i-amoktoo", "oktoo", "acquired"))
        self.mocker.replay()

        provider = self.get_provider()
        d = provider.shutdown_machines([
            OrchestraMachine("i-amok"),
            OrchestraMachine("i-ammissing")])
        return self.assert_missing(d, ["i-ammissing"])

    def test_shutdown_machines_some_unowned(self):
        self.setup_mocks()
        self.mock_get_systems(
            ("i-amok", "ok", "acquired"),
            ("i-amalien", "alien", "available"),
            ("i-amoktoo", "oktoo", "acquired"))
        self.mocker.replay()

        provider = self.get_provider()
        d = provider.shutdown_machines([
            OrchestraMachine("i-amok"),
            OrchestraMachine("i-amalien")])
        return self.assert_missing(d, ["i-amalien"])

    def test_destroy_environment(self):
        self.setup_mocks()
        self.mock_fs_put("http://somewhe.re/webdav/provider-state", "{}\n")
        return self.check_shutdown_all("destroy_environment")

    def test_destroy_environment_no_machines(self):
        self.setup_mocks()
        self.mock_fs_put("http://somewhe.re/webdav/provider-state", "{}\n")
        self.mock_get_systems()
        self.mocker.replay()

        provider = self.get_provider()
        d = provider.destroy_environment()
        d.addCallback(self.assertEquals, [])
        return d

    def test_destroy_environment_unwritable(self):
        self.setup_mocks()
        self.mock_fs_put(
            "http://somewhe.re/webdav/provider-state", "{}\n", 500)
        return self.check_shutdown_all("destroy_environment")
