from twisted.internet.defer import fail, succeed

from juju.errors import MachinesNotFound
from juju.lib.testing import TestCase
from juju.providers.orchestra.machine import OrchestraMachine

from .common import OrchestraTestMixin


class SomeError(Exception):
    pass


class GetMachinesTest(TestCase, OrchestraTestMixin):

    def assert_not_found(self, d, instance_ids):
        self.assertFailure(d, MachinesNotFound)

        def verify(error):
            self.assertEquals(error.instance_ids, instance_ids)
        d.addCallback(verify)
        return d

    def assert_machine(self, machine, uid, name):
        self.assertTrue(isinstance(machine, OrchestraMachine))
        self.assertEquals(machine.instance_id, uid)
        self.assertEquals(machine.dns_name, name)

    def _system(self, uid, name, mgmt_classes):
        return {"uid": uid,
                "name": name,
                "mgmt_classes": mgmt_classes,
                "netboot_enabled": True}

    def mock_get_systems_success(self):
        self.proxy_m.callRemote("get_systems")
        self.mocker.result(succeed([
            self._system("foo", "irrelevant", ["bad"]),
            self._system("bar", "barrr", ["acquired"]),
            self._system("baz", "bazzz", ["acquired"])]))

    def test_multiple_success(self):
        self.setup_mocks()
        self.mock_get_systems_success()
        self.mocker.replay()

        provider = self.get_provider()
        d = provider.get_machines()

        def verify(result):
            (bar, baz) = result
            self.assert_machine(bar, "bar", "barrr")
            self.assert_machine(baz, "baz", "bazzz")
        d.addCallback(verify)
        return d

    def test_multiple_failure(self):
        self.setup_mocks()
        self.mock_get_systems_success()
        self.mocker.replay()

        provider = self.get_provider()
        d = provider.get_machines(["foo", "bar"])
        return self.assert_not_found(d, ["foo"])

    def test_multiple_error(self):
        self.setup_mocks()
        self.proxy_m.callRemote("get_systems")
        self.mocker.result(fail(SomeError()))
        self.mocker.replay()

        provider = self.get_provider()
        d = provider.get_machines(["foo", "bar"])
        self.assertFailure(d, SomeError)
        return d

    def test_one_success(self):
        self.setup_mocks()
        self.mock_get_systems_success()
        self.mocker.replay()

        provider = self.get_provider()
        d = provider.get_machine("bar")
        d.addCallback(self.assert_machine, "bar", "barrr")
        return d

    def test_one_failure(self):
        self.setup_mocks()
        self.mock_get_systems_success()
        self.mocker.replay()

        provider = self.get_provider()
        d = provider.get_machine("foo")
        return self.assert_not_found(d, ["foo"])

    def test_one_error(self):
        self.setup_mocks()
        self.proxy_m.callRemote("get_systems")
        self.mocker.result(fail(SomeError()))
        self.mocker.replay()

        provider = self.get_provider()
        d = provider.get_machine("foo")
        self.assertFailure(d, SomeError)
        return d
