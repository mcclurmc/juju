from xmlrpclib import Fault
from yaml import dump

from twisted.internet.defer import succeed, inlineCallbacks

from juju.errors import ProviderError
from juju.lib.testing import TestCase
from juju.providers.orchestra.machine import OrchestraMachine
from juju.providers.orchestra.tests.common import OrchestraTestMixin


class OrchestraBootstrapTest(TestCase, OrchestraTestMixin):

    def mock_verify(self):
        self.mock_fs_put("http://somewhe.re/webdav/bootstrap-verify",
                         "storage is writable")

    def mock_save_state(self):
        data = dump({"zookeeper-instances": ["winston-uid"]})
        self.mock_fs_put("http://somewhe.re/webdav/provider-state", data)

    def mock_surprise_shutdown(self):
        self.proxy_m.callRemote("get_systems")
        self.mocker.result(succeed([{
            "uid": "winston-uid",
            "ks_meta": {
                "MACHINE_ID": "blah",
                "USER_DATA_BASE64": "userdata",
                "KEEP": "keep"},
            "mgmt_classes": ["acquired", "PRESERVE"]}]))
        self.proxy_m.callRemote("find_system", {"uid": "winston-uid"})
        self.mocker.result(succeed(["winston"]))
        self.proxy_m.callRemote("get_system_handle", "winston", "TOKEN")
        self.mocker.result(succeed("some-handle"))
        self.proxy_m.callRemote(
            "modify_system", "some-handle", "ks_meta", {"KEEP": "keep"},
            "TOKEN")
        self.mocker.result(succeed(True))
        self.proxy_m.callRemote(
            "modify_system", "some-handle",
            "mgmt_classes", ["available", "PRESERVE"], "TOKEN")
        self.mocker.result(succeed(True))
        self.proxy_m.callRemote(
            "modify_system", "some-handle", "netboot_enabled", True, "TOKEN")
        self.mocker.result(succeed(True))
        self.proxy_m.callRemote("save_system", "some-handle", "TOKEN")
        self.mocker.result(succeed(True))
        self.proxy_m.callRemote(
            "background_power_system",
            {"power": "off", "systems": ["winston"]}, "TOKEN")
        self.mocker.result(succeed("ignored"))

    def test_already_bootstrapped(self):
        self.setup_mocks()
        self.mock_find_zookeepers(("winston-uid", "winston"))
        self.mocker.replay()

        def verify_machines(machines):
            (machine,) = machines
            self.assertTrue(isinstance(machine, OrchestraMachine))
            self.assertEquals(machine.instance_id, "winston-uid")
        d = self.get_provider().bootstrap()
        d.addCallback(verify_machines)
        return d

    def test_no_machines_available(self):
        self.setup_mocks()
        self.mock_find_zookeepers()
        self.mock_verify()
        self.mock_get_systems(acceptable=False)
        self.mocker.replay()
        d = self.get_provider().bootstrap()
        self.assertFailure(d, ProviderError)

    def verify_auth_error(self, error):
        self.setup_mocks()
        self.mock_find_zookeepers()
        self.mock_verify()
        self.mock_get_systems()
        self.mock_acquire_system(error)
        self.mocker.replay()
        d = self.get_provider().bootstrap()
        self.assertFailure(d, type(error))

    def test_non_auth_fault(self):
        return self.verify_auth_error(Fault("blah", "some random error"))

    def test_non_auth_error(self):
        return self.verify_auth_error(Exception("fiddlesticks"))

    @inlineCallbacks
    def verify_change_failures(self, **kwargs):
        log = self.capture_logging("juju.orchestra")
        self.setup_mocks()
        self.mock_find_zookeepers()
        self.mock_verify()
        self.mock_get_systems()
        self.mock_acquire_system()
        self.mock_start_system(
            self.get_verify_ks_meta(0, "bootstrap_user_data"), **kwargs)
        self.mock_surprise_shutdown()
        self.mocker.replay()
        d = self.get_provider().bootstrap()
        yield self.assertFailure(d, ProviderError)
        self.assertIn(
            "Failed to launch machine winston-uid; attempting to revert.",
            log.getvalue())

    def test_cannot_modify_machine(self):
        """
        Check that failures when launching the machine cause an (attempt to)
        roll back to an unacquired state.
        """
        return self.verify_change_failures(fail_modify=True)

    def test_cannot_save_machine(self):
        """
        Check that failures when launching the machine cause an (attempt to)
        roll back to an unacquired state.
        """
        return self.verify_change_failures(fail_save=True)

    def test_launch_available_machine(self):
        self.setup_mocks()
        self.mock_find_zookeepers()
        self.mock_verify()
        self.mock_get_systems()
        self.mock_acquire_system()
        self.mock_start_system(
            self.get_verify_ks_meta(0, "bootstrap_user_data"))
        self.mock_describe_systems(succeed([{
            "uid": "winston-uid",
            "name": "winston",
            "mgmt_classes": ["acquired"],
            "netboot_enabled": True}]))
        self.mock_save_state()
        self.mocker.replay()

        def verify_machines(machines):
            (machine,) = machines
            self.assertTrue(isinstance(machine, OrchestraMachine))
            self.assertEquals(machine.instance_id, "winston-uid")
            self.assertEquals(machine.dns_name, "winston")
        d = self.get_provider().bootstrap()
        d.addCallback(verify_machines)
        return d
