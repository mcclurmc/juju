from base64 import b64decode
import os
from xmlrpclib import Fault
from yaml import dump, load

from twisted.internet.defer import fail, succeed
from twisted.web.error import Error
from twisted.web.xmlrpc import Proxy

from juju.lib.mocker import ANY, MATCH
from juju.providers.orchestra import MachineProvider

DATA_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), "data")

CONFIG = {"type": "orchestra",
          "juju-origin": "distro",
          "orchestra-server": "somewhe.re",
          "orchestra-user": "user",
          "orchestra-pass": "pass",
          "acquired-mgmt-class": "acquired",
          "available-mgmt-class": "available",
          "admin-secret": "SEEKRIT",
          "storage-url": "http://somewhe.re/webdav",
          "authorized-keys": "this-is-a-public-key"}


class OrchestraTestMixin(object):

    def get_provider(self):
        return MachineProvider("tetrascape", CONFIG)

    def setup_mocks(self):
        self.proxy_m = self.mocker.mock(Proxy)
        Proxy_m = self.mocker.replace(Proxy, spec=None)
        Proxy_m("http://somewhe.re/cobbler_api")
        self.mocker.result(self.proxy_m)
        self.getPage = self.mocker.replace("twisted.web.client.getPage")
        self.get_page_auth = self.mocker.replace(
            "juju.providers.orchestra.digestauth.get_page_auth")

    def mock_fs_get(self, url, code, content=None):
        self.getPage(url)
        if code == 200:
            self.mocker.result(succeed(content))
        else:
            self.mocker.result(fail(Error(str(code))))

    def mock_fs_put(self, url, expect, code=201):
        # NOTE: in some respects, it would be better to simulate the complete
        # interaction with the webdav provider; the factors that work against
        # doing so are:
        # 1) authentication is tested on DigestAuthenticator, on get_page_auth,
        #    and again on FileStorage; even if it were easy to do so, testing
        #    the same paths at yet another level starts to feel somewhat
        #    superfluous.
        # 2) it's not *easy* to do so: we'd have a unique storage URL base for
        #    every test, which involves a custom config dict for every test,
        #    and unique URLs to check for every test that hits webdav, and
        #    another base class to setUp/tearDown. that's not to say it's
        #    *hard* to do so, but it's time-consuming and costs more complexity
        #    -- in a large number of the orchestra tests -- than is warranted
        #    by whatever additional verification it might allow for.
        self.get_page_auth(url, ANY, method="PUT", postdata=expect)
        if code in (201, 204):
            self.mocker.result(succeed(""))
        else:
            self.mocker.result(fail(Error(str(code))))

    def mock_find_zookeepers(self, existing=None):
        url = "http://somewhe.re/webdav/provider-state"
        if existing is None:
            self.mock_fs_get(url, 404)
        else:
            uid, name = existing
            content = dump({"zookeeper-instances": [uid]})
            self.mock_fs_get(url, 200, content)
            self.mock_describe_systems(succeed([{
                "uid": uid, "name": name, "mgmt_classes": ["acquired"],
                "netboot_enabled": True}]))

    def mock_get_systems(self, acceptable=True, unacceptable=True):
        self.proxy_m.callRemote("find_system", {"netboot_enabled": "true",
                                                "mgmt_classes": "available"})
        systems = []
        if unacceptable:
            systems.append("bad-system")
        if acceptable:
            systems.append("good-system")
        self.mocker.result(systems)

        if unacceptable:
            self.proxy_m.callRemote("get_system", "bad-system")
            self.mocker.result({"mgmt_classes": ["available", "acquired"],
                                "uid": "bad-system-uid"})
        if acceptable:
            self.proxy_m.callRemote("get_system", "good-system")
            self.mocker.result({"mgmt_classes": ["preserve_me", "available"],
                                "uid": "winston-uid",
                                "netboot_enabled": True})

    def mock_acquire_system(self, unexpected_auth_error=None):
        self.proxy_m.callRemote("find_system", {"uid": "winston-uid"})
        self.mocker.result(succeed(["winston"]))
        self.proxy_m.callRemote("get_system_handle", "winston", "")
        if unexpected_auth_error is not None:
            self.mocker.result(fail(unexpected_auth_error))
            return
        self.mocker.result(fail(Fault("blah", "invalid token")))
        self.proxy_m.callRemote("login", "user", "pass")
        self.mocker.result(succeed("TOKEN"))
        self.proxy_m.callRemote("get_system_handle", "winston", "TOKEN")
        self.mocker.result(succeed("smith"))
        self.proxy_m.callRemote(
            "modify_system", "smith", "mgmt_classes",
            ["preserve_me", "acquired"], "TOKEN")
        self.mocker.result(succeed(True))
        self.proxy_m.callRemote("save_system", "smith", "TOKEN")
        self.mocker.result(succeed(True))

    def get_verify_ks_meta(self, machine_id, user_data_filename):

        def verify(ks_meta):
            self.assertEquals(ks_meta["MACHINE_ID"], str(machine_id))
            user_data = load(b64decode(ks_meta["USER_DATA_BASE64"]))
            expect_path = os.path.join(DATA_DIR, user_data_filename)
            with open(expect_path) as f:
                expect_user_data = load(f.read())
            self.assertEquals(user_data, expect_user_data)
            return True
        return verify

    def mock_start_system(
            self, verify_ks_meta, fail_modify=False, fail_save=False):
        self.proxy_m.callRemote("get_systems")
        self.mocker.result(succeed([{
            "uid": "winston-uid", "mgmt_classes": ["acquired"],
            "ks_meta": {}}])),
        self.proxy_m.callRemote("find_system", {"uid": "winston-uid"})
        self.mocker.result(succeed(["winston"]))
        self.proxy_m.callRemote("get_system_handle", "winston", "TOKEN")
        self.mocker.result(succeed("smith"))

        match_ks_meta = MATCH(verify_ks_meta)
        self.proxy_m.callRemote(
            "modify_system", "smith", "ks_meta", match_ks_meta, "TOKEN")
        if fail_modify:
            self.mocker.result(succeed(False))
            return
        self.mocker.result(succeed(True))

        self.proxy_m.callRemote(
            "modify_system", "smith", "netboot_enabled", True, "TOKEN")
        self.mocker.result(succeed(True))
        self.proxy_m.callRemote("save_system", "smith", "TOKEN")
        if fail_save:
            self.mocker.result(succeed(False))
            return
        self.mocker.result(succeed(True))

        self.proxy_m.callRemote(
            "background_power_system",
            {"power": "on", "systems": ["winston"]},
            "TOKEN")
        self.mocker.result(succeed("[some-timestamp]_power"))

    def mock_describe_systems(self, result):
        self.proxy_m.callRemote("get_systems")
        self.mocker.result(result)
