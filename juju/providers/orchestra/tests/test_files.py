from cStringIO import StringIO

from twisted.internet.defer import fail, succeed
from twisted.web.error import Error

from juju.errors import FileNotFound, ProviderError, ProviderInteractionError
from juju.lib.testing import TestCase
from juju.providers.orchestra import MachineProvider

from .test_digestauth import GetPageAuthTestCase


class SomeError(Exception):
    pass


def get_file_storage(custom_config=None):
    config = {"orchestra-server": "somewhereel.se",
              "orchestra-user": "fallback-user",
              "orchestra-pass": "fallback-pass",
              "acquired-mgmt-class": "acquired",
              "available-mgmt-class": "available"}
    if custom_config is None:
        config["storage-url"] = "http://somewhe.re"
        config["storage-user"] = "user"
        config["storage-pass"] = "pass"
    else:
        config.update(custom_config)
    provider = MachineProvider("blah", config)
    return provider.get_file_storage()


class FileStorageGetTest(TestCase):

    def setUp(self):
        self.uuid4_m = self.mocker.replace("uuid.uuid4")
        self.getPage = self.mocker.replace("twisted.web.client.getPage")

    def test_get_url(self):
        self.mocker.replay()
        fs = get_file_storage()
        self.assertEquals(fs.get_url("angry/birds"),
                          "http://somewhe.re/angry/birds")

    def test_get_url_fallback(self):
        self.mocker.replay()
        fs = get_file_storage({})
        self.assertEquals(fs.get_url("angry/birds"),
                          "http://somewhereel.se/webdav/angry/birds")

    def test_get(self):
        self.getPage("http://somewhe.re/rubber/chicken")
        self.mocker.result(succeed("pulley"))
        self.mocker.replay()

        fs = get_file_storage()
        d = fs.get("rubber/chicken")

        def verify(result):
            self.assertEquals(result.read(), "pulley")
        d.addCallback(verify)
        return d

    def check_get_error(self, result, err_type, err_message):
        self.getPage("http://somewhe.re/rubber/chicken")
        self.mocker.result(result)
        self.mocker.replay()

        fs = get_file_storage()
        d = fs.get("rubber/chicken")
        self.assertFailure(d, err_type)

        def verify(error):
            self.assertEquals(str(error), err_message)
        d.addCallback(verify)
        return d

    def test_get_error(self):
        return self.check_get_error(
            fail(SomeError("pow!")),
            ProviderInteractionError,
            "Unexpected SomeError interacting with provider: pow!")

    def test_get_404(self):
        return self.check_get_error(
            fail(Error("404")),
            FileNotFound,
            "File was not found: 'http://somewhe.re/rubber/chicken'")

    def test_get_bad_code(self):
        return self.check_get_error(
            fail(Error("999")),
            ProviderError,
            "Unexpected HTTP 999 trying to GET "
            "http://somewhe.re/rubber/chicken")


class FileStoragePutTest(GetPageAuthTestCase):

    def setup_mock(self):
        self.uuid4_m = self.mocker.replace("uuid.uuid4")

    def get_file_storage(self, with_user=True):
        storage_url = self.get_base_url()
        custom_config = {"storage-url": storage_url}
        if with_user:
            custom_config["storage-user"] = "user"
            custom_config["storage-pass"] = "pass"
        return get_file_storage(custom_config)

    def test_no_auth_error(self):
        self.add_plain("peregrine", "PUT", "", "croissant", 999)
        fs = self.get_file_storage()
        d = fs.put("peregrine", StringIO("croissant"))
        self.assertFailure(d, ProviderError)

        def verify(error):
            self.assertIn("Unexpected HTTP 999 trying to PUT ", str(error))
        d.addCallback(verify)
        return d

    def test_no_auth_201(self):
        self.add_plain("peregrine", "PUT", "", "croissant", 201)
        fs = self.get_file_storage()
        d = fs.put("peregrine", StringIO("croissant"))
        d.addCallback(self.assertEquals, True)
        return d

    def test_no_auth_204(self):
        self.add_plain("peregrine", "PUT", "", "croissant", 204)
        fs = self.get_file_storage()
        d = fs.put("peregrine", StringIO("croissant"))
        d.addCallback(self.assertEquals, True)
        return d

    def auth_common(self, username, status, with_user=True):
        self.setup_mock()
        self.uuid4_m()
        self.mocker.result("dinner")
        self.mocker.replay()

        url = self.get_url("possum")

        def check(response):
            self.assertTrue(response.startswith(
                'Digest username="%s", realm="sparta", nonce="meh", uri="%s"'
                % (username, url)))
            self.assertIn(
                'qop="auth", nc="00000001", cnonce="dinner"', response)
        self.add_auth(
            "possum", "PUT", "", "Digest realm=sparta, nonce=meh, qop=auth",
            check, expect_content="canabalt", status=status)

        fs = self.get_file_storage(with_user)
        return fs.put("possum", StringIO("canabalt"))

    def test_auth_error(self):
        d = self.auth_common("user", 808)
        self.assertFailure(d, ProviderError)

        def verify(error):
            self.assertIn("Unexpected HTTP 808 trying to PUT", str(error))
        d.addCallback(verify)
        return d

    def test_auth_bad_credentials(self):
        d = self.auth_common("user", 401)
        self.assertFailure(d, ProviderError)

        def verify(error):
            self.assertEquals(
                str(error),
                "The supplied storage credentials were not accepted by the "
                "server")
        d.addCallback(verify)
        return d

    def test_auth_201(self):
        d = self.auth_common("user", 201)
        d.addCallback(self.assertEquals, True)
        return d

    def test_auth_204(self):
        d = self.auth_common("user", 204)
        d.addCallback(self.assertEquals, True)
        return d

    def test_auth_fallback_error(self):
        d = self.auth_common("fallback-user", 747, False)
        self.assertFailure(d, ProviderError)

        def verify(error):
            self.assertIn("Unexpected HTTP 747 trying to PUT", str(error))
        d.addCallback(verify)
        return d

    def test_auth_fallback_201(self):
        d = self.auth_common("fallback-user", 201, False)
        d.addCallback(self.assertEquals, True)
        return d

    def test_auth_fallback_204(self):
        d = self.auth_common("fallback-user", 204, False)
        d.addCallback(self.assertEquals, True)
        return d
