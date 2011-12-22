from xmlrpclib import Fault

from twisted.internet.defer import fail, succeed
from twisted.web.xmlrpc import Proxy

from juju.errors import MachinesNotFound, ProviderError
from juju.lib.testing import TestCase
from juju.providers.orchestra.cobbler import CobblerCaller, CobblerClient

_CONFIG = {"orchestra-server": "somewhe.re",
           "orchestra-user": "user",
           "orchestra-pass": "pass",
           "acquired-mgmt-class": "acquired",
           "available-mgmt-class": "available"}

_SOME_SYSTEM = {"name": "some-name",
                "uid": "some-uid",
                "mgmt_classes": ["preserve_me", "available"]}


def _crazy_system(uid):
    return {"uid": uid, "mgmt_classes": ["acquired", "available"]}


class SomeError(Exception):
    pass


class CobblerCallerTestsMixin(object):

    def test_call_error(self):
        """Test that an arbitrary exception is propagated"""
        caller = self.common_prefix(fail(SomeError()))
        d = caller.call("foo", ("bar", "baz"), auth=self.auth)
        self.assertFailure(d, SomeError)
        return d

    def test_call_unknown_fault(self):
        """Test that an xmlrpc Fault is propagated"""
        caller = self.common_prefix(fail(Fault("blah", "blah")))
        d = caller.call("foo", ("bar", "baz"), auth=self.auth)
        self.assertFailure(d, Fault)
        return d

    def test_call_success(self):
        """Test that results are propagated"""
        caller = self.common_prefix(succeed("result"))
        d = caller.call("foo", ("bar", "baz"), auth=self.auth)

        def verify(result):
            self.assertEquals(result, "result")
        d.addCallback(verify)
        return d

    def test_check_call_error(self):
        """Test that an arbitrary exception is propagated"""
        caller = self.common_prefix(fail(SomeError()))
        d = caller.check_call(
            "foo", ("bar", "baz"), auth=self.auth, expect="unused")
        self.assertFailure(d, SomeError)
        return d

    def test_check_call_unknown_fault(self):
        """Test that an xmlrpc Fault is propagated"""
        caller = self.common_prefix(fail(Fault("blah", "blah")))
        d = caller.check_call(
            "foo", ("bar", "baz"), auth=self.auth, expect="unused")
        self.assertFailure(d, Fault)
        return d

    def test_check_call_failure(self):
        """Test that unexpected results become errors"""
        caller = self.common_prefix(succeed("result"))
        d = caller.check_call(
            "foo", ("bar", "baz"), auth=self.auth, expect="bad")
        self.assertFailure(d, ProviderError)
        return d

    def test_check_call_success(self):
        """Test that results are propagated"""
        caller = self.common_prefix(succeed("result"))
        d = caller.check_call(
            "foo", ("bar", "baz"), auth=self.auth, expect="result")

        def verify(result):
            self.assertEquals(result, "result")
        d.addCallback(verify)
        return d


class CobblerCallerNoAuthTest(TestCase, CobblerCallerTestsMixin):

    auth = False

    def common_prefix(self, result):
        self.proxy_m = self.mocker.mock(Proxy)
        Proxy_m = self.mocker.replace(Proxy, spec=None)
        Proxy_m("http://somewhe.re/cobbler_api")
        self.mocker.result(self.proxy_m)
        self.proxy_m.callRemote("foo", "bar", "baz")
        self.mocker.result(result)
        self.mocker.replay()
        return CobblerCaller(_CONFIG)


class CobblerCallerLoggedInTest(TestCase, CobblerCallerTestsMixin):

    auth = True

    def common_prefix(self, result):
        self.proxy_m = self.mocker.mock(Proxy)
        Proxy_m = self.mocker.replace(Proxy, spec=None)
        Proxy_m("http://somewhe.re/cobbler_api")
        self.mocker.result(self.proxy_m)
        # assume cobbler_api accepts "" as an auth token
        self.proxy_m.callRemote("foo", "bar", "baz", "")
        self.mocker.result(result)
        self.mocker.replay()
        return CobblerCaller(_CONFIG)


class CobblerCallerLoginTest(TestCase, CobblerCallerTestsMixin):

    auth = True

    def common_prefix(self, result):
        self.proxy_m = self.mocker.mock(Proxy)
        Proxy_m = self.mocker.replace(Proxy, spec=None)
        Proxy_m("http://somewhe.re/cobbler_api")
        self.mocker.result(self.proxy_m)
        self.proxy_m.callRemote("foo", "bar", "baz", "")
        self.mocker.result(fail(Fault("blah", "blah invalid token blah")))
        self.proxy_m.callRemote("login", "user", "pass")
        self.mocker.result(succeed("TOKEN"))
        self.proxy_m.callRemote("foo", "bar", "baz", "TOKEN")
        self.mocker.result(result)
        self.mocker.replay()
        return CobblerCaller(_CONFIG)


class CobblerCallerBadCredentialsTest(TestCase):

    def common_prefix(self):
        self.proxy_m = self.mocker.mock(Proxy)
        Proxy_m = self.mocker.replace(Proxy, spec=None)
        Proxy_m("http://somewhe.re/cobbler_api")
        self.mocker.result(self.proxy_m)
        self.proxy_m.callRemote("foo", "bar", "baz", "")
        self.mocker.result(fail(Fault("blah", "blah invalid token blah")))
        self.proxy_m.callRemote("login", "user", "pass")

    def login_fail_prefix(self):
        self.common_prefix()
        self.mocker.result(fail(Fault("blah", "blah login failed blah")))
        self.mocker.replay()
        return CobblerCaller(_CONFIG)

    def login_error_prefix(self):
        self.common_prefix()
        self.mocker.result(fail(Fault("blah", "blah")))
        self.mocker.replay()
        return CobblerCaller(_CONFIG)

    def verify_failed(self, d):
        self.assertFailure(d, ProviderError)

        def verify(error):
            self.assertEquals(str(error),
                              "Cobbler server rejected credentials.")
        d.addCallback(verify)
        return d

    def test_call_fail(self):
        """Test bad credentials"""
        caller = self.login_fail_prefix()
        d = caller.call("foo", ("bar", "baz"), auth=True)
        return self.verify_failed(d)

    def test_check_call_fail(self):
        """Test bad credentials"""
        caller = self.login_fail_prefix()
        d = caller.check_call("foo", ("bar", "baz"), auth=True, expect="x")
        return self.verify_failed(d)

    def test_call_error(self):
        """Test that an xmlrpc Fault is propagated"""
        caller = self.login_error_prefix()
        d = caller.call("foo", ("bar", "baz"), auth=True)
        self.assertFailure(d, Fault)
        return d

    def test_check_call_error(self):
        """Test that an xmlrpc Fault is propagated"""
        caller = self.login_error_prefix()
        d = caller.check_call("foo", ("bar", "baz"), auth=True, expect="x")
        self.assertFailure(d, Fault)
        return d


class CobblerClientTestCase(TestCase):

    def setup_mock(self):
        self.proxy_m = self.mocker.mock(Proxy)
        Proxy_m = self.mocker.replace(Proxy, spec=None)
        Proxy_m("http://somewhe.re/cobbler_api")
        self.mocker.result(self.proxy_m)

    def mock_get_name(self, result):
        self.proxy_m.callRemote("find_system", {"uid": "some-uid"})
        self.mocker.result(result)

    def mock_get_system(self, result, name="some-name"):
        self.proxy_m.callRemote("get_system", name)
        self.mocker.result(result)

    def mock_get_system_handle(self, result):
        self.proxy_m.callRemote("get_system_handle", "some-name", "")
        self.mocker.result(result)

    def mock_modify_system(self, result, key="some-key", value="some-value"):
        self.proxy_m.callRemote(
            "modify_system", "some-handle", key, value, "")
        self.mocker.result(result)

    def mock_save_system(self, result):
        self.proxy_m.callRemote("save_system", "some-handle", "")
        self.mocker.result(result)

    def mock_find_system(self, result):
        self.proxy_m.callRemote("find_system", {"mgmt_classes": "available",
                                                "netboot_enabled": "true"})
        self.mocker.result(result)

    def mock_power_system(self, state, result):
        self.proxy_m.callRemote("background_power_system",
                                {"power": state, "systems": ["some-name"]}, "")
        self.mocker.result(result)

    def mock_get_systems(self, result):
        self.proxy_m.callRemote("get_systems")
        self.mocker.result(result)

    def call_cobbler_method(self, method_name, *args):
        cobbler = CobblerClient(_CONFIG)
        method = getattr(cobbler, method_name)
        return method(*args)

    def check_not_found(self, d, uids=["some-uid"]):
        self.assertFailure(d, MachinesNotFound)

        def verify(error):
            self.assertEquals(error.instance_ids, uids)
        d.addCallback(verify)
        return d

    def check_bad_call(self, d, method, args):
        self.assertFailure(d, ProviderError)

        def verify(error):
            self.assertEquals(
                str(error),
                "Bad result from call to %s with %s: got False, expected True"
                % (method, args))
        d.addCallback(verify)
        return d

    def check_get_systems_failure(self, method_name, *args):
        self.mock_get_systems(succeed([]))
        self.mocker.replay()
        d = self.call_cobbler_method(method_name, *args)
        return self.check_not_found(d)

    def check_get_systems_error(self, method_name, *args):
        self.mock_get_systems(fail(SomeError()))
        self.mocker.replay()
        d = self.call_cobbler_method(method_name, *args)
        self.assertFailure(d, SomeError)
        return d

    def check_get_name_failure(self, method_name, *args):
        self.mock_get_name(succeed([]))
        self.mocker.replay()
        d = self.call_cobbler_method(method_name, *args)
        return self.check_not_found(d)

    def check_get_name_insane(self, method_name, *args):
        self.mock_get_name(succeed(["some-name", "another-name"]))
        self.mocker.replay()
        d = self.call_cobbler_method(method_name, *args)
        self.assertFailure(d, ProviderError)

        def check_error(error):
            self.assertEquals(
                str(error),
                "Got multiple names for machine some-uid: some-name, "
                "another-name")
        d.addCallback(check_error)
        return d

    def check_get_name_error(self, method_name, *args):
        self.mock_get_name(fail(SomeError()))
        self.mocker.replay()
        d = self.call_cobbler_method(method_name, *args)
        self.assertFailure(d, SomeError)
        return d

    def check_get_system_handle_failure(self, method_name, *args):
        self.mock_get_system_handle(fail(
            Fault("blah", "blah unknown system name blah")))
        self.mocker.replay()
        d = self.call_cobbler_method(method_name, *args)
        return self.check_not_found(d)

    def check_get_system_handle_fault(self, method_name, *args):
        self.mock_get_system_handle(fail(Fault("blah", "blah")))
        self.mocker.replay()
        d = self.call_cobbler_method(method_name, *args)
        self.assertFailure(d, Fault)
        return d

    def check_get_system_handle_error(self, method_name, *args):
        self.mock_get_system_handle(fail(SomeError()))
        self.mocker.replay()
        d = self.call_cobbler_method(method_name, *args)
        self.assertFailure(d, SomeError)
        return d

    def check_modify_system_failure(self, key, value, method_name, *args):
        self.mock_modify_system(succeed(False), key, value)
        self.mocker.replay()
        d = self.call_cobbler_method(method_name, *args)
        return self.check_bad_call(
            d, "modify_system", ("some-handle", key, value))

    def check_modify_system_error(self, key, value, method_name, *args):
        self.mock_modify_system(fail(SomeError()), key, value)
        self.mocker.replay()
        d = self.call_cobbler_method(method_name, *args)
        self.assertFailure(d, SomeError)
        return d

    def check_save_system_failure(self, method_name, *args):
        self.mock_save_system(succeed(False))
        self.mocker.replay()
        d = self.call_cobbler_method(method_name, *args)
        return self.check_bad_call(d, "save_system", ("some-handle",))

    def check_save_system_error(self, method_name, *args):
        self.mock_save_system(fail(SomeError()))
        self.mocker.replay()
        d = self.call_cobbler_method(method_name, *args)
        self.assertFailure(d, SomeError)
        return d


class DescribeSystemsTest(CobblerClientTestCase):

    def test_error(self):
        """Test that an arbitrary exception from get_systems is propagated"""
        self.setup_mock()
        self.mock_get_systems(fail(SomeError()))
        self.mocker.replay()
        d = self.call_cobbler_method("describe_systems")
        self.assertFailure(d, SomeError)
        return d

    def test_failure(self):
        """Test that a bad result from get_systems is an error"""
        self.setup_mock()
        self.mock_get_systems(succeed([
            {"uid": "something-else", "mgmt_classes": "acquired"}]))
        self.mocker.replay()
        d = self.call_cobbler_method("describe_systems", "something")
        return self.check_not_found(d, ["something"])

    def test_success_all(self):
        """Test that the result of get_systems is correctly filtered"""
        self.setup_mock()
        self.mock_get_systems(succeed([
            {"uid": "bar", "mgmt_classes": ["acquired"]},
            {"uid": "baz", "mgmt_classes": ["other"]},
            {"uid": "foo", "mgmt_classes": ["acquired"]}]))
        self.mocker.replay()
        d = self.call_cobbler_method("describe_systems")

        def verify(result):
            expect = [{"uid": "bar", "mgmt_classes": ["acquired"]},
                      {"uid": "foo", "mgmt_classes": ["acquired"]}]
            self.assertEquals(result, expect)
        d.addCallback(verify)
        return d

    def test_not_acquired_some(self):
        """Test that an incomplete result from get_systems is an error"""
        self.setup_mock()
        self.mock_get_systems(succeed([
            {"uid": "bar", "mgmt_classes": ["irrelevant"]},
            {"uid": "baz", "mgmt_classes": ["acquired"]},
            {"uid": "foo", "mgmt_classes": ["omg-b0rken"]}]))
        self.mocker.replay()
        d = self.call_cobbler_method("describe_systems", "foo", "baz")
        return self.check_not_found(d, ["foo"])

    def test_success_some(self):
        """Test that a complete result from get_systems works"""
        self.setup_mock()
        self.mock_get_systems(succeed([
            {"uid": "bar", "mgmt_classes": ["ignored"]},
            {"uid": "baz", "mgmt_classes": ["acquired"]},
            {"uid": "foo", "mgmt_classes": ["acquired"]}]))
        self.mocker.replay()
        d = self.call_cobbler_method("describe_systems", "foo", "baz")

        def verify(result):
            expect = [{"uid": "foo", "mgmt_classes": ["acquired"]},
                      {"uid": "baz", "mgmt_classes": ["acquired"]}]
            self.assertEquals(result, expect)
        d.addCallback(verify)
        return d

    def test_success_none(self):
        """Test that an empty complete result from get_systems works"""
        self.setup_mock()
        self.mock_get_systems(succeed([]))
        self.mocker.replay()
        d = self.call_cobbler_method("describe_systems")

        def verify(result):
            self.assertEquals(result, [])
        d.addCallback(verify)
        return d


class AcquireSystemTest(CobblerClientTestCase):

    def test_find_error(self):
        """Test that an arbitrary exception from find_system is propagated"""
        self.setup_mock()
        self.mock_find_system(fail(SomeError()))
        self.mocker.replay()
        d = self.call_cobbler_method("acquire_system")
        self.assertFailure(d, SomeError)
        return d

    def test_find_failure(self):
        """Test that an empty result from find_system is an error"""
        self.setup_mock()
        self.mock_find_system(succeed([]))
        self.mocker.replay()
        d = self.call_cobbler_method("acquire_system")
        self.assertFailure(d, ProviderError)

        def verify(error):
            self.assertEquals(str(error),
                              "Could not find any Cobbler systems marked as "
                              "available and configured for network boot.")
        d.addCallback(verify)
        return d

    def test_get_system_error(self):
        """Test that an arbitrary exception from get_system is propagated"""
        self.setup_mock()
        self.mock_find_system(succeed(["some-name", "other-name"]))
        self.mock_get_system(fail(SomeError()))
        self.mocker.replay()
        d = self.call_cobbler_method("acquire_system")
        self.assertFailure(d, SomeError)
        return d

    def test_find_all_insane(self):
        """
        Test that it is an error for all systems to have inconsistent
        acquisition state
        """
        self.setup_mock()
        self.mock_find_system(succeed(["some-name", "other-name"]))
        self.mock_get_system(succeed(_crazy_system("some-uid")))
        self.mock_get_system(succeed(_crazy_system("other-uid")),
                             name="other-name")
        self.mocker.replay()
        d = self.call_cobbler_method("acquire_system")
        self.assertFailure(d, ProviderError)

        def verify(error):
            self.assertEquals(str(error),
                              "All available Cobbler systems were also marked "
                              "as acquired (instances: some-uid, other-uid).")
        d.addCallback(verify)
        return d

    def test_get_name_failure(self):
        """Test error when no matching system found"""
        self.setup_mock()
        self.mock_find_system(succeed(["some-name"]))
        self.mock_get_system(succeed(_SOME_SYSTEM))
        return self.check_get_name_failure("acquire_system")

    def test_get_name_error(self):
        """Test that an arbitrary exception from find_system is propagated"""
        self.setup_mock()
        self.mock_find_system(succeed(["some-name"]))
        self.mock_get_system(succeed(_SOME_SYSTEM))
        return self.check_get_name_error("acquire_system")

    def test_get_system_handle_failure(self):
        """Test error when system doesn't exist"""
        self.setup_mock()
        self.mock_find_system(succeed(["some-name"]))
        self.mock_get_system(succeed(_SOME_SYSTEM))
        self.mock_get_name(succeed(["some-name"]))
        return self.check_get_system_handle_failure("acquire_system")

    def test_get_system_handle_unknown_fault(self):
        """Test error on unknown xmlrpc Fault"""
        self.setup_mock()
        self.mock_find_system(succeed(["some-name"]))
        self.mock_get_system(succeed(_SOME_SYSTEM))
        self.mock_get_name(succeed(["some-name"]))
        return self.check_get_system_handle_fault("acquire_system")

    def test_get_system_handle_error(self):
        """
        Test that an arbitrary exception from get_system_handle is propagated
        """
        self.setup_mock()
        self.mock_find_system(succeed(["some-name"]))
        self.mock_get_system(succeed(_SOME_SYSTEM))
        self.mock_get_name(succeed(["some-name"]))
        return self.check_get_system_handle_error("acquire_system")

    def test_modify_system_failure(self):
        """Test error on failure to modify_system"""
        self.setup_mock()
        self.mock_find_system(succeed(["some-name"]))
        self.mock_get_system(succeed(_SOME_SYSTEM))
        self.mock_get_name(succeed(["some-name"]))
        self.mock_get_system_handle(succeed("some-handle"))
        return self.check_modify_system_failure(
            "mgmt_classes", ["preserve_me", "acquired"],
            "acquire_system")

    def test_modify_system_error(self):
        """Test propagate unknown error from modify_system"""
        self.setup_mock()
        self.mock_find_system(succeed(["some-name"]))
        self.mock_get_system(succeed(_SOME_SYSTEM))
        self.mock_get_name(succeed(["some-name"]))
        self.mock_get_system_handle(succeed("some-handle"))
        return self.check_modify_system_error(
            "mgmt_classes", ["preserve_me", "acquired"],
            "acquire_system")

    def test_save_system_failure(self):
        """Test error on failure to save_system"""
        self.setup_mock()
        self.mock_find_system(succeed(["some-name"]))
        self.mock_get_system(succeed(_SOME_SYSTEM))
        self.mock_get_name(succeed(["some-name"]))
        self.mock_get_system_handle(succeed("some-handle"))
        self.mock_modify_system(
            succeed(True), "mgmt_classes", ["preserve_me", "acquired"])
        return self.check_save_system_failure("acquire_system")

    def test_save_system_error(self):
        """Test propagate unknown error from save_system"""
        self.setup_mock()
        self.mock_find_system(succeed(["some-name"]))
        self.mock_get_system(succeed(_SOME_SYSTEM))
        self.mock_get_name(succeed(["some-name"]))
        self.mock_get_system_handle(succeed("some-handle"))
        self.mock_modify_system(
            succeed(True), "mgmt_classes", ["preserve_me", "acquired"])
        return self.check_save_system_error("acquire_system")

    def test_eventual_success(self):
        """Test full interaction with expected results"""
        self.setup_mock()
        self.mock_find_system(succeed(["bad-name", "some-name"]))
        self.mock_get_system(succeed("~"), name="bad-name")
        self.mock_get_system(succeed(_SOME_SYSTEM))
        self.mock_get_name(succeed(["some-name"]))
        self.mock_get_system_handle(succeed("some-handle"))
        self.mock_modify_system(
            succeed(True), "mgmt_classes", ["preserve_me", "acquired"])
        self.mock_save_system(succeed(True))
        self.mocker.replay()
        d = self.call_cobbler_method("acquire_system")

        def verify(result):
            self.assertEquals(result, "some-uid")
        d.addCallback(verify)
        return d


class StartSystemTest(CobblerClientTestCase):

    args = ("start_system", "some-uid", 123, "blah user data")
    ks_meta = {
        "MACHINE_ID": 123, "PRESERVE_ME": "important",
        "USER_DATA_BASE64": "YmxhaCB1c2VyIGRhdGE="}

    def mock_get_systems_success(self):
        self.mock_get_systems(succeed([{
            "mgmt_classes": ["acquired"], "uid": "some-uid", "blah": "blah",
            "ks_meta": {"PRESERVE_ME": "important"}}]))

    def test_get_ks_meta_failure(self):
        self.setup_mock()
        return self.check_get_systems_failure(*self.args)

    def test_get_ks_meta_error(self):
        self.setup_mock()
        return self.check_get_systems_error(*self.args)

    def test_get_name_failure(self):
        """Test error when no matching system found"""
        self.setup_mock()
        self.mock_get_systems_success()
        return self.check_get_name_failure(*self.args)

    def test_get_name_insane(self):
        """Test error when multiple matching systems found"""
        self.setup_mock()
        self.mock_get_systems_success()
        return self.check_get_name_insane(*self.args)

    def test_get_name_error(self):
        """Test that an arbitrary exception from find_system is propagated"""
        self.setup_mock()
        self.mock_get_systems_success()
        return self.check_get_name_error(*self.args)

    def test_get_system_handle_failure(self):
        """Test error when system doesn't exist"""
        self.setup_mock()
        self.mock_get_systems_success()
        self.mock_get_name(succeed(["some-name"]))
        return self.check_get_system_handle_failure(*self.args)

    def test_get_system_handle_unknown_fault(self):
        """Test error on unknown xmlrpc Fault"""
        self.setup_mock()
        self.mock_get_systems_success()
        self.mock_get_name(succeed(["some-name"]))
        return self.check_get_system_handle_fault(*self.args)

    def test_get_system_handle_error(self):
        """
        Test that an arbitrary exception from get_system_handle is propagated
        """
        self.setup_mock()
        self.mock_get_systems_success()
        self.mock_get_name(succeed(["some-name"]))
        return self.check_get_system_handle_error(*self.args)

    def test_modify_ks_meta_failure(self):
        """Test error on failure to modify_system"""
        self.setup_mock()
        self.mock_get_systems_success()
        self.mock_get_name(succeed(["some-name"]))
        self.mock_get_system_handle(succeed("some-handle"))
        return self.check_modify_system_failure(
            "ks_meta", self.ks_meta, *self.args)

    def test_modify_ks_meta_error(self):
        """Test propagate unknown error from modify_system"""
        self.setup_mock()
        self.mock_get_systems_success()
        self.mock_get_name(succeed(["some-name"]))
        self.mock_get_system_handle(succeed("some-handle"))
        return self.check_modify_system_error(
            "ks_meta", self.ks_meta, *self.args)

    def test_modify_netboot_enabled_failure(self):
        """Test error on failure to modify_system"""
        self.setup_mock()
        self.mock_get_systems_success()
        self.mock_get_name(succeed(["some-name"]))
        self.mock_get_system_handle(succeed("some-handle"))
        self.mock_modify_system(succeed(True), "ks_meta", self.ks_meta)
        return self.check_modify_system_failure(
            "netboot_enabled", True, *self.args)

    def test_modify_netboot_enabled_error(self):
        """Test propagate unknown error from modify_system"""
        self.setup_mock()
        self.mock_get_systems_success()
        self.mock_get_name(succeed(["some-name"]))
        self.mock_get_system_handle(succeed("some-handle"))
        self.mock_modify_system(succeed(True), "ks_meta", self.ks_meta)
        return self.check_modify_system_error(
            "netboot_enabled", True, *self.args)

    def test_save_system_failure(self):
        """Test error on failure to save_system"""
        self.setup_mock()
        self.mock_get_systems_success()
        self.mock_get_name(succeed(["some-name"]))
        self.mock_get_system_handle(succeed("some-handle"))
        self.mock_modify_system(succeed(True), "ks_meta", self.ks_meta)
        self.mock_modify_system(succeed(True), "netboot_enabled", True)
        return self.check_save_system_failure(*self.args)

    def test_save_system_error(self):
        """Test propagate unknown error from save_system"""
        self.setup_mock()
        self.mock_get_systems_success()
        self.mock_get_name(succeed(["some-name"]))
        self.mock_get_system_handle(succeed("some-handle"))
        self.mock_modify_system(succeed(True), "ks_meta", self.ks_meta)
        self.mock_modify_system(succeed(True), "netboot_enabled", True)
        return self.check_save_system_error(*self.args)

    def test_power_error(self):
        """Test propagate unknown error from power call"""
        self.setup_mock()
        self.mock_get_systems_success()
        self.mock_get_name(succeed(["some-name"]))
        self.mock_get_system_handle(succeed("some-handle"))
        self.mock_modify_system(succeed(True), "ks_meta", self.ks_meta)
        self.mock_modify_system(succeed(True), "netboot_enabled", True)
        self.mock_save_system(succeed(True))
        self.mock_power_system("on", fail(SomeError()))
        self.mocker.replay()
        d = self.call_cobbler_method(*self.args)
        self.assertFailure(d, SomeError)
        return d

    def test_get_systems_failure(self):
        """Test error when system doesn't exist"""
        self.setup_mock()
        self.mock_get_systems_success()
        self.mock_get_name(succeed(["some-name"]))
        self.mock_get_system_handle(succeed("some-handle"))
        self.mock_modify_system(succeed(True), "ks_meta", self.ks_meta)
        self.mock_modify_system(succeed(True), "netboot_enabled", True)
        self.mock_save_system(succeed(True))
        self.mock_power_system("on", succeed("ignored"))
        return self.check_get_systems_failure(*self.args)

    def test_get_systems_error(self):
        """Test propagate unknown error trying to get_systems"""
        self.setup_mock()
        self.mock_get_systems_success()
        self.mock_get_name(succeed(["some-name"]))
        self.mock_get_system_handle(succeed("some-handle"))
        self.mock_modify_system(succeed(True), "ks_meta", self.ks_meta)
        self.mock_modify_system(succeed(True), "netboot_enabled", True)
        self.mock_save_system(succeed(True))
        self.mock_power_system("on", succeed("ignored"))
        return self.check_get_systems_error(*self.args)

    def test_actual_success(self):
        """Test full interaction with expected results"""
        self.setup_mock()
        self.mock_get_systems_success()
        self.mock_get_name(succeed(["some-name"]))
        self.mock_get_system_handle(succeed("some-handle"))
        self.mock_modify_system(succeed(True), "ks_meta", self.ks_meta)
        self.mock_modify_system(succeed(True), "netboot_enabled", True)
        self.mock_save_system(succeed(True))
        self.mock_power_system("on", succeed("ignored"))
        self.mock_get_systems(succeed([{
            "mgmt_classes": ["acquired"], "uid": "some-uid", "blah": "blah"}]))
        self.mocker.replay()
        d = self.call_cobbler_method(*self.args)
        d.addCallback(self.assertEquals, {
            "mgmt_classes": ["acquired"], "uid": "some-uid", "blah": "blah"})
        return d


class ShutdownSystemTest(CobblerClientTestCase):

    system = {"uid": "some-uid",
              "mgmt_classes": ["acquired", "preserve"],
              "ks_meta": {
                  "PRESERVE": "important",
                  "MACHINE_ID": "blah",
                  "USER_DATA_BASE64": "blob"}}

    def test_get_systems_failure(self):
        """Test error when system doesn't exist"""
        self.setup_mock()
        self.mock_get_systems(succeed([]))
        self.mocker.replay()
        d = self.call_cobbler_method("shutdown_system", "some-uid")
        self.assertFailure(d, MachinesNotFound)
        return d

    def test_get_systems_error(self):
        """Test propagate unknown error trying to get_systems"""
        self.setup_mock()
        self.mock_get_systems(fail(SomeError()))
        self.mocker.replay()
        d = self.call_cobbler_method("shutdown_system", "some-uid")
        self.assertFailure(d, SomeError)
        return d

    def test_get_name_failure(self):
        """Test error when no matching system found"""
        self.setup_mock()
        self.mock_get_systems(succeed([self.system]))
        return self.check_get_name_failure("shutdown_system", "some-uid")

    def test_get_name_insane(self):
        """Test error when multiple matching systems found"""
        self.setup_mock()
        self.mock_get_systems(succeed([self.system]))
        return self.check_get_name_insane("shutdown_system", "some-uid")

    def test_get_name_error(self):
        """Test that an arbitrary exception from find_system is propagated"""
        self.setup_mock()
        self.mock_get_systems(succeed([self.system]))
        return self.check_get_name_error("shutdown_system", "some-uid")

    def test_get_system_handle_failure(self):
        """Test error when system doesn't exist"""
        self.setup_mock()
        self.mock_get_systems(succeed([self.system]))
        self.mock_get_name(succeed(["some-name"]))
        return self.check_get_system_handle_failure(
            "shutdown_system", "some-uid")

    def test_get_system_handle_unknown_fault(self):
        """Test error on unknown xmlrpc Fault"""
        self.setup_mock()
        self.mock_get_systems(succeed([self.system]))
        self.mock_get_name(succeed(["some-name"]))
        return self.check_get_system_handle_fault(
            "shutdown_system", "some-uid")

    def test_get_system_handle_error(self):
        """
        Test that an arbitrary exception from get_system_handle is propagated
        """
        self.setup_mock()
        self.mock_get_systems(succeed([self.system]))
        self.mock_get_name(succeed(["some-name"]))
        return self.check_get_system_handle_error(
            "shutdown_system", "some-uid")

    def test_modify_ks_meta_failure(self):
        """Test error on failure to modify_system"""
        self.setup_mock()
        self.mock_get_systems(succeed([self.system]))
        self.mock_get_name(succeed(["some-name"]))
        self.mock_get_system_handle(succeed("some-handle"))
        return self.check_modify_system_failure(
            "ks_meta", {"PRESERVE": "important"},
            "shutdown_system", "some-uid")

    def test_modify_ks_meta_error(self):
        """Test propagate unknown error from modify_system"""
        self.setup_mock()
        self.mock_get_systems(succeed([self.system]))
        self.mock_get_name(succeed(["some-name"]))
        self.mock_get_system_handle(succeed("some-handle"))
        return self.check_modify_system_error(
            "ks_meta", {"PRESERVE": "important"},
            "shutdown_system", "some-uid")

    def test_modify_mgmt_classes_failure(self):
        """Test error on failure to modify_system"""
        self.setup_mock()
        self.mock_get_systems(succeed([self.system]))
        self.mock_get_name(succeed(["some-name"]))
        self.mock_get_system_handle(succeed("some-handle"))
        self.mock_modify_system(
            succeed(True), "ks_meta", {"PRESERVE": "important"})
        return self.check_modify_system_failure(
            "mgmt_classes", ["available", "preserve"],
            "shutdown_system", "some-uid")

    def test_modify_mgmt_classes_error(self):
        """Test propagate unknown error from modify_system"""
        self.setup_mock()
        self.mock_get_systems(succeed([self.system]))
        self.mock_get_name(succeed(["some-name"]))
        self.mock_get_system_handle(succeed("some-handle"))
        self.mock_modify_system(
            succeed(True), "ks_meta", {"PRESERVE": "important"})
        return self.check_modify_system_error(
            "mgmt_classes", ["available", "preserve"],
            "shutdown_system", "some-uid")

    def test_modify_netboot_enabled_failure(self):
        """Test error on failure to modify_system"""
        self.setup_mock()
        self.mock_get_systems(succeed([self.system]))
        self.mock_get_name(succeed(["some-name"]))
        self.mock_get_system_handle(succeed("some-handle"))
        self.mock_modify_system(
            succeed(True), "ks_meta", {"PRESERVE": "important"})
        self.mock_modify_system(
            succeed(True), "mgmt_classes", ["available", "preserve"])
        return self.check_modify_system_failure(
            "netboot_enabled", True,
            "shutdown_system", "some-uid")

    def test_modify_netboot_enabled_error(self):
        """Test propagate unknown error from modify_system"""
        self.setup_mock()
        self.mock_get_systems(succeed([self.system]))
        self.mock_get_name(succeed(["some-name"]))
        self.mock_get_system_handle(succeed("some-handle"))
        self.mock_modify_system(
            succeed(True), "ks_meta", {"PRESERVE": "important"})
        self.mock_modify_system(
            succeed(True), "mgmt_classes", ["available", "preserve"])
        return self.check_modify_system_error(
            "netboot_enabled", True,
            "shutdown_system", "some-uid")

    def test_save_system_failure(self):
        """Test error on failure to save_system"""
        self.setup_mock()
        self.mock_get_systems(succeed([self.system]))
        self.mock_get_name(succeed(["some-name"]))
        self.mock_get_system_handle(succeed("some-handle"))
        self.mock_modify_system(
            succeed(True), "ks_meta", {"PRESERVE": "important"})
        self.mock_modify_system(
            succeed(True), "mgmt_classes", ["available", "preserve"])
        self.mock_modify_system(succeed(True), "netboot_enabled", True)
        return self.check_save_system_failure("shutdown_system", "some-uid")

    def test_save_system_error(self):
        """Test propagate unknown error from save_system"""
        self.setup_mock()
        self.mock_get_systems(succeed([self.system]))
        self.mock_get_name(succeed(["some-name"]))
        self.mock_get_system_handle(succeed("some-handle"))
        self.mock_modify_system(
            succeed(True), "ks_meta", {"PRESERVE": "important"})
        self.mock_modify_system(
            succeed(True), "mgmt_classes", ["available", "preserve"])
        self.mock_modify_system(succeed(True), "netboot_enabled", True)
        return self.check_save_system_error("shutdown_system", "some-uid")

    def test_power_error(self):
        """Test propagate unknown error from power call"""
        self.setup_mock()
        self.mock_get_systems(succeed([self.system]))
        self.mock_get_name(succeed(["some-name"]))
        self.mock_get_system_handle(succeed("some-handle"))
        self.mock_modify_system(
            succeed(True), "ks_meta", {"PRESERVE": "important"})
        self.mock_modify_system(
            succeed(True), "mgmt_classes", ["available", "preserve"])
        self.mock_modify_system(succeed(True), "netboot_enabled", True)
        self.mock_save_system(succeed(True))
        self.mock_power_system("off", fail(SomeError()))
        self.mocker.replay()
        d = self.call_cobbler_method("shutdown_system", "some-uid")
        self.assertFailure(d, SomeError)
        return d

    def test_power_probably_success_cant_tell(self):
        """Test full interaction with expected results"""
        # Theoretically a test for successful shutdown, but we can't (yet?)
        # verify the shutdown operation's success.
        self.setup_mock()
        self.mock_get_systems(succeed([self.system]))
        self.mock_get_name(succeed(["some-name"]))
        self.mock_get_system_handle(succeed("some-handle"))
        self.mock_modify_system(
            succeed(True), "ks_meta", {"PRESERVE": "important"})
        self.mock_modify_system(
            succeed(True), "mgmt_classes", ["available", "preserve"])
        self.mock_modify_system(succeed(True), "netboot_enabled", True)
        self.mock_save_system(succeed(True))
        self.mock_power_system("off", succeed("ignored"))
        self.mocker.replay()
        d = self.call_cobbler_method("shutdown_system", "some-uid")
        d.addCallback(self.assertEquals, True)
        return d
