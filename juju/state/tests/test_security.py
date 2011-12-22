import base64
import yaml
import zookeeper

from twisted.internet.defer import inlineCallbacks, succeed, returnValue

from juju.state.auth import make_identity, make_ace
from juju.state.errors import (
    StateChanged, StateNotFound, PrincipalNotFound)

from juju.state.security import (
    ACL, Principal, GroupPrincipal, OTPPrincipal, TokenDatabase,
    SecurityPolicy, SecurityPolicyConnection)

from juju.lib.testing import TestCase
from juju.tests.common import get_test_zookeeper_address

from txzookeeper.client import ZOO_OPEN_ACL_UNSAFE

from txzookeeper.tests.utils import deleteTree


class PrincipalTests(TestCase):

    @inlineCallbacks
    def setUp(self):
        zookeeper.set_debug_level(0)
        self.client = yield self.get_zookeeper_client().connect()

    def tearDown(self):
        deleteTree(handle=self.client.handle)
        self.client.close()

    def test_name(self):
        """Principals have names."""
        principal = Principal("foobar", "secret")
        self.assertEqual(principal.name, "foobar")

    def test_get_token(self):
        """An identity token can be gotten from a Principal."""
        principal = Principal("foobar", "secret")
        self.assertEqual(principal.get_token(),
                         make_identity("foobar:secret"))

    @inlineCallbacks
    def test_activate(self):
        """A principal can be used with a client connection."""
        client = yield self.get_zookeeper_client().connect()
        self.addCleanup(lambda: client.close())
        admin_credentials = "admin:admin"
        test_credentials = "test:test"
        yield self.client.add_auth("digest", admin_credentials)

        acl = [make_ace(make_identity(admin_credentials), all=True),
               make_ace(make_identity(
                   test_credentials), read=True, create=True)]

        yield client.create("/acl-test", "content", acls=acl)

        # Verify the acl is active
        yield self.assertFailure(
            client.get("/acl-test"), zookeeper.NoAuthException)

        # Attach the principal to the connection
        principal = Principal("test", "test")
        yield principal.attach(client)
        content, stat = yield client.get("/acl-test")
        self.assertEqual(content, "content")

class GroupPrincipalTests(TestCase):

    @inlineCallbacks
    def setUp(self):
        zookeeper.set_debug_level(0)
        self.client = yield self.get_zookeeper_client().connect()

    def tearDown(self):
        deleteTree(handle=self.client.handle)
        self.client.close()

    def test_uninitialized_usage(self):
        """Attempting to access the name before initialized raises an error"""
        principal = GroupPrincipal(self.client, "/group-a")
        try:
            principal.name
        except RuntimeError:
            pass
        else:
            self.fail("Uninitialized usage should raise error")

    @inlineCallbacks
    def test_create(self):
        """An identity token can be gotten from a Principal."""
        principal = GroupPrincipal(self.client, "/group-a")
        yield principal.create("group/a", "zebra")
        self.assertEqual(principal.name, "group/a")
        yield self.assertFailure(
            principal.create("group/a", "zebra"),
            RuntimeError)

    @inlineCallbacks
    def test_initialize(self):
        principal = GroupPrincipal(self.client, "/group-a")
        yield principal.create("group/a", "zebra")

        principal = GroupPrincipal(self.client, "/group-a")
        yield principal.initialize()
        self.assertEqual(principal.name, "group/a")

        principal = GroupPrincipal(self.client, "/group-b")
        yield self.assertFailure(principal.initialize(), StateNotFound)

    @inlineCallbacks
    def test_get_token(self):
        """An identity token can be gotten from a Principal."""
        principal = GroupPrincipal(self.client, "/group-a")
        yield principal.create("foobar", "secret")
        self.assertEqual(principal.get_token(),
                         make_identity("foobar:secret"))

    @inlineCallbacks
    def test_add_member(self):
        group = GroupPrincipal(self.client, "/group-a")
        yield group.create("group/a", "zebra")

        principal = Principal("aladdin", "genie")
        yield group.add_member(principal)
        acl, stat = yield self.client.get_acl("/group-a")
        self.assertEqual(
            acl[1:],
            [make_ace(principal.get_token(), read=True)])
        # Adding a member again is fine
        yield group.add_member(principal)

    @inlineCallbacks
    def test_remove_member(self):
        group = GroupPrincipal(self.client, "/group-a")
        yield group.create("group/a", "zebra")

        principal = Principal("aladdin", "genie")
        # Removing a member that doesn't exist is a no-op
        yield group.remove_member(principal)
        yield group.add_member(principal)
        yield group.remove_member(principal.name)

        acl, stat = yield self.client.get_acl("/group-a")
        self.assertEqual(acl[1:], [])

    @inlineCallbacks
    def test_activate(self):
        """A principal can be used with a client connection."""
        client = yield self.get_zookeeper_client().connect()
        self.addCleanup(lambda: client.close())

        admin_credentials = "admin:admin"
        test_credentials = "test:test"
        yield self.client.add_auth("digest", admin_credentials)

        acl = [make_ace(make_identity(admin_credentials), all=True),
               make_ace(make_identity(
                   test_credentials), read=True, create=True)]

        yield client.create("/acl-test", "content", acls=acl)

        # Verify the acl is active
        yield self.assertFailure(
            client.get("/acl-test"), zookeeper.NoAuthException)

        # Attach the principal to the connection
        group = GroupPrincipal(self.client, "/group-b")
        yield group.create("test", "test")
        yield group.attach(client)
        content, stat = yield client.get("/acl-test")
        self.assertEqual(content, "content")


class OTPPrincipalTests(TestCase):

    @inlineCallbacks
    def setUp(self):
        zookeeper.set_debug_level(0)
        self.client = yield self.get_zookeeper_client().connect()
        self.client.create("/otp")

    def tearDown(self):
        deleteTree(handle=self.client.handle)
        self.client.close()

    def set_otp_test_ace(self, test_ace=ZOO_OPEN_ACL_UNSAFE):
        """Set an additional OTP ACL entry for test cleanup."""
        OTPPrincipal.set_additional_otp_ace(test_ace)
        self.addCleanup(lambda: OTPPrincipal.set_additional_otp_ace(None))

    def test_using_uncreated_raises(self):
        """Principals have names."""
        principal = OTPPrincipal(self.client)
        try:
            principal.name
        except RuntimeError:
            pass
        else:
            self.fail("Use of an uncreated OTP principal should raise error.")

    @inlineCallbacks
    def test_get_token(self):
        """An identity token can be gotten from a OTPPrincipal.

        The token returned is that of the stored credentials, not
        the serialized one time password principal.
        """
        self.set_otp_test_ace()

        principal = OTPPrincipal(self.client)
        yield principal.create("foobar", "secret")
        self.assertEqual(principal.get_token(),
                         make_identity("foobar:secret"))
        self.assertEqual(principal.name, "foobar")

    @inlineCallbacks
    def test_create(self):
        """A principal can be used with a client connection."""
        self.set_otp_test_ace()

        principal = OTPPrincipal(self.client)
        yield principal.create("foobar", "secret")

        children = yield self.client.get_children("/otp")
        self.assertEqual(len(children), 1)
        otp_path = "/otp/%s" % (children.pop())

        data, stat = yield self.client.get(otp_path)

        credentials = yaml.load(data)
        self.assertEqual(credentials["name"], "foobar")
        self.assertEqual(credentials["password"], "secret")

        acl, stat = yield self.client.get_acl(otp_path)
        self.assertEqual(len(acl), 2)

    @inlineCallbacks
    def test_serialize(self):
        """The principal can be serialized to just the OTP data."""
        self.set_otp_test_ace()

        principal = OTPPrincipal(self.client)
        yield principal.create("foobar", "secret")

        otp_data = principal.serialize()
        path, user, password = base64.b64decode(otp_data).split(":")
        acl, stat = yield self.client.get_acl(path)

        self.assertEqual(principal.get_token(),
                         make_identity("foobar:secret"))
        self.assertEqual(principal.name, "foobar")

    @inlineCallbacks
    def test_consume(self):
        """The OTP serialization can be used to retrievethe actual credentials.
        """
        principal = OTPPrincipal(self.client)
        yield principal.create("foobar", "secret")
        otp_data = principal.serialize()
        path, _ = base64.b64decode(otp_data).split(":", 1)
        acl, stat = yield self.client.get_acl(path)

        # Verify that the OTP data is secure
        yield self.assertFailure(
            self.client.get(path), zookeeper.NoAuthException)

        name, password = yield OTPPrincipal.consume(self.client, otp_data)
        self.assertEqual(name, "foobar")
        self.assertEqual(password, "secret")
        children = yield self.client.get_children("/otp")
        self.assertFalse(children)


class TokenDatabaseTest(TestCase):

    @inlineCallbacks
    def setUp(self):
        zookeeper.set_debug_level(0)
        self.client = yield self.get_zookeeper_client().connect()
        self.db = TokenDatabase(self.client, "/token-test")

    def tearDown(self):
        deleteTree(handle=self.client.handle)
        self.client.close()

    @inlineCallbacks
    def test_add(self):
        principal = Principal("zebra", "zoo")
        yield self.db.add(principal)
        content, stat = yield self.client.get("/token-test")
        data = yaml.load(content)
        self.assertEqual(data, {"zebra": principal.get_token()})

    @inlineCallbacks
    def test_remove(self):
        principal = Principal("zebra", "zoo")
        yield self.db.add(principal)
        yield self.db.remove(principal)
        content, stat = yield self.client.get("/token-test")
        data = yaml.load(content)
        self.assertEqual(data, {"zebra": principal.get_token()})

    @inlineCallbacks
    def test_get(self):
        principal = Principal("zebra", "zoo")
        yield self.db.add(principal)
        token = yield self.db.get(principal.name)
        self.assertEqual(token, principal.get_token())

    @inlineCallbacks
    def test_get_nonexistant(self):
        principal = Principal("zebra", "zoo")
        error = yield self.assertFailure(self.db.get(principal.name),
                                   PrincipalNotFound)
        self.assertEquals(str(error), "Principal 'zebra' not found")


class PolicyTest(TestCase):

    @inlineCallbacks
    def setUp(self):
        zookeeper.set_debug_level(0)
        self.client = yield self.get_zookeeper_client().connect()
        self.tokens = TokenDatabase(self.client)
        yield self.tokens.add(Principal("admin", "admin"))
        self.policy = SecurityPolicy(self.client, self.tokens)

    def tearDown(self):
        deleteTree(handle=self.client.handle)
        self.client.close()

    @inlineCallbacks
    def test_default_no_owner_no_rules_gives_admin_access(self):
        """By default the policy setups a global access for the cli admins.
        """
        acl = yield self.policy("/random")
        self.assertIn(
            make_ace(Principal("admin", "admin").get_token(), all=True), acl)

    @inlineCallbacks
    def test_default_no_rules_gives_global_authenticated_access(self):
        """If no rules match, the default acl gives authenticated users access.

        XXX/TODO: This is intended as a temporary crutch for
        integration of the security machinery, not a long term
        solution.
        """
        acl = yield self.policy("/random")
        self.assertIn(make_ace("auth", "world", all=True), acl)

    @inlineCallbacks
    def test_rule_match_suppress_open_access(self):
        """If a rule returns an acl, then no default access is given."""
        principal = Principal("foobar", "foobar")
        self.policy.add_rule(lambda policy, path: [
            make_ace(principal.get_token(), all=True)])
        acl = yield self.policy("/random")
        # Check for matched rule ACL
        self.assertIn(make_ace(principal.get_token(), all=True), acl)
        # Verify no default access
        self.assertNotIn(make_ace("auth", "world", all=True), acl)

    @inlineCallbacks
    def test_rule_that_returns_deferred(self):
        """If a rule may do additional lookups, resulting in deferred values.
        """
        principal = Principal("foobar", "foobar")
        self.policy.add_rule(lambda policy, path: succeed([
            make_ace(principal.get_token(), all=True)]))
        acl = yield self.policy("/random")
        # Check for matched rule ACL
        self.assertIn(make_ace(principal.get_token(), all=True), acl)
        # Verify no default access
        self.assertNotIn(make_ace("auth", "world", all=True), acl)

    @inlineCallbacks
    def test_owner_ace(self):
        """If an owner is set, all nodes ACLs will have an owner ACE.
        """
        owner = Principal("john", "doe")
        self.policy.set_owner(owner)
        acl = yield self.policy("/random")
        self.assertIn(make_ace(owner.get_token(), all=True), acl)


class SecureConnectionTest(TestCase):

    @inlineCallbacks
    def setUp(self):
        zookeeper.set_debug_level(0)
        self.client = yield SecurityPolicyConnection(
            get_test_zookeeper_address()).connect()

        admin = Principal("admin", "admin")

        self.token_db = TokenDatabase(self.client)
        yield self.token_db.add(admin)
        self.policy = SecurityPolicy(self.client, self.token_db, owner=admin)
        attach_defer = admin.attach(self.client)
        # Trick to speed up the auth response processing (fixed in ZK trunk)
        self.client.exists("/")
        yield attach_defer

    def tearDown(self):
        deleteTree(handle=self.client.handle)
        self.client.close()

    @inlineCallbacks
    def test_create_without_policy(self):
        """If no policy is set the connection behaves normally"""
        def rule(policy, path):
            return [make_ace(Principal("magic", "not").get_token(), all=True)]

        self.policy.add_rule(rule)
        yield self.client.create("/xyz")
        acl, stat = yield self.client.get_acl("/xyz")
        self.assertEqual(acl, [ZOO_OPEN_ACL_UNSAFE])

    @inlineCallbacks
    def test_create_with_policy(self):
        """If a policy is set ACL are determined by the policy."""

        def rule(policy, path):
            return [make_ace(Principal("magic", "not").get_token(), all=True)]

        self.policy.add_rule(rule)
        self.client.set_security_policy(self.policy)

        yield self.client.create("/xyz")
        acl, stat = yield self.client.get_acl("/xyz")

        self.assertEqual(
            acl,
            [make_ace(Principal("magic", "not").get_token(), all=True),
             make_ace(Principal("admin", "admin").get_token(), all=True)])


class ACLTest(TestCase):

    @inlineCallbacks
    def setUp(self):
        zookeeper.set_debug_level(0)
        self.client = yield self.get_zookeeper_client().connect()
        self.tokens = TokenDatabase(self.client)
        self.admin = Principal("admin", "admin")
        yield self.tokens.add(self.admin)
        self.policy = SecurityPolicy(self.client, self.tokens)
        attach_deferred = self.admin.attach(self.client)

        self.client.exists("/")
        yield attach_deferred

    def tearDown(self):
        deleteTree(handle=self.client.handle)
        self.client.close()

    @inlineCallbacks
    def test_acl_on_non_existant_node(self):
        acl = ACL(self.client, "abc")
        yield self.assertFailure(acl.grant("admin", all=True), StateNotFound)

    @inlineCallbacks
    def test_acl_without_admin(self):
        """A client needs an attached principle with the admin perm to set acl.
        """
        client = yield self.get_zookeeper_client().connect()
        principal = Principal("zebra", "stripes")
        yield self.tokens.add(principal)
        attach_deferred = principal.attach(client)
        yield self.client.create(
            "/abc",
            acls=[make_ace(self.admin.get_token(), all=True)])
        yield attach_deferred

        acl = ACL(client, "/abc")
        yield self.assertFailure(
            acl.grant("zebra", all=True),
            zookeeper.NoAuthException)

    @inlineCallbacks
    def test_grant(self):
        path = yield self.client.create("/abc")
        acl = ACL(self.client, path)
        yield acl.grant("admin", all=True)
        node_acl, stat = yield self.client.get_acl(path)
        self.assertEqual(
            node_acl,
            [ZOO_OPEN_ACL_UNSAFE,
             make_ace(self.admin.get_token(), all=True)])

    @inlineCallbacks
    def test_grant_additive(self):
        path = yield self.client.create("/abc")
        acl = ACL(self.client, "/abc")
        yield acl.grant("admin", read=True)
        yield acl.grant("admin", write=True)
        test_ace = make_ace(":", read=True, write=True)
        node_acl, stat = yield self.client.get_acl(path)
        self.assertEqual(node_acl[-1]["perms"], test_ace["perms"])

    @inlineCallbacks
    def test_grant_not_in_token_database(self):
        path = yield self.client.create("/abc")
        acl = ACL(self.client, path)
        yield self.assertFailure(acl.grant("zebra"), PrincipalNotFound)

    @inlineCallbacks
    def test_prohibit(self):
        principal = Principal("zebra", "stripes")
        yield self.tokens.add(principal)

        path = yield self.client.create("/abc", acls=[
            make_ace(self.admin.get_token(), all=True),
            make_ace(principal.get_token(), write=True)])

        acl = ACL(self.client, path)
        yield acl.prohibit("zebra")

        acl, stat = yield self.client.get_acl(path)
        self.assertEqual(
            acl, [make_ace(self.admin.get_token(), all=True)])

    @inlineCallbacks
    def test_prohibit_non_existant_node(self):
        acl = ACL(self.client, "/abc")
        yield self.assertFailure(
            acl.prohibit("zebra"), StateNotFound)

    @inlineCallbacks
    def test_prohibit_not_in_acl(self):
        principal = Principal("zebra", "stripes")
        yield self.tokens.add(principal)

        path = yield self.client.create("/abc", acls=[
            make_ace(self.admin.get_token(), all=True)])

        acl = ACL(self.client, path)
        # We get to the same end state so its fine.
        yield acl.prohibit("zebra")

        acl, stat = yield self.client.get_acl(path)
        self.assertEqual(
            acl, [make_ace(self.admin.get_token(), all=True)])
