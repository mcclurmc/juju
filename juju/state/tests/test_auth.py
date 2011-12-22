import hashlib
import base64
import zookeeper

from juju.lib.testing import TestCase
from juju.state.auth import make_identity, make_ace


class AuthTestCase(TestCase):

    def test_make_identity(self):
        username = "admin"
        password = "pass"

        credentials = "%s:%s" % (username, password)

        identity = "%s:%s" %(
            username,
            base64.b64encode(hashlib.new("sha1", credentials).digest()))
        self.assertEqual(identity, make_identity(credentials))

    def test_make_identity_with_colon_in_password(self):
        username = "admin"
        password = ":pass:"

        credentials = "%s:%s" % (username, password)

        identity = "%s:%s" %(
            username,
            base64.b64encode(hashlib.new("sha1", credentials).digest()))
        self.assertEqual(identity, make_identity(credentials))

    def test_make_identity_invalid_credential(self):
        credentials = "abc"
        self.assertRaises(SyntaxError, make_identity, credentials)

    def test_make_ace(self):
        identity = "admin:moss"

        ace = make_ace(identity, write=True, create=True)
        self.assertEqual(ace["id"], identity)
        self.assertEqual(ace["scheme"], "digest")
        self.assertEqual(
            ace["perms"], zookeeper.PERM_WRITE|zookeeper.PERM_CREATE)

    def test_make_ace_with_unknown_perm(self):
        identity = "admin:moss"
        self.assertRaises(
            SyntaxError, make_ace, identity, read=True, extra=True)

    def test_make_ace_no_perms(self):
        identity = "admin:moss"
        self.assertRaises(SyntaxError, make_ace, identity)

    def test_world_scheme(self):
        identity = "anyone"
        result = make_ace(identity, scheme="world", all=True)
        self.assertEqual(result,
                         {"perms": zookeeper.PERM_ALL,
                          "scheme": "world",
                          "id": "anyone"})

    def test_unknown_scheme_raises_assertion(self):
        identity = "admin:moss"
        self.assertRaises(AssertionError, make_ace, identity, scheme="mickey")

    def test_make_ace_with_false_raises(self):
        """Permissions can only be enabled via ACL, other usage raises."""
        identity = "admin:max"
        try:
            make_ace(identity, write=False, create=True)
        except SyntaxError, e:
            self.assertEqual(
                e.args[0],
                "Permissions can only be enabled via ACL - %s" % "write")
        else:
            self.fail("Should have raised exception.")

    def test_make_ace_with_nonbool_raises(self):
        """Permissions can only be enabled via ACL, other usage raises."""
        identity = "admin:max"
        try:
            make_ace(identity, write=None, create=True)
        except SyntaxError, e:
            self.assertEqual(
                e.args[0],
                "Permissions can only be enabled via ACL - %s" % "write")
        else:
            self.fail("Should have raised exception.")
