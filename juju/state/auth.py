import hashlib
import base64
import zookeeper


def make_identity(credentials):
    """
    Given a principal credentials in the form of principal_id:password,
    transform it into an identity of the form principal_id:hash that can be
    used for an access control list entry.
    """
    if not ":" in credentials:
        raise SyntaxError(
            "Credentials in wrong format, should be principal_id:password")
    user, password = credentials.split(":", 1)
    identity = "%s:%s" % (
        user,
        base64.b64encode(hashlib.new("sha1", credentials).digest()))
    return identity


PERM_MAP = {
    "read": zookeeper.PERM_READ,
    "write": zookeeper.PERM_WRITE,
    "delete": zookeeper.PERM_DELETE,
    "create": zookeeper.PERM_CREATE,
    "admin": zookeeper.PERM_ADMIN,
    "all": zookeeper.PERM_ALL}

VALID_SCHEMES = ["digest", "world"]


def make_ace(identity, scheme="digest", **permissions):
    """
    Given a user identity, and boolean keyword arguments corresponding to
    permissions construct an access control entry (ACE).
    """
    assert scheme in VALID_SCHEMES
    ace_permissions = 0

    for name in permissions:
        if name not in PERM_MAP:
            raise SyntaxError("Invalid permission keyword %r" % name)

        if not isinstance(permissions[name], bool) or not permissions[name]:
            raise SyntaxError(
                "Permissions can only be enabled via ACL - %s" % name)

        ace_permissions = ace_permissions | PERM_MAP[name]

    if not ace_permissions:
        raise SyntaxError("No permissions specified")

    access_control_entry = {
        "id": identity, "scheme": scheme, "perms": ace_permissions}
    return access_control_entry
