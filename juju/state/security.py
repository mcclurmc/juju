import base64
import random
import string
import yaml

from zookeeper import (
    BadArgumentsException, BadVersionException,
    NoNodeException, NodeExistsException, SEQUENCE)

from twisted.internet.defer import inlineCallbacks, returnValue
from txzookeeper.client import ZOO_OPEN_ACL_UNSAFE, ZookeeperClient

from juju.state.auth import make_identity, make_ace
from juju.state.errors import (
    StateNotFound, PrincipalNotFound)
from juju.state.utils import YAMLState


ZOO_OPEN_AUTH_ACL_UNSAFE = make_ace("auth", "world", all=True)


class Principal(object):
    """An juju/zookeeper principal.
    """

    def __init__(self, name, password):
        self._name = name
        self._password = password

    @property
    def name(self):
        """A principal has a login name."""
        return self._name

    def get_token(self):
        """A principal identity token can be retrieved.

        An identity token is used to construct ACLs.
        """
        return make_identity("%s:%s" % (self._name, self._password))

    def attach(self, connection):
        """A principal can be attached to a connection."""
        return connection.add_auth(
            "digest", "%s:%s" % (self._name, self._password))


class GroupPrincipal(object):
    """A group principal is a principal that can have multiple members.

    Group principals are persistent with their credentials stored in zk.
    Membership of the group allows a group member to retrieve and utilize
    these credentials.
    """

    def __init__(self, client, path):
        self._client = client
        self._path = path
        self._name = None
        self._password = None

    @inlineCallbacks
    def initialize(self):
        """Initialize the group.

        The group must always be initialized before attribute
        use. Groups are persistent principals. If a group is being used
        as a principal, it must first be loladed via initialize before
        any access to principal attributes or principal methods.
        """

        try:
            data, stat = yield self._client.get(self._path)
        except NoNodeException:
            raise StateNotFound("Group does not exist at %r" % self._path)

        credentials = yaml.load(data)
        self._name = credentials["name"]
        self._password = credentials["password"]

    def _check(self):
        if self._name is None:
            raise RuntimeError("initialize() must be called before usage")

    @property
    def name(self):
        """A principal has a login name."""
        self._check()
        return self._name

    def get_token(self):
        """A principal identity token can be retrieved.

        An identity token is used to construct ACLs.
        """
        self._check()
        return make_identity("%s:%s" % (self._name, self._password))

    @inlineCallbacks
    def attach(self, connection):
        self._check()
        yield connection.add_auth(
            "digest", "%s:%s" % (self._name, self._password))

    @inlineCallbacks
    def create(self, name, password):
        """Create the group with the given name and password."""
        try:
            yield self._client.create(
                self._path,
                yaml.safe_dump(dict(name=name, password=password)))
        except NodeExistsException:
            raise RuntimeError("Group already exists at %r" % self._path)
        self._name = name
        self._password = password

    @inlineCallbacks
    def add_member(self, principal):
        """Add a principal as a member of the group.

        A member of the group can use the group as an additional principal
        attached to the connection.
        """
        self._check()
        acl, stat = yield self._client.get_acl(self._path)
        token = principal.get_token()

        for ace in acl:
            if ace["id"] == token:
                return

        acl.append(make_ace(token, read=True))

        yield self._client.set_acl(self._path, acl)

    @inlineCallbacks
    def remove_member(self, name):
        """Remove a principal from a group by principal name"""
        self._check()
        acl, stat = yield self._client.get_acl(self._path)

        found = False
        for ace in acl:
            if ace["id"].split(":")[0] == name:
                acl.remove(ace)
                found = True
                break

        if not found:
            return

        yield self._client.set_acl(self._path, acl)


class OTPPrincipal(object):
    """One Time Password (OTP) Principal.

    Its common for juju to need to pass credentials for newly
    created principals over unsecure channels to external
    processes. In order to mitigate the risks of interception of these
    credentials, a one time password is used that enables the
    retrieval of the intended principal credential by the external
    process.
    """

    # Additional OTP node ACL entry, see :method set_otp_additional_ace:
    _extra_otp_ace = None

    def __init__(self, client, path="/otp/otp-"):
        self._client = client
        self._name = None
        self._password = None
        self._otp_name = None
        self._otp = None
        self._path = path

    def _generate_string(self, size=16):
        return "".join(random.sample(string.letters, size))

    def _check(self):
        if not self._name:
            raise RuntimeError("OTPPrincipal must be created before use")

    @property
    def name(self):
        """A principal has a login name."""
        self._check()
        return self._name

    def get_token(self):
        """A principal identity token can be retrieved.

        An identity token is used to construct ACLs.
        """
        self._check()
        return make_identity("%s:%s" % (self._name, self._password))

    @inlineCallbacks
    def attach(self, connection):
        raise NotImplemented("OTP Principals shouldn't attach")

    @classmethod
    @inlineCallbacks
    def consume(cls, client, otp_data):
        """Consume an OTP serialization to retrieve the actual credentials.

        returns a username, password tuple, and destroys the OTP node.
        """
        # Decode the data to get the path and otp credentials
        path, credentials = base64.b64decode(otp_data).split(":", 1)
        yield client.add_auth("digest", credentials)
        # Load the otp principal data
        data, stat = yield client.get(path)
        principal_data = yaml.load(data)
        # Consume the otp node
        yield client.delete(path)
        returnValue((principal_data["name"], principal_data["password"]))

    @inlineCallbacks
    def create(self, name, password=None, otp_name=None, otp=None):
        """Create an OTP for a principal.
        """
        if self._name:
            raise ValueError("OTPPrincipal has already been created.")

        self._name = name
        self._password = password or self._generate_string()
        self._otp_name = otp_name or self._generate_string()
        self._otp = otp or self._generate_string()

        acl = [make_ace(
            make_identity("%s:%s" % (self._otp_name, self._otp)), read=True)]

        # Optional additional ACL entry for unit test teardown.
        if self._extra_otp_ace:
            acl.append(self._extra_otp_ace)

        self._path = yield self._client.create(
            self._path,
            yaml.safe_dump(dict(name=name, password=password)),
            acls=acl,
            flags=SEQUENCE)

        returnValue(self)

    def serialize(self):
        """Return a serialization of the OTP path and credentials.

        This can be sent to an external process such that they can access
        the OTP identity.
        """
        return base64.b64encode(
            "%s:%s:%s" % (self._path, self._otp_name, self._otp))

    @classmethod
    def set_additional_otp_ace(cls, ace):
        """This method sets an additional ACl entry to be added to OTP nodes.

        This method is meant for testing only, to ease construction of unit
        test tear downs when OTP nodes are created.
        """
        cls._extra_otp_ace = ace


class TokenDatabase(object):
    """A hash map of principal names to their identity tokens.

    Identity tokens are used to construct node ACLs.
    """
    def __init__(self, client, path="/auth-tokens"):
        self._state = YAMLState(client, path)

    @inlineCallbacks
    def add(self, principal):
        """Add a principal to the token database.
        """
        yield self._state.read()
        self._state[principal.name] = principal.get_token()
        yield self._state.write()

    @inlineCallbacks
    def get(self, name):
        """Return the identity token for a principal name.
        """
        yield self._state.read()
        try:
            returnValue(self._state[name])
        except KeyError:
            raise PrincipalNotFound(name)

    @inlineCallbacks
    def remove(self, name):
        """Remove a principal by name from the token database.
        """
        yield self._state.read()
        if name in self._state:
            del self._state[name]
            yield self._state.write()


class SecurityPolicy(object):
    """The security policy generates ACLs for new nodes based on their path.
    """
    def __init__(self, client, token_db, rules=(), owner=None):
        self._client = client
        self._rules = list(rules)
        self._token_db = token_db
        self._owner = None

    def set_owner(self, principal):
        """If an owner is set all nodes ACLs will grant access to the owner.
        """
        assert not self._owner, "Owner already assigned"
        self._owner = principal

    def add_rule(self, rule):
        """Add a security rule to the policy.

        A rule is a callable object accepting the policy and the path as
        arguments. The rule should return a list of ACL entries that apply
        to the node at the given path. Rules may return deferred values.
        """
        self._rules.append(rule)

    @inlineCallbacks
    def __call__(self, path):
        """Given a node path, determine the ACL.
        """
        acl_entries = []

        for rule in self._rules:
            entries = yield rule(self, path)
            if entries:
                acl_entries.extend(entries)

        # XXX/TODO - Remove post security-integration
        # Allow incremental integration
        if not acl_entries:
            acl_entries.append(ZOO_OPEN_AUTH_ACL_UNSAFE)

        # Give cli admin access by default
        admin_token = yield self._token_db.get("admin")
        acl_entries.append(make_ace(admin_token, all=True))

        # Give owner access by default
        if self._owner:
            acl_entries.append(
                make_ace(self._owner.get_token(), all=True))

        returnValue(acl_entries)


class SecurityPolicyConnection(ZookeeperClient):
    """A ZooKeeper Connection that delegates default ACLs to a security policy.
    """
    _policy = None

    def set_security_policy(self, policy):
        self._policy = policy

    @inlineCallbacks
    def create(self, path, data="", acls=[ZOO_OPEN_ACL_UNSAFE], flags=0):
        """Creates a zookeeper node at the given path, with the given data.

        The secure connection mixin, defers ACL values to a security policy
        set on the connection if any.
        """
        if self._policy and acls == [ZOO_OPEN_ACL_UNSAFE]:
            acls = yield self._policy(path)
        result = yield super(SecurityPolicyConnection, self).create(
            path, data, acls=acls, flags=flags)
        returnValue(result)


class ACL(object):
    """A ZooKeeper Node ACL.

    Allows for permission grants and removals to principals by name.
    """

    def __init__(self, client, path):
        self._client = client
        self._path = path
        self._token_db = TokenDatabase(client)

    @inlineCallbacks
    def grant(self, principal_name, **perms):
        """Grant permissions on node to the given principal name."""

        token = yield self._token_db.get(principal_name)
        ace = make_ace(token, **perms)

        def add(acl):
            index = self._principal_index(acl, principal_name)
            if index is not None:
                acl_ace = acl[index]
                acl_ace["perms"] = ace["perms"] | acl_ace["perms"]
                return acl
            acl.append(ace)
            return acl

        yield self._update_acl(add)

    @inlineCallbacks
    def prohibit(self, principal_name):
        """Remove all grant for the given principal name."""

        def remove(acl):
            index = self._principal_index(acl, principal_name)
            if index is None:
                # We got to the same end goal.
                return acl
            acl.pop(index)
            return acl

        yield self._update_acl(remove)

    @inlineCallbacks
    def _update_acl(self, change_func):
        """Update an ACL using the given change function to get a new acl.

        Goal is to be tolerant of non-conflicting concurrent updates.
        """
        while True:
            try:
                acl, stat = yield self._client.get_acl(self._path)
            except (BadArgumentsException, NoNodeException):
                raise StateNotFound(self._path)
            acl = change_func(acl)
            try:
                yield self._client.set_acl(
                    self._path, acl, version=stat["aversion"])
                break
            except BadVersionException:
                pass

    def _principal_index(self, acl, principal_name):
        """Determine the index into the ACL of a given principal ACE."""
        for index in range(len(acl)):
            ace = acl[index]
            if ace["id"].split(":", 1)[0] == principal_name:
                return index
        return None
