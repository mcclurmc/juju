"""
This file holds the generic errors which are sensible for several
areas of juju.
"""


class JujuError(Exception):
    """All errors in juju are subclasses of this.

    This error should not be raised by itself, though, since it means
    pretty much nothing.  It's useful mostly as something to catch instead.
    """


class IncompatibleVersion(JujuError):
    """Raised when there is a mismatch in versions using the topology.

    This mismatch will occur when the /topology node has the key
    version set to a version different from
    juju.state.topology.VERSION in the code itself. This scenario
    can occur when a new client accesses an environment deployed with
    previous code, or upon the update of the code in the environment
    itself.

    Although this checking is done at the level of the topology, upon
    every read, the error is defined here because of its
    generality. Doing the check in the topology is just because of the
    centrality of that piece within juju.
    """

    def __init__(self, current, wanted):
        self.current = current
        self.wanted = wanted

    def __str__(self):
        return (
            "Incompatible juju protocol versions (found %r, want %r)" % (
                self.current, self.wanted))


class FileNotFound(JujuError):
    """Raised when a file is not found, obviously! :-)

    @ivar path: Path of the directory or file which wasn't found.
    """

    def __init__(self, path):
        self.path = path

    def __str__(self):
        return "File was not found: %r" % (self.path,)


class CharmError(JujuError):
    """An error occurred while processing a charm."""

    def __init__(self, path, message):
        self.path = path
        self.message = message

    def __str__(self):
        return "Error processing %r: %s" % (self.path, self.message)


class CharmInvocationError(JujuError):
    """A charm's hook invocation exited with an error"""

    def __init__(self, path, exit_code):
        self.path = path
        self.exit_code = exit_code

    def __str__(self):
        return "Error processing %r: exit code %s." % (
            self.path, self.exit_code)


class FileAlreadyExists(JujuError):
    """Raised when something refuses to overwrite an existing file.

    @ivar path: Path of the directory or file which wasn't found.
    """

    def __init__(self, path):
        self.path = path

    def __str__(self):
        return "File already exists, won't overwrite: %r" % (self.path,)


class NoConnection(JujuError):
    """Raised when the CLI is unable to establish a Zookeeper connection."""


class InvalidHost(NoConnection):
    """Raised when the CLI cannot connect to ZK because of an invalid host."""


class InvalidUser(NoConnection):
    """Raised when the CLI cannot connect to ZK because of an invalid user."""


class EnvironmentNotFound(NoConnection):
    """Raised when the juju environment cannot be found."""

    def __init__(self, info="no details available"):
        self._info = info

    def __str__(self):
        return "juju environment not found: %s" % self._info


class EnvironmentPending(NoConnection):
    """Raised when the juju environment is not accessible."""


class ProviderError(JujuError):
    """Raised when an exception occurs in a provider."""


class CloudInitError(ProviderError):
    """Raised when a cloud-init file is misconfigured"""


class MachinesNotFound(ProviderError):
    """Raised when a provider can't fulfil a request for machines."""

    def __init__(self, instance_ids):
        self.instance_ids = list(instance_ids)

    def __str__(self):
        return "Cannot find machine%s: %s" % (
            "" if len(self.instance_ids) == 1 else "s",
            ", ".join(self.instance_ids))


class ProviderInteractionError(ProviderError):
    """Raised when an unexpected error occurs interacting with a provider"""


class CannotTerminateMachine(JujuError):
    """Cannot terminate machine because of some reason"""

    def __init__(self, id, reason):
        self.id = id
        self.reason = reason

    def __str__(self):
        return "Cannot terminate machine %d: %s" % (self.id, self.reason)


class InvalidPlacementPolicy(JujuError):
    """The provider does not support the user specified placement policy.
    """

    def __init__(self, user_policy, provider_type, provider_policies):
        self.user_policy = user_policy
        self.provider_type = provider_type
        self.provider_policies = provider_policies

    def __str__(self):
        return (
            "Unsupported placement policy: %r "
            "for provider: %r, supported policies %s" % (
                self.user_policy,
                self.provider_type,
                ", ".join(self.provider_policies)))
