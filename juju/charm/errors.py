from juju.errors import CharmError, JujuError


class CharmURLError(CharmError):

    def __init__(self, url, message):
        self.url = url
        self.message = message

    def __str__(self):
        return "Bad charm URL %r: %s" % (self.url, self.message)


class CharmNotFound(CharmError):
    """A charm was not found in the repository."""

    def __init__(self, repository_path, charm_name):
        self.repository_path = repository_path
        self.charm_name = charm_name

    def __str__(self):
        return "Charm '%s' not found in repository %s" % (
            self.charm_name, self.repository_path)


class MetaDataError(JujuError):
    """Raised when an error in the info file of a charm is found."""


class InvalidCharmHook(CharmError):
    """A named hook was not found to be valid for the charm."""

    def __init__(self, charm_name, hook_name):
        self.charm_name = charm_name
        self.hook_name = hook_name

    def __str__(self):
        return "Charm %r does not contain hook %r" % (
            self.charm_name, self.hook_name)


class NewerCharmNotFound(CharmError):
    """A newer charm was not found."""

    def __init__(self, charm_id):
        self.charm_id = charm_id

    def __str__(self):
        return "Charm %r is the latest revision known" % self.charm_id


class ServiceConfigError(CharmError):
    """Indicates an issue related to definition of service options."""


class ServiceConfigValueError(JujuError):
    """Indicates an issue related to values of service options."""


class RepositoryNotFound(JujuError):
    """Indicates inability to locate an appropriate repository"""

    def __init__(self, specifier):
        self.specifier = specifier

    def __str__(self):
        if self.specifier is None:
            return "No repository specified"
        return "No repository found at %r" % self.specifier
