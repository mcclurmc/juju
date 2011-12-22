import yaml

from twisted.internet.defer import (
    inlineCallbacks, returnValue, succeed)

from zookeeper import NoNodeException

from juju.charm.config import ConfigOptions
from juju.charm.metadata import MetaData
from juju.charm.url import CharmURL
from juju.lib import under
from juju.state.base import StateBase
from juju.state.errors import CharmStateNotFound


def _charm_path(charm_id):
    return "/charms/%s" % under.quote(charm_id)


class CharmStateManager(StateBase):
    """Manages the state of charms in an environment."""

    @inlineCallbacks
    def add_charm_state(self, charm_id, charm, url):
        """Register metadata about the provided Charm.

        :param str charm_id: The key under which to store the Charm.

        :param charm: The Charm itself.

        :param url: The provider storage url for the Charm.
        """
        charm_data = {
            "config": charm.config.get_serialization_data(),
            "metadata": charm.metadata.get_serialization_data(),
            "sha256": charm.get_sha256(),
            "url": url
        }

        # XXX In the future we'll have to think about charm
        #     replacements here. For now this will do, and will
        #     explode reliably in case of conflicts.
        yield self._client.create(
            _charm_path(charm_id), yaml.safe_dump(charm_data))
        charm_state = CharmState(self._client, charm_id, charm_data)
        returnValue(charm_state)

    @inlineCallbacks
    def get_charm_state(self, charm_id):
        """Retrieve a CharmState for the given charm id."""
        try:
            content, stat = yield self._client.get(_charm_path(charm_id))
        except NoNodeException:
            raise CharmStateNotFound(charm_id)
        charm_data = yaml.load(content)
        charm_state = CharmState(self._client, charm_id, charm_data)
        returnValue(charm_state)


class CharmState(object):
    """State of a charm registered in an environment."""

    def __init__(self, client, charm_id, charm_data):
        self._client = client
        self._charm_url = CharmURL.parse(charm_id)
        self._charm_url.assert_revision()

        self._metadata = MetaData()
        self._metadata.parse_serialization_data(charm_data["metadata"])

        self._config = ConfigOptions()
        self._config.parse(charm_data["config"])

        # Just a health check:
        assert self._metadata.name == self.name

        self._sha256 = charm_data["sha256"]

        self._bundle_url = charm_data.get("url")

    @property
    def name(self):
        """The charm name."""
        return self._charm_url.name

    @property
    def revision(self):
        """The monotonically increasing charm revision number.
        """
        return self._charm_url.revision

    @property
    def bundle_url(self):
        """The url to the charm bundle in the provider storage."""
        return self._bundle_url

    @property
    def id(self):
        """The charm id"""
        return str(self._charm_url)

    def get_metadata(self):
        """Return deferred MetaData."""
        return succeed(self._metadata)

    def get_config(self):
        """Return deferred ConfigOptions."""
        return succeed(self._config)

    def get_sha256(self):
        """Return deferred sha256 for the charm."""
        return succeed(self._sha256)
