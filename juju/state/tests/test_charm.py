import yaml

from twisted.internet.defer import inlineCallbacks

from juju.charm.tests import local_charm_id
from juju.state.charm import CharmStateManager
from juju.state.errors import CharmStateNotFound
from juju.state.tests.common import StateTestBase


class CharmStateManagerTest(StateTestBase):

    @inlineCallbacks
    def setUp(self):
        yield super(CharmStateManagerTest, self).setUp()
        self.charm_state_manager = CharmStateManager(self.client)
        self.charm_id = local_charm_id(self.charm)

    @inlineCallbacks
    def test_add_charm(self):
        """
        Adding a Charm into a CharmStateManager should register
        the charm within the Zookeeper state, according to the
        specification.
        """
        charm_state = yield self.charm_state_manager.add_charm_state(
            self.charm_id, self.charm, "http://example.com/abc")
        self.assertEquals(charm_state.id, "local:series/dummy-1")

        children = yield self.client.get_children("/charms")
        self.assertEquals(children, ["local_3a_series_2f_dummy-1"])

        content, stat = yield self.client.get(
            "/charms/local_3a_series_2f_dummy-1")

        charm_data = yaml.load(content)
        self.assertEquals(charm_data, {
            "metadata": self.charm.metadata.get_serialization_data(),
            "config": self.charm.config.get_serialization_data(),
            "sha256": self.charm.get_sha256(),
            "url": "http://example.com/abc"
        })

    @inlineCallbacks
    def test_get_charm(self):
        """
        A CharmState should be available if one get()s a charm
        that was previously added into the manager.
        """
        yield self.charm_state_manager.add_charm_state(
            self.charm_id, self.charm, "")
        charm_state = yield self.charm_state_manager.get_charm_state(
            "local:series/dummy-1")
        self.assertEquals(charm_state.id, "local:series/dummy-1")

    @inlineCallbacks
    def test_charm_state_attributes(self):
        """
        Verify that the basic (invariant) attributes of the
        CharmState are correctly in place.
        """
        yield self.charm_state_manager.add_charm_state(
            self.charm_id, self.charm, "http://example.com/abc")
        charm_state = yield self.charm_state_manager.get_charm_state(
            "local:series/dummy-1")
        self.assertEquals(charm_state.name, "dummy")
        self.assertEquals(charm_state.revision, 1)
        self.assertEquals(charm_state.id, "local:series/dummy-1")
        self.assertEquals(charm_state.bundle_url, "http://example.com/abc")

    @inlineCallbacks
    def test_charm_state_metadata(self):
        """
        Check that the charm metadata was correctly saved and loaded.
        """
        yield self.charm_state_manager.add_charm_state(
            self.charm_id, self.charm, "")
        charm_state = yield self.charm_state_manager.get_charm_state(
            "local:series/dummy-1")
        metadata = yield charm_state.get_metadata()
        self.assertEquals(metadata.name, "dummy")

    @inlineCallbacks
    def test_charm_state_config_options(self):
        """Verify ConfigOptions present and correct."""
        from juju.charm.tests.test_config import sample_yaml_data

        yield self.charm_state_manager.add_charm_state(
            self.charm_id, self.charm, "")
        charm_state = yield self.charm_state_manager.get_charm_state(
            "local:series/dummy-1")
        config = yield charm_state.get_config()
        self.assertEquals(config.get_serialization_data(),
                          sample_yaml_data)

    @inlineCallbacks
    def test_get_non_existing_charm_prior_to_initialization(self):
        """
        Getting a charm before the charms node was even
        initialized should raise an error about the charm not
        being present.
        """
        try:
            yield self.charm_state_manager.get_charm_state(
                "local:series/dummy-1")
        except CharmStateNotFound, e:
            self.assertEquals(e.charm_id, "local:series/dummy-1")
        else:
            self.fail("Error not raised.")

    @inlineCallbacks
    def test_get_non_existing_charm(self):
        """
        Trying to retrieve a charm from the state when it was
        never added should raise an error.
        """
        yield self.charm_state_manager.add_charm_state(
            self.charm_id, self.charm, "")
        try:
            yield self.charm_state_manager.get_charm_state(
                "local:anotherseries/dummy-1")
        except CharmStateNotFound, e:
            self.assertEquals(e.charm_id, "local:anotherseries/dummy-1")
        else:
            self.fail("Error not raised.")

    @inlineCallbacks
    def test_get_sha256(self):
        """
        We should be able to retrieve the sha256 of a stored
        charm.
        """
        yield self.charm_state_manager.add_charm_state(
            self.charm_id, self.charm, "")
        charm_state = yield self.charm_state_manager.get_charm_state(
            "local:series/dummy-1")
        sha256 = yield charm_state.get_sha256()
        self.assertEquals(sha256, self.charm.get_sha256())
