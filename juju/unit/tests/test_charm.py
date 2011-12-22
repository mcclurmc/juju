from functools import partial
import os
import shutil

from twisted.internet.defer import inlineCallbacks, returnValue, succeed, fail
from twisted.web.error import Error
from twisted.web.client import downloadPage

from juju.charm import get_charm_from_path
from juju.charm.bundle import CharmBundle
from juju.charm.publisher import CharmPublisher
from juju.charm.tests import local_charm_id
from juju.charm.tests.test_directory import sample_directory
from juju.environment.config import EnvironmentsConfig
from juju.errors import FileNotFound
from juju.lib import under
from juju.state.environment import EnvironmentStateManager
from juju.state.errors import CharmStateNotFound
from juju.state.tests.common import StateTestBase

from juju.unit.charm import download_charm

from juju.lib.mocker import MATCH

SAMPLE_ENV = """\
environments:
  myfirstenv:
    type: dummy
    foo: bar
    storage-directory: %s
"""


class CharmPublisherTestBase(StateTestBase):

    @inlineCallbacks
    def setUp(self):
        yield super(CharmPublisherTestBase, self).setUp()

        config = self.get_test_environment_config()
        environment = config.get_default()

        # Store the environment to zookeeper
        environment_state_manager = EnvironmentStateManager(self.client)
        yield environment_state_manager.set_config_state(config, "myfirstenv")

        # Load the environment
        self.provider = environment.get_machine_provider()
        self.storage = self.provider.get_file_storage()

    def get_test_environment_config(self):
        sample_config = SAMPLE_ENV % self.makeDir()
        config = EnvironmentsConfig()
        config.parse(sample_config)
        return config

    @inlineCallbacks
    def publish_charm(self, charm_path=sample_directory):
        charm = get_charm_from_path(charm_path)
        publisher = CharmPublisher(self.client, self.storage)
        yield publisher.add_charm(local_charm_id(charm), charm)
        charm_states = yield publisher.publish()
        returnValue((charm, charm_states[0]))


class DownloadTestCase(CharmPublisherTestBase):

    @inlineCallbacks
    def test_charm_download_file(self):
        """Downloading a charm should store the charm locally.
        """
        charm, charm_state = yield self.publish_charm()
        charm_directory = self.makeDir()

        # Download the charm
        yield download_charm(
            self.client, charm_state.id, charm_directory)

        # Verify the downloaded copy
        checksum = charm.get_sha256()
        charm_id = local_charm_id(charm)
        charm_key = under.quote("%s:%s" % (charm_id, checksum))
        charm_path = os.path.join(charm_directory, charm_key)

        self.assertTrue(os.path.exists(charm_path))
        bundle = CharmBundle(charm_path)
        self.assertEquals(bundle.get_revision(), charm.get_revision())

        self.assertEqual(checksum, bundle.get_sha256())

    @inlineCallbacks
    def test_charm_missing_download_file(self):
        """Downloading a file that doesn't exist raises FileNotFound.
        """
        charm, charm_state = yield self.publish_charm()
        charm_directory = self.makeDir()

        # Delete the file
        file_path = charm_state.bundle_url[len("file://"):]
        os.remove(file_path)

        # Download the charm
        yield self.assertFailure(
            download_charm(self.client, charm_state.id, charm_directory),
            FileNotFound)

    @inlineCallbacks
    def test_charm_download_http(self):
        """Downloading a charm should store the charm locally.
        """
        mock_storage = self.mocker.patch(self.storage)

        def match_string(expected, value):
            self.assertTrue(isinstance(value, basestring))
            self.assertIn(expected, value)
            return True

        mock_storage.get_url(MATCH(
            partial(match_string, "local_3a_series_2f_dummy-1")))

        self.mocker.result("http://example.com/foobar.zip")

        download_page = self.mocker.replace(downloadPage)
        download_page(
            MATCH(partial(match_string, "http://example.com/foobar.zip")),
            MATCH(partial(match_string, "local_3a_series_2f_dummy-1")))

        def bundle_in_place(url, local_path):
            # must keep ref to charm else temp file goes out of scope.
            charm = get_charm_from_path(sample_directory)
            bundle = charm.as_bundle()
            shutil.copyfile(bundle.path, local_path)

        self.mocker.call(bundle_in_place)
        self.mocker.result(succeed(True))
        self.mocker.replay()

        charm, charm_state = yield self.publish_charm()
        charm_directory = self.makeDir()
        self.assertEqual(
            charm_state.bundle_url, "http://example.com/foobar.zip")

        # Download the charm
        yield download_charm(
            self.client, charm_state.id, charm_directory)

    @inlineCallbacks
    def test_charm_download_http_error(self):
        """Errors in donwloading a charm are reported as charm not found.
        """
        def match_string(expected, value):
            self.assertTrue(isinstance(value, basestring))
            self.assertIn(expected, value)
            return True

        mock_storage = self.mocker.patch(self.storage)
        mock_storage.get_url(
            MATCH(partial(match_string, "local_3a_series_2f_dummy-1")))
        remote_url = "http://example.com/foobar.zip"
        self.mocker.result(remote_url)

        download_page = self.mocker.replace(downloadPage)
        download_page(
            MATCH(partial(match_string, "http://example.com/foobar.zip")),
            MATCH(partial(match_string, "local_3a_series_2f_dummy-1")))

        self.mocker.result(fail(Error("400", "Bad Stuff", "")))
        self.mocker.replay()

        charm, charm_state = yield self.publish_charm()
        charm_directory = self.makeDir()
        self.assertEqual(charm_state.bundle_url, remote_url)

        error = yield self.assertFailure(
            download_charm(self.client, charm_state.id, charm_directory),
            FileNotFound)
        self.assertIn(remote_url, str(error))

    @inlineCallbacks
    def test_charm_download_not_found(self):
        """An error is raised if trying to download a non existant charm.
        """
        charm_directory = self.makeDir()

        # Download the charm
        error = yield self.assertFailure(
            download_charm(
                self.client, "local:mickey-21", charm_directory),
            CharmStateNotFound)

        self.assertEquals(str(error), "Charm 'local:mickey-21' was not found")
