import json
import os
from yaml import dump

from twisted.internet.defer import inlineCallbacks, succeed

from juju.charm.directory import CharmDirectory
from juju.charm.repository import LocalCharmRepository
from juju.charm.tests.test_metadata import test_repository_path
from juju.charm.url import CharmURL
from juju.control import main
from juju.errors import FileNotFound
from juju.environment.environment import Environment
from juju.unit.workflow import UnitWorkflowState

from .common import MachineControlToolTest


class CharmUpgradeTestBase(object):

    def add_charm(self, metadata, revision, repository_dir=None, bundle=False):
        if repository_dir is None:
            repository_dir = self.makeDir()
        series_dir = os.path.join(repository_dir, "series")
        os.mkdir(series_dir)
        charm_dir = self.makeDir()
        with open(os.path.join(charm_dir, "metadata.yaml"), "w") as f:
            f.write(dump(metadata))
        with open(os.path.join(charm_dir, "revision"), "w") as f:
            f.write(str(revision))
        if bundle:
            CharmDirectory(charm_dir).make_archive(
                os.path.join(series_dir, "%s.charm" % metadata["name"]))
        else:
            os.rename(charm_dir, os.path.join(series_dir, metadata["name"]))
        return LocalCharmRepository(repository_dir)

    def increment_charm(self, charm):
        metadata = charm.metadata.get_serialization_data()
        metadata["name"] = "mysql"
        repository = self.add_charm(metadata, charm.get_revision() + 1)
        return repository


class ControlCharmUpgradeTest(
    MachineControlToolTest, CharmUpgradeTestBase):

    @inlineCallbacks
    def setUp(self):
        yield super(ControlCharmUpgradeTest, self).setUp()
        config = {
            "environments": {"firstenv": {"type": "dummy"}}}

        self.write_config(dump(config))
        self.config.load()
        self.service_state1 = yield self.add_service_from_charm("mysql")
        self.service_unit1 = yield self.service_state1.add_unit_state()

        self.unit1_workflow = UnitWorkflowState(
            self.client, self.service_unit1, None, self.makeDir())
        yield self.unit1_workflow.set_state("started")

        self.environment = self.config.get_default()
        self.provider = self.environment.get_machine_provider()

        self.output = self.capture_logging()
        self.stderr = self.capture_stream("stderr")

    @inlineCallbacks
    def test_charm_upgrade(self):
        """
        'juju charm-upgrade <service_name>' will schedule
        a charm for upgrade.
        """
        repository = self.increment_charm(self.charm)

        mock_environment = self.mocker.patch(Environment)
        mock_environment.get_machine_provider()
        self.mocker.result(self.provider)

        finished = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()
        main(["upgrade-charm", "--repository", repository.path, "mysql"])
        yield finished

        # Verify the service has a new charm reference
        charm_id = yield self.service_state1.get_charm_id()
        self.assertEqual(charm_id, "local:series/mysql-2")

        # Verify the provider storage has been updated
        charm = yield repository.find(CharmURL.parse("local:series/mysql"))
        storage = self.provider.get_file_storage()
        try:
            yield storage.get(
                "local_3a_series_2f_mysql-2_3a_%s" % charm.get_sha256())
        except FileNotFound:
            self.fail("New charm not uploaded")

        # Verify the upgrade flag on the service units.
        upgrade_flag = yield self.service_unit1.get_upgrade_flag()
        self.assertTrue(upgrade_flag)

    @inlineCallbacks
    def test_missing_repository(self):
        finished = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()

        main(["upgrade-charm", "mysql"])
        yield finished

        self.assertIn("No repository specified", self.output.getvalue())

    @inlineCallbacks
    def test_upgrade_charm_with_unupgradeable_units(self):
        """If there are units that won't be upgraded, they will be reported,
        other units will be upgraded.
        """
        repository = self.increment_charm(self.charm)
        service_unit2 = yield self.service_state1.add_unit_state()

        finished = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()

        main(["upgrade-charm", "--repository", repository.path, "mysql"])
        yield finished

        # Verify report of unupgradeable units
        self.assertIn(
            ("Unit 'mysql/1' is not in a running state "
            "(state: 'uninitialized'), won't upgrade"),
            self.output.getvalue())

        # Verify flags only set on upgradeable unit.
        value = (yield service_unit2.get_upgrade_flag())
        self.assertFalse(value)
        value = (yield self.service_unit1.get_upgrade_flag())
        self.assertTrue(value)

    @inlineCallbacks
    def test_upgrade_charm_unknown_service(self):
        finished = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()
        main(["upgrade-charm", "--repository", self.makeDir(), "volcano"])
        yield finished
        self.assertIn(
            "Service 'volcano' was not found", self.stderr.getvalue())

    @inlineCallbacks
    def test_upgrade_charm_unknown_charm(self):
        """If a charm is not found in the repository, an error is given.
        """
        finished = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()
        repository_dir = self.makeDir()
        os.mkdir(os.path.join(repository_dir, "series"))
        main(["upgrade-charm", "--repository", repository_dir, "mysql"])
        yield finished
        self.assertIn(
            "Charm 'local:series/mysql' not found in repository",
            self.output.getvalue())

    @inlineCallbacks
    def test_upgrade_charm_unknown_charm_dryrun(self):
        """If a charm is not found in the repository, an error is given.
        """
        finished = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()
        repository_dir = self.makeDir()
        os.mkdir(os.path.join(repository_dir, "series"))
        main(["upgrade-charm", "--repository", repository_dir, "mysql", "--dry-run"])
        yield finished
        self.assertIn(
            "Charm 'local:series/mysql' not found in repository",
            self.output.getvalue())

    @inlineCallbacks
    def test_upgrade_charm_dryrun_reports_unupgradeable_units(self):
        """If there are units that won't be upgraded, dry-run will report them.
        """
        repository = self.increment_charm(self.charm)
        service_unit2 = yield self.service_state1.add_unit_state()

        finished = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()

        main(["upgrade-charm", "-n",
              "--repository", repository.path, "mysql"])
        yield finished

        # Verify dry run
        self.assertIn(
            "Service would be upgraded from charm", self.output.getvalue())
        # Verify report of unupgradeable units
        self.assertIn(
            ("Unit 'mysql/1' is not in a running state "
            "(state: 'uninitialized'), won't upgrade"),
            self.output.getvalue())

        # Verify no flags have been set.
        value = (yield service_unit2.get_upgrade_flag())
        self.assertFalse(value)
        value = (yield self.service_unit1.get_upgrade_flag())
        self.assertFalse(value)


    @inlineCallbacks
    def test_latest_local_dry_run(self):
        """Do nothing; log that local charm would be re-revisioned and used"""
        finished = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()

        metadata = self.charm.metadata.get_serialization_data()
        metadata["name"] = "mysql"
        repository = self.add_charm(metadata, 1)
        main(["upgrade-charm", "--dry-run",
                  "--repository", repository.path, "mysql"])
        yield finished
        charm_path = os.path.join(repository.path, "series", "mysql")
        self.assertIn(
            "%s would be set to revision 2" % charm_path,
            self.output.getvalue())
        self.assertIn(
            "Service would be upgraded from charm 'local:series/mysql-1' to "
            "'local:series/mysql-2'",
            self.output.getvalue())

        with open(os.path.join(charm_path, "revision")) as f:
            self.assertEquals(f.read(), "1")

        upgrade_flag = yield self.service_unit1.get_upgrade_flag()
        self.assertFalse(upgrade_flag)

    @inlineCallbacks
    def test_latest_local_live_fire(self):
        """Local charm should be re-revisioned and used; log that it was"""
        finished = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()

        metadata = self.charm.metadata.get_serialization_data()
        metadata["name"] = "mysql"
        repository = self.add_charm(metadata, 1)
        main(["upgrade-charm", "--repository", repository.path, "mysql"])

        yield finished
        charm_path = os.path.join(repository.path, "series", "mysql")
        self.assertIn(
            "Setting %s to revision 2" % charm_path,
            self.output.getvalue())

        with open(os.path.join(charm_path, "revision")) as f:
            self.assertEquals(f.read(), "2\n")

        upgrade_flag = yield self.service_unit1.get_upgrade_flag()
        self.assertTrue(upgrade_flag)

    @inlineCallbacks
    def test_latest_local_leapfrog_dry_run(self):
        """Do nothing; log that local charm would be re-revisioned and used"""
        finished = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()

        metadata = self.charm.metadata.get_serialization_data()
        metadata["name"] = "mysql"
        repository = self.add_charm(metadata, 0)
        main(["upgrade-charm", "--dry-run",
                  "--repository", repository.path, "mysql"])
        yield finished
        charm_path = os.path.join(repository.path, "series", "mysql")
        self.assertIn(
            "%s would be set to revision 2" % charm_path,
            self.output.getvalue())
        self.assertIn(
            "Service would be upgraded from charm 'local:series/mysql-1' to "
            "'local:series/mysql-2'",
            self.output.getvalue())

        with open(os.path.join(charm_path, "revision")) as f:
            self.assertEquals(f.read(), "0")

        upgrade_flag = yield self.service_unit1.get_upgrade_flag()
        self.assertFalse(upgrade_flag)

    @inlineCallbacks
    def test_latest_local_leapfrog_live_fire(self):
        """Local charm should be re-revisioned and used; log that it was"""
        finished = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()

        metadata = self.charm.metadata.get_serialization_data()
        metadata["name"] = "mysql"
        repository = self.add_charm(metadata, 0)
        main(["upgrade-charm", "--repository", repository.path, "mysql"])

        yield finished
        charm_path = os.path.join(repository.path, "series", "mysql")
        self.assertIn(
            "Setting %s to revision 2" % charm_path,
            self.output.getvalue())

        with open(os.path.join(charm_path, "revision")) as f:
            self.assertEquals(f.read(), "2\n")

        upgrade_flag = yield self.service_unit1.get_upgrade_flag()
        self.assertTrue(upgrade_flag)

    @inlineCallbacks
    def test_latest_local_bundle_dry_run(self):
        """Do nothing; log that nothing would be done"""
        finished = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()

        metadata = self.charm.metadata.get_serialization_data()
        metadata["name"] = "mysql"
        repository = self.add_charm(metadata, 1, bundle=True)
        main(["upgrade-charm", "--dry-run",
              "--repository", repository.path, "mysql"])
        yield finished
        self.assertIn(
            "Service already running latest charm",
            self.output.getvalue())

        upgrade_flag = yield self.service_unit1.get_upgrade_flag()
        self.assertFalse(upgrade_flag)

    @inlineCallbacks
    def test_latest_local_bundle_live_fire(self):
        """Do nothing; log that nothing was done"""
        finished = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()

        metadata = self.charm.metadata.get_serialization_data()
        metadata["name"] = "mysql"
        repository = self.add_charm(metadata, 1, bundle=True)
        main(["upgrade-charm", "--repository", repository.path, "mysql"])

        yield finished
        self.assertIn(
            "Charm 'local:series/mysql-1' is the latest revision known",
            self.output.getvalue())

        upgrade_flag = yield self.service_unit1.get_upgrade_flag()
        self.assertFalse(upgrade_flag)


class RemoteUpgradeCharmTest(MachineControlToolTest):

    @inlineCallbacks
    def setUp(self):
        yield super(RemoteUpgradeCharmTest, self).setUp()
        config = {
            "environments": {"firstenv": {"type": "dummy"}}}

        self.write_config(dump(config))
        self.config.load()

        charm = CharmDirectory(os.path.join(
            test_repository_path, "series", "mysql"))
        self.charm_state_manager.add_charm_state(
            "cs:series/mysql-1", charm, "")
        self.service_state1 = yield self.add_service_from_charm(
            "mysql", "cs:series/mysql-1")
        self.service_unit1 = yield self.service_state1.add_unit_state()

        self.unit1_workflow = UnitWorkflowState(
            self.client, self.service_unit1, None, self.makeDir())
        yield self.unit1_workflow.set_state("started")

        self.environment = self.config.get_default()
        self.provider = self.environment.get_machine_provider()

        self.output = self.capture_logging()
        self.stderr = self.capture_stream("stderr")

    @inlineCallbacks
    def test_latest_dry_run(self):
        """Do nothing; log that nothing would be done"""
        finished = self.setup_cli_reactor()
        self.setup_exit(0)
        getPage = self.mocker.replace("twisted.web.client.getPage")
        getPage(
            "https://store.juju.ubuntu.com/"
            "charm-info?charms=cs%3Aseries/mysql")
        self.mocker.result(succeed(json.dumps(
            {"cs:series/mysql": {"revision": 1, "sha256": "whatever"}})))
        self.mocker.replay()

        main(["upgrade-charm", "--dry-run", "mysql"])
        yield finished
        self.assertIn(
            "Service already running latest charm",
            self.output.getvalue())

        upgrade_flag = yield self.service_unit1.get_upgrade_flag()
        self.assertFalse(upgrade_flag)

    @inlineCallbacks
    def test_latest_live_fire(self):
        """Do nothing; log that nothing was done"""
        finished = self.setup_cli_reactor()
        self.setup_exit(0)

        getPage = self.mocker.replace("twisted.web.client.getPage")
        getPage(
            "https://store.juju.ubuntu.com/charm-info?charms=cs%3Aseries/mysql")
        self.mocker.result(succeed(json.dumps(
            {"cs:series/mysql": {"revision": 1, "sha256": "whatever"}})))
        self.mocker.replay()

        main(["upgrade-charm", "mysql"])

        yield finished
        self.assertIn(
            "Charm 'cs:series/mysql-1' is the latest revision known",
            self.output.getvalue())

        upgrade_flag = yield self.service_unit1.get_upgrade_flag()
        self.assertFalse(upgrade_flag)
