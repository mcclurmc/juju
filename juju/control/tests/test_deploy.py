import yaml
import logging

from twisted.internet.defer import inlineCallbacks, succeed

from juju.control import deploy, main
from juju.environment.environment import Environment
from juju.environment.config import EnvironmentsConfig
from juju.charm.errors import ServiceConfigValueError
from juju.state.environment import EnvironmentStateManager
from juju.state.errors import ServiceStateNameInUse
from juju.state.service import ServiceStateManager
from juju.state.relation import RelationStateManager

from juju.lib.mocker import MATCH

from .common import MachineControlToolTest


class ControlDeployTest(MachineControlToolTest):

    @inlineCallbacks
    def setUp(self):
        yield super(ControlDeployTest, self).setUp()

        config = {
            "environments": {
                "firstenv": {
                    "type": "dummy",
                    "admin-secret": "homer",
                    "placement": "unassigned",
                    "default-series": "series"}}}
        self.write_config(yaml.dump(config))
        self.config.load()

    def test_deploy_multiple_environments_none_specified(self):
        """
        If multiple environments are configured, with no default,
        one must be specified for the deploy command.
        """
        self.capture_logging()
        self.setup_cli_reactor()
        self.setup_exit(1)
        self.mocker.replay()

        config = {
            "environments": {
                "firstenv": {
                    "type": "dummy", "admin-secret": "homer"},
                "secondenv": {
                    "type": "dummy", "admin-secret": "marge"}}}

        self.write_config(yaml.dump(config))
        stderr = self.capture_stream("stderr")
        main(["deploy", "--repository", self.unbundled_repo_path, "mysql"])
        self.assertIn("There are multiple environments", stderr.getvalue())

    def test_no_repository(self):
        self.capture_logging()
        self.setup_cli_reactor()
        self.setup_exit(1)
        self.mocker.replay()

        stderr = self.capture_stream("stderr")
        main(["deploy", "local:redis"])
        self.assertIn("No repository specified", stderr.getvalue())

    def test_charm_not_found(self):
        self.capture_logging()
        self.setup_cli_reactor()
        self.setup_exit(1)
        self.mocker.replay()

        stderr = self.capture_stream("stderr")
        main([
            "deploy", "--repository", self.unbundled_repo_path, "local:redis"])
        self.assertIn(
            "Charm 'local:series/redis' not found in repository",
            stderr.getvalue())

    @inlineCallbacks
    def test_deploy_service_name_conflict(self):
        """Raise an error if a service name conflicts with an existing service
        """
        environment = self.config.get("firstenv")
        yield deploy.deploy(self.config, environment, self.unbundled_repo_path,
            "local:sample", "beekeeper", logging.getLogger("deploy"))
        # deploy the service a second time to generate a name conflict
        d = deploy.deploy(self.config, environment, self.unbundled_repo_path,
            "local:sample", "beekeeper", logging.getLogger("deploy"))
        error = yield self.failUnlessFailure(d, ServiceStateNameInUse)
        self.assertEqual(
            str(error),
            "Service name 'beekeeper' is already in use")

    @inlineCallbacks
    def test_deploy_no_service_name_short_charm_name(self):
        """Uses charm name as service name if possible."""
        environment = self.config.get("firstenv")
        yield deploy.deploy(self.config, environment, self.unbundled_repo_path,
                            "local:sample", None, logging.getLogger("deploy"))
        service = yield ServiceStateManager(
            self.client).get_service_state("sample")
        self.assertEqual(service.service_name, "sample")

    @inlineCallbacks
    def test_deploy_no_service_name_long_charm_name(self):
        """Uses charm name as service name if possible."""
        environment = self.config.get("firstenv")
        yield deploy.deploy(self.config, environment, self.unbundled_repo_path,
                            "local:series/sample", None,
                            logging.getLogger("deploy"))
        service = yield ServiceStateManager(
            self.client).get_service_state("sample")
        self.assertEqual(service.service_name, "sample")

    def xtest_deploy_with_nonexistent_environment_specified(self):
        self.capture_logging()
        self.setup_cli_reactor()
        self.setup_exit(1)
        self.mocker.replay()

        config = {
            "environments": {
                "firstenv": {
                    "type": "dummy", "admin-secret": "homer"},
                "secondenv": {
                    "type": "dummy", "admin-secret": "marge"}}}

        self.write_config(yaml.dump(config))

        stderr = self.capture_stream("stderr")
        main(["deploy", "--environment", "roman-candle",
              "--repository", self.unbundled_repo_path, "sample"])
        self.assertIn("Invalid environment 'roman-candle'", stderr.getvalue())

    def test_deploy_with_environment_specified(self):
        self.setup_cli_reactor()
        self.setup_exit(0)

        command = self.mocker.replace("juju.control.deploy.deploy")
        config = {
            "environments": {
                "firstenv": {
                    "type": "dummy", "admin-secret": "homer"},
                "secondenv": {
                    "type": "dummy", "admin-secret": "marge"}}}

        self.write_config(yaml.dump(config))

        def match_config(config):
            return isinstance(config, EnvironmentsConfig)

        def match_environment(environment):
            return isinstance(environment, Environment) and \
                   environment.name == "secondenv"

        def match_anything(*args):
            print args
            return True
        command(MATCH(match_config), MATCH(match_environment),
                self.unbundled_repo_path, "local:sample", None,
                MATCH(lambda x: isinstance(x, logging.Logger)),
                None, num_units=1)
        self.mocker.replay()
        self.mocker.result(succeed(True))

        self.capture_stream("stderr")
        main(["deploy", "--environment", "secondenv", "--repository",
              self.unbundled_repo_path, "local:sample"])

    @inlineCallbacks
    def test_deploy(self):
        """Create service, and service unit on machine from charm"""
        environment = self.config.get("firstenv")
        yield deploy.deploy(
            self.config, environment, self.unbundled_repo_path, "local:sample",
            "myblog", logging.getLogger("deploy"))
        topology = yield self.get_topology()

        service_id = topology.find_service_with_name("myblog")
        self.assertEqual(service_id, "service-%010d" % 0)
        exists = yield self.client.exists("/services/%s" % service_id)
        self.assertTrue(exists)

        machine_ids = topology.get_machines()
        self.assertEqual(
            machine_ids,
            ["machine-%010d" % 0, "machine-%010d" % 1])
        exists = yield self.client.exists("/machines/%s" % machine_ids[0])
        self.assertTrue(exists)

        unit_ids = topology.get_service_units(service_id)
        self.assertEqual(unit_ids, ["unit-%010d" % 0])
        exists = yield self.client.exists("/units/%s" % unit_ids[0])
        self.assertTrue(exists)

    @inlineCallbacks
    def test_deploy_multiple_units(self):
        """Create service, and service unit on machine from charm"""
        environment = self.config.get("firstenv")
        yield deploy.deploy(
            self.config, environment, self.unbundled_repo_path, "local:sample",
            "myblog", logging.getLogger("deploy"), num_units=5)
        topology = yield self.get_topology()

        service_id = topology.find_service_with_name("myblog")
        self.assertEqual(service_id, "service-%010d" % 0)
        exists = yield self.client.exists("/services/%s" % service_id)
        self.assertTrue(exists)

        # Verify standard placement policy - unit placed on a new machine
        machine_ids = topology.get_machines()
        self.assertEqual(
            set(machine_ids),
            set(["machine-%010d" % i for i in xrange(6)]))
        for i in xrange(6):
            self.assertTrue(
                (yield self.client.exists("/machines/%s" % machine_ids[i])))

        unit_ids = topology.get_service_units(service_id)
        self.assertEqual(
            set(unit_ids),
            set(["unit-%010d" % i for i in xrange(5)]))
        for i in xrange(5):
            self.assertTrue(
                (yield self.client.exists("/units/%s" % unit_ids[i])))

    @inlineCallbacks
    def test_deploy_sends_environment(self):
        """Uses charm name as service name if possible."""
        environment = self.config.get("firstenv")

        yield deploy.deploy(
            self.config, environment, self.unbundled_repo_path, "local:sample",
            None, logging.getLogger("deploy"))

        env_state_manager = EnvironmentStateManager(self.client)
        env_config = yield env_state_manager.get_config()

        self.assertEquals(yaml.load(env_config.serialize("firstenv")),
                          yaml.load(self.config.serialize("firstenv")))

    @inlineCallbacks
    def test_deploy_reuses_machines(self):
        """Verify that if machines are not in use, deploy uses them."""
        environment = self.config.get("firstenv")
        yield deploy.deploy(
            self.config, environment, self.unbundled_repo_path, "local:mysql",
            None, logging.getLogger("deploy"))
        yield deploy.deploy(
            self.config, environment, self.unbundled_repo_path,
            "local:wordpress", None, logging.getLogger("deploy"))
        yield self.destroy_service("mysql")
        yield self.destroy_service("wordpress")
        yield deploy.deploy(
            self.config, environment, self.unbundled_repo_path,
            "local:wordpress", None, logging.getLogger("deploy"))
        yield deploy.deploy(
            self.config, environment, self.unbundled_repo_path, "local:mysql",
            None, logging.getLogger("deploy"))
        yield self.assert_machine_assignments("wordpress", [1])
        yield self.assert_machine_assignments("mysql", [2])

    def test_deploy_missing_config(self):
        """Missing config files should prevent the deployment"""
        stderr = self.capture_stream("stderr")
        self.setup_cli_reactor()
        self.setup_exit(1)
        self.mocker.replay()

        # missing config file
        main(["deploy", "--config", "missing",
            "--repository", self.unbundled_repo_path, "local:sample"])
        self.assertIn("Config file 'missing'", stderr.getvalue())

    @inlineCallbacks
    def test_deploy_with_bad_config(self):
        """Valid config options should be available to the deployed
        service."""
        config_file = self.makeFile(
            yaml.dump(dict(otherservice=dict(application_file="foo"))))
        environment = self.config.get("firstenv")

        failure = deploy.deploy(
            self.config, environment, self.unbundled_repo_path, "local:sample",
            "myblog", logging.getLogger("deploy"), config_file)
        error = yield self.assertFailure(failure, ServiceConfigValueError)
        self.assertIn(
            "Expected a YAML dict with service name ('myblog').", str(error))

    @inlineCallbacks
    def test_deploy_with_config(self):
        """Valid config options should be available to the deployed
        service."""
        config_file = self.makeFile(yaml.dump(dict(
                    myblog=dict(outlook="sunny",
                                username="tester01"))))
        environment = self.config.get("firstenv")

        yield deploy.deploy(
            self.config, environment, self.unbundled_repo_path, "local:dummy",
            "myblog", logging.getLogger("deploy"), config_file)

        # Verify that options in the yaml are available as state after
        # the deploy call (successfully applied)
        service = yield ServiceStateManager(
            self.client).get_service_state("myblog")
        config = yield service.get_config()
        self.assertEqual(config["outlook"], "sunny")
        self.assertEqual(config["username"], "tester01")
        # a default value from the config.yaml
        self.assertEqual(config["title"], "My Title")

    @inlineCallbacks
    def test_deploy_with_default_config(self):
        """Valid config options should be available to the deployed
        service."""
        environment = self.config.get("firstenv")

        # Here we explictly pass no config file but the services
        # associated config.yaml defines default which we expect to
        # find anyway.
        yield deploy.deploy(
            self.config, environment, self.unbundled_repo_path, "local:dummy",
            "myblog", logging.getLogger("deploy"), None)

        # Verify that options in the yaml are available as state after
        # the deploy call (successfully applied)
        service = yield ServiceStateManager(
            self.client).get_service_state("myblog")
        config = yield service.get_config()
        self.assertEqual(config["title"], "My Title")

    @inlineCallbacks
    def test_deploy_adds_peer_relations(self):
        """Deploy automatically adds a peer relations."""
        environment = self.config.get("firstenv")
        yield deploy.deploy(
            self.config, environment, self.unbundled_repo_path, "local:riak",
            None, logging.getLogger("deploy"))

        service_manager = ServiceStateManager(self.client)
        service_state = yield service_manager.get_service_state("riak")
        relation_manager = RelationStateManager(self.client)
        relations = yield relation_manager.get_relations_for_service(
            service_state)
        self.assertEqual(len(relations), 1)
        self.assertEqual(relations[0].relation_name, "ring")

    @inlineCallbacks
    def test_deploy_policy_from_environment(self):
        config = {
            "environments": {"firstenv": {
                    "placement": "local",
                    "type": "dummy",
                    "default-series": "series"}}}

        self.write_config(yaml.dump(config))
        self.config.load()

        finished = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()
        main(["deploy", "--environment", "firstenv", "--repository",
              self.unbundled_repo_path, "local:sample", "beekeeper"])
        yield finished

        # and verify its placed on node 0 (as per local policy)
        service = yield self.service_state_manager.get_service_state(
            "beekeeper")
        units = yield service.get_all_unit_states()
        unit = units[0]
        machine_id = yield unit.get_assigned_machine_id()
        self.assertEqual(machine_id, 0)
