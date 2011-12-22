import logging
import yaml

from twisted.internet.defer import inlineCallbacks

from juju.control import main, add_relation
from juju.control.tests.common import ControlToolTest
from juju.charm.tests.test_repository import RepositoryTestBase
from juju.state.tests.test_service import ServiceStateManagerTestBase


class ControlAddRelationTest(
    ServiceStateManagerTestBase, ControlToolTest, RepositoryTestBase):

    @inlineCallbacks
    def setUp(self):
        yield super(ControlAddRelationTest, self).setUp()
        config = {
            "environments": {
                "firstenv": {
                    "type": "dummy", "admin-secret": "homer"}}}
        self.write_config(yaml.dump(config))
        self.config.load()
        self.output = self.capture_logging()
        self.stderr = self.capture_stream("stderr")

    @inlineCallbacks
    def test_add_relation_method(self):
        """Test adding a relation via the supporting method in the cmd obj."""
        environment = self.config.get("firstenv")
        yield self.add_service_from_charm("mysql")
        yield self.add_service_from_charm("wordpress")
        yield add_relation.add_relation(
            self.config, environment, False,
            logging.getLogger("juju.control.cli"), "mysql", "wordpress")
        self.assertIn(
            "Added mysql relation to all service units.",
            self.output.getvalue())

    @inlineCallbacks
    def test_add_peer_relation(self):
        """Test that services that peer can have that relation added."""
        wait_on_reactor_stopped = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()
        yield self.add_service_from_charm("riak")
        main(["add-relation", "riak", "riak"])
        yield wait_on_reactor_stopped
        self.assertIn(
            "Added riak relation to all service units.",
            self.output.getvalue())

    @inlineCallbacks
    def test_add_relation(self):
        """Test that the command works when run from the CLI itself."""
        wait_on_reactor_stopped = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()
        yield self.add_service_from_charm("mysql")
        yield self.add_service_from_charm("wordpress")
        main(["add-relation", "mysql", "wordpress"])
        yield wait_on_reactor_stopped
        self.assertIn(
            "Added mysql relation to all service units.",
            self.output.getvalue())

    @inlineCallbacks
    def test_verbose_flag(self):
        """Test the verbose flag."""
        wait_on_reactor_stopped = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()
        yield self.add_service_from_charm("riak")
        main(["--verbose", "add-relation", "riak:ring", "riak:ring"])
        yield wait_on_reactor_stopped
        self.assertIn("Endpoint pairs", self.output.getvalue())
        self.assertIn(
            "Added riak relation to all service units.",
            self.output.getvalue())

    @inlineCallbacks
    def test_use_relation_name(self):
        """Test that the descriptor can be qualified with a relation_name."""
        yield self.add_service_from_charm("mysql-alternative")
        yield self.add_service_from_charm("wordpress")
        wait_on_reactor_stopped = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()
        yield self.add_service_from_charm("riak")
        main(["add-relation", "mysql-alternative:dev", "wordpress"])
        yield wait_on_reactor_stopped
        self.assertIn(
            "Added mysql relation to all service units.",
            self.output.getvalue())

    @inlineCallbacks
    def test_add_relation_multiple(self):
        """Test that the command can be create multiple relations."""
        environment = self.config.get("firstenv")
        yield self.add_service_from_charm("mysql")
        yield self.add_service_from_charm("wordpress")
        yield self.add_service_from_charm("varnish")
        yield add_relation.add_relation(
            self.config, environment, False,
            logging.getLogger("juju.control.cli"), "mysql", "wordpress")
        self.assertIn(
            "Added mysql relation to all service units.",
            self.output.getvalue())
        wait_on_reactor_stopped = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()
        main(["add-relation", "wordpress", "varnish"])
        yield wait_on_reactor_stopped
        self.assertIn(
            "Added varnish relation to all service units.",
            self.output.getvalue())

    # test for various errors

    def test_with_no_args(self):
        """Test that two descriptor arguments are required for command."""
        # in argparse, before reactor startup
        self.assertRaises(SystemExit, main, ["add-relation"])
        self.assertIn(
            "juju add-relation: error: too few arguments",
            self.stderr.getvalue())

    def test_too_many_arguments_provided(self):
        """Test command rejects more than 2 descriptor arguments."""
        self.assertRaises(
            SystemExit, main, ["add-relation", "foo", "fum", "bar"])
        self.assertIn(
            "juju: error: unrecognized arguments: bar",
            self.stderr.getvalue())

    @inlineCallbacks
    def test_missing_service_added(self):
        """Test command fails if a service is missing."""
        yield self.add_service_from_charm("mysql")
        # but not wordpress
        wait_on_reactor_stopped = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()
        main(["add-relation", "wordpress", "mysql"])
        yield wait_on_reactor_stopped
        self.assertIn(
            "Service 'wordpress' was not found",
            self.stderr.getvalue())

    @inlineCallbacks
    def test_no_common_relation_type(self):
        """Test command fails if the services cannot be added in a relation."""
        yield self.add_service_from_charm("mysql")
        yield self.add_service_from_charm("riak")
        wait_on_reactor_stopped = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()
        main(["add-relation", "riak", "mysql"])
        yield wait_on_reactor_stopped
        self.assertIn("No matching endpoints", self.stderr.getvalue())

    @inlineCallbacks
    def test_ambiguous_pairing(self):
        """Test command fails if more than one way to connect services."""
        yield self.add_service_from_charm("mysql-alternative")
        yield self.add_service_from_charm("wordpress")
        wait_on_reactor_stopped = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()
        main(["add-relation", "wordpress", "mysql-alternative"])
        yield wait_on_reactor_stopped
        self.assertIn(
            "Ambiguous relation 'wordpress mysql-alternative'; could refer "
            "to:\n  'wordpress:db mysql-alternative:dev' (mysql client / "
            "mysql server)\n  'wordpress:db mysql-alternative:prod' (mysql "
            "client / mysql server)",
            self.stderr.getvalue())

    @inlineCallbacks
    def test_missing_charm(self):
        """Test command fails if service is added w/o corresponding charm."""
        yield self.add_service("mysql_no_charm")
        yield self.add_service_from_charm("wordpress")
        wait_on_reactor_stopped = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()
        main(["add-relation", "wordpress", "mysql_no_charm"])
        yield wait_on_reactor_stopped
        self.assertIn("No matching endpoints", self.stderr.getvalue())

    @inlineCallbacks
    def test_relation_added_twice(self):
        """Test command fails if it's run twice."""
        yield self.add_service_from_charm("mysql")
        yield self.add_service_from_charm("wordpress")
        yield add_relation.add_relation(
            self.config, self.config.get("firstenv"), False,
            logging.getLogger("juju.control.cli"), "mysql", "wordpress")
        wait_on_reactor_stopped = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()
        main(["add-relation", "wordpress", "mysql"])
        yield wait_on_reactor_stopped
        self.assertIn("Relation already exists", self.stderr.getvalue())

    @inlineCallbacks
    def test_invalid_environment(self):
        """Test command with an environment that hasn't been set up."""
        wait_on_reactor_stopped = self.setup_cli_reactor()
        self.setup_exit(1)
        self.mocker.replay()
        main(["add-relation", "--environment", "roman-candle",
              "wordpress", "mysql"])
        yield wait_on_reactor_stopped
        self.assertIn(
            "Invalid environment 'roman-candle'",
            self.stderr.getvalue())
