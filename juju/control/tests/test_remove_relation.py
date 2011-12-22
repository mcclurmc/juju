import logging

import yaml

from twisted.internet.defer import inlineCallbacks, returnValue

from juju.control import main, remove_relation
from juju.control.tests.common import ControlToolTest
from juju.charm.tests.test_repository import RepositoryTestBase
from juju.state.errors import ServiceStateNotFound
from juju.state.tests.test_service import ServiceStateManagerTestBase


class ControlRemoveRelationTest(
    ServiceStateManagerTestBase, ControlToolTest, RepositoryTestBase):

    @inlineCallbacks
    def setUp(self):
        yield super(ControlRemoveRelationTest, self).setUp()
        config = {
            "environments": {
                "firstenv": {
                    "type": "dummy", "admin-secret": "homer"}}}
        self.write_config(yaml.dump(config))
        self.config.load()
        self.output = self.capture_logging()
        self.stderr = self.capture_stream("stderr")

    @inlineCallbacks
    def add_relation_state(self, *service_names):
        for service_name in service_names:
            # probe if service already exists
            try:
                yield self.service_state_manager.get_service_state(
                    service_name)
            except ServiceStateNotFound:
                yield self.add_service_from_charm(service_name)
        endpoint_pairs = yield self.service_state_manager.join_descriptors(
            *service_names)
        endpoints = endpoint_pairs[0]
        endpoints = endpoint_pairs[0]
        if endpoints[0] == endpoints[1]:
            endpoints = endpoints[0:1]
        relation_state = (yield self.relation_state_manager.add_relation_state(
            *endpoints))[0]
        returnValue(relation_state)

    @inlineCallbacks
    def assertRemoval(self, relation_state):
        topology = yield self.get_topology()
        self.assertFalse(topology.has_relation(relation_state.internal_id))

    @inlineCallbacks
    def test_remove_relation(self):
        """Test that the command works when run from the CLI itself."""
        wait_on_reactor_stopped = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()
        relation_state = yield self.add_relation_state("mysql", "wordpress")
        yield self.add_relation_state("varnish", "wordpress")
        main(["remove-relation", "mysql", "wordpress"])
        yield wait_on_reactor_stopped
        self.assertIn(
            "Removed mysql relation from all service units.",
            self.output.getvalue())
        yield self.assertRemoval(relation_state)

    @inlineCallbacks
    def test_remove_peer_relation(self):
        """Test that services that peer can have that relation removed."""
        wait_on_reactor_stopped = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()
        relation_state = yield self.add_relation_state("riak", "riak")
        main(["remove-relation", "riak", "riak"])
        yield wait_on_reactor_stopped
        self.assertIn(
            "Removed riak relation from all service units.",
            self.output.getvalue())
        yield self.assertRemoval(relation_state)

    @inlineCallbacks
    def test_remove_relation_command(self):
        """Test removing a relation via supporting method in the cmd obj."""
        relation_state = yield self.add_relation_state("mysql", "wordpress")
        environment = self.config.get("firstenv")
        yield remove_relation.remove_relation(
            self.config, environment, False,
            logging.getLogger("juju.control.cli"), "mysql", "wordpress")
        self.assertIn(
            "Removed mysql relation from all service units.",
            self.output.getvalue())
        yield self.assertRemoval(relation_state)

    @inlineCallbacks
    def test_verbose_flag(self):
        """Test the verbose flag."""
        relation_state = yield self.add_relation_state("riak", "riak")
        wait_on_reactor_stopped = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()
        main(["--verbose", "remove-relation", "riak:ring", "riak:ring"])
        yield wait_on_reactor_stopped
        self.assertIn("Endpoint pairs", self.output.getvalue())
        self.assertIn(
            "Removed riak relation from all service units.",
            self.output.getvalue())
        yield self.assertRemoval(relation_state)

    # test for various errors

    def test_with_no_args(self):
        """Test two descriptor arguments are required for command."""
        self.assertRaises(SystemExit, main, ["remove-relation"])
        self.assertIn(
            "juju remove-relation: error: too few arguments",
            self.stderr.getvalue())

    def test_too_many_arguments_provided(self):
        """Test that command rejects more than 2 descriptor arguments."""
        self.assertRaises(
            SystemExit, main, ["remove-relation", "foo", "fum", "bar"])
        self.assertIn(
            "juju: error: unrecognized arguments: bar",
            self.stderr.getvalue())

    @inlineCallbacks
    def test_missing_service(self):
        """Test command fails if a service in the relation is missing."""
        yield self.add_service_from_charm("mysql")
        # but not wordpress
        wait_on_reactor_stopped = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()
        main(["remove-relation", "wordpress", "mysql"])
        yield wait_on_reactor_stopped
        self.assertIn(
            "Service 'wordpress' was not found",
            self.stderr.getvalue())

    @inlineCallbacks
    def test_no_common_relation_type(self):
        """Test command fails if no common relation between services."""
        yield self.add_service_from_charm("mysql")
        yield self.add_service_from_charm("riak")
        wait_on_reactor_stopped = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()
        main(["remove-relation", "riak", "mysql"])
        yield wait_on_reactor_stopped
        self.assertIn("No matching endpoints", self.stderr.getvalue())

    @inlineCallbacks
    def test_ambiguous_pairing(self):
        """Test command fails because the relation is ambiguous."""
        yield self.add_service_from_charm("mysql-alternative")
        yield self.add_service_from_charm("wordpress")
        wait_on_reactor_stopped = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()
        main(["remove-relation", "wordpress", "mysql-alternative"])
        yield wait_on_reactor_stopped
        self.assertIn(
            "Ambiguous relation 'wordpress mysql-alternative'; could refer "
            "to:\n  'wordpress:db mysql-alternative:dev' (mysql client / "
            "mysql server)\n  'wordpress:db mysql-alternative:prod' (mysql "
            "client / mysql server)",
            self.stderr.getvalue())

    @inlineCallbacks
    def test_missing_charm(self):
        """Test command fails because service has no corresponding charm."""
        yield self.add_service("mysql_no_charm")
        yield self.add_service_from_charm("wordpress")
        wait_on_reactor_stopped = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()
        main(["remove-relation", "wordpress", "mysql_no_charm"])
        yield wait_on_reactor_stopped
        self.assertIn("No matching endpoints", self.stderr.getvalue())

    @inlineCallbacks
    def test_remove_relation_missing_relation(self):
        """Test that the command works when run from the CLI itself."""
        wait_on_reactor_stopped = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()
        yield self.add_service_from_charm("mysql")
        yield self.add_service_from_charm("wordpress")
        main(["remove-relation", "mysql", "wordpress"])
        yield wait_on_reactor_stopped
        self.assertIn(
            "Relation not found",
            self.output.getvalue())

