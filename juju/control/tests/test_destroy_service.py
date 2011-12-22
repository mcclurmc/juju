from twisted.internet.defer import inlineCallbacks
from yaml import dump

from juju.control import main

from .common import ControlToolTest
from juju.charm.tests.test_repository import RepositoryTestBase
from juju.state.tests.test_service import ServiceStateManagerTestBase
from juju.providers import dummy # for coverage/trial interaction


class ControlStopServiceTest(
    ServiceStateManagerTestBase, ControlToolTest, RepositoryTestBase):

    @inlineCallbacks
    def setUp(self):
        yield super(ControlStopServiceTest, self).setUp()
        config = {
            "environments": {"firstenv": {"type": "dummy"}}}

        self.write_config(dump(config))
        self.config.load()
        self.service_state1 = yield self.add_service_from_charm("mysql")
        self.service1_unit = yield self.service_state1.add_unit_state()
        self.service_state2 = yield self.add_service_from_charm("wordpress")

        yield self.add_relation(
            "database",
            (self.service_state1, "db", "server"),
            (self.service_state2, "db", "client"))

        self.output = self.capture_logging()
        self.stderr = self.capture_stream("stderr")

    @inlineCallbacks
    def test_stop_service(self):
        """
        'juju-control stop_service ' will shutdown all configured instances
        in all environments.
        """
        topology = yield self.get_topology()
        service_id = topology.find_service_with_name("mysql")
        self.assertNotEqual(service_id, None)

        finished = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()
        main(["destroy-service", "mysql"])
        yield finished
        topology = yield self.get_topology()
        self.assertFalse(topology.has_service(service_id))
        exists = yield self.client.exists("/services/%s" % service_id)
        self.assertFalse(exists)
        self.assertIn("Service 'mysql' destroyed.", self.output.getvalue())

    @inlineCallbacks
    def test_stop_unknown_service(self):
        finished = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()
        main(["destroy-service", "volcano"])
        yield finished
        self.assertIn(
            "Service 'volcano' was not found", self.stderr.getvalue())
