import argparse
import logging
import os

from twisted.internet.defer import (
    inlineCallbacks, returnValue, succeed, fail, Deferred)

from juju.agents.base import TwistedOptionNamespace
from juju.agents.machine import MachineAgent
from juju.errors import JujuError
from juju.charm.bundle import CharmBundle
from juju.charm.directory import CharmDirectory
from juju.charm.publisher import CharmPublisher
from juju.charm.tests import local_charm_id
from juju.charm.tests.test_repository import RepositoryTestBase
from juju.lib import under
from juju.lib.mocker import MATCH
from juju.state.environment import EnvironmentStateManager
from juju.state.machine import MachineStateManager, MachineState
from juju.state.service import ServiceStateManager
from juju.tests.common import get_test_zookeeper_address


from .common import AgentTestBase


MATCH_BUNDLE = MATCH(lambda x: isinstance(x, CharmBundle))


class MachineAgentTest(AgentTestBase, RepositoryTestBase):

    agent_class = MachineAgent

    @inlineCallbacks
    def setUp(self):
        yield super(MachineAgentTest, self).setUp()

        self.output = self.capture_logging(
            "juju.agents.machine", level=logging.DEBUG)

        config = self.get_test_environment_config()
        environment = config.get_default()

        # Store the environment to zookeeper
        environment_state_manager = EnvironmentStateManager(self.client)
        yield environment_state_manager.set_config_state(config, "myfirstenv")

        # Load the environment with the charm state and charm binary
        self.provider = environment.get_machine_provider()
        self.storage = self.provider.get_file_storage()
        self.charm = CharmDirectory(self.sample_dir1)
        self.publisher = CharmPublisher(self.client, self.storage)
        yield self.publisher.add_charm(local_charm_id(self.charm), self.charm)

        charm_states = yield self.publisher.publish()
        self.charm_state = charm_states[0]

        # Create a service from the charm from which we can create units for
        # the machine.
        self.service_state_manager = ServiceStateManager(self.client)
        self.service = yield self.service_state_manager.add_service_state(
            "fatality-blog", self.charm_state)

    def process_kill(self, pid):
        try:
            os.kill(pid, 9)
        except OSError:
            pass

    @inlineCallbacks
    def get_agent_config(self):
        # gets invoked by AgentTestBase.setUp
        options = yield super(MachineAgentTest, self).get_agent_config()
        machine_state_manager = MachineStateManager(self.client)

        self.machine_state = yield machine_state_manager.add_machine_state()

        self.change_environment(
            JUJU_MACHINE_ID="0",
            JUJU_HOME=self.juju_directory)
        options["machine_id"] = str(self.machine_state.id)

        # Start the agent with watching enabled
        returnValue(options)

    @inlineCallbacks
    def test_start_begins_watch_and_initializes_directories(self):
        self.agent.set_watch_enabled(True)
        mock_machine_state = self.mocker.patch(MachineState)
        mock_machine_state.watch_assigned_units(
            self.agent.watch_service_units)
        self.mocker.replay()
        yield self.agent.startService()

        self.assertTrue(os.path.isdir(self.agent.charms_directory))
        self.assertTrue(os.path.isdir(self.agent.units_directory))
        self.assertTrue(os.path.isdir(self.agent.unit_state_directory))
        self.assertIn(
            "Machine agent started id:%s" % self.agent.get_machine_id(),
            self.output.getvalue())
        yield self.agent.stopService()

    def test_agent_machine_id_environment_extraction(self):
        self.change_args("es-agent")
        parser = argparse.ArgumentParser()
        self.agent.setup_options(parser)

        config = parser.parse_args(namespace=TwistedOptionNamespace())
        self.assertEqual(
            config["machine_id"], "0")

    def test_get_agent_name(self):
        self.assertEqual(self.agent.get_agent_name(), "Machine:0")

    def test_agent_machine_id_cli_error(self):
        """
        If the machine id can't be found, a detailed error message
        is given.
        """
        # initially setup by get_agent_config in setUp
        self.change_environment(JUJU_MACHINE_ID="")
        self.change_args("es-agent",
                         "--zookeeper-servers",
                         get_test_zookeeper_address(),
                         "--juju-directory",
                         self.makeDir())
        parser = argparse.ArgumentParser()
        self.agent.setup_options(parser)
        options = parser.parse_args(namespace=TwistedOptionNamespace())

        e = self.assertRaises(
            JujuError,
            self.agent.configure,
            options)

        self.assertIn(
            ("--machine-id must be provided in the command line,"
            " or $JUJU_MACHINE_ID in the environment"),
            str(e))

    def test_agent_machine_id_cli_extraction(self):
        """Command line passing of machine id works and has precedence
        over environment arg passing."""
        self.change_environment(JUJU_MACHINE_ID=str(21))
        self.change_args("es-agent", "--machine-id", "0")

        parser = argparse.ArgumentParser()
        self.agent.setup_options(parser)

        config = parser.parse_args(namespace=TwistedOptionNamespace())
        self.assertEqual(
            config["machine_id"], "0")

    def test_machine_agent_knows_its_machine_id(self):
        self.assertEqual(self.agent.get_machine_id(), "0")

    @inlineCallbacks
    def test_charm_download(self):
        """
        Downloading a charm should store the charm locally.
        """
        yield self.agent.startService()
        yield self.agent.download_charm(self.charm_state)

        checksum = self.charm.get_sha256()
        charm_id = local_charm_id(self.charm)
        charm_key = under.quote("%s:%s" % (charm_id, checksum))
        charm_path = os.path.join(self.agent.charms_directory, charm_key)

        self.assertTrue(os.path.exists(charm_path))
        bundle = CharmBundle(charm_path)
        self.assertEquals(
            bundle.get_revision(), self.charm.get_revision())
        self.assertEquals(bundle.get_sha256(), checksum)
        self.assertIn(
            "Downloading charm %s" % charm_id, self.output.getvalue())

    @inlineCallbacks
    def test_watch_new_service_unit(self):
        """
        Adding a new service unit is detected by the watch.
        """
        self.agent.set_watch_enabled(True)
        yield self.agent.startService()

        mock_deployment = self.mocker.patch(self.agent.deploy_factory)
        mock_deployment.start("0", get_test_zookeeper_address(), MATCH_BUNDLE)

        test_deferred = Deferred()

        def test_complete(machine_id, servers, bundle):
            test_deferred.callback(True)

        self.mocker.call(test_complete)
        self.mocker.replay()

        # Create a new service unit
        self.service_unit = yield self.service.add_unit_state()
        yield self.service_unit.assign_to_machine(self.machine_state)

        yield test_deferred
        self.assertIn(
            "Units changed old:set([]) new:set(['fatality-blog/0'])",
            self.output.getvalue())

    @inlineCallbacks
    def test_watch_new_service_unit_error(self):
        """
        An error while starting a new service is logged
        """
        # Inject an error into the service deployment
        mock_agent = self.mocker.patch(self.agent)
        mock_agent.start_service_unit("fatality-blog/0")
        self.mocker.result(fail(SyntaxError("Bad")))
        self.mocker.replay()

        yield self.agent.watch_service_units(None, set(["fatality-blog/0"]))
        self.assertIn("Starting service unit: %s" % "fatality-blog/0",
                      self.output.getvalue())
        self.assertIn("Error starting unit: %s" % "fatality-blog/0",
                      self.output.getvalue())
        self.assertIn("SyntaxError: Bad", self.output.getvalue())

    @inlineCallbacks
    def test_service_unit_removed(self):
        """
        Service unit removed with manual invocation of watch_service_units.
        """
        # Start the agent with watching enabled
        self.agent.set_watch_enabled(True)
        yield self.agent.startService()

        test_deferred = Deferred()

        mock_deployment = self.mocker.patch(self.agent.deploy_factory)
        mock_deployment.start("0", get_test_zookeeper_address(), MATCH_BUNDLE)
        self.mocker.result(succeed(True))
        mock_deployment.destroy()
        self.mocker.result(succeed(True))

        def test_complete():
            test_deferred.callback(True)

        self.mocker.call(test_complete)
        self.mocker.replay()

        # Create a new service unit
        self.service_unit = yield self.service.add_unit_state()
        yield self.service_unit.assign_to_machine(self.machine_state)

        # And now remove it
        yield self.service_unit.unassign_from_machine()
        yield test_deferred

    @inlineCallbacks
    def test_watch_removed_service_unit_error(self):
        """
        An error while removing a service unit is logged
        """
        mock_agent = self.mocker.patch(self.agent)
        mock_agent.kill_service_unit("fatality-blog/0")
        self.mocker.result(fail(OSError("Bad")))
        self.mocker.replay()

        yield self.agent.watch_service_units(set(["fatality-blog/0"]), set())
        self.assertIn("Stopping service unit: %s" % "fatality-blog/0",
                      self.output.getvalue())
        self.assertIn("Error stopping unit: %s" % "fatality-blog/0",
                      self.output.getvalue())
        self.assertIn("OSError: Bad", self.output.getvalue())
