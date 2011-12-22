import logging
import os

from twisted.internet.defer import inlineCallbacks

from juju.errors import JujuError
from juju.machine.unit import get_deploy_factory

from juju.unit.charm import download_charm

from juju.state.charm import CharmStateManager
from juju.state.environment import GlobalSettingsStateManager
from juju.state.machine import MachineStateManager
from juju.state.service import ServiceStateManager


from .base import BaseAgent


log = logging.getLogger("juju.agents.machine")


class MachineAgent(BaseAgent):
    """An juju Machine Agent.

    The machine agent is responsible for monitoring service units
    assigned to a machine. If a new unit is assigned to machine, the
    machine agent will download the charm, create a working
    space for the service unit agent, and then launch it.

    Additionally the machine agent will monitor the running service
    unit agents on the machine, via their ephemeral nodes, and
    restart them if they die.
    """

    name = "juju-machine-agent"
    unit_agent_module = "juju.agents.unit"

    @property
    def charms_directory(self):
        return os.path.join(self.config["juju_directory"], "charms")

    @property
    def units_directory(self):
        return os.path.join(self.config["juju_directory"], "units")

    @property
    def unit_state_directory(self):
        return os.path.join(self.config["juju_directory"], "state")

    @inlineCallbacks
    def start(self):
        """Start the machine agent.

        Creates state directories on the machine, retrieves the machine state,
        and enables watch on assigned units.
        """
        # Initialize directory paths.
        if not os.path.exists(self.charms_directory):
            os.makedirs(self.charms_directory)

        if not os.path.exists(self.units_directory):
            os.makedirs(self.units_directory)

        if not os.path.exists(self.unit_state_directory):
            os.makedirs(self.unit_state_directory)

        # Get state managers we'll be utilizing.
        self.service_state_manager = ServiceStateManager(self.client)
        self.charm_state_manager = CharmStateManager(self.client)

        # Retrieve the machine state for the machine we represent.
        machine_manager = MachineStateManager(self.client)
        self.machine_state = yield machine_manager.get_machine_state(
            self.get_machine_id())

        # Watch assigned units for the machine.
        if self.get_watch_enabled():
            self.machine_state.watch_assigned_units(
                self.watch_service_units)

        # Find out what provided the machine, and how to deploy units.
        settings = GlobalSettingsStateManager(self.client)
        self.provider_type = yield settings.get_provider_type()
        self.deploy_factory = get_deploy_factory(self.provider_type)

        # Connect the machine agent, broadcasting presence to the world.
        yield self.machine_state.connect_agent()
        log.info("Machine agent started id:%s deploy:%r provider:%r" % (
            self.get_machine_id(), self.deploy_factory, self.provider_type))

    def download_charm(self, charm_state):
        """Retrieve a charm from the provider storage to the local machine.

        Utilizes a local charm cache to avoid repeated downloading of the
        same charm.
        """
        log.debug("Downloading charm %s to %s",
                  charm_state.id, self.charms_directory)
        return download_charm(
            self.client, charm_state.id, self.charms_directory)

    @inlineCallbacks
    def watch_service_units(self, old_units, new_units):
        """Callback invoked when the assigned service units change.
        """
        if old_units is None:
            old_units = set()

        log.debug(
            "Units changed old:%s new:%s", old_units, new_units)

        stopped = old_units - new_units
        started = new_units - old_units

        for unit_name in stopped:
            log.debug("Stopping service unit: %s ...", unit_name)
            try:
                yield self.kill_service_unit(unit_name)
            except Exception:
                log.exception("Error stopping unit: %s", unit_name)

        for unit_name in started:
            log.debug("Starting service unit: %s ...", unit_name)
            try:
                yield self.start_service_unit(unit_name)
            except Exception:
                log.exception("Error starting unit: %s", unit_name)

    @inlineCallbacks
    def start_service_unit(self, service_unit_name):
        """Start a service unit on the machine.

        Downloads the charm, and extract it to the service unit directory,
        and launch the service unit agent within the unit directory.
        """

        # Retrieve the charm state to get at the charm.
        unit_state = yield self.service_state_manager.get_unit_state(
            service_unit_name)
        charm_id = yield unit_state.get_charm_id()
        charm_state = yield self.charm_state_manager.get_charm_state(
            charm_id)

        # Download the charm.
        bundle = yield self.download_charm(charm_state)

        # Use deployment to setup the workspace and start the unit agent.
        deployment = self.deploy_factory(
            service_unit_name, self.config["juju_directory"])

        running = yield deployment.is_running()
        if not running:
            log.debug("Starting service unit %s", service_unit_name)
            yield deployment.start(
                self.get_machine_id(), self.client.servers, bundle)
            log.info("Started service unit %s", service_unit_name)

    def kill_service_unit(self, service_unit_name):
        """Stop service unit and destroy disk state, ala SIGKILL or lxc-destroy
        """
        deployment = self.deploy_factory(
            service_unit_name, self.config["juju_directory"])
        log.info("Stopping service unit %s...", service_unit_name)
        return deployment.destroy()

    def get_machine_id(self):
        """Get the id of the machine as known within the zk state."""
        return self.config["machine_id"]

    def get_agent_name(self):
        return "Machine:%s" % (self.get_machine_id())

    def configure(self, options):
        super(MachineAgent, self).configure(options)
        if not options.get("machine_id"):
            msg = ("--machine-id must be provided in the command line, "
                   "or $JUJU_MACHINE_ID in the environment")
            raise JujuError(msg)

    @classmethod
    def setup_options(cls, parser):
        super(MachineAgent, cls).setup_options(parser)

        machine_id = os.environ.get("JUJU_MACHINE_ID", "")
        parser.add_argument(
            "--machine-id", default=machine_id)
        return parser

if __name__ == '__main__':
    MachineAgent().run()
