import os
import logging
import shutil
import tempfile

from twisted.internet.defer import inlineCallbacks, returnValue

from juju.errors import JujuError
from juju.state.service import ServiceStateManager, RETRY_HOOKS
from juju.hooks.protocol import UnitSettingsFactory
from juju.hooks.executor import HookExecutor

from juju.unit.address import get_unit_address
from juju.unit.lifecycle import UnitLifecycle, HOOK_SOCKET_FILE
from juju.unit.workflow import UnitWorkflowState

from juju.unit.charm import download_charm

from juju.agents.base import BaseAgent


log = logging.getLogger("juju.agents.unit")


class UnitAgent(BaseAgent):
    """An juju Unit Agent.

    Provides for the management of a charm, via hook execution in response to
    external events in the coordination space (zookeeper).
    """
    name = "juju-unit-agent"

    @classmethod
    def setup_options(cls, parser):
        super(UnitAgent, cls).setup_options(parser)
        unit_name = os.environ.get("JUJU_UNIT_NAME", "")
        parser.add_argument("--unit-name", default=unit_name)

    @property
    def unit_name(self):
        return self.config["unit_name"]

    def get_agent_name(self):
        return "unit:%s" % self.unit_name

    def configure(self, options):
        """Configure the unit agent."""
        super(UnitAgent, self).configure(options)
        if not options.get("unit_name"):
            msg = ("--unit-name must be provided in the command line, "
                   "or $JUJU_UNIT_NAME in the environment")
            raise JujuError(msg)
        self.executor = HookExecutor()

        self.api_factory = UnitSettingsFactory(
            self.executor.get_hook_context,
            logging.getLogger("unit.hook.api"))
        self.api_socket = None
        self.workflow = None

    @inlineCallbacks
    def start(self):
        """Start the unit agent process."""
        self.service_state_manager = ServiceStateManager(self.client)
        self.executor.start()

        # Retrieve our unit and configure working directories.
        service_name = self.unit_name.split("/")[0]
        service_state = yield self.service_state_manager.get_service_state(
            service_name)

        self.unit_state = yield service_state.get_unit_state(
            self.unit_name)
        self.unit_directory = os.path.join(
            self.config["juju_directory"],
            "units",
            self.unit_state.unit_name.replace("/", "-"))

        self.state_directory = os.path.join(
            self.config["juju_directory"], "state")

        # Setup the server portion of the cli api exposed to hooks.
        from twisted.internet import reactor
        self.api_socket = reactor.listenUNIX(
            os.path.join(self.unit_directory, HOOK_SOCKET_FILE),
            self.api_factory)

        # Setup the unit state's address
        address = yield get_unit_address(self.client)
        yield self.unit_state.set_public_address(
            (yield address.get_public_address()))
        yield self.unit_state.set_private_address(
            (yield address.get_private_address()))

        # Inform the system, we're alive.
        yield self.unit_state.connect_agent()

        self.lifecycle = UnitLifecycle(
            self.client,
            self.unit_state,
            service_state,
            self.unit_directory,
            self.executor)

        self.workflow = UnitWorkflowState(
            self.client,
            self.unit_state,
            self.lifecycle,
            self.state_directory)

        if self.get_watch_enabled():
            yield self.unit_state.watch_resolved(self.cb_watch_resolved)
            yield self.unit_state.watch_hook_debug(self.cb_watch_hook_debug)
            yield service_state.watch_config_state(
                self.cb_watch_config_changed)

        # Fire initial transitions, only if successful
        if (yield self.workflow.transition_state("installed")):
            yield self.workflow.transition_state("started")

        # Upgrade can only be processed if we're in a running state so
        # for case of a newly started unit, do it after the unit is started.
        if self.get_watch_enabled():
            yield self.unit_state.watch_upgrade_flag(
                self.cb_watch_upgrade_flag)

    @inlineCallbacks
    def stop(self):
        """Stop the unit agent process."""
        if self.workflow:
            yield self.workflow.transition_state("stopped")
        if self.api_socket:
            yield self.api_socket.stopListening()
        yield self.api_factory.stopFactory()

    @inlineCallbacks
    def cb_watch_resolved(self, change):
        """Update the unit's state, when its resolved.

        Resolved operations form the basis of error recovery for unit
        workflows. A resolved operation can optionally specify hook
        execution. The unit agent runs the error recovery transition
        if the unit is not in a running state.
        """
        # Would be nice if we could fold this into an atomic
        # get and delete primitive.
        # Check resolved setting
        resolved = yield self.unit_state.get_resolved()
        if resolved is None:
            returnValue(None)

        # Clear out the setting
        yield self.unit_state.clear_resolved()

        # Verify its not already running
        if (yield self.workflow.get_state()) == "started":
            returnValue(None)

        log.info("Resolved detected, firing retry transition")

        # Fire a resolved transition
        try:
            if resolved["retry"] == RETRY_HOOKS:
                yield self.workflow.fire_transition_alias("retry_hook")
            else:
                yield self.workflow.fire_transition_alias("retry")
        except Exception:
            log.exception("Unknown error while transitioning for resolved")

    @inlineCallbacks
    def cb_watch_hook_debug(self, change):
        """Update the hooks to be debugged when the settings change.
        """
        debug = yield self.unit_state.get_hook_debug()
        debug_hooks = debug and debug.get("debug_hooks") or None
        self.executor.set_debug(debug_hooks)

    @inlineCallbacks
    def cb_watch_upgrade_flag(self, change):
        """Update the unit's charm when requested.
        """
        upgrade_flag = yield self.unit_state.get_upgrade_flag()
        if upgrade_flag:
            log.info("Upgrade detected, starting upgrade")
            upgrade = CharmUpgradeOperation(self)
            try:
                yield upgrade.run()
            except Exception:
                log.exception("Error while upgrading")

    @inlineCallbacks
    def cb_watch_config_changed(self, change):
        """Trigger hook on configuration change"""
        # Verify it is running
        current_state = yield self.workflow.get_state()
        log.debug("Configuration Changed")

        if  current_state != "started":
            log.debug(
                "Configuration updated on service in a non-started state")
            returnValue(None)

        yield self.workflow.fire_transition("reconfigure")


class CharmUpgradeOperation(object):
    """A unit agent charm upgrade operation."""

    def __init__(self, agent):
        self._agent = agent
        self._log = logging.getLogger("unit.upgrade")
        self._charm_directory = tempfile.mkdtemp(
            suffix="charm-upgrade", prefix="tmp")

    def retrieve_charm(self, charm_id):
        return download_charm(
            self._agent.client, charm_id, self._charm_directory)

    def _remove_tree(self, result):
        if os.path.exists(self._charm_directory):
            shutil.rmtree(self._charm_directory)
        return result

    def run(self):
        d = self._run()
        d.addBoth(self._remove_tree)
        return d

    @inlineCallbacks
    def _run(self):
        self._log.info("Starting charm upgrade...")

        # Verify the workflow state
        workflow_state = yield self._agent.workflow.get_state()
        if not workflow_state in ("started",):
            self._log.warning(
                "Unit not in an upgradeable state: %s", workflow_state)
            # Upgrades can only be supported while the unit is
            # running, we clear the flag because we don't support
            # persistent upgrade requests across unit starts. The
            # upgrade request will need to be reissued, after
            # resolving or restarting the unit.
            yield self._agent.unit_state.clear_upgrade_flag()
            returnValue(False)

        # Get, check, and clear the flag. Do it first so a second upgrade
        # will restablish the upgrade request.
        upgrade_flag = yield self._agent.unit_state.get_upgrade_flag()
        if not upgrade_flag:
            self._log.warning("No upgrade flag set.")
            returnValue(False)

        self._log.debug("Clearing upgrade flag.")
        yield self._agent.unit_state.clear_upgrade_flag()

        # Retrieve the service state
        service_state_manager = ServiceStateManager(self._agent.client)
        service_state = yield service_state_manager.get_service_state(
            self._agent.unit_name.split("/")[0])

        # Verify unit state, upgrade flag, and newer version requested.
        service_charm_id = yield service_state.get_charm_id()
        unit_charm_id = yield self._agent.unit_state.get_charm_id()

        if service_charm_id == unit_charm_id:
            self._log.debug("Unit already running latest charm")
            yield self._agent.unit_state.clear_upgrade_flag()
            returnValue(True)

        # Retrieve charm
        self._log.debug("Retrieving charm %s", service_charm_id)
        charm = yield self.retrieve_charm(service_charm_id)

        # Stop hook executions
        self._log.debug("Stopping hook execution.")
        yield self._agent.executor.stop()

        # Note the current charm version
        self._log.debug("Setting unit charm id to %s", service_charm_id)
        yield self._agent.unit_state.set_charm_id(service_charm_id)

        # Extract charm
        self._log.debug("Extracting new charm.")
        charm.extract_to(
            os.path.join(self._agent.unit_directory, "charm"))

        # Upgrade
        self._log.debug("Invoking upgrade transition.")

        success = yield self._agent.workflow.fire_transition(
            "upgrade_charm")

        if success:
            self._log.debug("Unit upgraded.")
        else:
            self._log.warning("Upgrade failed.")

        returnValue(success)

if __name__ == '__main__':
    UnitAgent.run()
