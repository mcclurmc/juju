import logging

from twisted.internet.defer import inlineCallbacks, returnValue, succeed
from zookeeper import NoNodeException

from juju.environment.config import EnvironmentsConfig
from juju.errors import ProviderError
from juju.lib.twistutils import concurrent_execution_guard
from juju.state.errors import MachineStateNotFound, StateChanged, StopWatcher
from juju.state.firewall import FirewallManager
from juju.state.machine import MachineStateManager
from juju.state.service import ServiceStateManager

from .base import BaseAgent


log = logging.getLogger("juju.agents.provision")


class ProvisioningAgent(BaseAgent):

    name = "juju-provisoning-agent"

    _current_machines = ()

    # time in seconds
    machine_check_period = 60

    def get_agent_name(self):
        return "provision:%s" % (self.environment.type)

    @inlineCallbacks
    def start(self):
        self._running = True

        self.environment = yield self.configure_environment()
        self.provider = self.environment.get_machine_provider()
        self.machine_state_manager = MachineStateManager(self.client)
        self.service_state_manager = ServiceStateManager(self.client)
        self.firewall_manager = FirewallManager(
            self.client, self.is_running, self.provider)

        if self.get_watch_enabled():
            self.machine_state_manager.watch_machine_states(
                self.watch_machine_changes)
            self.service_state_manager.watch_service_states(
                self.firewall_manager.watch_service_changes)
            from twisted.internet import reactor
            reactor.callLater(
                self.machine_check_period, self.periodic_machine_check)
            log.info("Started provisioning agent")
        else:
            log.info("Started provisioning agent without watches enabled")

    def stop(self):
        log.info("Stopping provisioning agent")
        self._running = False
        return succeed(True)

    def is_running(self):
        """Whether this agent is running or not."""
        return self._running

    @inlineCallbacks
    def configure_environment(self):
        """The provisioning agent configure its environment on start or change.

        The environment contains the configuration th agent needs to interact
        with its machine provider, in order to do its work. This configuration
        data is deployed lazily over an encrypted connection upon first usage.

        The agent waits for this data to exist before completing its startup.
        """
        try:
            get_d, watch_d = self.client.get_and_watch("/environment")
            environment_data, stat = yield get_d
            watch_d.addCallback(self._on_environment_changed)
        except NoNodeException:
            # Wait till the environment node appears. play twisted gymnastics
            exists_d, watch_d = self.client.exists_and_watch("/environment")
            stat = yield exists_d
            if stat:
                environment = yield self.configure_environment()
            else:
                watch_d.addCallback(
                    lambda result: self.configure_environment())
            if not stat:
                environment = yield watch_d
            returnValue(environment)

        config = EnvironmentsConfig()
        config.parse(environment_data)
        returnValue(config.get_default())

    @inlineCallbacks
    def _on_environment_changed(self, event):
        """Reload the environment if its data changes."""

        if event.type_name == "deleted":
            return

        self.environment = yield self.configure_environment()
        self.provider = self.environment.get_machine_provider()

    def periodic_machine_check(self):
        """A periodic checking of machine states and provider machines.

        In addition to the on demand changes to zookeeper states that are
        monitored by L{watch_machine_changes}, the periodic machine check
        performs non zookeeper state related verification by periodically
        checking the last current provider machine states against the
        last known zookeeper state.

        Primarily this helps in recovering from transient error conditions
        which may have prevent processing of an individual machine state, as
        well as verifying the current state of the provider's running machines
        against the zk state, thus pruning unused resources.
        """
        from twisted.internet import reactor
        d = self.process_machines(self._current_machines)
        d.addBoth(
            lambda result: reactor.callLater(
                self.machine_check_period, self.periodic_machine_check))
        return d

    @inlineCallbacks
    def watch_machine_changes(self, old_machines, new_machines):
        """Watches and processes machine state changes.

        This function is used to subscribe to topology changes, and
        specifically changes to machines within the topology. It performs
        work against the machine provider to ensure that the currently
        running state of the juju cluster corresponds to the topology
        via creation and deletion of machines.

        The subscription utilized is a permanent one, meaning that this
        function will automatically be rescheduled to run whenever a topology
        state change happens that involves machines.

        This functional also caches the current set of machines as an agent
        instance attribute.

        @param old_machines machine ids as existed in the previous topology.
        @param new_machines machine ids as exist in the current topology.
        """
        if not self._running:
            raise StopWatcher()
        log.debug("Machines changed old:%s new:%s", old_machines, new_machines)
        self._current_machines = new_machines
        try:
            yield self.process_machines(self._current_machines)
        except Exception:
            # Log and effectively retry later in periodic_machine_check
            log.exception(
                "Got unexpected exception in processing machines,"
                " will retry")

    @concurrent_execution_guard("_processing_machines")
    @inlineCallbacks
    def process_machines(self, current_machines):
        """Ensure the currently running machines correspond to state.

        At the end of each process_machines execution, verify that all
        running machines within the provider correspond to machine_ids within
        the topology. If they don't then shut them down.

        Utilizes concurrent execution guard, to ensure that this is only being
        executed at most once per process.
        """
        # XXX this is obviously broken, but the margins of 80 columns prevent
        # me from describing. hint think concurrent agents, and use a lock.

        # map of instance_id -> machine
        try:
            provider_machines = yield self.provider.get_machines()
        except ProviderError:
            log.exception("Cannot get machine list")
            return

        provider_machines = dict(
            [(m.instance_id, m) for m in provider_machines])

        instance_ids = []
        for machine_state_id in current_machines:
            try:
                instance_id = yield self.process_machine(
                    machine_state_id, provider_machines)
            except (StateChanged,
                    MachineStateNotFound,
                    ProviderError):
                log.exception("Cannot process machine %s", machine_state_id)
                continue
            instance_ids.append(instance_id)

        # Terminate all unused juju machines running within the cluster.
        unused = set(provider_machines.keys()) - set(instance_ids)
        for instance_id in unused:
            log.info("Shutting down machine id:%s ...", instance_id)
            machine = provider_machines[instance_id]
            try:
                yield self.provider.shutdown_machine(machine)
            except ProviderError:
                log.exception("Cannot shutdown machine %s", instance_id)
                continue

    @inlineCallbacks
    def process_machine(self, machine_state_id, provider_machine_map):
        """Ensure a provider machine for a machine state id.

        For each machine_id in new machines which represents the current state
        of the topology:

          * Check to ensure its state reflects that it has been
            launched. If it hasn't then create the machine and update
            the state.

          * Watch the machine's assigned services so that changes can
            be applied to the firewall for service exposing support.
        """
        # fetch the machine state
        machine_state = yield self.machine_state_manager.get_machine_state(
            machine_state_id)
        instance_id = yield machine_state.get_instance_id()

        # Verify a machine id has state and is running, else launch it.
        if instance_id is None or not instance_id in provider_machine_map:
            log.info("Starting machine id:%s ...", machine_state.id)
            machines = yield self.provider.start_machine(
                {"machine-id": machine_state.id})
            instance_id = machines[0].instance_id
            yield machine_state.set_instance_id(instance_id)

        # The firewall manager also needs to be checked for any
        # outstanding retries on this machine
        yield self.firewall_manager.process_machine(machine_state)
        returnValue(instance_id)

if __name__ == '__main__':
    ProvisioningAgent().run()
