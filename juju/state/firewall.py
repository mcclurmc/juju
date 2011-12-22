import logging

from twisted.internet.defer import inlineCallbacks

from juju.errors import MachinesNotFound
from juju.state.errors import (
    ServiceStateNotFound, ServiceUnitStateNotFound, StateChanged,
    StopWatcher)
from juju.state.machine import MachineStateManager
from juju.state.service import ServiceStateManager


log = logging.getLogger("juju.state.expose")

NotExposed = object()


class FirewallManager(object):
    """Manages the opening and closing of ports in the firewall.
    """

    def __init__(self, client, is_running, provider):
        """Initialize a Firewall Manager.

        :param client: A connected zookeeper client.
        :param is_running: A function (usually a bound method) that
            returns whether the associated agent is still running or
            not.
        :param provider: A machine provider, used for making the
            actual changes in the environment to firewall settings.
        """
        self.machine_state_manager = MachineStateManager(client)
        self.service_state_manager = ServiceStateManager(client)
        self.is_running = is_running
        self.provider = provider

        # Track all currently watched machines, using machine ID.
        self._watched_machines = set()

        # Map service name to either NotExposed or set of exposed unit names.
        # If a service name is present in the dictionary, it means its
        # respective expose node is being watched.
        self._watched_services = {}

        # Machines to retry open_close_ports because of earlier errors
        self._retry_machines_on_port_error = set()

        # Registration of observers for corresponding actions
        self._open_close_ports_observers = set()
        self._open_close_ports_on_machine_observers = set()

    @inlineCallbacks
    def process_machine(self, machine_state):
        """Ensures watch is setup per machine and performs any necessary retry.

        :param machine_state: The machine state of the machine to be checked.

        The watch that is established, via
        :method:`juju.state.machine.MachineState.watch_assigned_units`,
        handles the scenario where a service or service unit is
        removed from the topology. Because the service unit is no
        longer in the topology, the corresponding watch terminates and
        is unable to `open_close_ports` in response to the
        change. However, the specific machine watch will be called in
        this case, and that suffices to determine that its port policy
        should be checked.

        In addition, this method can rely on the fact that the
        provisioning agent periodically rechecks machines so as to
        support retries of security group operations that failed for
        that provider. This method is called by the corresponding
        :method:`juju.agents.provision.ProvisioningAgent.process_machine`
        in the provisioning agent.
        """
        if machine_state.id in self._retry_machines_on_port_error:
            self._retry_machines_on_port_error.remove(machine_state.id)
            try:
                yield self.open_close_ports_on_machine(machine_state.id)
            except StopWatcher:
                # open_close_ports_on_machine can also be called from
                # a watch, so simply ignore this since it's just used
                # to shutdown a watch in the case of agent shutdown
                pass

        def cb_watch_assigned_units(old_units, new_units):
            """Watch assigned units for changes possibly require port mgmt.

            """
            log.debug("Assigned units for machine %r: old=%r, new=%r",
                      machine_state.id, old_units, new_units)
            return self.open_close_ports_on_machine(machine_state.id)

        if machine_state.id not in self._watched_machines:
            self._watched_machines.add(machine_state.id)
            yield machine_state.watch_assigned_units(cb_watch_assigned_units)

    @inlineCallbacks
    def watch_service_changes(self, old_services, new_services):
        """Manage watching service exposed status.

        This method is called upon every change to the set of services
        currently deployed. All services are then watched for changes
        to their exposed flag setting.

        :param old_services: the set of services before this change.
        :param new_services: the current set of services.
        """
        removed_services = old_services - new_services
        for service_name in removed_services:
            self._watched_services.pop(service_name, None)
        for service_name in new_services:
            yield self._setup_new_service_watch(service_name)

    @inlineCallbacks
    def _setup_new_service_watch(self, service_name):
        """Sets up the watching of the exposed flag for a new service.

        If `service_name` is not watched (as known by
        `self._watched_services`), adds the watch and a corresponding
        entry in self._watched_services.

        (This dict is necessary because there is currently no way to
        introspect a service for whether it is watched or not.)
        """
        if service_name in self._watched_services:
            return  # already watched
        self._watched_services[service_name] = NotExposed
        try:
            service_state = yield self.service_state_manager.get_service_state(
                service_name)
        except ServiceStateNotFound:
            log.debug("Cannot setup watch, since service %r no longer exists",
                      service_name)
            self._watched_services.pop(service_name, None)
            return

        @inlineCallbacks
        def cb_watch_service_exposed_flag(exposed):
            if not self.is_running():
                raise StopWatcher()

            if exposed:
                log.debug("Service %r is exposed", service_name)
            else:
                log.debug("Service %r is unexposed", service_name)

            try:
                unit_states = yield service_state.get_all_unit_states()
            except StateChanged:
                log.debug("Stopping watch on %r, no longer in topology",
                          service_name)
                raise StopWatcher()
            for unit_state in unit_states:
                yield self.open_close_ports(unit_state)

            if not exposed:
                log.debug("Service %r is unexposed", service_name)
                self._watched_services[service_name] = NotExposed
            else:
                log.debug("Service %r is exposed", service_name)
                self._watched_services[service_name] = set()
                yield self._setup_service_unit_watch(service_state)

        yield service_state.watch_exposed_flag(cb_watch_service_exposed_flag)
        log.debug("Started watch of %r on changes to being exposed",
                  service_name)

    @inlineCallbacks
    def _setup_service_unit_watch(self, service_state):
        """Setup watches on service units of newly exposed `service_name`."""
        @inlineCallbacks
        def cb_check_service_units(old_service_units, new_service_units):
            watched_units = self._watched_services.get(
                service_state.service_name, NotExposed)
            if not self.is_running() or watched_units is NotExposed:
                raise StopWatcher()

            removed_service_units = old_service_units - new_service_units
            for unit_name in removed_service_units:
                watched_units.discard(unit_name)
                if not self.is_running():
                    raise StopWatcher()
                try:
                    unit_state = yield service_state.get_unit_state(unit_name)
                except (ServiceUnitStateNotFound, StateChanged):
                    log.debug("Not setting up watch on %r, not in topology",
                              unit_name)
                    continue
                yield self.open_close_ports(unit_state)

            for unit_name in new_service_units:
                if unit_name not in watched_units:
                    watched_units.add(unit_name)
                    yield self._setup_watch_ports(service_state, unit_name)

        yield service_state.watch_service_unit_states(cb_check_service_units)
        log.debug("Started watch of service units for exposed service %r",
                 service_state.service_name)

    @inlineCallbacks
    def _setup_watch_ports(self, service_state, unit_name):
        """Setup the watching of ports for `unit_name`."""
        try:
            unit_state = yield service_state.get_unit_state(unit_name)
        except (ServiceUnitStateNotFound, StateChanged):
            log.debug("Cannot setup watch on %r (no longer exists), ignoring",
                      unit_name)
            return

        @inlineCallbacks
        def cb_watch_ports(value):
            """Permanently watch ports until service is no longer exposed."""
            watched_units = self._watched_services.get(
                service_state.service_name, NotExposed)
            if (not self.is_running() or watched_units is NotExposed or
                unit_name not in watched_units):
                log.debug("Stopping ports watch for %r", unit_name)
                raise StopWatcher()
            yield self.open_close_ports(unit_state)

        yield unit_state.watch_ports(cb_watch_ports)
        log.debug("Started watch of %r on changes to open ports", unit_name)

    def add_open_close_ports_observer(self, observer):
        """Set `observer` for calls to `open_close_ports`.

        :param observer: The callback is called with the corresponding
            :class:`juju.state.service.UnitState`.
        """
        self._open_close_ports_observers.add(observer)

    @inlineCallbacks
    def open_close_ports(self, unit_state):
        """Called upon changes that *may* open/close ports for a service unit.
        """
        if not self.is_running():
            raise StopWatcher()
        try:
            try:
                machine_id = yield unit_state.get_assigned_machine_id()
            except StateChanged:
                log.debug("Stopping watch, machine %r no longer in topology",
                          unit_state.unit_name)
                raise StopWatcher()
            if machine_id is not None:
                yield self.open_close_ports_on_machine(machine_id)
        finally:
            # Ensure that the observations runs after the
            # corresponding action completes.  In particular, tests
            # that use observation depend on this ordering to ensure
            # that the action has in fact happened before they can
            # proceed.
            observers = list(self._open_close_ports_observers)
            for observer in observers:
                yield observer(unit_state)

    def add_open_close_ports_on_machine_observer(self, observer):
        """Add `observer` for calls to `open_close_ports`.

        :param observer: A callback receives the machine id for each call.
        """
        self._open_close_ports_on_machine_observers.add(observer)

    @inlineCallbacks
    def open_close_ports_on_machine(self, machine_id):
        """Called upon changes that *may* open/close ports for a machine.

        :param machine_id: The machine ID of the machine that needs to
             be checked.

        This machine supports multiple service units being assigned to a
        machine; all service units are checked each time this is
        called to determine the active set of ports to be opened.
        """
        if not self.is_running():
            raise StopWatcher()
        try:
            machine_state = yield self.machine_state_manager.get_machine_state(
                machine_id)
            instance_id = yield machine_state.get_instance_id()
            machine = yield self.provider.get_machine(instance_id)
            unit_states = yield machine_state.get_all_service_unit_states()
            policy_ports = set()
            for unit_state in unit_states:
                service_state = yield self.service_state_manager.\
                    get_service_state(unit_state.service_name)
                exposed = yield service_state.get_exposed_flag()
                if exposed:
                    ports = yield unit_state.get_open_ports()
                    for port in ports:
                        policy_ports.add(
                            (port["port"], port["proto"]))
            current_ports = yield self.provider.get_opened_ports(
                machine, machine_id)
            to_open = policy_ports - current_ports
            to_close = current_ports - policy_ports
            for port, proto in to_open:
                yield self.provider.open_port(machine, machine_id, port, proto)
            for port, proto in to_close:
                yield self.provider.close_port(
                    machine, machine_id, port, proto)
        except MachinesNotFound:
            log.info("No provisioned machine for machine %r", machine_id)
        except Exception:
            log.exception("Got exception in opening/closing ports, will retry")
            self._retry_machines_on_port_error.add(machine_id)
        finally:
            # Ensure that the observation runs after the corresponding
            # action completes.  In particular, tests that use
            # observation depend on this ordering to ensure that this
            # action has happened before they can proceed.
            observers = list(self._open_close_ports_on_machine_observers)
            for observer in observers:
                yield observer(machine_id)
