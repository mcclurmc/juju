import yaml

import zookeeper

from twisted.internet.defer import (
    inlineCallbacks, returnValue)

from txzookeeper.utils import retry_change


from juju.state.agent import AgentStateMixin
from juju.state.errors import (
    MachineStateNotFound, StateChanged, MachineStateInUse)
from juju.state.base import StateBase
from juju.state.utils import remove_tree


class MachineStateManager(StateBase):
    """Manages the state of machines in an environment."""

    @inlineCallbacks
    def add_machine_state(self):
        """Create a new machine state.

        @return: MachineState for the created machine.
        """
        path = yield self._client.create(
            "/machines/machine-",
            flags=zookeeper.SEQUENCE)
        _, internal_id = path.rsplit("/", 1)

        def add_machine(topology):
            topology.add_machine(internal_id)
        yield self._retry_topology_change(add_machine)

        returnValue(MachineState(self._client, internal_id))

    @inlineCallbacks
    def remove_machine_state(self, machine_id):
        """Remove machine state identified by `machine_id` if present.

        Returns True if machine state was actually removed.
        """
        internal_id = "machine-%010d" % machine_id
        must_delete = [False]

        def remove_machine(topology):
            # Removing a non-existing machine again won't fail, since
            # the end intention is preserved.  This makes dealing
            # with concurrency easier.
            if topology.has_machine(internal_id):
                if topology.machine_has_units(internal_id):
                    raise MachineStateInUse(machine_id)
                topology.remove_machine(internal_id)
                must_delete[0] = True
            else:
                must_delete[0] = False
        yield self._retry_topology_change(remove_machine)
        if must_delete[0]:
            # If the process is interrupted here, this node will stay
            # around, but it's not a big deal since it's not being
            # referenced by the topology anymore.
            yield remove_tree(self._client, "/machines/%s" % (internal_id,))
        returnValue(must_delete[0])

    @inlineCallbacks
    def get_machine_state(self, machine_id):
        """Return deferred machine state with the given id.

        @return MachineState with the given id.

        @raise MachineStateNotFound if the id is not found.
        """
        if isinstance(machine_id, str) and machine_id.isdigit():
            machine_id = int(machine_id)
        if isinstance(machine_id, int):
            internal_id = "machine-%010d" % machine_id
        else:
            raise MachineStateNotFound(machine_id)

        topology = yield self._read_topology()
        if not topology.has_machine(internal_id):
            raise MachineStateNotFound(machine_id)

        machine_state = MachineState(self._client, internal_id)
        returnValue(machine_state)

    @inlineCallbacks
    def get_all_machine_states(self):
        """Get information on all machines.

        @return: list of MachineState instances.
        """
        topology = yield self._read_topology()
        machines = []
        for machine_id in topology.get_machines():
            # topology yields internal ids -> map to public
            machine_state = MachineState(self._client, machine_id)
            machines.append(machine_state)

        returnValue(machines)

    def watch_machine_states(self, callback):
        """Observe changes in the known machines through the watch function.

        @param callback: A function/method which accepts two sets of machine
            ids: the old machines, and the new ones. The old machines set
            variable will be None the first time this function is called.

        Note that there are no guarantees that this function will be
        called once for *every* change in the topology, which means
        that multiple modifications may be observed as a single call.

        This method currently sets a perpetual watch (errors
        will make it bail out). To stop the watch cleanly raise an
        juju.state.errors.StopWatch exception.
        """

        def watch_topology(old_topology, new_topology):
            if old_topology is None:
                old_machines = None
            else:
                old_machines = set(_public_machine_id(x) for x in
                                   old_topology.get_machines())
            new_machines = set(_public_machine_id(x) for x in
                               new_topology.get_machines())
            if old_machines != new_machines:
                return callback(old_machines, new_machines)

        return self._watch_topology(watch_topology)


class MachineState(StateBase, AgentStateMixin):

    def __init__(self, client, internal_id):
        super(MachineState, self).__init__(client)
        self._internal_id = internal_id

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other):
        if not isinstance(other, MachineState):
            return False
        return self.id == other.id

    def __str__(self):
        return "<MachineState id:%s>" % self._internal_id

    @property
    def id(self):
        """High-level id built using the sequence as an int."""
        return _public_machine_id(self._internal_id)

    @property
    def internal_id(self):
        """Machine's internal id, of the form machine-NNNNNNNNNN."""
        return self._internal_id

    @property
    def _zk_path(self):
        """Return the path within zookeeper.

        This attribute should not be used outside of the .state
        package or for debugging.
        """
        return "/machines/" + self.internal_id

    def _get_agent_path(self):
        """Get the zookeeper path for the machine agent."""
        return "%s/agent" % self._zk_path

    def set_instance_id(self, instance_id):
        """Set the provider-specific machine id in this machine state."""

        def set_id(old_content, stat):
            if not stat:
                raise StateChanged("Machine got removed!")
            if old_content:
                data = yaml.load(old_content)
            else:
                data = {}
            data["provider-machine-id"] = instance_id
            return yaml.safe_dump(data)
        return retry_change(self._client, self._zk_path, set_id)

    @inlineCallbacks
    def get_instance_id(self):
        """Retrieve the provider-specific machine id for this machine."""
        try:
            content, stat = yield self._client.get(self._zk_path)
        except zookeeper.NoNodeException:
            raise StateChanged("Machine got removed!")
        data = yaml.load(content)
        if data:
            returnValue(data.get("provider-machine-id"))

    def watch_assigned_units(self, callback):
        """Observe changes in service units assigned to this machine.

        @param callback: A function/method which accepts two sets of unit
            names: the old assigned units, and the new ones. The old units
            set variable will be None the first time this function is called,
            and the new one will be None if the machine itself is ever
            deleted.

        Note that there are no guarantees that this function will be
        called once for *every* change in the topology, which means
        that multiple modifications may be observed as a single call.

        This method currently sets a perpetual watch (errors
        will make it bail out). To stop the watch cleanly raise an
        juju.state.errors.StopWatch exception.
        """
        return self._watch_topology(
            _WatchAssignedUnits(self._internal_id, callback))

    @inlineCallbacks
    def get_all_service_unit_states(self):
        # avoid circular imports by deferring the import until now
        from juju.state.service import ServiceUnitState

        topology = yield self._read_topology()
        service_unit_states = []
        for internal_service_unit_id in topology.get_service_units_in_machine(
                self.internal_id):
            internal_service_id = topology.get_service_unit_service(
                internal_service_unit_id)
            service_name = topology.get_service_name(internal_service_id)
            unit_sequence = topology.get_service_unit_sequence(
                internal_service_id, internal_service_unit_id)
            service_unit_state = ServiceUnitState(
                self._client, internal_service_id, service_name, unit_sequence,
                internal_service_unit_id)
            service_unit_states.append(service_unit_state)
        returnValue(service_unit_states)


class _WatchAssignedUnits(object):
    """Helper to implement MachineState.watch_assigned_units(). See above."""

    def __init__(self, internal_id, callback):
        self._internal_id = internal_id
        self._callback = callback
        self._old_units = None

    def __call__(self, old_topology, new_topology):
        if new_topology.has_machine(self._internal_id):
            unit_ids = new_topology.get_service_units_in_machine(
                self._internal_id)
            # Translate the internal ids to nice unit names.
            new_units = self._get_unit_names(new_topology, unit_ids)
        else:
            # Machine state is gone, so no units there of course. This can
            # only be visible in practice if the change happens fast
            # enough for the client to see the unassignment and removal as
            # a single change, since the topology enforces
            # unassignment-before-removal.
            new_units = set()
        if (new_units or self._old_units) and new_units != self._old_units:
            maybe_deferred = self._callback(self._old_units, new_units)
            self._old_units = new_units
            # The callback can return a deferred, to postpone its execution.
            # As a side effect, this watch won't fire again until the returned
            # deferred has not fired.
            return maybe_deferred

    def _get_unit_names(self, topology, internal_ids):
        """Translate internal ids to nice unit names."""
        unit_names = set()
        for internal_id in internal_ids:
            service_id = topology.get_service_unit_service(internal_id)
            unit_names.add(
                topology.get_service_unit_name(service_id, internal_id))
        return unit_names


def _public_machine_id(internal_id):
    """Convert an internal_id to an external one.

    That's an implementation detail, and shouldn't be used elsewhere.
    """
    _, sequence = internal_id.rsplit("-", 1)
    return int(sequence)
