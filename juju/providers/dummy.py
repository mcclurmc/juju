import logging
import os
import tempfile

from twisted.internet.defer import inlineCallbacks, returnValue, succeed, fail

from txzookeeper import ZookeeperClient

from juju.errors import (
    EnvironmentNotFound, MachinesNotFound, ProviderError)


from juju.machine import ProviderMachine
from juju.state.placement import UNASSIGNED_POLICY
from juju.providers.common.files import FileStorage

log = logging.getLogger("juju.providers")


class DummyMachine(ProviderMachine):
    """Provider machine implementation specific to the dummy provider."""

    def __init__(self, *args, **kw):
        super(DummyMachine, self).__init__(*args, **kw)
        self._opened_ports = set()


class MachineProvider(object):

    def __init__(self, environment_name, config):
        self.environment_name = environment_name
        self.config = config
        self._machines = []
        self._state = None
        self._storage = None

    def get_placement_policy(self):
        """Get the unit placement policy for the provider.

        :param preference: A user specified plcaement policy preference
        """
        return self.config.get("placement", UNASSIGNED_POLICY)

    @property
    def provider_type(self):
        return "dummy"

    def connect(self, share=False):
        """Connect to the zookeeper juju running in the machine provider.

        @param share: Requests sharing of the connection with other clients
            attempting to connect to the same provider, if that's feasible.
            Unused for the dummy provider.
        """
        return ZookeeperClient(
            os.environ.get("ZOOKEEPER_ADDRESS", "127.0.0.1:2181"),
            session_timeout=1000).connect()

    def get_machines(self, instance_ids=()):
        """List all the machine running in the provider."""
        if not instance_ids:
            return succeed(self._machines[:])

        machines_by_id = dict(((m.instance_id, m) for m in self._machines))
        machines = []
        missing_instance_ids = []
        for instance_id in instance_ids:
            if instance_id in machines_by_id:
                machines.append(machines_by_id[instance_id])
            else:
                missing_instance_ids.append(instance_id)
        if missing_instance_ids:
            return fail(MachinesNotFound(missing_instance_ids))
        return succeed(machines)

    def start_machine(self, machine_data, master=False):
        """Start a machine in the provider."""
        if not "machine-id" in machine_data:
            return fail(ProviderError(
                "Machine state `machine-id` required in machine_data"))
        dns_name = machine_data.get("dns-name")
        machine = DummyMachine(len(self._machines), dns_name)
        self._machines.append(machine)
        return succeed([machine])

    def get_machine(self, instance_id):
        """Retrieve a machine by provider machine id.
        """
        for machine in self._machines:
            if instance_id == machine.instance_id:
                return succeed(machine)
        return fail(MachinesNotFound([instance_id]))

    def bootstrap(self):
        """
        Bootstrap juju on the machine provider.
        """
        if self._machines:
            return succeed(self._machines[:1])
        return self.start_machine({"machine-id": 0})

    @inlineCallbacks
    def shutdown_machines(self, machines):
        """
        Terminate any machine resources associated to the provider.
        """
        instance_ids = [m.instance_id for m in machines]
        machines = yield self.get_machines(instance_ids)
        for machine in machines:
            self._machines.remove(machine)
        returnValue(machines)

    def shutdown_machine(self, machine):
        """Terminate the given machine"""
        if not isinstance(machine, DummyMachine):
            return fail(ProviderError("Invalid machine for provider"))
        for m in self._machines:
            if m.instance_id == machine.instance_id:
                self._machines.remove(m)
                return m
        return fail(ProviderError("Machine not found %r" % machine))

    @inlineCallbacks
    def destroy_environment(self):
        yield self.save_state({})
        machines = yield self.get_machines()
        machines = yield self.shutdown_machines(machines)
        returnValue(machines)

    def save_state(self, state):
        """Save the state to the provider."""
        self._state = state
        return succeed(None)

    def load_state(self):
        """Load the state from the provider."""
        if self._state:
            state = self._state
        else:
            state = {}
        return succeed(state)

    def get_file_storage(self):
        """Retrieve the C{FileStorage} provider abstracion."""
        if self._storage:
            return self._storage
        storage_path = self.config.get("storage-directory")
        if storage_path is None:
            storage_path = tempfile.mkdtemp()
        self._storage = FileStorage(storage_path)
        return self._storage

    def get_serialization_data(self):
        config = self.config.copy()
        # Introduce an additional variable to simulate actual
        # providers which may serialize additional values
        # from the environment or other external sources.
        config["dynamicduck"] = "magic"
        return config

    def open_port(self, machine, machine_id, port, protocol="tcp"):
        """Dummy equivalent of ec2-authorize-group"""
        if not isinstance(machine, DummyMachine):
            return fail(ProviderError("Invalid machine for provider"))
        machine._opened_ports.add((port, protocol))
        log.debug("Opened %s/%s on provider machine %r",
                  port, protocol, machine.instance_id)
        return succeed(None)

    def close_port(self, machine, machine_id, port, protocol="tcp"):
        """Dummy equivalent of ec2-revoke-group"""
        if not isinstance(machine, DummyMachine):
            return fail(ProviderError("Invalid machine for provider"))
        try:
            machine._opened_ports.remove((port, protocol))
            log.debug("Closed %s/%s on provider machine %r",
                      port, protocol, machine.instance_id)
        except KeyError:
            pass
        return succeed(None)

    def get_opened_ports(self, machine, machine_id):
        """Dummy equivalent of ec2-describe-group

        This returns the current exposed ports in the environment for
        this machine. This directly goes against the provider. For
        EC2, this would be eventually consistent.
        """
        if not isinstance(machine, DummyMachine):
            return fail(ProviderError("Invalid machine for provider"))
        return succeed(machine._opened_ports)

    def get_zookeeper_machines(self):
        if self._machines:
            return succeed(self._machines[:1])
        return fail(EnvironmentNotFound("not bootstrapped"))
