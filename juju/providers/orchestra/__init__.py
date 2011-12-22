import logging

from twisted.internet.defer import fail, inlineCallbacks, returnValue, succeed

from juju.errors import ProviderError
from juju.providers.common.base import MachineProviderBase

from .cobbler import CobblerClient
from .files import FileStorage
from .launch import OrchestraLaunchMachine
from .machine import machine_from_dict, OrchestraMachine

log = logging.getLogger("juju.orchestra")


class MachineProvider(MachineProviderBase):
    """MachineProvider for use in an Orchestra environment"""

    def __init__(self, environment_name, config):
        super(MachineProvider, self).__init__(environment_name, config)
        self.cobbler = CobblerClient(config)
        self._storage = FileStorage(config)

    @property
    def provider_type(self):
        return "orchestra"

    def get_file_storage(self):
        """Return a WebDAV-backed FileStorage abstraction."""
        return self._storage

    def start_machine(self, machine_data, master=False):
        """Start an Orchestra machine.

        :param dict machine_data: desired characteristics of the new machine;
            it must include a "machine-id" key, and may include a "constraints"
            key (which is currently ignored by this provider).

        :param bool master: if True, machine will initialize the juju admin
            and run a provisioning agent, in addition to running a machine
            agent.
        """
        if "machine-id" not in machine_data:
            return fail(ProviderError(
                "Cannot launch a machine without specifying a machine-id"))
        machine_id = machine_data["machine-id"]
        return OrchestraLaunchMachine(self, master).run(machine_id)

    @inlineCallbacks
    def get_machines(self, instance_ids=()):
        """List machines running in the provider.

        :param list instance_ids: ids of instances you want to get. Leave empty
            to list every
            :class:`juju.providers.orchestra.machine.OrchestraMachine`
            owned by this provider.

        :return: a list of
            :class:`juju.providers.orchestra.machine.OrchestraMachine`
        :rtype: :class:`twisted.internet.defer.Deferred`

        :raises: :exc:`juju.errors.MachinesNotFound`
        """
        instances = yield self.cobbler.describe_systems(*instance_ids)
        returnValue([machine_from_dict(i) for i in instances])

    @inlineCallbacks
    def shutdown_machines(self, machines):
        """Terminate machines associated with this provider.

        :param machines: machines to shut down
        :type machines: list of
            :class:`juju.providers.orchestra.machine.OrchestraMachine`

        :return: list of terminated
            :class:`juju.providers.orchestra.machine.OrchestraMachine`
            instances
        :rtype: :class:`twisted.internet.defer.Deferred`
        """
        if not machines:
            returnValue([])

        for machine in machines:
            if not isinstance(machine, OrchestraMachine):
                raise ProviderError("Can only shut down OrchestraMachines; "
                                    "got a %r" % type(machine))

        ids = [m.instance_id for m in machines]
        killable_machines = yield self.get_machines(ids)
        for machine in killable_machines:
            yield self.cobbler.shutdown_system(machine.instance_id)
        returnValue(killable_machines)

    def open_port(self, machine, machine_id, port, protocol="tcp"):
        """Authorizes `port` using `protocol` for `machine`."""
        log.warn("Firewalling is not yet implemented in Orchestra")
        return succeed(None)

    def close_port(self, machine, machine_id, port, protocol="tcp"):
        """Revokes `port` using `protocol` for `machine`."""
        log.warn("Firewalling is not yet implemented in Orchestra")
        return succeed(None)

    def get_opened_ports(self, machine, machine_id):
        """Returns a set of open (port, protocol) pairs for `machine`."""
        log.warn("Firewalling is not yet implemented in Orchestra")
        return succeed(set())
