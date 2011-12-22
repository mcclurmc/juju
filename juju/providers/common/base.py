import copy
from operator import itemgetter

from twisted.internet.defer import inlineCallbacks, returnValue

from juju.environment.errors import EnvironmentsConfigError
from juju.state.placement import UNASSIGNED_POLICY


from .bootstrap import Bootstrap
from .connect import ZookeeperConnect
from .findzookeepers import find_zookeepers
from .state import SaveState, LoadState
from .utils import get_user_authorized_keys


class MachineProviderBase(object):
    """Base class supplying common functionality for MachineProviders.

    To write a working subclass, you will need to override the following
    methods:

        * :meth:`get_file_storage`
        * :meth:`start_machine`
        * :meth:`get_machines`
        * :meth:`shutdown_machines`
        * :meth:`open_port`
        * :meth:`close_port`
        * :meth:`get_opened_ports`

    You may want to override the following methods, but you should be careful
    to call :class:`MachineProviderBase`'s implementation (or be very sure you
    don't need to:

        * :meth:`__init__`
        * :meth:`get_serialization_data`

    You probably shouldn't override anything else.
    """

    def __init__(self, environment_name, config):
        if ("authorized-keys-path" in config and
            "authorized-keys" in config):
            raise EnvironmentsConfigError(
                "Environment config cannot define both authorized-keys "
                "and authorized-keys-path. Pick one!")

        self.environment_name = environment_name
        self.config = config

    def get_placement_policy(self):
        """Get the unit placement policy for the provider.

        :param preference: A user specified plcaement policy preference
        """
        return self.config.get("placement", UNASSIGNED_POLICY)

    def get_serialization_data(self):
        """Get provider configuration suitable for serialization."""
        data = copy.deepcopy(self.config)
        data["authorized-keys"] = get_user_authorized_keys(data)
        # Not relevant, on a remote system.
        data.pop("authorized-keys-path", None)
        return data

    #================================================================
    # Subclasses need to implement their own versions of everything
    # in the following block

    def get_file_storage(self):
        """Retrieve the provider FileStorage abstraction."""
        raise NotImplementedError()

    def start_machine(self, machine_data, master=False):
        """Start a machine in the provider.

        :param dict machine_data: desired characteristics of the new machine;
            it must include a "machine-id" key, and may include a "constraints"
            key to specify the underlying OS and hardware (where available).

        :param bool master: if True, machine will initialize the juju admin
            and run a provisioning agent, in addition to running a machine
            agent.
        """
        raise NotImplementedError()

    def get_machines(self, instance_ids=()):
        """List machines running in the provider.

        :param list instance_ids: ids of instances you want to get. Leave empty
            to list every :class:`juju.machine.ProviderMachine` owned by
            this provider.

        :return: a list of :class:`juju.machine.ProviderMachine` instances
        :rtype: :class:`twisted.internet.defer.Deferred`

        :raises: :exc:`juju.errors.MachinesNotFound`
        """
        raise NotImplementedError()

    def shutdown_machines(self, machines):
        """Terminate machines associated with this provider.

        :param machines: machines to shut down
        :type machines: list of :class:`juju.machine.ProviderMachine`

        :return: list of terminated :class:`juju.machine.ProviderMachine`
            instances
        :rtype: :class:`twisted.internet.defer.Deferred`
        """
        raise NotImplementedError()

    def open_port(self, machine, machine_id, port, protocol="tcp"):
        """Authorizes `port` using `protocol` for `machine`."""
        raise NotImplementedError()

    def close_port(self, machine, machine_id, port, protocol="tcp"):
        """Revokes `port` using `protocol` for `machine`."""
        raise NotImplementedError()

    def get_opened_ports(self, machine, machine_id):
        """Returns a set of open (port, protocol) pairs for `machine`."""
        raise NotImplementedError()

    #================================================================
    # Subclasses will not generally need to override the methods in
    # this block

    def get_zookeeper_machines(self):
        """Find running zookeeper instances.

        :return: all running or starting machines configured to run zookeeper,
            as a list of :class:`juju.machine.ProviderMachine`
        :rtype: :class:`twisted.internet.defer.Deferred`

        :raises: :class:`juju.errors.EnvironmentNotFound`
        """
        return find_zookeepers(self)

    def connect(self, share=False):
        """Attempt to connect to a running zookeeper node.

        :param bool share: where feasible, attempt to share a connection with
            other clients

        :return: an open :class:`txzookeeper.client.ZookeeperClient`
        :rtype: :class:`twisted.internet.defer.Deferred`

        :raises: :exc:`juju.errors.EnvironmentNotFound` when no zookeepers
            exist
        """
        return ZookeeperConnect(self).run(share=share)

    def bootstrap(self):
        """Bootstrap an juju server in the provider."""
        return Bootstrap(self).run()

    def get_machine(self, instance_id):
        """Retrieve a provider machine by instance id.

        :param str instance_id: :attr:`instance_id` of the
            :class:`juju.machine.ProviderMachine` you want.

        :return: the requested :class:`juju.machine.ProviderMachine`
        :rtype: :class:`twisted.internet.defer.Deferred`

        :raises: :exc:`juju.errors.MachinesNotFound`
        """
        d = self.get_machines([instance_id])
        d.addCallback(itemgetter(0))
        return d

    def shutdown_machine(self, machine):
        """Terminate one machine associated with this provider.

        :param machine: :class:`juju.machine.ProviderMachine` to shut down.

        :return: the terminated :class:`juju.machine.ProviderMachine`.
        :rtype: :class:`twisted.internet.defer.Deferred`

        :raises: :exc:`juju.errors.MachinesNotFound`
        """
        d = self.shutdown_machines([machine])
        d.addCallback(itemgetter(0))
        return d

    @inlineCallbacks
    def destroy_environment(self):
        """Clear juju state and terminate all associated machines

        :rtype: :class:`twisted.internet.defer.Deferred`
        """
        try:
            yield self.save_state({})
        finally:
            live_machines = yield self.get_machines()
            killed_machines = yield self.shutdown_machines(live_machines)
            returnValue(killed_machines)

    def save_state(self, state):
        """Save state to the provider.

        :param dict state: state to persist

        :rtype: :class:`twisted.internet.defer.Deferred`
        """
        return SaveState(self).run(state)

    def load_state(self):
        """Load state from the provider.

        :return: the current state dict, or False
        :rtype: :class:`twisted.internet.defer.Deferred`
        """
        return LoadState(self).run()
