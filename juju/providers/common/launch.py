from twisted.internet.defer import inlineCallbacks, returnValue

from .cloudinit import CloudInit
from .utils import get_user_authorized_keys


class LaunchMachine(object):
    """Abstract class with generic instance-launching logic.

    To create your own subclass, you will certainly need to override
    :meth:`start_machine`, which will very probably want to use the incomplete
    :class:`juju.providers.common.cloudinit.CloudInit` returned by
    :meth:`_create_cloud_init`.

    :param provider: the `MachineProvider` that will administer the machine

    :param bool master: if True, the machine will run a zookeeper and a
        provisioning agent, in addition to the machine agent that every
        machine runs automatically.

    :param dict constraints: specifies the underlying OS and hardware (where
        available)

    .. automethod:: _create_cloud_init
    """

    def __init__(self, provider, master=False, constraints=None):
        self._provider = provider
        self._master = master
        self._constraints = constraints or {}

    @inlineCallbacks
    def run(self, machine_id):
        """Launch an instance node within the machine provider environment.

        :param str machine_id: the juju machine ID to assign
        """
        # XXX at some point, we'll want to start multiple zookeepers
        # that know about each other; for now, this'll do
        if self._master:
            zookeepers = []
        else:
            zookeepers = yield self._provider.get_zookeeper_machines()

        machines = yield self.start_machine(machine_id, zookeepers)
        if self._master:
            yield self._on_new_zookeepers(machines)
        returnValue(machines)

    def start_machine(self, machine_id, zookeepers):
        """Actually launch a machine for the appropriate provider.

        :param str machine_id: the juju machine ID to assign

        :param zookeepers: the machines currently running zookeeper, to which
            the new machine will need to connect
        :type zookeepers: list of :class:`juju.machine.ProviderMachine`

        :return: a singe-entry list containing a provider-specific
            :class:`juju.machine.ProviderMachine` representing the newly-
            launched machine
        :rtype: :class:`twisted.internet.defer.Deferred`
        """
        raise NotImplementedError()

    def _create_cloud_init(self, machine_id, zookeepers):
        """Construct a provider-independent but incomplete :class:`CloudInit`.

        :return: a :class:`juju.providers.common.cloudinit.cloudInit`; it
            will not be ready to render, but will be configured to the greatest
            extent possible with the available information.
        :rtype: :class:`twisted.internet.defer.Deferred`
        """
        config = self._provider.config
        cloud_init = CloudInit()
        cloud_init.add_ssh_key(get_user_authorized_keys(config))
        cloud_init.set_machine_id(machine_id)
        cloud_init.set_zookeeper_machines(zookeepers)
        origin = config.get("juju-origin")
        if origin:
            if origin.startswith("lp:"):
                cloud_init.set_juju_source(branch=origin)
            elif origin == "ppa":
                cloud_init.set_juju_source(ppa=True)
            else:
                # Ignore other values, just use the distro for sanity
                cloud_init.set_juju_source(distro=True)
        if self._master:
            cloud_init.enable_bootstrap()
            cloud_init.set_zookeeper_secret(config["admin-secret"])
        return cloud_init

    def _on_new_zookeepers(self, machines):
        instance_ids = [m.instance_id for m in machines]
        return self._provider.save_state({"zookeeper-instances": instance_ids})
