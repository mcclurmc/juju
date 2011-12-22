from twisted.internet.defer import inlineCallbacks, returnValue

from juju.errors import EnvironmentNotFound, MachinesNotFound


def _require(x):
    if not x:
        raise EnvironmentNotFound("is the environment bootstrapped?")


@inlineCallbacks
def find_zookeepers(provider):
    """Find running zookeeper instances.

    :param provider: the MachineProvider in charge of the juju

    :return: all running or starting machines configured to run zookeeper, as a
        list of :class:`juju.machine.ProviderMachine`
    :rtype: :class:`twisted.internet.defer.Deferred`

    :raises: :class:`juju.errors.EnvironmentNotFound`
    """
    state = yield provider.load_state()
    _require(state)
    instance_ids = state.get("zookeeper-instances")
    _require(instance_ids)

    machines = []
    missing_instance_ids = []
    for instance_id in instance_ids:
        try:
            machine = yield provider.get_machine(instance_id)
            machines.append(machine)
        except MachinesNotFound as e:
            missing_instance_ids.extend(e.instance_ids)

    if machines:
        returnValue(machines)
    raise EnvironmentNotFound("machines are not running (%s)"
                              % ", ".join(missing_instance_ids))
