"""Various unit placement strategies for use in deploy and add-unit.

The API used by the `place_unit` method is:

machine_state = yield placement_strategy(zk_client,
                                         machine_state_manager,
                                         unit_state)

The placement strategy is passed the machine manager for the
deployment and the unit_state it is attempting to place. According to
its policy it should yield back the machine_state for where it placed
the unit.
"""

from twisted.internet.defer import inlineCallbacks, returnValue

from juju.state.errors import NoUnusedMachines
from juju.errors import InvalidPlacementPolicy, JujuError
from juju.state.machine import MachineStateManager

LOCAL_POLICY = "local"
UNASSIGNED_POLICY = "unassigned"


@inlineCallbacks
def _local_placement(client, machine_state_manager, unit_state):
    """Assigns unit to the machine/0, aka the bootstrap node.

    Primary use is intended for local development.
    """
    machine = yield machine_state_manager.get_machine_state(0)
    yield unit_state.assign_to_machine(machine)
    returnValue(machine)


@inlineCallbacks
def _unassigned_placement(client, machine_state_manager, unit_state):
    """Assigns unit on a machine without any units.

    If no such machine is found, a new machine is created, and the
    unit assigned to it.
    """
    try:
        machine_state = yield unit_state.assign_to_unused_machine()
    except NoUnusedMachines:
        machine_state = yield machine_state_manager.add_machine_state()
        yield unit_state.assign_to_unused_machine()

    returnValue(machine_state)


_PLACEMENT_LOOKUP = {
    LOCAL_POLICY: _local_placement,
    UNASSIGNED_POLICY: _unassigned_placement,
    }


def pick_policy(preference, provider):
    policies = provider.get_placement_policies()
    if not preference:
        return policies[0]
    if preference not in policies:
        raise InvalidPlacementPolicy(
            preference, provider.provider_type, policies)
    return preference


def place_unit(client, policy_name, unit_state):
    """Return machine state of unit_states assignment.

    :param client: A connected zookeeper client.
    :param policy_name: The name of the unit placement policy.
    :param unit_state: The unit to be assigned.
    :param provider_type: The type of the machine environment provider.
    """

    machine_state_manager = MachineStateManager(client)

    # default policy handling
    if policy_name is None:
        placement = _unassigned_placement
    else:
        placement = _PLACEMENT_LOOKUP.get(policy_name)
        if placement is None:
            # We should never get here, pick policy should always pick valid.
            raise JujuError("Invalid policy:%r for provider" % policy_name)

    return placement(client, machine_state_manager, unit_state)
