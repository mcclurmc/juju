import os
from itertools import tee

from twisted.internet.defer import inlineCallbacks, returnValue

from juju.environment.errors import EnvironmentsConfigError
from juju.state.errors import ServiceUnitStateMachineNotAssigned
from juju.state.machine import MachineStateManager
from juju.state.service import ServiceStateManager


def get_environment(options):
    environment = options.environments.get(options.environment)
    if environment is None and options.environment:
        raise EnvironmentsConfigError(
            "Invalid environment %r" % options.environment)
    elif environment is None:
        environment = options.environments.get_default()
    return environment


@inlineCallbacks
def get_ip_address_for_machine(client, provider, machine_id):
    """Returns public DNS name and machine state for the machine id.

    :param client: a connected zookeeper client.
    :param provider: the `MachineProvider` in charge of the juju.
    :param machine_id: machine ID of the desired machine to connect to.
    :return: tuple of the DNS name and a `MachineState`.
    """
    manager = MachineStateManager(client)
    machine_state = yield manager.get_machine_state(machine_id)
    instance_id = yield machine_state.get_instance_id()
    provider_machine = yield provider.get_machine(instance_id)
    returnValue((provider_machine.dns_name, machine_state))


@inlineCallbacks
def get_ip_address_for_unit(client, provider, unit_name):
    """Returns public DNS name and unit state for the service unit.

    :param client: a connected zookeeper client.
    :param provider: the `MachineProvider` in charge of the juju.
    :param unit_name: service unit running on a machine to connect to.
    :return: tuple of the DNS name and a `MachineState`.
    :raises: :class:`juju.state.errors.ServiceUnitStateMachineNotAssigned`
    """
    manager = ServiceStateManager(client)
    service_unit = yield manager.get_unit_state(unit_name)
    machine_id = yield service_unit.get_assigned_machine_id()
    if machine_id is None:
        raise ServiceUnitStateMachineNotAssigned(unit_name)
    returnValue(
        ((yield service_unit.get_public_address()), service_unit))


def expand_path(p):
    return os.path.abspath(os.path.expanduser(p))


class ParseError(Exception):
    """Used to support returning custom parse errors in passthrough parsing.

    Enables similar support to what is seen in argparse, without using its
    internals.
    """


def parse_passthrough_args(args, flags_taking_arg=()):
    """Scans left to right, partitioning flags and positional args.

    :param args: Unparsed args from argparse
    :param flags_taking_arg: One character flags that combine
       with arguments.
    :return: tuple of flags and positional args
    :raises: :class:`juju.control.utils.ParseError`

    TODO May need to support long options for other passthrough commands.
    """
    args = iter(args)
    flags_taking_arg = set(flags_taking_arg)
    flags = []
    positional = []
    while True:
        args, peek_args = tee(args)
        try:
            peeked = peek_args.next()
        except StopIteration:
            break
        if peeked.startswith("-"):
            flags.append(args.next())
            # Only need to consume the next arg if the flag both takes
            # an arg (say -L) and then it has an extra arg following
            # (8080:localhost:80), rather than being combined, such as
            # -L8080:localhost:80
            if len(peeked) == 2 and peeked[1] in flags_taking_arg:
                try:
                    flags.append(args.next())
                except StopIteration:
                    raise ParseError(
                        "argument -%s: expected one argument" % peeked[1])
        else:
            # At this point no more flags for the command itself (more
            # can follow after the first positional arg, as seen in
            # working with ssh, for example), so consume the rest and
            # stop parsing options
            positional = list(args)
            break
    return flags, positional
