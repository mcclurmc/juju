"""Implementation of terminate-machine subcommand"""

from twisted.internet.defer import inlineCallbacks

from juju.errors import CannotTerminateMachine
from juju.environment.errors import EnvironmentsConfigError
from juju.state.errors import MachineStateNotFound
from juju.state.machine import MachineStateManager


def configure_subparser(subparsers):
    """Configure terminate-machine subcommand"""
    sub_parser = subparsers.add_parser(
        "terminate-machine", help=command.__doc__)
    sub_parser.add_argument(
        "--environment", "-e",
        help="Environment to terminate machines.")
    sub_parser.add_argument(
        "machine_ids", metavar="ID", type=int, nargs="*",
        help="Machine IDs to terminate")
    return sub_parser


def command(options):
    """Terminate machines in an environment."""
    environment = options.environments.get(options.environment)
    if environment is None and options.environment:
        raise EnvironmentsConfigError(
            "Invalid environment %r" % options.environment)
    elif environment is None:
        environment = options.environments.get_default()
    return terminate_machine(
        options.environments,
        environment,
        options.verbose,
        options.log,
        options.machine_ids)


@inlineCallbacks
def terminate_machine(config, environment, verbose, log, machine_ids):
    """Terminates the machines in `machine_ids`.

    Like the underlying code in MachineStateManager, it's permissible
    if the machine ID is already terminated or even never running. If
    we determine this is not desired behavior, presumably propagate
    that back to the state manager.

    XXX However, we currently special case support of not terminating
    the "root" machine, that is the one running the provisioning
    agent. At some point, this will be managed like any other service,
    but until then it seems best to ensure it's not terminated at this
    level.
    """

    provider = environment.get_machine_provider()
    client = yield provider.connect()
    terminated_machine_ids = []
    try:
        machine_state_manager = MachineStateManager(client)
        for machine_id in machine_ids:
            if machine_id == 0:
                raise CannotTerminateMachine(
                    0, "environment would be destroyed")
            removed = yield machine_state_manager.remove_machine_state(
                machine_id)
            if not removed:
                raise MachineStateNotFound(machine_id)
            terminated_machine_ids.append(machine_id)
    finally:
        yield client.close()
        if terminated_machine_ids:
            log.info(
                "Machines terminated: %s",
                ", ".join(str(id) for id in terminated_machine_ids))
