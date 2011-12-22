"""Implementation of add unit subcommand"""

from twisted.internet.defer import inlineCallbacks

from juju.control.utils import get_environment
from juju.state.placement import place_unit
from juju.state.service import ServiceStateManager


def configure_subparser(subparsers):
    """Configure add-unit subcommand"""
    sub_parser = subparsers.add_parser("add-unit", help=command.__doc__)
    sub_parser.add_argument(
        "--environment", "-e",
        help="juju environment to operate in.")
    sub_parser.add_argument(
        "--num-units", "-n", default=1, type=int, metavar="NUM",
        help="Number of service units to add.")
    sub_parser.add_argument(
        "service_name",
        help="Name of the service a unit should be added for")

    return sub_parser


def command(options):
    """Add a new service unit."""
    environment = get_environment(options)
    return add_unit(
        options.environments,
        environment,
        options.verbose,
        options.log,
        options.service_name,
        options.num_units)


@inlineCallbacks
def add_unit(config, environment, verbose, log, service_name, num_units):
    """Add a unit of a service to the environment.
    """
    provider = environment.get_machine_provider()
    placement_policy = provider.get_placement_policy()
    client = yield provider.connect()

    try:
        service_manager = ServiceStateManager(client)
        service_state = yield service_manager.get_service_state(service_name)
        for i in range(num_units):
            unit_state = yield service_state.add_unit_state()
            yield place_unit(client, placement_policy, unit_state)
            log.info("Unit %r added to service %r",
                     unit_state.unit_name, service_state.service_name)
    finally:
        yield client.close()
