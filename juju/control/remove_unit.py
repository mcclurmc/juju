"""Implementation of remove unit subcommand"""

from twisted.internet.defer import inlineCallbacks

from juju.state.service import ServiceStateManager, parse_service_name
from juju.control.utils import get_environment


def configure_subparser(subparsers):
    """Configure remove-unit subcommand"""
    sub_parser = subparsers.add_parser("remove-unit", help=command.__doc__)
    sub_parser.add_argument(
        "--environment", "-e",
        help="juju environment to operate in.")
    sub_parser.add_argument(
        "unit_names", nargs="+", metavar="SERVICE_UNIT",
        help="Name of the service unit to remove.")
    return sub_parser


def command(options):
    """Remove a service unit."""
    environment = get_environment(options)
    return remove_unit(
        options.environments,
        environment,
        options.verbose,
        options.log,
        options.unit_names)


@inlineCallbacks
def remove_unit(config, environment, verbose, log, unit_names):
    provider = environment.get_machine_provider()
    client = yield provider.connect()
    try:
        service_manager = ServiceStateManager(client)
        for unit_name in unit_names:
            service_name = parse_service_name(unit_name)
            service_state = yield service_manager.get_service_state(
                service_name)
            unit_state = yield service_state.get_unit_state(unit_name)
            yield service_state.remove_unit_state(unit_state)
            log.info("Unit %r removed from service %r",
                     unit_state.unit_name, service_state.service_name)
    finally:
        yield client.close()
