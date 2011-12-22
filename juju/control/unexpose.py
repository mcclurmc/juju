"""Implementation of unexpose subcommand"""

from twisted.internet.defer import inlineCallbacks

from juju.control.utils import get_environment
from juju.state.service import ServiceStateManager


def configure_subparser(subparsers):
    """Configure unexpose subcommand"""
    sub_parser = subparsers.add_parser("unexpose", help=command.__doc__)
    sub_parser.add_argument(
        "--environment", "-e",
        help="juju environment to operate in.")
    sub_parser.add_argument(
        "service_name",
        help="Name of the service that should be unexposed.")
    return sub_parser


def command(options):
    """Remove internet access to a service."""
    environment = get_environment(options)
    return unexpose(
        options.environments,
        environment,
        options.verbose,
        options.log,
        options.service_name)


@inlineCallbacks
def unexpose(
    config, environment, verbose, log, service_name):
    """Unexpose a service."""
    provider = environment.get_machine_provider()
    client = yield provider.connect()
    try:
        service_manager = ServiceStateManager(client)
        service_state = yield service_manager.get_service_state(service_name)
        already_exposed = yield service_state.get_exposed_flag()
        if already_exposed:
            yield service_state.clear_exposed_flag()
            log.info("Service %r was unexposed.", service_name)
        else:
            log.info("Service %r was not exposed.", service_name)
    finally:
        yield client.close()
