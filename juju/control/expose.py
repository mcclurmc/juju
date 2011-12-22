"""Implementation of expose subcommand"""

from twisted.internet.defer import inlineCallbacks

from juju.control.utils import get_environment
from juju.state.service import ServiceStateManager


def configure_subparser(subparsers):
    """Configure expose subcommand"""
    sub_parser = subparsers.add_parser("expose", help=command.__doc__)
    sub_parser.add_argument(
        "--environment", "-e",
        help="juju environment to operate in.")
    sub_parser.add_argument(
        "service_name",
        help="Name of the service that should be exposed.")
    return sub_parser


def command(options):
    """Expose a service to the internet."""
    environment = get_environment(options)
    return expose(
        options.environments,
        environment,
        options.verbose,
        options.log,
        options.service_name)


@inlineCallbacks
def expose(
    config, environment, verbose, log, service_name):
    """Expose a service."""
    provider = environment.get_machine_provider()
    client = yield provider.connect()
    try:
        service_manager = ServiceStateManager(client)
        service_state = yield service_manager.get_service_state(service_name)
        already_exposed = yield service_state.get_exposed_flag()
        if not already_exposed:
            yield service_state.set_exposed_flag()
            log.info("Service %r was exposed.", service_name)
        else:
            log.info("Service %r was already exposed.", service_name)
    finally:
        yield client.close()
