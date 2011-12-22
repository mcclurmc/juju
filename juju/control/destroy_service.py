"""Implementation of destroy service subcommand"""

from twisted.internet.defer import inlineCallbacks

from juju.state.service import ServiceStateManager
from juju.control.utils import get_environment


def configure_subparser(subparsers):
    """Configure destroy-service subcommand"""
    sub_parser = subparsers.add_parser("destroy-service", help=command.__doc__)
    sub_parser.add_argument(
        "--environment", "-e",
        help="Environment to add the relation in.")
    sub_parser.add_argument(
        "service_name",
        help="Name of the service to stop")
    return sub_parser


def command(options):
    """Destroy a running service, its units, and break its relations."""
    environment = get_environment(options)
    return destroy_service(
        options.environments,
        environment,
        options.verbose,
        options.log,
        options.service_name)


@inlineCallbacks
def destroy_service(config, environment, verbose, log, service_name):
    provider = environment.get_machine_provider()
    client = yield provider.connect()
    service_manager = ServiceStateManager(client)
    service_state = yield service_manager.get_service_state(service_name)
    yield service_manager.remove_service_state(service_state)
    log.info("Service %r destroyed.", service_state.service_name)
