"""Implementation of add-relation juju subcommand"""

from twisted.internet.defer import inlineCallbacks

from juju.control.utils import get_environment
from juju.state.errors import NoMatchingEndpoints, AmbiguousRelation
from juju.state.relation import RelationStateManager
from juju.state.service import ServiceStateManager


def configure_subparser(subparsers):
    """Configure add-relation subcommand"""
    sub_parser = subparsers.add_parser("add-relation", help=command.__doc__)
    sub_parser.add_argument(
        "--environment", "-e",
        help="Environment to add the relation in.")
    sub_parser.add_argument(
        "--verbose",
        help="Provide additional information when running the command.")
    sub_parser.add_argument(
        "descriptors", nargs=2, metavar="<service name>[:<relation name>]",
        help="Define the relation endpoints to be joined.")
    return sub_parser


def command(options):
    """Add a relation between services in juju."""
    environment = get_environment(options)
    return add_relation(
        options.environments,
        environment,
        options.verbose,
        options.log,
        *options.descriptors)


@inlineCallbacks
def add_relation(env_config, environment, verbose, log, *descriptors):
    """Add relation between relation endpoints described by `descriptors`"""
    provider = environment.get_machine_provider()
    client = yield provider.connect()
    relation_state_manager = RelationStateManager(client)
    service_state_manager = ServiceStateManager(client)
    endpoint_pairs = yield service_state_manager.join_descriptors(
            *descriptors)

    if verbose:
        log.info("Endpoint pairs: %s", endpoint_pairs)

    if len(endpoint_pairs) == 0:
        raise NoMatchingEndpoints()
    elif len(endpoint_pairs) > 1:
        raise AmbiguousRelation(descriptors, endpoint_pairs)

    # At this point we just have one endpoint pair. We need to pick
    # just one of the endpoints if it's a peer endpoint, since that's
    # our current API - join descriptors takes two descriptors, but
    # add_relation_state takes one or two endpoints. TODO consider
    # refactoring.
    endpoints = endpoint_pairs[0]
    if endpoints[0] == endpoints[1]:
        endpoints = endpoints[0:1]
    yield relation_state_manager.add_relation_state(*endpoints)
    yield client.close()

    log.info("Added %s relation to all service units.",
        endpoints[0].relation_type)
