"""Implementation of remove-relation juju subcommand"""

from twisted.internet.defer import inlineCallbacks

from juju.control.utils import get_environment
from juju.state.errors import AmbiguousRelation, NoMatchingEndpoints
from juju.state.relation import RelationStateManager
from juju.state.service import ServiceStateManager


def configure_subparser(subparsers):
    """Configure remove-relation subcommand"""
    sub_parser = subparsers.add_parser("remove-relation", help=command.__doc__)
    sub_parser.add_argument(
        "--environment", "-e",
        help="Environment to add the relation in.")
    sub_parser.add_argument(
        "--verbose",
        help="Provide additional information when running the command.")
    sub_parser.add_argument(
        "descriptors", nargs=2, metavar="<service name>[:<relation name>]",
        help="Define the relation endpoints for the relation to be removed.")
    return sub_parser


def command(options):
    """Add a relation between services in juju."""
    environment = get_environment(options)
    return remove_relation(
        options.environments,
        environment,
        options.verbose,
        options.log,
        *options.descriptors)


@inlineCallbacks
def remove_relation(env_config, environment, verbose, log, *descriptors):
    """Remove relation between relation endpoints described by `descriptors`"""
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
    relation_state = yield relation_state_manager.get_relation_state(
        *endpoints)
    yield relation_state_manager.remove_relation_state(relation_state)
    yield client.close()

    log.info("Removed %s relation from all service units.",
        endpoints[0].relation_type)
