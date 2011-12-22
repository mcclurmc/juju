"""Implementation of resolved subcommand"""

import argparse

from twisted.internet.defer import inlineCallbacks, returnValue

from juju.control.utils import get_environment

from juju.state.service import ServiceStateManager, RETRY_HOOKS, NO_HOOKS
from juju.state.relation import RelationStateManager
from juju.state.errors import RelationStateNotFound
from juju.unit.workflow import is_unit_running, is_relation_running


def configure_subparser(subparsers):
    """Configure resolved subcommand"""
    sub_parser = subparsers.add_parser(
        "resolved",
        help=command.__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=resolved.__doc__)
    sub_parser.add_argument(
        "--retry", "-r", action="store_true",
        help="Retry failed hook."),
    sub_parser.add_argument(
        "--environment", "-e",
        help="juju environment to operate in.")
    sub_parser.add_argument(
        "service_unit_name",
        help="Name of the service unit that should be resolved")
    sub_parser.add_argument(
       "relation_name", nargs="?", default=None,
        help="Name of the unit relation that should be resolved")
    return sub_parser


def command(options):
    """Mark an error as resolved in a unit or unit relation."""
    environment = get_environment(options)
    return resolved(
        options.environments,
        environment,
        options.verbose,
        options.log,
        options.service_unit_name,
        options.relation_name,
        options.retry)


@inlineCallbacks
def resolved(
    config, environment, verbose, log, unit_name, relation_name, retry):
    """Mark an error as resolved in a unit or unit relation.

    If one of a unit's charm non-relation hooks returns a non-zero exit
    status, the entire unit can be considered to be in a non-running state.

    As a resolution, the the unit can be manually returned a running state
    via the juju resolved command. Optionally this command can also
    rerun the failed hook.

    This resolution also applies separately to each of the unit's relations.
    If one of the relation-hooks failed. In that case there is no
    notion of retrying (the change is gone), but resolving will allow
    additional relation hooks for that relation to proceed.
    """
    provider = environment.get_machine_provider()
    client = yield provider.connect()
    service_manager = ServiceStateManager(client)
    relation_manager = RelationStateManager(client)

    unit_state = yield service_manager.get_unit_state(unit_name)
    service_state = yield service_manager.get_service_state(
        unit_name.split("/")[0])

    retry = retry and RETRY_HOOKS or NO_HOOKS

    if not relation_name:
        running, workflow_state = yield is_unit_running(client, unit_state)
        if running:
            log.info("Unit %r already running: %s", unit_name, workflow_state)
            client.close()
            returnValue(False)

        yield unit_state.set_resolved(retry)
        log.info("Marked unit %r as resolved", unit_name)
        returnValue(True)

    # Check for the matching relations
    service_relations = yield relation_manager.get_relations_for_service(
        service_state)
    service_relations = [
        sr for sr in service_relations if sr.relation_name == relation_name]
    if not service_relations:
        raise RelationStateNotFound()

    # Verify the relations are in need of resolution.
    resolved_relations = {}
    for service_relation in service_relations:
        unit_relation = yield service_relation.get_unit_state(unit_state)
        running, state = yield is_relation_running(client, unit_relation)
        if not running:
            resolved_relations[unit_relation.internal_relation_id] = retry

    if not resolved_relations:
        log.warning("Matched relations are all running")
        client.close()
        returnValue(False)

    # Mark the relations as resolved.
    yield unit_state.set_relation_resolved(resolved_relations)
    log.info(
        "Marked unit %r relation %r as resolved", unit_name, relation_name)
    client.close()
