"""Implementation of charm-upgrade subcommand"""

from twisted.internet.defer import inlineCallbacks

from juju.control.utils import get_environment, expand_path

from juju.charm.directory import CharmDirectory
from juju.charm.errors import NewerCharmNotFound
from juju.charm.publisher import CharmPublisher
from juju.charm.repository import resolve
from juju.charm.url import CharmURL

from juju.state.service import ServiceStateManager
from juju.unit.workflow import is_unit_running


def configure_subparser(subparsers):
    """Configure charm-upgrade subcommand"""
    sub_parser = subparsers.add_parser("upgrade-charm", help=command.__doc__)
    sub_parser.add_argument(
        "--dry-run", "-n", action="store_true",
        help="Dry-Run, show which charm would be deployed for upgrade.")
    sub_parser.add_argument(
        "--environment", "-e",
        help="juju environment to operate in.")
    sub_parser.add_argument(
        "--repository",
        help="Directory for charm lookup and retrieval",
        type=expand_path)
    sub_parser.add_argument(
        "service_name",
        help="Name of the service that should be upgraded")
    return sub_parser


def command(options):
    """Upgrade a service's charm."""
    environment = get_environment(options)
    return upgrade_charm(
        options.environments,
        environment,
        options.verbose,
        options.log,
        options.repository,
        options.service_name,
        options.dry_run)


@inlineCallbacks
def upgrade_charm(
    config, environment, verbose, log, repository_path, service_name, dry_run):
    """Upgrades a service's charm.

    First determines if an upgrade is available, then updates the
    service charm reference, and marks the units as needing upgrades.
    """
    provider = environment.get_machine_provider()
    client = yield provider.connect()

    service_manager = ServiceStateManager(client)
    service_state = yield service_manager.get_service_state(service_name)
    old_charm_id = yield service_state.get_charm_id()

    old_charm_url = CharmURL.parse(old_charm_id)
    old_charm_url.assert_revision()
    repo, charm_url = resolve(
        str(old_charm_url.with_revision(None)),
        repository_path,
        environment.default_series)
    new_charm_url = charm_url.with_revision(
        (yield repo.latest(charm_url)))

    if charm_url.collection.schema == "local":
        if old_charm_url.revision >= new_charm_url.revision:
            new_revision = old_charm_url.revision + 1
            charm = yield repo.find(new_charm_url)
            if isinstance(charm, CharmDirectory):
                if dry_run:
                    log.info("%s would be set to revision %s",
                             charm.path, new_revision)
                else:
                    log.info("Setting %s to revision %s",
                             charm.path, new_revision)
                    charm.set_revision(new_revision)
                new_charm_url.revision = new_revision

    new_charm_id = str(new_charm_url)

    # Verify its newer than what's deployed
    if not new_charm_url.revision > old_charm_url.revision:
        if dry_run:
            log.info("Service already running latest charm %r", old_charm_id)
        else:
            raise NewerCharmNotFound(old_charm_id)
    elif dry_run:
        log.info("Service would be upgraded from charm %r to %r",
                 old_charm_id, new_charm_id)

    # On dry run, stop before modifying state.
    if not dry_run:
        # Publish the new charm
        storage = provider.get_file_storage()
        publisher = CharmPublisher(client, storage)
        charm = yield repo.find(new_charm_url)
        yield publisher.add_charm(new_charm_id, charm)
        result = yield publisher.publish()
        charm_state = result[0]

        # Update the service charm reference
        yield service_state.set_charm_id(charm_state.id)

    # Mark the units for upgrades
    units = yield service_state.get_all_unit_states()
    for unit in units:
        running, state = yield is_unit_running(client, unit)
        if not running:
            log.info(
                "Unit %r is not in a running state (state: %r), won't upgrade",
                unit.unit_name, state or "uninitialized")
            continue

        if not dry_run:
            yield unit.set_upgrade_flag()
