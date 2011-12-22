import os

import yaml

from twisted.internet.defer import inlineCallbacks

from juju.control.utils import get_environment, expand_path

from juju.charm.errors import ServiceConfigValueError
from juju.charm.publisher import CharmPublisher
from juju.charm.repository import resolve

from juju.state.endpoint import RelationEndpoint
from juju.state.environment import EnvironmentStateManager
from juju.state.placement import place_unit
from juju.state.relation import RelationStateManager
from juju.state.service import ServiceStateManager


def configure_subparser(subparsers):
    sub_parser = subparsers.add_parser("deploy", help=command.__doc__)

    sub_parser.add_argument(
        "--environment", "-e",
        help="Environment to deploy the charm in.")

    sub_parser.add_argument(
        "--num-units", "-n", default=1, type=int, metavar="NUM",
        help="Number of service units to deploy.")

    sub_parser.add_argument(
        "--repository",
        help="Directory for charm lookup and retrieval",
        default=None,
        type=expand_path)

    sub_parser.add_argument(
        "charm", nargs=None,
        help="Charm name")

    sub_parser.add_argument(
        "service_name", nargs="?",
        help="Service name of deployed charm")

    sub_parser.add_argument(
        "--config",
        help="YAML file containing service options")

    return sub_parser


def command(options):
    """
    Deploy a charm to juju!
    """
    environment = get_environment(options)
    return deploy(
        options.environments,
        environment,
        options.repository,
        options.charm,
        options.service_name,
        options.log,
        options.config,
        num_units=options.num_units)


def parse_config_options(config_file, service_name):
    if not os.path.exists(config_file) or \
            not os.access(config_file, os.R_OK):
        raise ServiceConfigValueError(
            "Config file %r not accessible." % config_file)

    options = yaml.load(open(config_file, "r").read())
    if not options or not isinstance(options, dict) or \
            service_name not in options:
        raise ServiceConfigValueError(
            "Invalid options file passed to --config.\n"
            "Expected a YAML dict with service name (%r)." % service_name)
    # remove the service name prefix for application to state
    return options[service_name]


@inlineCallbacks
def deploy(env_config, environment, repository_path, charm_name,
           service_name, log, config_file=None, num_units=1):
    """Deploy a charm within an environment.

    This will publish the charm to the environment, creating
    a service from the charm, and get it set to be launched
    on a new machine.
    """
    repo, charm_url = resolve(
        charm_name, repository_path, environment.default_series)

    # Validate config options prior to deployment attempt
    service_options = {}
    service_name = service_name or charm_url.name
    if config_file:
        service_options = parse_config_options(config_file, service_name)

    charm = yield repo.find(charm_url)
    charm_id = str(charm_url.with_revision(charm.get_revision()))

    provider = environment.get_machine_provider()
    placement_policy = provider.get_placement_policy()
    client = yield provider.connect()

    try:
        storage = yield provider.get_file_storage()
        service_manager = ServiceStateManager(client)
        environment_state_manager = EnvironmentStateManager(client)
        yield environment_state_manager.set_config_state(
            env_config, environment.name)

        # Publish the charm to juju
        publisher = CharmPublisher(client, storage)
        yield publisher.add_charm(charm_id, charm)
        result = yield publisher.publish()

        # In future we might have multiple charms be published at
        # the same time.  For now, extract the charm_state from the
        # list.
        charm_state = result[0]

        # Create the service state
        service_state = yield service_manager.add_service_state(
            service_name, charm_state)

        # Use the charm's ConfigOptions instance to validate service
        # options.. Invalid options passed will thrown an exception
        # and prevent the deploy.
        state = yield service_state.get_config()
        charm_config = yield charm_state.get_config()
        # return the validated options with the defaults included
        service_options = charm_config.validate(service_options)

        state.update(service_options)
        yield state.write()

        # Create desired number of service units
        for i in xrange(num_units):
            unit_state = yield service_state.add_unit_state()
            yield place_unit(client, placement_policy, unit_state)

        # Check if we have any peer relations to establish
        if charm.metadata.peers:
            relation_manager = RelationStateManager(client)
            for peer_name, peer_info in charm.metadata.peers.items():
                yield relation_manager.add_relation_state(
                    RelationEndpoint(service_name,
                                     peer_info["interface"],
                                     peer_name,
                                     "peer"))

        log.info("Charm deployed as service: %r", service_name)
    finally:
        yield client.close()
