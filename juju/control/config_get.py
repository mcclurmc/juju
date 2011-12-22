import argparse
import yaml

from twisted.internet.defer import inlineCallbacks

from juju.control.utils import get_environment
from juju.state.service import ServiceStateManager


def configure_subparser(subparsers):
    sub_parser = subparsers.add_parser(
        "get",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        help=config_get.__doc__,
        description=command.__doc__)

    sub_parser.add_argument(
        "--environment", "-e",
        help="Environment to utilize.")

    sub_parser.add_argument(
        "--schema", "-s", action="store_true", default=False,
        help="Display the schema only")

    sub_parser.add_argument(
        "service_name",
        help="The name of the service to retrieve settings for")

    return sub_parser


def command(options):
    """Get service config options.

    Charms may define dynamic options which may be tweaked at
    deployment time, or over the lifetime of the service.  This
    command allows display the current value of these settings
    in yaml format.

    $ juju get wordpress

    {'service': 'wordpress',
     'charm': 'local:series/wordpress-3',
     'settings': {'blog-title': {
                    'description': 'A descriptive title used for the blog.',
                    'type': 'string',
                    'value': 'Hello World'}}},
    """
    environment = get_environment(options)

    return config_get(environment,
                      options.service_name,
                      options.schema)


@inlineCallbacks
def config_get(environment, service_name, display_schema):
    """Get service settings.
    """
    provider = environment.get_machine_provider()
    client = yield provider.connect()

    # Get the service
    service_manager = ServiceStateManager(client)
    service = yield service_manager.get_service_state(service_name)

    # Retrieve schema
    charm = yield service.get_charm_state()
    schema = yield charm.get_config()
    schema_dict = schema.as_dict()
    display_dict = {"service": service.service_name,
                    "charm": (yield service.get_charm_id()),
                    "settings": schema_dict}

    # Get current settings
    settings = yield service.get_config()
    settings = dict(settings.items())

    # Merge current settings into schema/display dict
    for k, v in schema_dict.items():
        if k in settings:
            v['value'] = settings[k]
            # Defaults are confusing inline display to the value for longer
            # strings. And there already the current value, if one wasn't set.

            # We do defaults for upgraded config values, which haven't been
            # set yet.
            if 'default' in v:
                del v['default']
        else:
            v['value'] = "-Not set-"

    print yaml.safe_dump(
        display_dict,
        indent=4, default_flow_style=False, width=80, allow_unicode=True)
    client.close()
