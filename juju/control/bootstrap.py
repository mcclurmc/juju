from twisted.internet.defer import inlineCallbacks

from juju.control.utils import get_environment


def configure_subparser(subparsers):
    """Configure bootstrap subcommand"""
    sub_parser = subparsers.add_parser("bootstrap", help=command.__doc__)
    sub_parser.add_argument(
        "--environment", "-e",
        help="juju environment to operate in.")
    return sub_parser


@inlineCallbacks
def command(options):
    """
    Bootstrap machine providers in the specified environment.
    """
    environment = get_environment(options)
    provider = environment.get_machine_provider()
    options.log.info("Bootstrapping environment %r (type: %s)..." % (
        environment.name, environment.type))
    yield provider.bootstrap()
