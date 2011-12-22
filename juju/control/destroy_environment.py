from twisted.internet.defer import inlineCallbacks, returnValue

from juju.control.utils import get_environment


def configure_subparser(subparsers):
    """Configure destroy-environment subcommand"""
    sub_parser = subparsers.add_parser(
        "destroy-environment", help=command.__doc__)
    sub_parser.add_argument(
        "--environment", "-e",
        help="juju environment to operate in.")
    return sub_parser


@inlineCallbacks
def command(options):
    """
    Terminate all machines and other associated resources for an environment.
    """
    environment = get_environment(options)
    provider = environment.get_machine_provider()

    value = raw_input(
        "WARNING: this command will destroy the %r environment (type: %s).\n"
        "This includes all machines, services, data, and other resources. "
        "Continue [y/N]" % (
            environment.name, environment.type))

    if value.strip().lower() not in ("y", "yes"):
        options.log.info("Environment destruction aborted")
        returnValue(None)
    options.log.info("Destroying environment %r (type: %s)..." % (
        environment.name, environment.type))
    yield provider.destroy_environment()
