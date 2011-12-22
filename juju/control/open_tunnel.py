from twisted.internet.defer import inlineCallbacks, Deferred

from juju.control.utils import get_environment


def configure_subparser(subparsers):
    sub_parser = subparsers.add_parser("open-tunnel", help=command.__doc__)
    sub_parser.add_argument(
        "--environment", "-e", help="Environment to operate on.")
    # TODO Coming next:
    #sub_parser.add_argument(
    #    "unit_or_machine", nargs="*", help="Name of unit or machine")
    return sub_parser


@inlineCallbacks
def command(options):
    """Establish a tunnel to the environment.
    """
    environment = get_environment(options)
    provider = environment.get_machine_provider()
    yield provider.connect(share=True)

    options.log.info("Tunnel to the environment is open. "
                     "Press CTRL-C to close it.")

    yield hanging_deferred()


def hanging_deferred():
    # Hang forever.
    return Deferred()
