import argparse
import logging
import sys
import zookeeper

from .command import Commander
from .utils import ParseError
from juju.environment.config import EnvironmentsConfig

import add_relation
import add_unit
import bootstrap
import config_get
import config_set
import debug_hooks
import debug_log
import deploy
import destroy_environment
import destroy_service
import expose
import open_tunnel
import remove_relation
import remove_unit
import resolved
import scp
import status
import ssh
import terminate_machine
import unexpose
import upgrade_charm

import initialize


SUBCOMMANDS = [
    add_relation,
    add_unit,
    bootstrap,
    config_get,
    config_set,
    debug_log,
    debug_hooks,
    deploy,
    destroy_environment,
    destroy_service,
    expose,
    open_tunnel,
    remove_relation,
    remove_unit,
    resolved,
    scp,
    status,
    ssh,
    terminate_machine,
    unexpose,
    upgrade_charm
    ]

ADMIN_SUBCOMMANDS = [
    initialize]

log = logging.getLogger("juju.control.cli")


class JujuParser(argparse.ArgumentParser):

    def add_subparsers(self, **kwargs):
        kwargs.setdefault("parser_class", argparse.ArgumentParser)
        return super(JujuParser, self).add_subparsers(**kwargs)

    def error(self, message):
        self.print_help(sys.stderr)
        self.exit(2, '%s: error: %s\n' % (self.prog, message))


class JujuFormatter(argparse.HelpFormatter):

    def _metavar_formatter(self, action, default_metavar):
        """Override to get rid of redundant printing of positional args.
        """
        if action.metavar is not None:
            result = action.metavar
        elif default_metavar == "==SUPPRESS==":
            result = ""
        else:
            result = default_metavar

        def format(tuple_size):
            if isinstance(result, tuple):
                return result
            else:
                return (result, ) * tuple_size
        return format


def setup_parser(subcommands, **kw):
    """Setup a command line argument/option parser."""
    parser = JujuParser(formatter_class=JujuFormatter, **kw)
    parser.add_argument(
        "--verbose", "-v", default=False,
        action="store_true",
        help="Enable verbose logging")

    parser.add_argument(
        "--log-file", "-l", default=sys.stderr, type=argparse.FileType('a'),
        help="Log output to file")

    subparsers = parser.add_subparsers()

    for module in subcommands:
        configure_subparser = getattr(module, "configure_subparser", None)
        passthrough = getattr(module, "passthrough", None)
        if configure_subparser:
            sub_parser = configure_subparser(subparsers)
        else:
            sub_parser = subparsers.add_parser(
                module.__name__.split('.')[-1], help=module.command.__doc__)
        sub_parser.set_defaults(
            command=Commander(module.command, passthrough=passthrough),
            parser=sub_parser)

    return parser


def setup_logging(options):
    level = logging.DEBUG if options.verbose else logging.INFO
    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(message)s",
        level=level,
        stream=options.log_file)

    if level is not logging.DEBUG:
        zookeeper.set_debug_level(0)


def admin(args):
    """juju Admin command line interface entry point.

    The admin cli is used to provide an entry point into infrastructure
    tools like initializing the zookeeper layout, launching machine and
    provisioning agents, etc. Its not intended to be used by end users
    but consumed internally by the framework.
    """
    parser = setup_parser(
        subcommands=ADMIN_SUBCOMMANDS,
        prog="juju-admin",
        description="juju cloud orchestration internal tools")

    parser.set_defaults(log=log)
    options = parser.parse_args(args)
    setup_logging(options)

    options.command(options)


def main(args):
    """The main end user cli command for juju users."""
    env_config = EnvironmentsConfig()
    env_config.load_or_write_sample()
    parser = setup_parser(
        subcommands=SUBCOMMANDS,
        prog="juju",
        description="juju cloud orchestration admin")
    parser.set_defaults(environments=env_config, log=log)

    # Some commands, like juju ssh, do a further parse on options by
    # delegating to another command (such as the underlying ssh). But
    # first need to parse nonstrictly all args to even determine what
    # command is even being used.
    options, extra = parser.parse_known_args(args)
    if options.command.passthrough:
        try:
            # Augments options with subparser specific passthrough parsing
            options.command.passthrough(options, extra)
        except ParseError, e:
            options.parser.error(str(e))
    else:
        # Otherwise, do be strict
        options = parser.parse_args(args)
    setup_logging(options)
    options.command(options)
