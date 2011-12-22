"""
Command for distributed debug logging output via the cli.
"""

from fnmatch import fnmatch
import logging
import sys

from twisted.internet.defer import inlineCallbacks

from juju.control.options import ensure_abs_path
from juju.control.utils import get_environment
from juju.state.environment import GlobalSettingsStateManager
from juju.lib.zklog import LogIterator


def configure_subparser(subparsers):
    """Configure debug-log subcommand"""
    sub_parser = subparsers.add_parser("debug-log", help=command.__doc__)
    sub_parser.add_argument(
        "-e", "--environment",
        help="juju environment to operate in.")

    sub_parser.add_argument(
        "-r", "--replay", default=False,
        action="store_true",
        help="Display all existing logs first.")

    sub_parser.add_argument(
        "-i", "--include", action="append",
        help=("Filter log messages to only show these log channels or agents."
              "Multiple values can be specified, also supports unix globbing.")
        )

    sub_parser.add_argument(
        "-x", "--exclude", action="append",
        help=("Filter log messages to exclude these log channels or agents."
              "Multiple values can be specified, also supports unix globbing.")
        )

    sub_parser.add_argument(
        "-l", "--level", default="DEBUG",
        choices=("DEBUG", "INFO", "ERROR", "WARNING", "CRITICAL"),
        help="Log level to show")

    sub_parser.add_argument(
        "-n", "--limit", type=int,
        help="Show n log messages and exit.")

    sub_parser.add_argument(
        "-o", "--output", default="-",
        help="File to log to, defaults to stdout",
        type=ensure_abs_path)

    return sub_parser


def command(options):
    """Distributed juju debug log watching."""
    environment = get_environment(options)
    return debug_log(
        options.environments,
        environment,
        options.log,
        options)


@inlineCallbacks
def debug_log(config, environment, log, options):
    provider = environment.get_machine_provider()
    client = yield provider.connect()

    log.info("Enabling distributed debug log.")

    settings_manager = GlobalSettingsStateManager(client)
    yield settings_manager.set_debug_log(True)

    if not options.limit:
        log.info("Tailing logs - Ctrl-C to stop.")

    iterator = LogIterator(client, replay=options.replay)

    # Setup the logging output with the user specified file.
    if options.output == "-":
        log_file = sys.stdout
    else:
        log_file = open(options.output, "a")
    handler = logging.StreamHandler(log_file)

    log_level = logging.getLevelName(options.level)
    handler.setLevel(log_level)

    formatter = logging.Formatter(
        "%(asctime)s %(context)s: %(name)s %(levelname)s: %(message)s")
    handler.setFormatter(formatter)

    def match(data):
        local_name = data["context"].split(":")[-1]

        if options.exclude:
            for exclude in options.exclude:
                if fnmatch(local_name, exclude) or \
                   fnmatch(data["context"], exclude) or \
                   fnmatch(data["name"], exclude):
                    return False

        if options.include:
            for include in options.include:
                if fnmatch(local_name, include) or \
                   fnmatch(data["context"], include) or \
                   fnmatch(data["name"], include):
                    return True
            return False

        return True

    count = 0
    try:
        while True:
            entry = yield iterator.next()
            if not match(entry):
                continue
            # json doesn't distinguish lists v. tuples but python string
            # formatting doesn't accept lists.
            entry["args"] = tuple(entry["args"])
            record = logging.makeLogRecord(entry)
            if entry["levelno"] < handler.level:
                continue
            handler.handle(record)
            count += 1
            if options.limit is not None and count == options.limit:
                break
    finally:
        yield settings_manager.set_debug_log(False)
        client.close()
