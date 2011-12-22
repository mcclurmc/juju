from argparse import RawDescriptionHelpFormatter
import os

from twisted.internet.defer import inlineCallbacks, returnValue

from juju.control.utils import (
    get_environment, get_ip_address_for_machine, get_ip_address_for_unit,
    parse_passthrough_args, ParseError)


def configure_subparser(subparsers):
    sub_parser = subparsers.add_parser(
        "scp",
        help=command.__doc__,
        usage=("%(prog)s [-h] [-e ENV] "
               "[remote_host:]file1 ... [remote_host:]file2"),
        formatter_class=RawDescriptionHelpFormatter,
        description=(
            "positional arguments:\n"
            "  [remote_host:]file    The remote host can the name of either\n"
            "                        a Juju unit/machine or a remote system"))
    sub_parser.add_argument(
        "--environment", "-e",
        help="Environment to operate on.", metavar="ENV")
    return sub_parser


def passthrough(options, extra):
    """Second parsing phase to parse `extra` to passthrough to scp itself.

    Partitions into flags and file specifications.
    """
    flags, positional = parse_passthrough_args(extra, "cFiloPS")
    if not positional:
        raise ParseError("too few arguments")
    options.scp_flags = flags
    options.paths = positional


def open_scp(flags, paths):
    # XXX - TODO - Might be nice if we had the ability to get the user's
    # private key path and utilize it here, ie the symmetric end to
    # get user public key.
    args = ["scp"]
    # Unlike ssh, choose not to share connections by default, given
    # that the target usage may be for large files. The user's ssh
    # config would probably be the best place to get this anyway.
    args.extend(flags)
    args.extend(paths)
    os.execvp("scp", args)


@inlineCallbacks
def _expand_unit_or_machine(client, provider, path):
    """Expands service unit or machine ID into DNS name"""
    parts = path.split(":")
    if len(parts) > 1:
        remote_system = parts[0]
        ip_address = None
        if remote_system.isdigit():
            # machine id, will not pick up dotted IP addresses
            ip_address, _ = yield get_ip_address_for_machine(
                client, provider, remote_system)
        elif "/" in remote_system:
            # service unit
            ip_address, _ = yield get_ip_address_for_unit(
                client, provider, remote_system)
        if ip_address:
            returnValue("ubuntu@%s:%s" % (ip_address, ":".join(parts[1:])))
    returnValue(path)  # no need to expand


@inlineCallbacks
def command(options):
    """Use scp to copy files to/from given unit or machine.

    Unlike juju ssh, no attempt to verify liveness of the agent,
    instead it's just a matter of whether the underlying scp will work
    or not.
    """
    environment = get_environment(options)
    provider = environment.get_machine_provider()
    client = yield provider.connect()
    try:
        paths = [(yield _expand_unit_or_machine(client, provider, path))
                 for path in options.paths]
        open_scp(options.scp_flags, paths)
    finally:
        yield client.close()
