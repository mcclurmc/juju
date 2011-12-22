from argparse import RawDescriptionHelpFormatter
import os

from twisted.internet.defer import inlineCallbacks

from juju.control.utils import (
    get_environment, get_ip_address_for_machine, get_ip_address_for_unit,
    parse_passthrough_args, ParseError)
from juju.state.errors import MachineStateNotFound
from juju.state.sshforward import prepare_ssh_sharing


def configure_subparser(subparsers):
    sub_parser = subparsers.add_parser(
        "ssh",
        help=command.__doc__,
        usage=("%(prog)s [-h] [-e ENV] unit_or_machine [command]"),
        formatter_class=RawDescriptionHelpFormatter,
        description=(
            "positional arguments:\n"
            "  unit_or_machine       Name of unit or machine\n"
            "  [command]             Optional command to run on machine"))
    sub_parser.add_argument(
        "--environment", "-e",
        help="Environment to operate on.", metavar="ENV")
    return sub_parser


def passthrough(options, extra):
    """Second parsing phase to parse `extra` to passthrough to ssh itself.

    Partitions into flags, unit_or_machine, and optional ssh command.
    """
    flags, positional = parse_passthrough_args(extra, "bcDeFIiLlmOopRSWw")
    if not positional:
        raise ParseError("too few arguments")
    options.ssh_flags = flags
    options.unit_or_machine = positional.pop(0)
    options.ssh_command = positional  # if any


def open_ssh(flags, ip_address, ssh_command):
    # XXX - TODO - Might be nice if we had the ability to get the user's
    # private key path and utilize it here, ie the symmetric end to
    # get user public key.
    args = ["ssh"]
    args.extend(prepare_ssh_sharing())
    args.extend(flags)
    args.extend(["ubuntu@%s" % ip_address])
    args.extend(ssh_command)
    os.execvp("ssh", args)


@inlineCallbacks
def command(options):
    """Launch an ssh shell on the given unit or machine.
    """
    environment = get_environment(options)
    provider = environment.get_machine_provider()
    client = yield provider.connect()
    label = machine = unit = None

    # First check if it's a juju machine id
    if options.unit_or_machine.isdigit():
        options.log.debug(
            "Fetching machine address using juju machine id.")
        ip_address, machine = yield get_ip_address_for_machine(
            client, provider, options.unit_or_machine)
        machine.get_ip_address = get_ip_address_for_machine
        label = "machine"
    # Next check if it's a unit
    elif "/" in options.unit_or_machine:
        options.log.debug(
            "Fetching machine address using unit name.")
        ip_address, unit = yield get_ip_address_for_unit(
            client, provider, options.unit_or_machine)
        unit.get_ip_address = get_ip_address_for_unit
        label = "unit"
    else:
        raise MachineStateNotFound(options.unit_or_machine)

    agent_state = machine or unit

    # Now verify the relevant agent is operational via its agent.
    exists_d, watch_d = agent_state.watch_agent()
    exists = yield exists_d

    if not exists:
        # If not wait on it.
        options.log.info("Waiting for %s to come up." % label)
        yield watch_d

    # Double check the address we have is valid, else refetch.
    if ip_address is None:
        ip_address, machine = yield agent_state.get_ip_address(
            client, provider, options.unit_or_machine)

    yield client.close()

    options.log.info("Connecting to %s %s at %s",
                     label, options.unit_or_machine, ip_address)
    open_ssh(options.ssh_flags, ip_address, options.ssh_command)
