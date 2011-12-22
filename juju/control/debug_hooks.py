"""
Command for debugging hooks on a service unit.
"""
import base64
import os

from twisted.internet.defer import inlineCallbacks, returnValue

from juju.control.utils import get_ip_address_for_unit
from juju.control.utils import get_environment

from juju.charm.errors import InvalidCharmHook
from juju.state.charm import CharmStateManager
from juju.state.service import ServiceStateManager


def configure_subparser(subparsers):
    sub_parser = subparsers.add_parser("debug-hooks", help=command.__doc__)
    sub_parser.add_argument(
        "-e", "--environment",
        help="juju environment to operate in.")

    sub_parser.add_argument(
        "unit_name",
        help="Name of unit")

    sub_parser.add_argument(
        "hook_names", default=["*"], nargs="*",
        help="Name of hook, defaults to all")

    return sub_parser


@inlineCallbacks
def validate_hooks(client, unit_state, hook_names):

    # Assemble a list of valid hooks for the charm.
    valid_hooks = ["start", "stop", "install", "config-changed"]
    service_manager = ServiceStateManager(client)
    endpoints = yield service_manager.get_relation_endpoints(
        unit_state.service_name)
    endpoint_names = [ep.relation_name for ep in endpoints]
    for endpoint_name in endpoint_names:
        valid_hooks.extend([
            endpoint_name + "-relation-joined",
            endpoint_name + "-relation-changed",
            endpoint_name + "-relation-departed",
            endpoint_name + "-relation-broken",
        ])

    # Verify the debug names.
    for hook_name in hook_names:
        if hook_name in valid_hooks:
            continue
        break
    else:
        returnValue(True)

    # We dereference to the charm to give a fully qualified error
    # message.  I wish this was a little easier to dereference, the
    # service_manager.get_relation_endpoints effectively does this
    # already.
    service_manager = ServiceStateManager(client)
    service_state = yield service_manager.get_service_state(
        unit_state.service_name)
    charm_id = yield service_state.get_charm_id()
    charm_manager = CharmStateManager(client)
    charm = yield charm_manager.get_charm_state(charm_id)
    raise InvalidCharmHook(charm.id, hook_name)


@inlineCallbacks
def command(options):
    """Interactively debug a hook remotely on a service unit.
    """
    environment = get_environment(options)
    provider = environment.get_machine_provider()
    client = yield provider.connect()

    # Verify unit and retrieve ip address
    options.log.debug("Retrieving unit and machine information.")
    ip_address, unit = yield get_ip_address_for_unit(
        client, provider, options.unit_name)

    # Verify hook name
    if options.hook_names != ["*"]:
        options.log.debug("Verifying hook names...")
        yield validate_hooks(client, unit, options.hook_names)

    # Enable debug log
    options.log.debug(
        "Enabling hook debug on unit (%r)..." % options.unit_name)
    yield unit.enable_hook_debug(options.hook_names)

    # If we don't have an ipaddress the unit isn't up yet, wait for it.
    if not ip_address:
        options.log.info("Waiting for unit")
        # Wait and verify the agent is running.
        while 1:
            exists_d, watch_d = unit.watch_agent()
            exists = yield exists_d
            if exists:
                options.log.info("Unit running")
                break
            yield watch_d

        # Refetch the unit address
        ip_address, unit = yield get_ip_address_for_unit(
            client, provider, options.unit_name)

    # Connect via ssh and start tmux.
    options.log.info("Connecting to remote machine %s...", ip_address)

    # Encode the script as base64 so that we can deliver it with a single
    # ssh command while still retaining standard input on the terminal fd.
    script = SCRIPT.replace("{unit_name}", options.unit_name)
    script_b64 = base64.encodestring(script).replace("\n", "").strip()
    cmd = '"F=`mktemp`; echo %s | base64 -d > \$F; . \$F"' % script_b64

    # Yield to facilitate testing.
    yield os.system(
        "ssh -t ubuntu@%s 'sudo /bin/bash -c %s'" % (ip_address, cmd))

    options.log.info("Debug session ended.")
    # Ends hook debugging.
    yield client.close()


SCRIPT = r"""
# Wait for tmux to be installed.
while [ ! -f /usr/bin/tmux ]; do
    sleep 1
done

if [ ! -f ~/.tmux.conf ]; then
	if [ -f /usr/share/byobu/profiles/tmux ]; then
		# Use byobu/tmux profile for familiar keybindings and branding
		echo "source-file /usr/share/byobu/profiles/tmux" > ~/.tmux.conf
	else
		# Otherwise, use the legacy juju/tmux configuration
		cat > ~/.tmux.conf <<END

# Status bar
set-option -g status-bg black
set-option -g status-fg white

set-window-option -g window-status-current-bg red
set-window-option -g window-status-current-attr bright

set-option -g status-right ''

# Panes
set-option -g pane-border-fg white
set-option -g pane-active-border-fg white

# Monitor activity on windows
set-window-option -g monitor-activity on

# Screen bindings, since people are more familiar with that.
set-option -g prefix C-a
bind C-a last-window
bind a send-key C-a

bind | split-window -h
bind - split-window -v

# Fix CTRL-PGUP/PGDOWN for vim
set-window-option -g xterm-keys on

# Prevent ESC key from adding delay and breaking Vim's ESC > arrow key
set-option -s escape-time 0

END
	fi
fi

# The beauty below is a workaround for a bug in tmux (1.5 in Oneiric) or
# epoll that doesn't support /dev/null or whatever.  Without it the
# command hangs.
tmux new-session -d -s {unit_name} 2>&1 | cat > /dev/null || true
tmux attach -t {unit_name}
"""
