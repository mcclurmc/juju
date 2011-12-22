"""
An SSH forwarding connection using ssh as a spawned process from the twisted
reactor.
"""
import os
import logging

from twisted.internet.protocol import ProcessProtocol
from juju.errors import (
    FileNotFound, NoConnection, InvalidHost, InvalidUser)


log = logging.getLogger("juju.state.sshforward")


def _verify_ports(*ports):
    for port in ports:
        if not isinstance(port, int):
            if not port.isdigit():
                raise SyntaxError("Port must be integer, got %s." % (port))


class TunnelProtocol(ProcessProtocol):

    def errReceived(self, data):
        """
        Bespoke stderr interpretation to determine error and connection states.
        """
        log.error("SSH tunnel error %s", data)


class ClientTunnelProtocol(ProcessProtocol):

    def __init__(self, client, error_deferred):
        self._client = client
        self._deferred = error_deferred

    @property
    def error(self):
        return self._deferred is None

    def errReceived(self, data):
        """Bespoke stderr interpretation to determine error/connection states.

        On errors, invokes the client.close method.
        """
        # Even with a null host file, and ignoring strict host checking
        # we'll end up with this output, suppress it as its effectively
        # normal for our usage...
        if data.startswith("Warning: Permanently added"):
            return

        message = ex = None

        if data.startswith("ssh: Could not resolve hostname"):
            message = "Invalid host for SSH forwarding: %s" % data
            ex = InvalidHost(message)
        elif data.startswith("Permission denied"):
            message = "Invalid SSH key"
            ex = InvalidUser(message)
        elif "Connection refused" in data:
            ex = NoConnection("Connection refused")
        else:
            # Handle any other error
            message = "SSH forwarding error: %s" % data
            ex = NoConnection(message)

        if self._deferred:
            # The provider will retry repeatedly till connections work
            # only log uncommon errors.
            if message:
                log.error(message)
            self._deferred.errback(ex)
            self._deferred = None

        self._client.close()
        return True


def forward_port(remote_user, local_port, remote_host, remote_port,
                 private_key_path=None, process_protocol=None,
                 share=False):
    """
    Fork an ssh process to enable port forwarding from the given local host
    port to the remote host, remote port.

    @param local_port: The local port that should be bound for the forward.
    @type int

    @param remote_user: The user for login into the remote host.
    @type str

    @param remote_host: The name or ip address of the remote host.
    @type str

    @param remote_port: The remote port that is forwarded to.
    @type int


    @param private_key: The identity file that the private key should be read
                        from. If none is specified.

    @param process_protocl: The process interaction protocol
    @type C{ProcessProtocol}
    """
    _verify_ports(local_port, remote_port)

    info = {"remote_user": remote_user,
            "local_port": local_port,
            "remote_host": remote_host,
            "remote_port": remote_port}

    from twisted.internet import reactor

    args = [
        "ssh",
        "-T",
        "-o", "PasswordAuthentication no",
        "-Llocalhost:%(local_port)s:localhost:%(remote_port)s" % info,
        "%(remote_user)s@%(remote_host)s" % info]

    args[2:2] = prepare_ssh_sharing(auto_master=share)

    if private_key_path:
        private_key_path = os.path.expandvars(os.path.expanduser(
            private_key_path))
        if not os.path.exists(private_key_path):
            raise FileNotFound("Private key file not found: %r." % \
                               private_key_path)
        args[2:2] = ["-i", private_key_path]
        log.debug("Using private key from %s." % private_key_path)

    # use the existing process environment to utilize an ssh agent if present.
    log.debug("Spawning SSH process with %s." % (
        " ".join("%s=\"%s\"" % pair for pair in info.items())))

    if process_protocol is None:
        process_protocol = TunnelProtocol()

    return reactor.spawnProcess(
        process_protocol,
        "/usr/bin/ssh",
        args,
        env=os.environ)


def prepare_ssh_sharing(auto_master=False):
    path = os.path.expanduser("~/.juju/ssh/master-%r@%h:%p")
    pathdir = os.path.dirname(path)
    if auto_master and not os.path.isdir(pathdir):
        os.makedirs(pathdir)
    if auto_master:
        master = "auto"
    else:
        master = "no"
    args = [
        "-o", "ControlPath " + path,
        "-o", "ControlMaster " + master,
    ]
    return args
