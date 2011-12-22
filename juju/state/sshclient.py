import re
import socket
import time

from twisted.internet.defer import Deferred, inlineCallbacks, returnValue
from txzookeeper.client import ConnectionTimeoutException

from juju.errors import NoConnection, InvalidHost, InvalidUser
from juju.state.security import SecurityPolicyConnection
from juju.state.sshforward import forward_port, ClientTunnelProtocol

from .utils import PortWatcher, get_open_port

SERVER_RE = re.compile("^(\S+):(\d+)$")


class SSHClient(SecurityPolicyConnection):
    """
    A ZookeeperClient which will internally handle an SSH tunnel
    to connect to the remote host.
    """

    remote_user = "ubuntu"
    _process = None

    @inlineCallbacks
    def _internal_connect(self, server, timeout, share=False):
        """Connect to the remote host provided via an ssh port forward.

        An SSH process is fired with port forwarding established on localhost
        22181, which the zookeeper client connects to.

        :param server: Remote host to connect to, specified as hostname:port
        :type string

        :param timeout: An timeout interval in seconds.
        :type float

        Returns a connected client or error.
        """
        hostname, port = self._parse_servers(server or self._servers)
        start_time = time.time()

        # Determine which port we'll be using.
        local_port = get_open_port()
        port_watcher = PortWatcher("localhost", local_port, timeout)

        tunnel_error = Deferred()
        # On a tunnel error, stop the port watch early and bail with error.
        tunnel_error.addErrback(port_watcher.stop)
        # If a tunnel error happens now or later, close the connection.
        tunnel_error.addErrback(lambda x: self.close())

        # Setup tunnel via an ssh process for port forwarding.
        protocol = ClientTunnelProtocol(self, tunnel_error)
        self._process = forward_port(
            self.remote_user, local_port, hostname, int(port),
            process_protocol=protocol, share=share)

        # Wait for the tunneled port to open.
        try:
            yield port_watcher.async_wait()
        except socket.error:
            self.close()  # Stop the tunnel process.
            raise ConnectionTimeoutException("could not connect")
        else:
            # If we stopped because of a tunnel error, raise it.
            if protocol.error:
                yield tunnel_error

        # Check timeout
        new_timeout = timeout - (time.time() - start_time)
        if new_timeout <= 0:
            self.close()
            raise ConnectionTimeoutException(
                "could not connect before timeout")

        # Connect the client
        try:
            yield super(SSHClient, self).connect(
                "localhost:%d" % local_port, new_timeout)
        except:
            self.close()  # Stop the tunnel
            raise

        returnValue(self)

    def _parse_servers(self, servers):
        """Extract a server host and port."""
        match = SERVER_RE.match(servers)
        hostname, port = match.groups()
        return hostname, port

    @inlineCallbacks
    def connect(self, server=None, timeout=60, share=False):
        """Probe ZK is accessible via ssh tunnel, return client on success."""
        until = time.time() + timeout
        num_retries = 0
        while time.time() < until:
            num_retries += 1
            try:
                yield self._internal_connect(
                    server, timeout=until - time.time(), share=share)
            except ConnectionTimeoutException:
                # Reraises implicitly, but with the number of retries
                # (see the outside of this loop); this circumstance
                # would occur if the port watcher timed out before we
                # got anything from the tunnel
                break
            except InvalidHost:
                # No point in retrying if the host itself is invalid
                self.close()
                raise
            except InvalidUser:
                # Or if the user doesn't have a login
                self.close()
                raise
            except NoConnection:
                # Otherwise retry after ssh tunnel forwarding failures
                self.close()
            else:
                returnValue(self)
        self.close()
        # we raise ConnectionTimeoutException (rather than one of our own, with
        # the same meaning) to maintain ZookeeperClient interface
        raise ConnectionTimeoutException(
            "could not connect before timeout after %d retries" % num_retries)

    def close(self):
        """Close the zookeeper connection, and the associated ssh tunnel."""
        super(SSHClient, self).close()
        if self._process is not None:
            self._process.signalProcess("TERM")
            self._process.loseConnection()
            self._process = None
