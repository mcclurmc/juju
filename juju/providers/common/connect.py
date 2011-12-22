import random

from twisted.internet.defer import inlineCallbacks, returnValue

from txzookeeper.client import ConnectionTimeoutException

from juju.errors import EnvironmentNotFound, EnvironmentPending, NoConnection
from juju.lib.twistutils import sleep
from juju.state.sshclient import SSHClient

from .utils import log


class ZookeeperConnect(object):

    def __init__(self, provider):
        self._provider = provider

    @inlineCallbacks
    def run(self, share=False):
        """Attempt to connect to a running zookeeper node, retrying as needed.

        :param bool share: where feasible, attempt to share a connection with
            other clients.

        :return: an open :class:`txzookeeper.client.ZookeeperClient`
        :rtype: :class:`twisted.internet.defer.Deferred`

        :raises: :exc:`juju.errors.EnvironmentNotFound` when no zookeepers
            exist

        Internally this method catches all
        :exc:`juju.errors.EnvironmentPending`, since
        this exception explicitly means that a retry is feasible.

        TODO consider supporting a timeout for this method, instead of
        any such timeouts being done externally.
        """
        log.info("Connecting to environment...")
        while True:
            try:
                client = yield self._internal_connect(share)
                log.info("Connected to environment.")
                returnValue(client)
            except EnvironmentPending as e:
                log.debug("Retrying connection: %s", e)
            except EnvironmentNotFound:
                # Expected if not bootstrapped, simply raise up
                raise
            except Exception as e:
                # Otherwise this is unexpected, log with some details
                log.exception("Cannot connect to environment: %s", e)
                raise

    @inlineCallbacks
    def _internal_connect(self, share):
        """Attempt connection to one of the ZK nodes."""
        candidates = yield self._provider.get_zookeeper_machines()
        assigned = [machine for machine in candidates if machine.dns_name]
        if not assigned:
            yield sleep(1)  # Momentarily backoff
            raise EnvironmentPending("No machines have assigned addresses")

        chosen = random.choice(assigned)
        log.debug("Connecting to environment using %s...", chosen.dns_name)
        try:
            client = yield SSHClient().connect(
                chosen.dns_name + ":2181", timeout=30, share=share)
        except (NoConnection, ConnectionTimeoutException) as e:
            raise EnvironmentPending(
                "Cannot connect to environment using %s "
                "(perhaps still initializing): %s" % (
                    chosen.dns_name, str(e)))

        yield self.wait_for_initialization(client)
        returnValue(client)

    @inlineCallbacks
    def wait_for_initialization(self, client):
        exists_d, watch_d = client.exists_and_watch("/initialized")
        exists = yield exists_d
        if not exists:
            log.debug("Environment still initializing. Will wait.")
            yield watch_d
        else:
            log.debug("Environment is initialized.")
