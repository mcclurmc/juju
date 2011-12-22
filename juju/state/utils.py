from collections import namedtuple
from UserDict import DictMixin
import socket
import errno
import time

from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.threads import deferToThread
from txzookeeper.utils import retry_change
import yaml
import zookeeper

from juju.state.errors import StateChanged
from juju.state.errors import StateNotFound


class PortWatcher(object):

    def __init__(self, host, port, timeout, listen=False):
        """Watches a `port` on `host` until available.

        Used with `sync_wait` or `async_wait` methods.

        Times out after `timeout` seconds. Normally the watcher is
        used to determine when a port starts listening for client use,
        but the parameter `listen` may be used to wait for when the
        port can be used by a server (because of previous usage
        without properly closing).
        """
        self._host = host
        self._port = port
        self._timeout = timeout
        self._stop = False
        self._listen = listen

    def stop(self, result=None):
        """Interrupt port watching in its loop."""
        self._stop = True
        return result

    def sync_wait(self):
        """Waits until the port is available, or `socket.error`."""
        until = time.time() + self._timeout
        while time.time() < until and not self._stop:
            sock = socket.socket()
            sock.settimeout(1)
            try:
                if self._listen:
                    sock.bind((self._host, self._port))
                else:
                    sock.connect((self._host, self._port))
            except socket.timeout:
                time.sleep(0.5)
            except socket.error, e:
                if e.args[0] not in (
                    errno.EWOULDBLOCK, errno.ECONNREFUSED, errno.EADDRINUSE):
                    raise
                else:
                    # Sleep, otherwise this code will create useless sockets
                    time.sleep(0.5)
            else:
                sock.close()
                return True

        if self._stop:
            return

        raise socket.timeout("could not connect before timeout")

    def async_wait(self, *ignored):
        """Returns a deferred that is called back on port available.

        An exception is returned through the deferred errback if the
        port wait timed out, or another problem occurs."""
        return deferToThread(self.sync_wait)


@inlineCallbacks
def remove_tree(client, path):
    children = yield client.get_children(path)
    for child in children:
        yield remove_tree(client, "%s/%s" % (path, child))
    yield client.delete(path)


def get_open_port(host=""):
    """Get an open port on the machine.
    """
    temp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    temp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    temp_sock.bind((host, 0))
    port = temp_sock.getsockname()[1]
    temp_sock.close()
    del temp_sock
    return port


def dict_merge(d1, d2):
    """Return a union of dicts if they have no conflicting values.

    Else raise a StateChanged error.
    """
    must_match = set(d1).intersection(d2)
    for k in must_match:
        if not d1[k] == d2[k]:
            raise StateChanged()

    d = {}
    d.update(d1)
    d.update(d2)
    return d


class DeletedItem(namedtuple("DeletedItem", "key old")):
    """Represents deleted items when :class:`YAMLState` writes."""
    def __str__(self):
        return "Setting deleted: %r (was %.100r)" % (self.key, self.old)


class ModifiedItem(namedtuple("ModifiedItem", "key old new")):
    """Represents modified items when :class:`YAMLState` writes."""
    def __str__(self):
        return "Setting changed: %r=%.100r (was %.100r)" % \
            (self.key, self.new, self.old)


class AddedItem(namedtuple("AddedItem", "key new")):
    """Represents added items when :class:`YAMLState` writes."""
    def __str__(self):
        return "Setting changed: %r=%.100r (was unset)" % \
            (self.key, self.new)


class YAMLState(DictMixin):
    """Provides a dict like interface around a Zookeeper node
    containing serialised YAML data. The dict provided represents the
    local view of all node data.

    `write` writes this information into the Zookeeper node, using a
    retry until success and merges against any existing keys in ZK.

    YAMLState(client, path)

    `client`: a Zookeeper client
    `path`: the path of the Zookeeper node to manage

    The state of this object always represents the product of the
    pristine settings (from Zookeeper) and the pending writes.

    All mutation to the dict expects the use of inlineCallbacks and a
    yield. This includes set and update.
    """
    # By always updating 'self' on mutation we don't need to do any
    # special handling on data access (gets).

    def __init__(self, client, path):
        self._client = client
        self._path = path
        self._pristine_cache = None
        self._cache = {}

    @inlineCallbacks
    def read(self, required=False):
        """Read Zookeeper state.

        Read in the current Zookeeper state for this node. This
        operation should be called prior to other interactions with
        this object.

        `required`: boolean indicating if the node existence should be
        required at read time. Normally write will create the node if
        the path is possible. This allows for simplified catching of
        errors.

        """
        self._pristine_cache = {}
        self._cache = {}
        try:
            data, stat = yield self._client.get(self._path)
            data = yaml.load(data)
            if data:
                self._pristine_cache = data
                self._cache = data.copy()
        except zookeeper.NoNodeException:
            if required:
                raise StateNotFound(self._path)

    def _check(self):
        """Verify that sync was called for operations which expect it."""
        if self._pristine_cache is None:
            raise ValueError(
                "You must call .read() on %s instance before use." % (
                    self.__class__.__name__,))

    ## DictMixin Interface
    def keys(self):
        return self._cache.keys()

    def __getitem__(self, key):
        self._check()
        return self._cache[key]

    def __setitem__(self, key, value):
        self._check()
        self._cache[key] = value

    def __delitem__(self, key):
        self._check()
        del self._cache[key]

    @inlineCallbacks
    def write(self):
        """Write object state to Zookeeper.

        This will write the current state of the object to Zookeeper,
        taking the final merged state as the new one, and resetting
        any write buffers.
        """
        self._check()
        cache = self._cache
        pristine_cache = self._pristine_cache
        self._pristine_cache = cache.copy()

        # Used by `apply_changes` function to return the changes to
        # this scope.
        changes = []

        def apply_changes(content, stat):
            """Apply the local state to the Zookeeper node state."""
            del changes[:]
            current = yaml.load(content) if content else {}
            missing = object()
            for key in set(pristine_cache).union(cache):
                old_value = pristine_cache.get(key, missing)
                new_value = cache.get(key, missing)
                if old_value != new_value:
                    if new_value != missing:
                        current[key] = new_value
                        if old_value != missing:
                            changes.append(
                                ModifiedItem(key, old_value, new_value))
                        else:
                            changes.append(AddedItem(key, new_value))
                    elif key in current:
                        del current[key]
                        changes.append(DeletedItem(key, old_value))
            return yaml.safe_dump(current)

        # Apply the change till it takes.
        yield retry_change(self._client, self._path, apply_changes)
        returnValue(changes)
