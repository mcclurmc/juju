from cStringIO import StringIO
from yaml import safe_dump, load

from juju.errors import FileNotFound

_STATE_FILE = "provider-state"


class LoadState(object):
    """Generic state-loading operation.

    Note that most juju state should be stored in zookeeper nodes; this
    is only for state which must be knowable without access to a zookeeper.
    """

    def __init__(self, provider):
        self._provider = provider

    def run(self):
        """Actually load the state.

        :rtype: dict or False
        """
        storage = self._provider.get_file_storage()
        d = storage.get(_STATE_FILE)
        d.addCallback(self._deserialize)
        d.addErrback(self._no_data)
        return d

    def _deserialize(self, data):
        return load(data.read()) or False

    def _no_data(self, failure):
        failure.trap(FileNotFound)
        return False


class SaveState(object):
    """Generic state-saving operation.

    Note that most juju state should be stored in zookeeper nodes; this
    is only for state which must be knowable without access to a zookeeper.
    """

    def __init__(self, provider):
        self._provider = provider

    def run(self, state):
        """Actually save new state.

        :param dict state: state to save.
        """
        storage = self._provider.get_file_storage()
        data = safe_dump(state)
        return storage.put(_STATE_FILE, StringIO(data))
