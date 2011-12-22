import errno
import os
import signal
from StringIO import StringIO
import subprocess
import yaml

from twisted.internet.defer import inlineCallbacks, returnValue

from juju.errors import ProviderError, FileNotFound
from juju.providers.common.files import FileStorage


SERVER_URL_KEY = "local-storage-url"


class StorageServer(object):

    def __init__(
        self, pid_file, storage_dir=None, host=None, port=None, log_file=None):
        """Management facade for a web server on top of the provider storage.

        :param pid_file: Path to the web server pid file.
        :param host: Host interface to bind to.
        :param port: Port to bind to.
        :param log_file: Path to store log output.
        """
        if storage_dir:
            storage_dir = os.path.abspath(storage_dir)
        self._storage_dir = storage_dir
        self._host = host
        self._port = port
        self._pid_file = pid_file
        self._log_file = log_file

    @inlineCallbacks
    def start(self):
        """Start the storage server.

        Also stores the storage server url directly into provider storage.
        """
        assert (self._storage_dir and self._host
                and self._port and self._log_file), "Missing start params."
        assert os.path.exists(self._storage_dir), "Invalid storage directory"

        storage = LocalStorage(self._storage_dir)
        yield storage.put(
            SERVER_URL_KEY,
            StringIO(yaml.safe_dump(
                {"storage-url": "http://%s:%s/" % (
                    self._host, self._port)})))

        subprocess.check_output(
            ["twistd",
            "--pidfile", self._pid_file,
            "--logfile", self._log_file,
            "-d", self._storage_dir,
            "web", "--port",
            "tcp:%s:interface=%s" % (self._port, self._host),
             "--path", self._storage_dir])

    def stop(self):
        """Stop the storage server.
        """
        try:
            with open(self._pid_file) as pid_file:
                pid = int(pid_file.read().strip())
        except IOError:
            # No pid, move on
            return

        try:
            os.kill(pid, 0)
        except OSError, e:
            if e.errno == errno.ESRCH:  # No such process, already dead.
                return
            raise

        os.kill(pid, signal.SIGKILL)


class LocalStorage(FileStorage):

    @inlineCallbacks
    def get_url(self, key):
        """Get a network url to a local provider storage.

        The command line tools directly utilize the disk backed
        storage. The agents will use the read only web interface
        provided by the StorageServer to download resources, as
        in the local provider scenario they won't always have
        direct disk access.
        """
        try:
            storage_data = (yield self.get(SERVER_URL_KEY)).read()
        except FileNotFound:
            storage_data = ""

        if not storage_data or not "storage-url" in storage_data:
            raise ProviderError("Storage not initialized")
        url = yaml.load(storage_data)["storage-url"]
        returnValue(url + key)
