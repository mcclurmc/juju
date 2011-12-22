import os
from StringIO  import StringIO
import yaml

from twisted.internet.defer import inlineCallbacks
from twisted.web.client import getPage

from juju.lib.testing import TestCase


from juju.errors import ProviderError
from juju.providers.local.files import (
    LocalStorage, StorageServer, SERVER_URL_KEY)
from juju.state.utils import get_open_port


class WebFileStorageTest(TestCase):

    def setUp(self):
        self._storage_path = self.makeDir()
        self._storage = LocalStorage(self._storage_path)
        self._log_path = self.makeFile()
        self._pid_path = self.makeFile()
        self._port = get_open_port()
        self._server = StorageServer(
            self._pid_path, self._storage_path, "localhost",
            get_open_port(), self._log_path)

    @inlineCallbacks
    def test_start_stop(self):
        yield self._storage.put("abc", StringIO("hello world"))
        yield self._server.start()
        storage_url = yield self._storage.get_url("abc")
        contents = yield getPage(storage_url)
        self.assertEqual("hello world", contents)
        self._server.stop()
        # Stopping multiple times is fine.
        self._server.stop()

    def test_start_missing_args(self):
        server = StorageServer(self._pid_path)
        return self.assertFailure(server.start(), AssertionError)

    def test_start_invalid_directory(self):
        os.rmdir(self._storage_path)
        return self.assertFailure(self._server.start(), AssertionError)

    def test_stop_missing_pid(self):
        server = StorageServer(self._pid_path)
        server.stop()


class FileStorageTest(TestCase):

    def setUp(self):
        self._storage = LocalStorage(self.makeDir())

    @inlineCallbacks
    def test_get_url(self):
        yield self.assertFailure(self._storage.get_url("abc"), ProviderError)
        self._storage.put(SERVER_URL_KEY, StringIO("abc"))
        yield self.assertFailure(self._storage.get_url("abc"), ProviderError)
        self._storage.put(
            SERVER_URL_KEY,
            StringIO(yaml.dump({"storage-url": "http://localhost/"})))

        self.assertEqual((yield self._storage.get_url("abc")),
                         "http://localhost/abc")
