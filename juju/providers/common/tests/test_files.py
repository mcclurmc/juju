import os
from StringIO import StringIO

from twisted.internet.defer import inlineCallbacks

from juju.lib.testing import TestCase
from juju.providers.common.files import FileStorage

from juju.errors import FileNotFound


class FileStorageTest(TestCase):

    def setUp(self):
        super(FileStorageTest, self).setUp()
        self.storage_dir = self.makeDir()
        self.storage = FileStorage(self.storage_dir)

    def test_get_file_non_existent(self):
        return self.failUnlessFailure(self.storage.get("/abc"), FileNotFound)

    def test_get_url(self):
        url = self.storage.get_url("/abc.txt")
        self.assertEqual(url, "file://%s/abc.txt" % self.storage_dir)

    @inlineCallbacks
    def test_get_file(self):
        path = os.path.join(self.storage_dir, "abc.txt")
        self.makeFile("content", path=path)
        fh = yield self.storage.get("/abc.txt")
        self.assertEqual(fh.read(), "content")

    @inlineCallbacks
    def test_put_and_get_file(self):
        file_obj = StringIO("rabbits")
        yield self.storage.put("/magic/beans.txt", file_obj)
        fh = yield self.storage.get("/magic/beans.txt")
        self.assertEqual(fh.read(), "rabbits")

    @inlineCallbacks
    def test_put_same_path_multiple(self):
        file_obj = StringIO("rabbits")
        yield self.storage.put("/magic/beans.txt", file_obj)
        file_obj = StringIO("elephant")
        yield self.storage.put("/magic/beans.txt", file_obj)
        fh = yield self.storage.get("/magic/beans.txt")
        self.assertEqual(fh.read(), "elephant")

    @inlineCallbacks
    def test_put_file_relative_path(self):
        file_obj = StringIO("moon")
        yield self.storage.put("zebra/../zoo/reptiles/snakes.txt", file_obj)
        fh = yield self.storage.get("/zoo/reptiles/snakes.txt")
        self.assertEqual(fh.read(), "moon")

    def test_put_file_invalid_relative_path(self):
        """Relative paths work as long as their contained in the storage path.
        """
        file_obj = StringIO("moon")
        return self.failUnlessFailure(
            self.storage.put("../../etc/profile.txt", file_obj),
            AssertionError)
