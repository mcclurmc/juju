import hashlib

from juju.lib.testing import TestCase
from juju.lib.filehash import compute_file_hash


class FileHashTest(TestCase):

    def test_compute_file_hash(self):
        for type in (hashlib.sha256, hashlib.md5):
            filename = self.makeFile("content")
            digest = compute_file_hash(type, filename)
            self.assertEquals(digest, type("content").hexdigest())
