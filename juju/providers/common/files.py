"""
Directory based file storage (for local and dummy).
"""

import os

from twisted.internet.defer import fail, succeed
from juju.errors import FileNotFound


class FileStorage(object):

    def __init__(self, path):
        self._path = path

    def get(self, name):
        file_path = os.path.join(
            self._path, *filter(None, name.split("/")))
        if os.path.exists(file_path):
            return succeed(open(file_path))
        return fail(FileNotFound(file_path))

    def put(self, remote_path, file_object):
        store_path = os.path.join(
            self._path, *filter(None, remote_path.split("/")))
        store_path = os.path.abspath(store_path)
        if not store_path.startswith(self._path):
            return fail(AssertionError("Invalid Remote Path %s" % remote_path))

        parent_store_path = os.path.dirname(store_path)
        if not os.path.exists(parent_store_path):
            os.makedirs(parent_store_path)
        with open(store_path, "wb") as f:
            f.write(file_object.read())
        return succeed(True)

    def get_url(self, name):
        file_path = os.path.abspath(os.path.join(
            self._path, *filter(None, name.split("/"))))
        return "file://%s" % file_path
