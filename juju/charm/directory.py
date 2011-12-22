import os
import zipfile
import tempfile

from juju.charm.base import CharmBase, get_revision
from juju.charm.bundle import CharmBundle
from juju.charm.config import ConfigOptions
from juju.charm.metadata import MetaData


class CharmDirectory(CharmBase):
    """Directory that holds charm content.

    :param path: Path to charm directory

    The directory must contain the following files::

    - ``metadata.yaml``

    """

    def __init__(self, path):
        self.path = path
        self.metadata = MetaData(os.path.join(path, "metadata.yaml"))

        revision_content = None
        revision_path = os.path.join(self.path, "revision")
        if os.path.exists(revision_path):
            with open(revision_path) as f:
                revision_content = f.read()
        self._revision = get_revision(
            revision_content, self.metadata, self.path)
        if self._revision is None:
            self.set_revision(0)
        elif revision_content is None:
            self.set_revision(self._revision)

        self.config = ConfigOptions()
        self.config.load(os.path.join(path, "config.yaml"))
        self._temp_bundle = None
        self._temp_bundle_file = None

    def get_revision(self):
        return self._revision

    def set_revision(self, revision):
        self._revision = revision
        with open(os.path.join(self.path, "revision"), "w") as f:
            f.write(str(revision) + "\n")

    def make_archive(self, path):
        """Create archive of directory and write to ``path``.

        :param path: Path to archive

        - build/* - This is used for packing the charm itself and any
                    similar tasks.
        - */.*    - Hidden files are all ignored for now.  This will most
                    likely be changed into a specific ignore list (.bzr, etc)
        """

        zf = zipfile.ZipFile(path, 'w', zipfile.ZIP_DEFLATED)

        for dirpath, dirnames, filenames in os.walk(self.path):
            relative_path = dirpath[len(self.path) + 1:]
            if relative_path and not self._ignore(relative_path):
                zf.write(dirpath, relative_path)
            for name in filenames:
                archive_name = os.path.join(relative_path, name)
                if not self._ignore(archive_name):
                    real_path = os.path.join(dirpath, name)
                    zf.write(real_path, archive_name)

        zf.close()

    def _ignore(self, path):
        if path == "build" or path.startswith("build/"):
            return True
        if path.startswith('.'):
            return True

    def as_bundle(self):
        if self._temp_bundle is None:
            prefix = "%s-%d.charm." % (self.metadata.name, self.get_revision())
            temp_file = tempfile.NamedTemporaryFile(prefix=prefix)
            self.make_archive(temp_file.name)
            self._temp_bundle = CharmBundle(temp_file.name)
            # Attach the life time of temp_file to self:
            self._temp_bundle_file = temp_file
        return self._temp_bundle

    def as_directory(self):
        return self

    def compute_sha256(self):
        """
        Compute sha256, based on the bundle.
        """
        return self.as_bundle().compute_sha256()
