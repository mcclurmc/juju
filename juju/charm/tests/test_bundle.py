import os
import hashlib
import inspect
import yaml
import zipfile

from juju.lib.testing import TestCase
from juju.lib.filehash import compute_file_hash
from juju.charm.metadata import MetaData
from juju.charm.bundle import CharmBundle
from juju.errors import CharmError
from juju.charm.directory import CharmDirectory
from juju.charm.provider import get_charm_from_path

from juju.charm import tests

repository_directory = os.path.join(
    os.path.dirname(inspect.getabsfile(tests)), "repository")

sample_directory = os.path.join(repository_directory, "series", "dummy")


class BundleTest(TestCase):

    def setUp(self):
        directory = CharmDirectory(sample_directory)

        # add sample directory
        self.filename = self.makeFile(suffix=".charm")
        directory.make_archive(self.filename)

    def test_initialization(self):
        bundle = CharmBundle(self.filename)
        self.assertEquals(bundle.path, self.filename)

    def test_error_not_zip(self):
        filename = self.makeFile("@#$@$")
        err = self.assertRaises(CharmError, CharmBundle, filename)
        self.assertEquals(
            str(err),
            "Error processing %r: must be a zip file (File is not a zip file)"
            % filename)

    def test_error_zip_but_doesnt_have_metadata_file(self):
        filename = self.makeFile()
        zf = zipfile.ZipFile(filename, 'w')
        zf.writestr("README.txt", "This is not a valid charm.")
        zf.close()

        err = self.assertRaises(CharmError, CharmBundle, filename)
        self.assertEquals(
            str(err),
            "Error processing %r: charm does not contain required "
            "file 'metadata.yaml'" % filename)

    def test_no_revision_at_all(self):
        filename = self.makeFile()
        zf_dst = zipfile.ZipFile(filename, "w")
        zf_src = zipfile.ZipFile(self.filename, "r")
        for name in zf_src.namelist():
            if name == "revision":
                continue
            zf_dst.writestr(name, zf_src.read(name))
        zf_src.close()
        zf_dst.close()

        err = self.assertRaises(CharmError, CharmBundle, filename)
        self.assertEquals(
            str(err), "Error processing %r: has no revision" % filename)

    def test_revision_in_metadata(self):
        filename = self.makeFile()
        zf_dst = zipfile.ZipFile(filename, "w")
        zf_src = zipfile.ZipFile(self.filename, "r")
        for name in zf_src.namelist():
            if name == "revision":
                continue
            content = zf_src.read(name)
            if name == "metadata.yaml":
                data = yaml.load(content)
                data["revision"] = 303
                content = yaml.dump(data)
            zf_dst.writestr(name, content)
        zf_src.close()
        zf_dst.close()

        charm = CharmBundle(filename)
        self.assertEquals(charm.get_revision(), 303)

    def test_competing_revisions(self):
        zf = zipfile.ZipFile(self.filename, "a")
        zf.writestr("revision", "999")
        data = yaml.load(zf.read("metadata.yaml"))
        data["revision"] = 303
        zf.writestr("metadata.yaml", yaml.dump(data))
        zf.close()

        charm = CharmBundle(self.filename)
        self.assertEquals(charm.get_revision(), 999)

    def test_cannot_set_revision(self):
        charm = CharmBundle(self.filename)
        self.assertRaises(NotImplementedError, charm.set_revision, 123)

    def test_bundled_config(self):
        """Make sure that config is accessible from a bundle."""
        from juju.charm.tests.test_config import sample_yaml_data
        bundle = CharmBundle(self.filename)
        self.assertEquals(bundle.config.get_serialization_data(),
                          sample_yaml_data)

    def test_info(self):
        bundle = CharmBundle(self.filename)
        self.assertTrue(bundle.metadata is not None)
        self.assertTrue(isinstance(bundle.metadata, MetaData))
        self.assertEquals(bundle.metadata.name, "dummy")

    def test_as_bundle(self):
        bundle = CharmBundle(self.filename)
        self.assertEquals(bundle.as_bundle(), bundle)

    def test_executable_extraction(self):
        sample_directory = os.path.join(
            repository_directory, "series", "varnish-alternative")
        charm_directory = CharmDirectory(sample_directory)
        source_hook_path = os.path.join(sample_directory, "hooks", "install")
        self.assertTrue(os.access(source_hook_path, os.X_OK))
        bundle = charm_directory.as_bundle()
        directory = bundle.as_directory()
        hook_path = os.path.join(directory.path, "hooks", "install")
        self.assertTrue(os.access(hook_path, os.X_OK))

    def get_charm_sha256(self):
        return compute_file_hash(hashlib.sha256, self.filename)

    def test_compute_sha256(self):
        sha256 = self.get_charm_sha256()
        bundle = CharmBundle(self.filename)
        self.assertEquals(bundle.compute_sha256(), sha256)

    def test_charm_base_inheritance(self):
        """
        get_sha256() should be implemented in the base class,
        and should use compute_sha256 to calculate the digest.
        """
        sha256 = self.get_charm_sha256()
        bundle = CharmBundle(self.filename)
        self.assertEquals(bundle.get_sha256(), sha256)

    def test_file_handle_as_path(self):
        sha256 = self.get_charm_sha256()
        fh = open(self.filename)
        bundle = CharmBundle(fh)
        self.assertEquals(bundle.get_sha256(), sha256)

    def test_extract_to(self):
        filename = self.makeFile()
        charm = get_charm_from_path(self.filename)
        f2 = charm.extract_to(filename)

        # f2 should be a charm directory
        self.assertInstance(f2, CharmDirectory)
        self.assertInstance(f2.get_sha256(), basestring)
        self.assertEqual(f2.path, filename)

    def test_as_directory(self):
        filename = self.makeFile()
        charm = get_charm_from_path(self.filename)
        f2 = charm.as_directory()

        # f2 should be a charm directory
        self.assertInstance(f2, CharmDirectory)
        self.assertInstance(f2.get_sha256(), basestring)
        # verify that it was extracted to a new temp dirname
        self.assertNotEqual(f2.path, filename)

        fn = os.path.split(f2.path)[1]
        # verify that it used the expected prefix
        self.assertStartsWith(fn, "tmp")
