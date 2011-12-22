import os
from yaml import load

from twisted.python.failure import Failure

from juju.environment.tests.test_config import EnvironmentsConfigTestBase
from juju.errors import ProviderInteractionError, JujuError
from juju.lib.testing import TestCase
from juju.providers.common.utils import (
    convert_unknown_error, format_cloud_init, get_user_authorized_keys)


class CommonProviderTests(EnvironmentsConfigTestBase):

    def setUp(self):
        super(CommonProviderTests, self).setUp()
        os.mkdir(os.path.join(self.tmp_home, ".ssh"))

    def test_key_retrieval(self):
        """
        The F{juju.providers.common.get_user_authorized_keys} will retrieve
        a key's contents from disk and return the key data.
        """
        data = "alice in wonderland"
        self.makeFile(
            data,
            path=os.path.join(self.tmp_home, ".ssh", "identity_foobar.pub"))
        config = {"authorized-keys-path": "identity_foobar.pub"}
        key_data = get_user_authorized_keys(config)
        self.assertEqual(data, key_data)

    def test_content_utilized(self):
        """
        The entire key content can be specified via configuration.
        """
        config = {"authorized-keys": "abc"}
        key_data = get_user_authorized_keys(config)
        self.assertEqual(key_data, config["authorized-keys"])

    def test_qualified_path(self):
        """
        An alternate path can be specified with user directory expansion.
        """
        alternate_dir = os.path.join(self.tmp_home, "es")
        os.mkdir(alternate_dir)
        self.makeFile(
            "public-abc",
            path=os.path.join(alternate_dir, "juju_key.pub"))
        config = {"authorized-keys-path": "~/es/juju_key.pub"}
        key_data = get_user_authorized_keys(config)
        self.assertEqual(key_data, "public-abc")

    def test_invalid_key_specified(self):
        """If an invalid key is specified, a LookupError is raised."""
        config = {"authorized-keys-path": "zebra_moon.pub"}
        self.assertRaises(LookupError, get_user_authorized_keys, config)

    def test_convert_unknown_error(self):
        error = self.assertRaises(
            ProviderInteractionError,
            convert_unknown_error,
            OSError("Bad"))
        self.assertInstance(error, JujuError)
        self.assertEqual(
            str(error), "Unexpected OSError interacting with provider: Bad")

    def test_convert_unknown_error_ignores_juju_error(self):
        error = self.assertRaises(
            JujuError,
            convert_unknown_error,
            JujuError("Magic"))
        self.assertEqual(
            str(error),
            "Magic")

    def test_convert_unknown_error_ignores_juju_failure(self):
        failure = convert_unknown_error(Failure(JujuError("Magic")))
        self.assertTrue(isinstance(failure, Failure))
        self.assertEqual(failure.value.__class__, JujuError)

    def test_convert_unknown_error_with_failure(self):
        failure = convert_unknown_error(Failure(OSError("Bad")))
        self.assertTrue(isinstance(failure, Failure))
        self.assertInstance(failure.value, ProviderInteractionError)
        self.assertEqual(
            str(failure.value),
            "Unexpected OSError interacting with provider: Bad")


class FormatCloudInitTest(TestCase):

    def test_format_cloud_init_with_data(self):
        """The format cloud init creates a user-data cloud-init config file.

        It can be used to pass an additional data dictionary as well for
        use by the launched machine.
        """
        packages = ["python-lxml"]
        scripts = ["wget http://lwn.net > /tmp/out"]
        repositories = ["ppa:juju/pkgs"]
        output = format_cloud_init(
            ["zebra"],
            packages=packages,
            scripts=scripts,
            repositories=repositories,
            data={"magic": [1, 2, 3]})

        lines = output.split("\n")
        self.assertEqual(lines.pop(0), "#cloud-config")
        config = load("\n".join(lines))
        self.assertEqual(config["ssh_authorized_keys"], ["zebra"])
        self.assertTrue(config["apt-update"])
        self.assertTrue(config["apt-upgrade"])
        formatted_repos = [dict(source=r) for r in repositories]
        self.assertEqual(config["apt_sources"], formatted_repos)
        self.assertEqual(config["runcmd"], scripts)
        self.assertEqual(config["machine-data"]["magic"], [1, 2, 3])
