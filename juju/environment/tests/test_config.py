import os
import yaml

from twisted.internet.defer import inlineCallbacks

from juju.environment import environment
from juju.environment.config import EnvironmentsConfig, SAMPLE_CONFIG
from juju.environment.environment import Environment
from juju.environment.errors import EnvironmentsConfigError
from juju.errors import FileNotFound, FileAlreadyExists

from juju.lib.testing import TestCase


DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "data"))

SAMPLE_ENV = """
environments:
  myfirstenv:
    type: dummy
    foo: bar
  mysecondenv:
    type: dummy
    nofoo: 1
"""

SAMPLE_ORCHESTRA = """
environments:
  sample:
    type: orchestra
    orchestra-server: somewhe.re
    orchestra-user: user
    orchestra-pass: pass
    admin-secret: garden
    acquired-mgmt-class: acquired
    available-mgmt-class: available
    storage-url: http://somewhereel.se
    default-series: oneiric
"""

SAMPLE_LOCAL = """
ensemble: environments

environments:
   sample:
     type: local
     admin-secret: sekret
     default-series: oneiric
"""


class EnvironmentsConfigTestBase(TestCase):

    @inlineCallbacks
    def setUp(self):
        yield super(EnvironmentsConfigTestBase, self).setUp()
        release_path = os.path.join(DATA_DIR, "lsb-release")
        self.patch(environment, "LSB_RELEASE_PATH", release_path)
        self.old_home = os.environ.get("HOME")
        self.tmp_home = self.makeDir()
        self.change_environment(HOME=self.tmp_home)
        self.default_path = os.path.join(self.tmp_home,
                                         ".juju/environments.yaml")
        self.other_path = os.path.join(self.tmp_home,
                                       ".juju/other-environments.yaml")
        self.config = EnvironmentsConfig()

    def write_config(self, config_text, other_path=False):
        if other_path:
            path = self.other_path
        else:
            path = self.default_path
        parent_name = os.path.dirname(path)
        if not os.path.exists(parent_name):
            os.makedirs(parent_name)
        with open(path, "w") as file:
            file.write(config_text)


class EnvironmentsConfigTest(EnvironmentsConfigTestBase):

    def test_get_default_path(self):
        self.assertEquals(self.config.get_default_path(), self.default_path)

    def compare_config(self, config1, sample_config2):
        config1 = yaml.load(config1)
        config2 = yaml.load(
            sample_config2 % config1["environments"]["sample"])
        self.assertEqual(config1, config2)

    def test_load_with_nonexistent_default_path(self):
        """
        Raise an error if load() is called without a path and the
        default doesn't exist.
        """
        try:
            self.config.load()
        except FileNotFound, error:
            self.assertEquals(error.path, self.default_path)
        else:
            self.fail("FileNotFound not raised")

    def test_load_with_nonexistent_custom_path(self):
        """
        Raise an error if load() is called with non-existing path.
        """
        path = "/non/existent/custom/path"
        try:
            self.config.load(path)
        except FileNotFound, error:
            self.assertEquals(error.path, path)
        else:
            self.fail("FileNotFound not raised")

    def test_write_sample_environment_default_path(self):
        """
        write_sample() should write a pre-defined sample configuration file.
        """
        self.config.write_sample()
        self.assertTrue(os.path.isfile(self.default_path))
        with open(self.default_path) as file:
            self.compare_config(file.read(), SAMPLE_CONFIG)
        dir_path = os.path.dirname(self.default_path)
        dir_stat = os.stat(dir_path)
        self.assertEqual(dir_stat.st_mode & 0777, 0700)
        stat = os.stat(self.default_path)
        self.assertEqual(stat.st_mode & 0777, 0600)

    def test_write_sample_contains_secret_key_and_control_bucket(self):
        """
        write_sample() should write a pre-defined sample with an ec2 machine
        provider type, a unique s3 control bucket, and an admin secret key.
        """
        uuid_factory = self.mocker.replace("uuid.uuid4")
        uuid_factory().hex
        self.mocker.result("abc")
        uuid_factory().hex
        self.mocker.result("xyz")
        self.mocker.replay()

        self.config.write_sample()
        self.assertTrue(os.path.isfile(self.default_path))

        with open(self.default_path) as file:
            config = yaml.load(file.read())
            self.assertEqual(
                config["environments"]["sample"]["type"], "ec2")
            self.assertEqual(
                config["environments"]["sample"]["control-bucket"],
                "juju-abc")
            self.assertEqual(
                config["environments"]["sample"]["admin-secret"],
                "xyz")

    def test_write_sample_environment_with_default_path_and_existing_dir(self):
        """
        write_sample() should not fail if the config directory already exists.
        """
        os.mkdir(os.path.dirname(self.default_path))
        self.config.write_sample()
        self.assertTrue(os.path.isfile(self.default_path))
        with open(self.default_path) as file:
            self.compare_config(file.read(), SAMPLE_CONFIG)

    def test_write_sample_environment_with_custom_path(self):
        """
        write_sample() may receive an argument with a custom path.
        """
        path = os.path.join(self.tmp_home, "sample-file")
        self.config.write_sample(path)
        self.assertTrue(os.path.isfile(path))
        with open(path) as file:
            self.compare_config(file.read(), SAMPLE_CONFIG)

    def test_write_sample_wont_overwrite_existing_configuration(self):
        """
        write_sample() must never overwrite an existing file.
        """
        path = self.other_path
        os.makedirs(os.path.dirname(path))
        with open(path, "w") as file:
            file.write("previous content")
        try:
            self.config.write_sample(path)
        except FileAlreadyExists, error:
            self.assertEquals(error.path, path)
        else:
            self.fail("FileAlreadyExists not raised")

    def test_load_empty_environments(self):
        """
        load() must raise an error if there are no enviroments defined
        in the configuration file.
        """
        # Use a different path to ensure the error message is right.
        self.write_config("""
            environments:
            """, other_path=True)
        try:
            self.config.load(self.other_path)
        except EnvironmentsConfigError, error:
            self.assertEquals(str(error),
                "Environments configuration error: %s: "
                "environments: expected dict, got None"
                % self.other_path)
        else:
            self.fail("EnvironmentsConfigError not raised")

    def test_load_environments_with_wrong_type(self):
        """
        load() must raise an error if the "environments:" option in
        the YAML configuration file doesn't have a mapping underneath it.
        """
        # Use a different path to ensure the error message is right.
        self.write_config("""
            environments:
              - list
            """, other_path=True)
        try:
            self.config.load(self.other_path)
        except EnvironmentsConfigError, error:
            self.assertEquals(str(error),
                "Environments configuration error: %s: "
                "environments: expected dict, got ['list']"
                % self.other_path)
        else:
            self.fail("EnvironmentsConfigError not raised")

    def test_wb_parse(self):
        """
        We'll have an exception, and use mocker here to test the
        implementation itself, because we don't want to repeat the
        same tests for both parse() and load(), so we'll just verify
        that one calls the other internally.
        """
        mock = self.mocker.patch(self.config)
        mock.parse(SAMPLE_ENV, self.other_path)
        self.write_config(SAMPLE_ENV, other_path=True)
        self.mocker.replay()
        self.config.load(self.other_path)

    def test_parse_errors_without_filename(self):
        """
        parse() may receive None as the file path, in which case the
        error should not mention it.
        """
        # Use a different path to ensure the error message is right.
        try:
            self.config.parse("""
                environments:
                """)
        except EnvironmentsConfigError, error:
            self.assertEquals(str(error),
                "Environments configuration error: "
                "environments: expected dict, got None")
        else:
            self.fail("EnvironmentsConfigError not raised")

    def test_get_environment_names(self):
        """
        get_names() should return of the environments names contained
        in the configuration file.
        """
        self.write_config(SAMPLE_ENV)
        self.config.load()
        self.assertEquals(self.config.get_names(),
                          ["myfirstenv", "mysecondenv"])

    def test_get_non_existing_environment(self):
        """
        Trying to get() a non-existing configuration name should return None.
        """
        self.config.parse(SAMPLE_ENV)
        self.assertEquals(self.config.get("non-existing"), None)

    def test_load_and_get_environment(self):
        """
        get() should return an Environment instance.
        """
        self.write_config(SAMPLE_ENV)
        self.config.load()
        self.assertEquals(type(self.config.get("myfirstenv")), Environment)

    def test_load_or_write_sample_with_non_existent_config(self):
        """
        When an environment configuration does not exist, the method
        load_or_write_sample() must write down a sample configuration
        file, and raise an error to let the user know his request did
        not work, and he should edit this file.
        """
        try:
            self.config.load_or_write_sample()
        except EnvironmentsConfigError, error:
            self.assertEquals(str(error),
                "No environments configured. Please edit: %s" %
                self.default_path)
            self.assertEquals(error.sample_written, True)
            with open(self.default_path) as file:
                self.compare_config(file.read(), SAMPLE_CONFIG)
        else:
            self.fail("EnvironmentsConfigError not raised")

    def test_environment_config_error_sample_written_defaults_to_false(self):
        """
        The error raised by load_or_write_sample() has a flag to let the
        calling site know if a sample file was actually written down or not.
        It must default to false, naturally.
        """
        error = EnvironmentsConfigError("message")
        self.assertFalse(error.sample_written)

    def test_load_or_write_sample_will_load(self):
        """
        load_or_write_sample() must load the configuration file if it exists.
        """
        self.write_config(SAMPLE_ENV)
        self.config.load_or_write_sample()
        self.assertTrue(self.config.get("myfirstenv"))

    def test_get_default_with_single_environment(self):
        """
        get_default() must return the one defined environment, when it's
        indeed a single one.
        """
        config = yaml.load(SAMPLE_ENV)
        del config["environments"]["mysecondenv"]
        self.write_config(yaml.dump(config))
        self.config.load()
        env = self.config.get_default()
        self.assertEquals(env.name, "myfirstenv")

    def test_get_default_with_named_default(self):
        """
        get_default() must otherwise return the environment named
        through the "default:" option.
        """
        config = yaml.load(SAMPLE_ENV)
        config["default"] = "mysecondenv"
        self.write_config(yaml.dump(config))
        self.config.load()
        env = self.config.get_default()
        self.assertEquals(env.name, "mysecondenv")

    def test_default_is_schema_protected(self):
        """
        The schema should mention the "default:" option as a string.
        """
        config = yaml.load(SAMPLE_ENV)
        config["default"] = 1
        self.write_config(yaml.dump(config))
        error = self.assertRaises(EnvironmentsConfigError, self.config.load)
        self.assertEquals(
            str(error),
            "Environments configuration error: %s: "
            "default: expected string, got 1" % self.default_path)

    def test_get_default_with_named_but_missing_default(self):
        """
        get_default() must raise an error if the environment named through
        the "default:" option isn't found.
        """
        config = yaml.load(SAMPLE_ENV)
        config["default"] = "non-existent"
        # Use a different path to ensure the error message is right.
        self.write_config(yaml.dump(config), other_path=True)
        self.config.load(self.other_path)
        try:
            self.config.get_default()
        except EnvironmentsConfigError, error:
            self.assertEquals(str(error),
                "Default environment 'non-existent' was not found: "
                + self.other_path)
        else:
            self.fail("EnvironmentsConfigError not raised")

    def test_get_default_without_computable_default(self):
        """
        get_default() must raise an error if there are multiple defined
        environments and no explicit default was defined.
        """
        # Use a different path to ensure the error message is right.
        self.write_config(SAMPLE_ENV, other_path=True)
        self.config.load(self.other_path)
        try:
            self.config.get_default()
        except EnvironmentsConfigError, error:
            self.assertEquals(str(error),
                "There are multiple environments and no explicit default "
                "(set one explicitly?): " + self.other_path)
        else:
            self.fail("EnvironmentsConfigError not raised")

    def test_ensure_provider_types_are_set(self):
        """
        The schema should refuse to receive a configuration which
        contains a machine provider configuration without any type
        information.
        """
        config = yaml.load(SAMPLE_ENV)
        # Delete the type.
        del config["environments"]["myfirstenv"]["type"]
        self.write_config(yaml.dump(config), other_path=True)

        try:
            self.config.load(self.other_path)
        except EnvironmentsConfigError, error:
            self.assertEquals(str(error),
                "Environments configuration error: %s: "
                "environments.myfirstenv.type: required value not found"
                % self.other_path)
        else:
            self.fail("EnvironmentsConfigError not raised")

    def test_serialize(self):
        """The config should be able to serialize itself."""

        self.write_config(SAMPLE_ENV)
        self.config.load()
        config = self.config.serialize()
        serialized = yaml.load(SAMPLE_ENV)

        for d in serialized["environments"].values():
            d["dynamicduck"] = "magic"

        self.assertEqual(yaml.load(config), serialized)

    def test_serialize_environment(self):
        """
        The config serialization can take an environment name, in
        which case that environment is serialized in isolation
        into a valid config file that can be loaded.
        """
        self.write_config(SAMPLE_ENV)
        self.config.load()

        data = yaml.load(SAMPLE_ENV)
        del data["environments"]["mysecondenv"]
        data["environments"]["myfirstenv"]["dynamicduck"] = "magic"

        self.assertEqual(
            yaml.load(self.config.serialize("myfirstenv")),
            data)

    def test_load_serialized_environment(self):
        """
        Serialize an environment, and then load it again
        via an EnvironmentsConfig.
        """
        self.write_config(SAMPLE_ENV)
        self.config.load()
        serialized = self.config.serialize("myfirstenv")
        config = EnvironmentsConfig()
        config.parse(serialized)
        self.assertTrue(
            isinstance(config.get("myfirstenv"), Environment))
        self.assertFalse(
            isinstance(config.get("mysecondenv"), Environment))

    def test_serialize_unknown_environment(self):
        """Serializing an unknown environment raises an error."""
        self.write_config(SAMPLE_ENV)
        self.config.load()

        self.assertRaises(
            EnvironmentsConfigError,
            self.config.serialize, "zebra")

    def test_serialize_custom_variables_outside_environment(self):
        """Serializing captures custom variables out of the environment."""
        data = yaml.load(SAMPLE_ENV)
        data["default"] = "myfirstenv"
        self.write_config(yaml.dump(data))
        self.config.load()
        serialized = self.config.serialize()

        config = EnvironmentsConfig()
        config.parse(serialized)
        environment = config.get_default()
        self.assertEqual(environment.name, "myfirstenv")

    def test_invalid_configuration_data_raise_environment_config_error(self):
        self.write_config("ZEBRA")
        self.assertRaises(EnvironmentsConfigError, self.config.load)

    def test_nonstring_configuration_data_raise_environment_config_error(self):
        error = self.assertRaises(
            EnvironmentsConfigError, self.config.parse, None)
        self.assertIn(
            "Configuration must be a string:\nNone", str(error))

    def test_yaml_load_error_raise_environment_config_error(self):
        self.write_config("\0")
        error = self.assertRaises(EnvironmentsConfigError, self.config.load)
        self.assertIn(
            "special characters are not allowed", str(error))

    def test_ec2_verifies_region(self):
        self.config.write_sample()
        with open(self.default_path) as file:
            config = yaml.load(file.read())
            config["environments"]["sample"]["region"] = "ap-southeast-2"

            self.write_config(yaml.dump(config), other_path=True)

        e = self.assertRaises(EnvironmentsConfigError,
                          self.config.load,
                          self.other_path)
        self.assertIn("expected 'us-east-1', got 'ap-southeast-2'",
                       str(e))

        with open(self.default_path) as file:
            config = yaml.load(file.read())
            # Authorized keys are required for environment serialization.
            config["environments"]["sample"]["authorized-keys"] = "mickey"
            config["environments"]["sample"]["region"] = "ap-southeast-1"
            self.write_config(yaml.dump(config), other_path=True)

        self.config.load(self.other_path)
        data = self.config.get_default().get_serialization_data()
        self.assertEqual(data["sample"]["region"], "ap-southeast-1")

    def assert_ec2_sample_config(self, delete_key):
        self.config.write_sample()
        with open(self.default_path) as file:
            config = yaml.load(file.read())
            del config["environments"]["sample"][delete_key]
            self.write_config(yaml.dump(config), other_path=True)

        try:
            self.config.load(self.other_path)
        except EnvironmentsConfigError, error:
            self.assertEquals(
                str(error),
                "Environments configuration error: %s: "
                "environments.sample.%s: required value not found"
                % (self.other_path, delete_key))
        else:
            self.fail("Did not properly require " + delete_key)

    def test_ec2_sample_config_without_admin_secret(self):
        self.assert_ec2_sample_config("admin-secret")

    def test_ec2_sample_config_without_control_buckets(self):
        self.assert_ec2_sample_config("control-bucket")

    def test_ec2_verifies_placement(self):
        self.config.write_sample()
        with open(self.default_path) as file:
            config = yaml.load(file.read())
            config["environments"]["sample"]["placement"] = "random"
            self.write_config(yaml.dump(config), other_path=True)

        e = self.assertRaises(EnvironmentsConfigError,
                          self.config.load,
                          self.other_path)
        self.assertIn("expected 'unassigned', got 'random'",
                       str(e))

        with open(self.default_path) as file:
            config = yaml.load(file.read())
            # Authorized keys are required for environment serialization.
            config["environments"]["sample"]["authorized-keys"] = "mickey"
            config["environments"]["sample"]["placement"] = "local"
            self.write_config(yaml.dump(config), other_path=True)

        self.config.load(self.other_path)
        data = self.config.get_default().get_serialization_data()
        self.assertEqual(data["sample"]["placement"], "local")

    def test_ec2_respects_default_series(self):
        self.config.write_sample()
        with open(self.default_path) as f:
            config = yaml.load(f.read())
        config["environments"]["sample"]["default-series"] = "astounding"
        self.write_config(yaml.dump(config), other_path=True)

        self.config.load(self.other_path)

        provider = self.config.get_default().get_machine_provider()
        self.assertEqual(provider.config["default-series"], "astounding")

    def test_orchestra_schema_requires(self):
        requires = (
            "type orchestra-server orchestra-user orchestra-pass "
            "admin-secret acquired-mgmt-class available-mgmt-class "
            "default-series").split()
        for require in requires:
            config = yaml.load(SAMPLE_ORCHESTRA)
            del config["environments"]["sample"][require]
            self.write_config(yaml.dump(config), other_path=True)

            try:
                self.config.load(self.other_path)
            except EnvironmentsConfigError as error:
                self.assertEquals(str(error),
                                  "Environments configuration error: %s: "
                                  "environments.sample.%s: "
                                  "required value not found"
                                  % (self.other_path, require))
            else:
                self.fail("Did not properly require %s when type == orchestra"
                          % require)

    def test_orchestra_respects_default_series(self):
        config = yaml.load(SAMPLE_ORCHESTRA)

        config["environments"]["sample"]["default-series"] = "magnificent"
        self.write_config(yaml.dump(config), other_path=True)
        self.config.load(self.other_path)

        provider = self.config.get_default().get_machine_provider()
        self.assertEqual(provider.config["default-series"], "magnificent")

    def test_orchestra_verifies_placement(self):
        config = yaml.load(SAMPLE_ORCHESTRA)

        config["environments"]["sample"]["placement"] = "local"
        self.write_config(yaml.dump(config), other_path=True)
        self.config.load(self.other_path)

        data = self.config.get_default().placement
        self.assertEqual(data, "local")

    def test_lxc_requires_data_dir(self):
        """lxc dev only supports local placement."""
        self.config.write_sample()
        config = yaml.load(SAMPLE_LOCAL)

        self.write_config(yaml.dump(config), other_path=True)
        error = self.assertRaises(
            EnvironmentsConfigError,
            self.config.load,
            self.other_path)
        self.assertIn("data-dir: required value not found", str(error))

    def test_lxc_verifies_placement(self):
        """lxc dev only supports local placement."""
        self.config.write_sample()
        config = yaml.load(SAMPLE_LOCAL)

        config["environments"]["sample"]["placement"] = "unassigned"
        self.write_config(yaml.dump(config), other_path=True)
        error = self.assertRaises(
            EnvironmentsConfigError,
            self.config.load,
            self.other_path)
        self.assertIn("expected 'local', got 'unassigned'", str(error))
