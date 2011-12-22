from juju.lib.testing import TestCase
from juju.providers.ec2.files import FileStorage
from juju.providers.ec2 import MachineProvider
from juju.environment.errors import EnvironmentsConfigError

from .common import EC2TestMixin


class ProviderTestCase(EC2TestMixin, TestCase):

    def setUp(self):
        super(ProviderTestCase, self).setUp()
        self.mocker.replay()

    def test_default_service_factory_construction(self):
        """
        Ensure that the AWSServiceRegion gets called by the MachineProvider
        with the right arguments.  This explores the mocking which is already
        happening within EC2TestMixin.
        """
        expected_kwargs = {"access_key": "",
                           "secret_key": "",
                           "ec2_uri": "https://ec2.us-east-1.amazonaws.com",
                           "s3_uri": ""}

        MachineProvider(self.env_name, {})
        self.assertEquals(self.service_factory_kwargs, expected_kwargs)

    def test_service_factory_construction(self):
        """
        Ensure that the AWSServiceRegion gets called by the MachineProvider
        with the right arguments when they are present in the configuration.
        This explores the mocking which is already happening within
        EC2TestMixin.
        """
        config = {"access-key": "secret-123",
                  "secret-key": "secret-abc",
                  "ec2-uri": "the-ec2-uri",
                  "s3-uri": "the-ec2-uri"}
        expected_kwargs = {}
        for key, value in config.iteritems():
            expected_kwargs[key.replace("-", "_")] = value
        MachineProvider(self.env_name, config)
        self.assertEquals(self.service_factory_kwargs, expected_kwargs)

    def test_service_factory_construction_region_provides_ec2_uri(self):
        """
        The EC2 service URI can be dereferenced by region name alone.
        This explores the mocking which is already happening within
        EC2TestMixin.
        """
        config = {"access-key": "secret-123",
                  "secret-key": "secret-abc",
                  "s3-uri": "the-ec2-uri",
                  "region": "eu-west-1"}
        expected_kwargs = {}
        for key, value in config.iteritems():
            expected_kwargs[key.replace("-", "_")] = value
        del expected_kwargs["region"]
        expected_kwargs["ec2_uri"] = "https://ec2.eu-west-1.amazonaws.com"
        MachineProvider(self.env_name, config)
        self.assertEquals(self.service_factory_kwargs, expected_kwargs)

    def test_provider_attributes(self):
        """
        The provider environment name and config should be available as
        parameters in the provider.
        """
        provider = self.get_provider()
        self.assertEqual(provider.environment_name, self.env_name)
        self.assertEqual(provider.config.get("type"), "ec2")
        self.assertEqual(provider.provider_type, "ec2")

    def test_get_file_storage(self):
        """The file storage is accessible via the machine provider."""
        provider = self.get_provider()
        storage = provider.get_file_storage()
        self.assertTrue(isinstance(storage, FileStorage))

    def test_config_serialization(self):
        """
        The provider configuration can be serialized to yaml.
        """
        keys_path = self.makeFile("my-keys")

        config = {"access-key": "secret-123",
                  "secret-key": "secret-abc",
                  "authorized-keys-path": keys_path}

        expected_serialization = config.copy()
        expected_serialization.pop("authorized-keys-path")
        expected_serialization["authorized-keys"] = "my-keys"

        provider = MachineProvider(self.env_name, config)
        serialized = provider.get_serialization_data()
        self.assertEqual(serialized, expected_serialization)

    def test_config_environment_extraction(self):
        """
        The provider serialization loads keys as needed from the environment.

        Variables from the configuration take precendence over those from
        the environment, when serializing.
        """
        config = {"access-key": "secret-12345",
                  "secret-key": "secret-abc",
                  "authorized-keys": "0123456789abcdef"}

        environ = {
            "AWS_SECRET_ACCESS_KEY": "secret-abc",
            "AWS_ACCESS_KEY_ID": "secret-123"}

        self.change_environment(**environ)
        provider = MachineProvider(
            self.env_name, {"access-key": "secret-12345",
                            "authorized-keys": "0123456789abcdef"})
        serialized = provider.get_serialization_data()
        self.assertEqual(config, serialized)


class FailCreateTest(TestCase):

    def test_conflicting_authorized_keys_options(self):
        """
        We can't handle two different authorized keys options, so deny
        constructing an environment that way.
        """
        config = {}
        config["authorized-keys"] = "File content"
        config["authorized-keys-path"] = "File path"
        error = self.assertRaises(EnvironmentsConfigError,
                                  MachineProvider, "some-env-name", config)
        self.assertEquals(
            str(error),
            "Environment config cannot define both authorized-keys and "
            "authorized-keys-path. Pick one!")
