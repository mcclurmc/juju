from twisted.internet.defer import inlineCallbacks

from juju.environment.errors import EnvironmentsConfigError
from juju.lib.testing import TestCase
from juju.providers.orchestra import MachineProvider

CONFIG = {"orchestra-server": "somewhe.re",
          "orchestra-user": "henricus",
          "orchestra-pass": "barbatus",
          "acquired-mgmt-class": "acquired",
          "available-mgmt-class": "available"}


class ProviderTestCase(TestCase):

    def test_create_provider(self):
        provider = MachineProvider(
            "tetrascape", CONFIG)
        self.assertEquals(provider.environment_name, "tetrascape")
        self.assertEquals(provider.config, CONFIG)

    def test_config_serialization(self):
        """
        The provider configuration can be serialized to yaml.
        """
        keys_path = self.makeFile("my-keys")

        config = {"orchestra-user": "chaosmonkey",
                  "orchestra-pass": "crash",
                  "acquired-mgmt-class": "madeup-1",
                  "available-mgmt-class": "madeup-2",
                  "orchestra-server": "127.0.0.01",
                  "authorized-keys-path": keys_path}

        expected = {"orchestra-user": "chaosmonkey",
                  "orchestra-pass": "crash",
                  "acquired-mgmt-class": "madeup-1",
                  "available-mgmt-class": "madeup-2",
                  "orchestra-server": "127.0.0.01",
                  "authorized-keys": "my-keys"}

        provider = MachineProvider("tetrascape", config)
        serialized = provider.get_serialization_data()
        self.assertEqual(serialized, expected)

    def test_conflicting_authorized_keys_options(self):
        """
        We can't handle two different authorized keys options, so deny
        constructing an environment that way.
        """
        config = CONFIG.copy()
        config["authorized-keys"] = "File content"
        config["authorized-keys-path"] = "File path"
        error = self.assertRaises(EnvironmentsConfigError,
                                  MachineProvider, "some-env-name", config)
        self.assertEquals(
            str(error),
            "Environment config cannot define both authorized-keys and "
            "authorized-keys-path. Pick one!")

    @inlineCallbacks
    def test_open_port(self):
        log = self.capture_logging("juju.orchestra")
        yield MachineProvider("blah", CONFIG).open_port(None, None, None)
        self.assertIn(
            "Firewalling is not yet implemented in Orchestra", log.getvalue())

    @inlineCallbacks
    def test_close_port(self):
        log = self.capture_logging("juju.orchestra")
        yield MachineProvider("blah", CONFIG).close_port(None, None, None)
        self.assertIn(
            "Firewalling is not yet implemented in Orchestra", log.getvalue())

    @inlineCallbacks
    def test_get_opened_ports(self):
        log = self.capture_logging("juju.orchestra")
        ports = yield MachineProvider("blah", CONFIG).get_opened_ports(None, None)
        self.assertEquals(ports, set())
        self.assertIn(
            "Firewalling is not yet implemented in Orchestra", log.getvalue())
