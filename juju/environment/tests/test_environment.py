from juju.environment.tests.test_config import (
    EnvironmentsConfigTestBase, SAMPLE_ENV)
from juju.providers import dummy


class EnvironmentTest(EnvironmentsConfigTestBase):

    def test_attributes(self):
        self.write_config(SAMPLE_ENV)
        self.config.load()
        env = self.config.get("myfirstenv")
        self.assertEquals(env.name, "myfirstenv")
        self.assertEquals(env.type, "dummy")

    def test_get_machine_provider(self):
        """
        get_machine_provider() should return a MachineProvider instance
        imported from a module named after the "type:" provided in the
        machine provider configuration.
        """
        self.write_config(SAMPLE_ENV)
        self.config.load()
        env = self.config.get("myfirstenv")
        machine_provider = env.get_machine_provider()
        self.assertEquals(type(machine_provider), dummy.MachineProvider)

    def test_get_machine_provider_passes_config_into_provider(self):
        """
        get_machine_provider() should pass the machine provider configuration
        when constructing the MachineProvider.
        """
        self.write_config(SAMPLE_ENV)
        self.config.load()
        env = self.config.get("myfirstenv")
        dummy_provider = env.get_machine_provider()
        self.assertEquals(dummy_provider.config.get("foo"), "bar")

    def test_get_machine_provider_should_cache_results(self):
        """
        get_machine_provider() must cache its results.
        """
        self.write_config(SAMPLE_ENV)
        self.config.load()
        env = self.config.get("myfirstenv")
        machine_provider1 = env.get_machine_provider()
        machine_provider2 = env.get_machine_provider()
        self.assertIdentical(machine_provider1, machine_provider2)

    def test_get_serialization_data(self):
        """
        Getting the serialization data returns a dictionary with the
        environment configuration.
        """
        self.write_config(SAMPLE_ENV)
        self.config.load()
        env = self.config.get("myfirstenv")
        data = env.get_serialization_data()
        self.assertEqual(
            data,
            {"myfirstenv":
             {"type": "dummy",
              "foo": "bar",
              "dynamicduck": "magic"}})

    def test_get_serialization_data_errors_passthrough(self):
        """Serialization errors are raised to the caller.
        """
        self.write_config(SAMPLE_ENV)
        self.config.load()
        env = self.config.get("myfirstenv")

        mock_env = self.mocker.patch(env)
        mock_env.get_machine_provider()
        mock_provider = self.mocker.mock(dummy.MachineProvider)
        self.mocker.result(mock_provider)
        mock_provider.get_serialization_data()
        self.mocker.throw(SyntaxError())
        self.mocker.replay()
        self.assertRaises(SyntaxError, env.get_serialization_data)
