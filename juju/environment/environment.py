from juju.lib.loader import get_callable


class Environment(object):
    """An environment where machines can be run on."""

    def __init__(self, name, environment_config):
        self._name = name
        self._environment_config = environment_config
        self._machine_provider = None

    @property
    def name(self):
        """The name of this environment."""
        return self._name

    def get_serialization_data(self):
        provider = self.get_machine_provider()
        data = {self.name: provider.get_serialization_data()}
        return data

    @property
    def type(self):
        """The type of the environment."""
        return self._environment_config["type"]

    def get_machine_provider(self):
        """Return a MachineProvider instance for the given provider name.

        The returned instance will be retrieved from the module named
        after the type of the given machine provider.
        """
        if self._machine_provider is None:
            provider_type = self._environment_config["type"]
            MachineProvider = get_callable(
                "juju.providers.%s.MachineProvider" % provider_type)
            self._machine_provider = MachineProvider(
                self._name, self._environment_config)
        return self._machine_provider

    @property
    def placement(self):
        """The name of the default placement policy.

        If the environment doesn't have a default unit placement
        policy None is returned
        """
        return self._environment_config.get("placement")

    @property
    def default_series(self):
        """The Ubuntu series to run on machines in this environment."""
        return self._environment_config.get("default-series")
