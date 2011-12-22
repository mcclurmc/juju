from juju.machine import ProviderMachine


class LocalMachine(ProviderMachine):
    """Represents host machine, when doing local development.
    """

    def __init__(self):
        super(LocalMachine, self).__init__("local", "localhost", "localhost")
        self.state = "running"  # a tautology
