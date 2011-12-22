from juju.errors import JujuError


class EnvironmentsConfigError(JujuError):
    """Raised when the environment configuration file has problems."""

    def __init__(self, message, sample_written=False):
        super(EnvironmentsConfigError, self).__init__(message)
        self.sample_written = sample_written
