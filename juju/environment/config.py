import os
import yaml
import uuid

from juju.lib.schema import (SchemaError, KeyDict, Dict, String,
                                 Constant, OneOf, SelectDict)
from juju.errors import FileNotFound, FileAlreadyExists

from juju.environment.environment import Environment
from juju.environment.errors import EnvironmentsConfigError


DEFAULT_CONFIG_PATH = "~/.juju/environments.yaml"

SAMPLE_CONFIG = """\
environments:
  sample:
    type: ec2
    control-bucket: %(control-bucket)s
    admin-secret: %(admin-secret)s
    default-series: oneiric
"""


SCHEMA = KeyDict({
    "default": String(),
    "environments": Dict(String(), SelectDict("type", {
        "ec2": KeyDict({"control-bucket": String(),
                        "admin-secret": String(),
                        "access-key": String(),
                        "secret-key": String(),
                        "region": OneOf(
                            Constant("us-east-1"),
                            Constant("us-west-1"),
                            Constant("us-west-2"),
                            Constant("eu-west-1"),
                            Constant("sa-east-1"),
                            Constant("ap-northeast-1"),
                            Constant("ap-southeast-1")),
                        "default-instance-type": String(),
                        "default-ami": String(),
                        "ec2-uri": String(),
                        "s3-uri": String(),
                        "placement": OneOf(
                                Constant("unassigned"),
                                Constant("local")),
                        "default-series": String()},
                       optional=["access-key", "secret-key",
                                 "default-instance-type", "default-ami",
                                 "region", "ec2-uri", "s3-uri", "placement"]),
        "orchestra": KeyDict({"orchestra-server": String(),
                              "orchestra-user": String(),
                              "orchestra-pass": String(),
                              "admin-secret": String(),
                              "acquired-mgmt-class": String(),
                              "available-mgmt-class": String(),
                              "storage-url": String(),
                              "storage-user": String(),
                              "storage-pass": String(),
                              "placement": String(),
                              "default-series": String()},
                             optional=["storage-url", "storage-user",
                                       "storage-pass", "placement"]),
        "local": KeyDict({"admin-secret": String(),
                        "data-dir": String(),
                        "placement": Constant("local"),
                        "default-series": String()},
                       optional=["placement"]),
        "dummy": KeyDict({})}))},
    optional=["default"])


class EnvironmentsConfig(object):
    """An environment configuration, with one or more environments.
    """

    def __init__(self):
        self._config = None
        self._loaded_path = None

    def get_default_path(self):
        """Return the default environment configuration file path."""
        return os.path.expanduser(DEFAULT_CONFIG_PATH)

    def _get_path(self, path):
        if path is None:
            return self.get_default_path()
        return path

    def load(self, path=None):
        """Load an enviornment configuration file.

        @param path: An optional environment configuration file path.
            Defaults to ~/.juju/environments.yaml

        This method will call the C{parse()} method with the content
        of the loaded file.
        """
        path = self._get_path(path)
        if not os.path.isfile(path):
            raise FileNotFound(path)
        with open(path) as file:
            self.parse(file.read(), path)

    def parse(self, content, path=None):
        """Parse an enviornment configuration.

        @param content: The content to parse.
        @param path: An optional environment configuration file path, used
            when raising errors.

        @raise EnvironmentsConfigError: On other issues.
        """
        if not isinstance(content, basestring):
            self._fail("Configuration must be a string", path, repr(content))

        try:
            config = yaml.load(content)
        except yaml.YAMLError, error:
            self._fail(error, path=path, content=content)

        if not isinstance(config, dict):
            self._fail("Configuration must be a dictionary", path, content)

        try:
            config = SCHEMA.coerce(config, [])
        except SchemaError, error:
            self._fail(error, path=path)
        self._config = config
        self._loaded_path = path

    def _fail(self, error, path, content=None):
        if path is None:
            path_info = ""
        else:
            path_info = " %s:" % (path,)

        error = str(error)
        if content:
            error += ":\n%s" % content

        raise EnvironmentsConfigError(
            "Environments configuration error:%s %s" %
            (path_info, error))

    def get_names(self):
        """Return the names of environments available in the configuration."""
        return sorted(self._config["environments"].iterkeys())

    def get(self, name):
        """Retrieve the Environment with the given name.

        @return: The Environment, or None if one isn't found.
        """
        environment_config = self._config["environments"].get(name)
        if environment_config is not None:
            return Environment(name, environment_config)
        return None

    def get_default(self):
        """Get the default environment for this configuration.

        The default environment is either the single defined environment
        in the configuration, or the one explicitly named through the
        "default:" option in the outermost scope.

        @raise EnvironmentsConfigError: If it can't determine a default
            environment.
        """
        environments_config = self._config.get("environments")
        if len(environments_config) == 1:
            return self.get(environments_config.keys()[0])
        default = self._config.get("default")
        if default:
            if default not in environments_config:
                raise EnvironmentsConfigError(
                    "Default environment '%s' was not found: %s" %
                    (default, self._loaded_path))
            return self.get(default)
        raise EnvironmentsConfigError("There are multiple environments and no "
                                     "explicit default (set one explicitly?): "
                                     "%s" % self._loaded_path)

    def write_sample(self, path=None):
        """Write down a sample configuration file.

        @param path: An optional environment configuration file path.
            Defaults to ~/.juju/environments.yaml
        """
        path = self._get_path(path)
        dirname = os.path.dirname(path)
        if os.path.exists(path):
            raise FileAlreadyExists(path)
        if not os.path.exists(dirname):
            os.mkdir(dirname, 0700)

        defaults = {
            "control-bucket": "juju-%s" % (uuid.uuid4().hex),
            "admin-secret": "%s" % (uuid.uuid4().hex)}

        with open(path, "w") as file:
            file.write(SAMPLE_CONFIG % defaults)
        os.chmod(path, 0600)

    def load_or_write_sample(self):
        """Try to load the configuration, and if it doesn't work dump a sample.

        This method will try to load the environment configuration from the
        default location, and if it doesn't work, it will write down a
        sample configuration there.

        This is handy for a default initialization.
        """
        try:
            self.load()
        except FileNotFound:
            self.write_sample()
            raise EnvironmentsConfigError("No environments configured. Please "
                                          "edit: %s" % self.get_default_path(),
                                          sample_written=True)

    def serialize(self, name=None):
        """Serialize the environments configuration.

        Optionally an environment name can be specified and only
        that environment will be serialized.

        Serialization dispatches to the individual environments as
        they may serialize information not contained within the
        original config file.
        """
        if not name:
            names = self.get_names()
        else:
            names = [name]

        config = self._config.copy()
        config["environments"] = {}

        for name in names:
            environment = self.get(name)
            if environment is None:
                raise EnvironmentsConfigError(
                    "Invalid environment %r" % name)
            data = environment.get_serialization_data()
            # all environment data should be contained
            # in a nested dict under the environment name.
            assert data.keys() == [name]
            config["environments"].update(data)

        return yaml.safe_dump(config)
