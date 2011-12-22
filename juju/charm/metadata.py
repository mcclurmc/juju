import logging
import os

import yaml

from juju.charm.errors import MetaDataError
from juju.errors import FileNotFound
from juju.lib.schema import (
    SchemaError, Bool, Constant, Dict, Int,
    KeyDict, OneOf, UnicodeOrString)


log = logging.getLogger("juju.charm")


UTF8_SCHEMA = UnicodeOrString("utf-8")

INTERFACE_SCHEMA = KeyDict({
        "interface": UTF8_SCHEMA,
        "limit": OneOf(Constant(None), Int()),
        "optional": Bool()})


class InterfaceExpander(object):
    """Schema coercer that expands the interface shorthand notation.

    We need this class because our charm shorthand is difficult to
    work with (unfortunately). So we coerce shorthand and then store
    the desired format in ZK.

    Supports the following variants::

      provides:
        server: riak
        admin: http
        foobar:
          interface: blah

      provides:
        server:
          interface: mysql
          limit:
          optional: false

    In all input cases, the output is the fully specified interface
    representation as seen in the mysql interface description above.
    """

    def __init__(self, limit):
        """Create relation interface reshaper.

        @limit: the limit for this relation. Used to provide defaults
            for a given kind of relation role (peer, provider, consumer)
        """
        self.limit = limit

    def coerce(self, value, path):
        """Coerce `value` into an expanded interface.

        Helper method to support each of the variants, either the
        charm does not specify limit and optional, such as foobar in
        the above example; or the interface spec is just a string,
        such as the ``server: riak`` example.
        """
        if not isinstance(value, dict):
            return {
                "interface": UTF8_SCHEMA.coerce(value, path),
                "limit": self.limit,
                "optional": False}
        else:
            # Optional values are context-sensitive and/or have
            # defaults, which is different than what KeyDict can
            # readily support. So just do it here first, then
            # coerce.
            if "limit" not in value:
                value["limit"] = self.limit
            if "optional" not in value:
                value["optional"] = False
            return INTERFACE_SCHEMA.coerce(value, path)


SCHEMA = KeyDict({
    "name": UTF8_SCHEMA,
    "revision": Int(),
    "summary": UTF8_SCHEMA,
    "description": UTF8_SCHEMA,
    "peers": Dict(UTF8_SCHEMA, InterfaceExpander(limit=1)),
    "provides": Dict(UTF8_SCHEMA, InterfaceExpander(limit=None)),
    "requires": Dict(UTF8_SCHEMA, InterfaceExpander(limit=1)),
    }, optional=set(["provides", "requires", "peers", "revision"]))


class MetaData(object):
    """Represents the charm info file.

    The main metadata for a charm (name, revision, etc) is maintained
    in the charm's info file.  This class is able to parse,
    validate, and provide access to data in the info file.
    """

    def __init__(self, path=None):
        self._data = {}
        if path is not None:
            self.load(path)

    @property
    def name(self):
        """The charm name."""
        return self._data.get("name")

    @property
    def obsolete_revision(self):
        """The charm revision.

        The charm revision acts as a version, but unlike e.g. package
        versions, the charm revision is a monotonically increasing
        integer. This should not be stored in metadata any more, but remains
        for backward compatibility's sake.
        """
        return self._data.get("revision")

    @property
    def summary(self):
        """The charm summary."""
        return self._data.get("summary")

    @property
    def description(self):
        """The charm description."""
        return self._data.get("description")

    @property
    def provides(self):
        """The charm provides relations."""
        return self._data.get("provides")

    @property
    def requires(self):
        """The charm requires relations."""
        return self._data.get("requires")

    @property
    def peers(self):
        """The charm peers relations."""
        return self._data.get("peers")

    def get_serialization_data(self):
        """Get internal dictionary representing the state of this instance.

        This is useful to embed this information inside other storage-related
        dictionaries.
        """
        return dict(self._data)

    def load(self, path):
        """Load and parse the info file.

        @param path: Path of the file to load.

        Internally, this function will pass the content of the file to
        the C{parse()} method.
        """
        if not os.path.isfile(path):
            raise FileNotFound(path)
        with open(path) as f:
            self.parse(f.read(), path)

    def parse(self, content, path=None):
        """Parse the info file described by the given content.

        @param content: Content of the info file to parse.
        @param path: Optional path of the loaded file.  Used when raising
            errors.

        @raise MetaDataError: When errors are found in the info data.
        """
        try:
            self.parse_serialization_data(yaml.load(content), path)
        except yaml.MarkedYAMLError, e:
            # Capture the path name on the error if present.
            if path is not None:
                e.problem_mark.name = path
            raise

        if "revision" in self._data and path:
            log.warning(
                "%s: revision field is obsolete. Move it to the 'revision' "
                "file." % path)

    def parse_serialization_data(self, serialization_data, path=None):
        """Parse the unprocessed serialization data and load in this instance.

        @param serialization_data: Unprocessed data matching the
            metadata schema.
        @param path: Optional path of the loaded file.  Used when
            raising errors.

        @raise MetaDataError: When errors are found in the info data.
        """
        try:
            self._data = SCHEMA.coerce(serialization_data, [])
        except SchemaError, error:
            if path:
                path_info = " %s:" % path
            else:
                path_info = ""
            raise MetaDataError("Bad data in charm info:%s %s" %
                                (path_info, error))
