from twisted.internet.defer import inlineCallbacks, returnValue
from juju.state.base import StateBase
from juju.state.errors import (UnitRelationStateNotFound, StateNotFound)
from juju.state.service import ServiceStateManager, parse_service_name
from juju.state.utils import YAMLState


class RelationChange(object):
    """Encapsulation of relation change variables passed to hook.
    """

    def __init__(self, relation_name, change_type, unit_name):
        self._relation_name = relation_name
        self._change_type = change_type
        self._unit_name = unit_name

    @property
    def relation_name(self):
        return self._relation_name

    @property
    def change_type(self):
        return self._change_type

    @property
    def unit_name(self):
        return self._unit_name


class HookContext(StateBase):
    """Context for hooks which don't depend on relation state. """

    def __init__(self, client, unit_name):
        super(HookContext, self).__init__(client)

        self._unit_name = unit_name
        self._service = None

        # A cache of retrieved nodes.
        self._node_cache = {}

        # A cache of node names to node ids.
        self._name_cache = {}

        # Service options
        self._config_options = None

        # Topology for resolving name<->id of units.
        self._topology = None

    @inlineCallbacks
    def _resolve_id(self, unit_id):
        """Resolve a unit id to a unit name."""
        if self._topology is None:
            self._topology = yield self._read_topology()
        unit_name = self._topology.get_service_unit_name_from_id(unit_id)
        returnValue(unit_name)

    @inlineCallbacks
    def _resolve_name(self, unit_name):
        """Resolve a unit name to a unit id with caching."""
        if unit_name in self._name_cache:
            returnValue(self._name_cache[unit_name])
        if self._topology is None:
            self._topology = yield self._read_topology()
        unit_id = self._topology.get_service_unit_id_from_name(unit_name)
        self._name_cache[unit_name] = unit_id
        returnValue(unit_id)

    @inlineCallbacks
    def get_local_unit_state(self):
        """Return ServiceUnitState for the local service unit."""
        service_state_manager = ServiceStateManager(self._client)
        unit_state = yield service_state_manager.get_unit_state(self._unit_name)
        returnValue(unit_state)

    @inlineCallbacks
    def get_local_service(self):
        """Return ServiceState for the local service."""
        if self._service is None:
            service_state_manager = ServiceStateManager(self._client)
            self._service = yield(
                service_state_manager.get_service_state(
                    parse_service_name(self._unit_name)))
        returnValue(self._service)

    def _settings_path(self, unit_id):
        return "/relations/%s/settings/%s" % (
            self._unit_relation.internal_relation_id, unit_id)

    @inlineCallbacks
    def get_config(self):
        """Gather the configuration options.

        Returns YAMLState for the service options of the current hook
        and caches them internally.This state object's `write` method
        must be called to publish changes to Zookeeper. `flush` will
        do this automatically.
        """
        if not self._config_options:
            service = yield self.get_local_service()
            self._config_options = yield service.get_config()
        returnValue(self._config_options)

    @inlineCallbacks
    def flush(self):
        """Flush pending state."""
        config = yield self.get_config()
        yield config.write()


class RelationHookContext(HookContext):
    """A hook execution data cache and write buffer of relation settings.

    Performs caching of any relation settings examined by the
    hook. Also buffers all writes till the flush method is invoked.
    """

    def __init__(self, client, unit_relation, change, members=None,
                 unit_name=None):
        """
        @param unit_relation: The unit relation state associated to the hook.
        @param change: A C{RelationChange} instance.
        """
        # Zookeeper client.
        super(RelationHookContext, self).__init__(client, unit_name=unit_name)
        self._unit_relation = unit_relation
        # The current change we're being executed against.
        self._change = change

        # A cache of related units in the relation.
        self._members = members

    @inlineCallbacks
    def get_members(self):
        """Gets the related unit members of the relation with caching."""
        if self._members is not None:
            returnValue(self._members)

        container = yield self._unit_relation.get_related_unit_container()
        unit_ids = yield self._client.get_children(container)
        if self._unit_relation.internal_unit_id in unit_ids:
            unit_ids.remove(self._unit_relation.internal_unit_id)

        members = []
        for unit_id in unit_ids:
            unit_name = yield self._resolve_id(unit_id)
            members.append(unit_name)

        self._members = members
        returnValue(members)

    @inlineCallbacks
    def _setup_relation_state(self, unit_name=None):
        """For a given unit name make sure we have YAMLState."""
        if unit_name is None:
            unit_name = yield self._resolve_id(
                self._unit_relation.internal_unit_id)

        if unit_name in self._node_cache:
            returnValue(self._node_cache[unit_name])

        unit_id = yield self._resolve_name(unit_name)
        path = self._settings_path(unit_id)

        # verify the unit relation path exists
        relation_data = YAMLState(self._client, path)
        try:
            yield relation_data.read(required=True)
        except StateNotFound:
            raise UnitRelationStateNotFound(
                self._unit_relation.internal_relation_id,
                self._change.relation_name,
                unit_name)

        # cache the value
        self._node_cache[unit_name] = relation_data
        returnValue(relation_data)

    @inlineCallbacks
    def get(self, unit_name):
        """Get the relation settings for a unit.

        Returns the settings as a dictionary.
        """
        relation_data = yield self._setup_relation_state(unit_name)
        returnValue(dict(relation_data))

    @inlineCallbacks
    def get_value(self, unit_name, key):
        """Get a relation setting value for a unit."""
        settings = yield self.get(unit_name)
        if not settings:
            returnValue("")
        returnValue(settings.get(key, ""))

    @inlineCallbacks
    def set(self, data):
        """Set the relation settings for a unit.

        @param data: A dictionary containing the settings.

        **Warning**, this method will replace existing values for the
        unit relation with those from the ``data`` dictionary.
        """
        if not isinstance(data, dict):
            raise TypeError("A dictionary is required.")

        state = yield self._setup_relation_state()
        state.update(data)

    @inlineCallbacks
    def set_value(self, key, value):
        """Set a relation value for a unit."""
        state = yield self._setup_relation_state()
        state[key] = value

    @inlineCallbacks
    def delete_value(self, key):
        """Delete a relation value for a unit."""
        state = yield self._setup_relation_state()
        try:
            del state[key]
        except KeyError:
            # deleting a non-existent key is a no-op
            pass

    def has_read(self, unit_name):
        """Has the context been used to access the settings of the unit.
        """
        return unit_name in self._node_cache

    @inlineCallbacks
    def flush(self):
        """Flush all writes to the unit settings.

        A flush will attempt to intelligently merge values modified on
        the context to the current state of the underlying settings
        node.  It supports externally modified or deleted values that
        are unchanged on the context, to be preserved.

        The change items to the relation YAMLState is returned (this
        could also be done with config settings, but given their
        usage model, doesn't seem to be worth logging).
        """
        rel_state = yield self._setup_relation_state()
        relation_setting_changes = yield rel_state.write()
        yield super(RelationHookContext, self).flush()
        returnValue(relation_setting_changes)

