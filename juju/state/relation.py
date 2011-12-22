import logging
from os.path import basename, dirname

import yaml
import zookeeper

from twisted.internet.defer import (
    inlineCallbacks, returnValue, maybeDeferred, Deferred)

from txzookeeper.utils import retry_change

from juju.state.base import StateBase
from juju.state.errors import (
    DuplicateEndpoints, IncompatibleEndpoints, RelationAlreadyExists,
    RelationStateNotFound, StateChanged, UnitRelationStateNotFound,
    UnknownRelationRole)


class RelationStateManager(StateBase):
    """Manages the state of relations in an environment."""

    @inlineCallbacks
    def add_relation_state(self, *endpoints):
        """Add new relation state with the common relation type of `endpoints`.

        There must be one or two endpoints specified, with the same
        `relation_type`. Their corresponding services will be assigned
        atomically.
        """

        # the TOODs in the following comments in this function are for
        # type checking to be implemented ASAP. However, this will
        # require some nontrivial test modification, so it's best to
        # be done in a future branch. This is because these tests use
        # such invalid role names as ``role``, ``dev``, and ``prod``;
        # or they add non-peer relations of only one endpoint.

        if len(endpoints) == 1:
            # TODO verify that the endpoint is a peer endpoint only
            pass
        elif len(endpoints) == 2:
            if endpoints[0] == endpoints[1]:
                raise DuplicateEndpoints(endpoints)

            # TODO verify that the relation roles are client or server
            # only
            if (endpoints[0].relation_role in ("client", "server") or
                endpoints[1].relation_role in ("client", "server")):
                if not endpoints[0].may_relate_to(endpoints[1]):
                    raise IncompatibleEndpoints(endpoints)
        else:
            raise TypeError("Requires 1 or 2 endpoints, %d given" % \
                            len(endpoints))

        # first check so as to prevent unnecessary garbage in ZK in
        # case this relation has been previously added
        topology = yield self._read_topology()
        if topology.has_relation_between_endpoints(endpoints):
            raise RelationAlreadyExists(endpoints)

        relation_type = endpoints[0].relation_type
        relation_id = yield self._add_relation_state(relation_type)
        services = []
        for endpoint in endpoints:
            service_id = topology.find_service_with_name(endpoint.service_name)
            yield self._add_service_relation_state(
                relation_id, service_id, endpoint)
            services.append(ServiceRelationState(self._client,
                                                 service_id,
                                                 relation_id,
                                                 endpoint.relation_role,
                                                 endpoint.relation_name))

        def add_relation(topology):
            if topology.has_relation_between_endpoints(endpoints):
                raise RelationAlreadyExists(endpoints)
            topology.add_relation(relation_id, relation_type)
            for service_relation in services:
                if not topology.has_service(service_id):
                    raise StateChanged()
                topology.assign_service_to_relation(
                    relation_id,
                    service_relation.internal_service_id,
                    service_relation.relation_name,
                    service_relation.relation_role)
        yield self._retry_topology_change(add_relation)

        returnValue((RelationState(self._client, relation_id), services))

    @inlineCallbacks
    def _add_relation_state(self, relation_type):
        path = yield self._client.create(
            "/relations/relation-", flags=zookeeper.SEQUENCE)
        internal_id = basename(path)
        # create the settings container, for individual units settings.
        yield self._client.create(path + "/settings")
        returnValue(internal_id)

    @inlineCallbacks
    def _add_service_relation_state(
        self, relation_id, service_id, endpoint):
        """Add a service relation state.
        """
        # Add service container in relation.
        node_data = yaml.safe_dump(
            {"name": endpoint.relation_name, "role": endpoint.relation_role})
        path = "/relations/%s/%s" % (relation_id, endpoint.relation_role)

        try:
            yield self._client.create(path, node_data)
        except zookeeper.NodeExistsException:
            # If its not, then update the node, and continue.
            yield retry_change(
                self._client, path, lambda content, stat: node_data)

    @inlineCallbacks
    def remove_relation_state(self, relation_state):
        """Remove the relation of the given id.

        :param relation_state: Either a relation state or a service relation
               state.
        The relation is removed from the topology, however its container
        node is not removed, as associated units will still be processing
        its removal.
        """
        if isinstance(relation_state, RelationState):
            relation_id = relation_state.internal_id
        elif isinstance(relation_state, ServiceRelationState):
            relation_id = relation_state.internal_relation_id

        def remove_relation(topology):
            if not topology.has_relation(relation_id):
                raise StateChanged()

            topology.remove_relation(relation_id)

        yield self._retry_topology_change(remove_relation)

    @inlineCallbacks
    def get_relations_for_service(self, service_state):
        """Get the relations associated to the service.
        """
        relations = []
        internal_service_id = service_state.internal_id
        topology = yield self._read_topology()
        for info in topology.get_relations_for_service(internal_service_id):
            relation_id, relation_type, service_info = info
            relations.append(
                ServiceRelationState(
                    self._client,
                    internal_service_id,
                    relation_id,
                    **service_info))
        returnValue(relations)

    @inlineCallbacks
    def get_relation_state(self, *endpoints):
        """Return `relation_state` connecting the endpoints.

        Raises `RelationStateNotFound if no such relation exists.
        """
        topology = yield self._read_topology()
        internal_id = topology.get_relation_between_endpoints(endpoints)
        if internal_id is None:
            raise RelationStateNotFound()
        returnValue(RelationState(self._client, internal_id))


class RelationState(StateBase):
    """Represents a connection between one or more services.

    The relation state is representative of the entire connection and its
    endpoints, while the :class ServiceRelationState: is representative
    of one of the service endpoints.
    """
    def __init__(self, client, internal_relation_id):
        super(RelationState, self).__init__(client)
        self._internal_id = internal_relation_id

    @property
    def internal_id(self):
        return self._internal_id


class ServiceRelationState(object):
    """A state representative of a relation between one or more services."""

    def __init__(self, client, service_id, relation_id, role, name):
        self._client = client
        self._service_id = service_id
        self._relation_id = relation_id
        self._role = role
        self._name = name

    def __repr__(self):
        return "<ServiceRelationState name:%s role:%s id:%s>" % (
            self.relation_name, self.relation_role, self.internal_relation_id)

    @property
    def internal_relation_id(self):
        return self._relation_id

    @property
    def internal_service_id(self):
        return self._service_id

    @property
    def relation_name(self):
        """The service's name for the relation."""
        return self._name

    @property
    def relation_role(self):
        """The service's role within the relation."""
        return self._role

    @inlineCallbacks
    def add_unit_state(self, unit_state):
        """Add a unit to the service relation.

        This api is intended for use by the unit agent, as it
        also creates an ephemeral presence node, denoting the
        active existance of the unit in the relation.

        returns a unit relation state.
        """
        settings_path = "/relations/%s/settings/%s" % (
            self._relation_id, unit_state.internal_id)

        # We create settings node first, so that presence node events
        # have a chance to inspect state.

        # Prepopulate the relation node with the node's private address.
        private_address = yield unit_state.get_private_address()
        try:
            yield self._client.create(
                settings_path,
                yaml.safe_dump({"private-address": private_address}))
        except zookeeper.NodeExistsException:
            # previous persistent settings are not an error, but
            # update the unit address
            def update_address(content, stat):
                unit_map = yaml.load(content)
                if not unit_map:
                    unit_map = {}
                unit_map["private-address"] = private_address
                return yaml.safe_dump(unit_map)
            yield retry_change(self._client, settings_path, update_address)

        # Update the unit name -> id mapping on the relation node
        def update_unit_mapping(content, stat):
            if content:
                unit_map = yaml.load(content)
            else:
                unit_map = {}
            # If its already present, we're done, just return the
            # existing content, to avoid unstable yaml dict
            # serialization.
            if unit_state.internal_id in unit_map:
                return content
            unit_map[unit_state.internal_id] = unit_state.unit_name
            return yaml.dump(unit_map)

        yield retry_change(self._client,
                           "/relations/%s" % self._relation_id,
                           update_unit_mapping)

        # Create the presence node.
        alive_path = "/relations/%s/%s/%s" % (
            self._relation_id, self._role, unit_state.internal_id)

        try:
            yield self._client.create(alive_path, flags=zookeeper.EPHEMERAL)
        except zookeeper.NodeExistsException:
            # Concurrent creation is okay, end state is the same.
            pass

        returnValue(
            UnitRelationState(
                self._client,
                self._service_id,
                unit_state.internal_id,
                self._relation_id))

    @inlineCallbacks
    def get_unit_state(self, unit_state):
        """Given a service unit state, return its unit relation state."""
        alive_path = "/relations/%s/%s/%s" % (
            self._relation_id, self._role, unit_state.internal_id)
        stat = yield self._client.exists(alive_path)

        if not stat:
            raise UnitRelationStateNotFound(
                self._relation_id, self._name, unit_state.unit_name)

        returnValue(
            UnitRelationState(
                self._client,
                self._service_id,
                unit_state.internal_id,
                self._relation_id))

    @inlineCallbacks
    def get_service_states(self):
        """Get all the services associated with this relation.

        @return: list of ServiceState instance associated with this relation.
        """
        from juju.state.service import ServiceStateManager, ServiceState
        service_manager = ServiceStateManager(self._client)
        services = []
        topology = yield service_manager._read_topology()
        for service_id in topology.get_relation_services(
                self.internal_relation_id):
            service_name = topology.get_service_name(service_id)
            service = ServiceState(self._client, service_id, service_name)
            services.append(service)
        returnValue(services)


class UnitRelationState(StateBase):
    """A service unit's relation state.
    """

    def __init__(self, client, service_id, unit_id, relation_id):
        super(UnitRelationState, self).__init__(client)
        self._service_id = service_id
        self._unit_id = unit_id
        self._relation_id = relation_id

        # cached value
        self._cached_relation_role = None

    @property
    def internal_service_id(self):
        return self._service_id

    @property
    def internal_unit_id(self):
        return self._unit_id

    @property
    def internal_relation_id(self):
        return self._relation_id

    @inlineCallbacks
    def set_data(self, data):
        """Set the relation local configuration data for a unit.

        This call overwrites any data currently in the node with the
        dictionary supplied as `data`.
        """
        unit_settings_path = "/relations/%s/settings/%s" % (
            self._relation_id, self._unit_id)

        # encode as a YAML string
        data = yaml.safe_dump(data)

        yield retry_change(
            self._client, unit_settings_path,
            lambda content, stat: data)

    @inlineCallbacks
    def get_data(self):
        """Get the relation local configuration data for a unit.
        """
        data, stat = yield self._client.get(
            "/relations/%s/settings/%s" % (
                self._relation_id, self._unit_id))
        returnValue(data)

    @inlineCallbacks
    def get_relation_role(self):
        """Return the service's role within the relation.
        """
        if self._cached_relation_role is not None:
            returnValue(self._cached_relation_role)

        # Perhaps this information could be passed directly into the
        # unit relation state constructor, its already got 4 params though.
        topology = yield self._read_topology()
        relation_type, info = topology.get_relation_service(self._relation_id,
                                                            self._service_id)
        relation_role = info["role"]

        self._cached_relation_role = relation_role
        returnValue(relation_role)

    @inlineCallbacks
    def get_related_unit_container(self):
        """Return the path to the relation role container of the related units.
        """
        relation_role = yield self.get_relation_role()
        if relation_role == "server":
            endpoint_container = "/relations/%s/%s" % (self._relation_id,
                                                       "client")
        elif relation_role == "client":
            endpoint_container = "/relations/%s/%s" % (self._relation_id,
                                                       "server")
        elif relation_role == "peer":
            endpoint_container = "/relations/%s/peer" % self._relation_id
        else:
            topology = yield self._read_topology()
            service_name = topology.get_service_name(self._service_id)
            raise UnknownRelationRole(
                self._relation_id, relation_role, service_name)

        returnValue(endpoint_container)

    @inlineCallbacks
    def watch_related_units(self, callback):
        """Register a callback to be invoked when related units change.

        @param: callback a function that gets invoked when related
        units of the appropriate role are added, removed, or change
        their settings.  The callback should expect three keyword
        arguments, old_units, new_units, and modified. If there is a
        membership change, the old_units and new_units parameters will
        be passed containing the related unit membership (as a list)
        before and after the change. If a related unit changes, the
        modified parameter will be passed with a list of changed
        units.

        The callback will be invoked in parallel for different changes
        to different nodes. However it will be invoked serially for
        changes to a single node.

        This method returns a watcher instance, that exposes an api
        for starting and stopping the watch and the callback
        invocation.

        See C{RelationUnitWatcherBase} for additional details.
        """
        relation_role = yield self.get_relation_role()
        endpoint_container = yield self.get_related_unit_container()

        # Determine the watcher implementation.
        if relation_role == "server":
            watcher_factory = ClientServerUnitWatcher
        elif relation_role == "client":
            watcher_factory = ClientServerUnitWatcher
        elif relation_role == "peer":
            watcher_factory = PeerUnitWatcher

        watcher = watcher_factory(
            self._client, self, endpoint_container, callback)
        returnValue(watcher)


class RelationUnitWatcherBase(StateBase):
    """Unit relation observation of other units.

    When a service unit is participating in a relation, it needs to
    watch other units within the relation to observe their setting
    and membership changes in order to invoke its own charm hooks.

    This base class provides for most of the behavior of watching
    other units within a relation. Various subclasses provide for
    concrete implementations of this logic based on the relation role
    and thereby the units within the relation that need watching.

    The two focus points of watching relations, deal with watching the
    presence nodes of other units within the relation, and watching
    their respective settings nodes. Which units in particular are
    watched are determined by the relation role of the service as per
    the charm specification.

    The watcher will concurrently execute the callback in parallel for
    changes to different nodes. However for changes to a single node
    the callback will be executed serially.
    """

    def __init__(self,
                 client,
                 watcher_unit,
                 unit_container_path,
                 callback):
        super(RelationUnitWatcherBase, self).__init__(client)
        self._units = []
        self._watcher_unit = watcher_unit
        self._container_path = unit_container_path
        self._callback = callback
        self._stopped = None
        self._unit_name_map = None
        self._log = logging.getLogger("unit.relation.watch")

    def _watch_container(self, watch_established_callback=None):
        """Watch the service role container, for related units.
        """
        child_d, watch_d = self._client.get_children_and_watch(
                self._container_path)

        # After we've established a container watch we should
        # invoke the watch established callback, if any.
        if watch_established_callback is not None:
            child_d.addCallback(watch_established_callback)

        # Setup child watches, and invoke user callbacks for membership.
        child_d.addCallback(self._cb_container_children)

        # After processing children, setup the container watch callback.
        child_d.addCallback(lambda result: watch_d.addCallback(
            self._cb_container_child_change))

        # Handle container nonexistant errors
        child_d.addErrback(self._eb_no_container, watch_established_callback)

        return child_d

    def _eb_no_container(self, failure, watch_established_callback=None):
        """Handle the case where the service-role container does not exist.

        We establish an existance watch with a callback to start the unit
        watching.
        """
        failure.trap(zookeeper.NoNodeException)

        # Establish an exists watch on the container.
        exists_d, watch_d = self._client.exists_and_watch(self._container_path)

        # After the container watch is established, invoke any est. callback
        if watch_established_callback:
            exists_d.addCallback(watch_established_callback)

        # Set a callback, to watch the container when its created.
        watch_d.addCallback(self._cb_container_created)

        # Check if the container has been created prior to exists call.
        exists_d.addCallback(self._cb_container_exists)

        # return an empty set of children (no container yet)
        return []

    def _cb_container_exists(self, stat):
        """If the container exists, start watching it.

        This is used as a callback from the no container error
        handler, as it establishes a watch on the container, to verify
        that the container does not already exist.
        """
        if stat:
            return self._watch_container()

    def _cb_container_created(self, event):
        """Once the service role container is created, establish watches for it
        """
        if event.type_name == "created":
            return self._watch_container()

    @inlineCallbacks
    def _resolve_unit_names(self, *unit_ids):
        """Resolve the names of units given their ids.

        Takes multiple lists of unit ids as parameters, and returns
        corresponding lists of unit names as results.
        """
        if not self._unit_name_map:
            content, stat = yield self._client.get(
                dirname(self._container_path))
            self._unit_name_map = yaml.load(content)
        results = []
        for unit_id_list in unit_ids:
            names = []
            for unit_id in unit_id_list:
                names.append(self._unit_name_map[unit_id])
            results.append(names)
        returnValue(results)

    def _cb_container_children(self, children):
        """Process children of the service role container.

        Establishes watches on the settings of units in a relation
        role container.

        @param children: A list of unit ids within the relation role
                         container.
        """
        # Filter the units we're interested in.
        children = self._filter_units(children)

        # If there is no delta from the last known state, we're done.
        if self._units == children:
            return

        # Determine if we have any new nodes.
        added = set(children) - set(self._units)

        if added:
            # If we do have new units, invalidate the unit name cache
            self._unit_name_map = None

            # Setup watches on new children so we catch all changes but
            # don't attach handlers till after the container callback
            # is complete. This way we ensure we get membership changes
            # before modification changes.
            settings_watches = self._watch_settings_for_units(added)
        else:
            settings_watches = []

        # Resolve unit ids to names
        callback_d = self._resolve_unit_names(self._units, children)

        # Update the list of known children
        self._units = children

        # Invoke callback
        callback_d.addCallback(
            lambda (old_units, new_units): maybeDeferred(
                self._callback,
                old_units=sorted(old_units),
                new_units=sorted(new_units)))

        # Attach change handlers to new node watches
        if settings_watches:
            callback_d.addCallback(
                lambda result: [w.addCallback(self._cb_unit_change) for w \
                                in settings_watches])

        return callback_d

    def _watch_settings_for_units(self, added):
        """Setup watches on new unit relation setting nodes.
        """
        settings_watches = []
        # Watch new settings node for changes.
        for unit_id in added:
            settings_path = "/relations/%s/settings/%s" % (
                self._watcher_unit.internal_relation_id,
                unit_id)

            # Since we have a concurrent execution model, unit tests,
            # will error out since this callback might still be
            # utilizing the zookeeper api, after the client is
            # closed. Verify the connection is open, before we invoke
            # zk apis.
            if not self._client.connected:
                return

            # We're only concerned with settings changes, the container
            # watch will handle add/removes
            exists_d, watch_d = self._client.exists_and_watch(settings_path)
            settings_watches.append(watch_d)

        return settings_watches

    def _cb_container_child_change(self, event):
        """Processes container child events.

        These changes correspond to the addition and removal of unit
        relation presence nodes within the relation.
        """
        self._log.debug("relation membership change")
        # If the watcher has been stopped, don't observe child changes.
        if self._stopped:
            return

        # Restablish child watch on presence nodes and fetch children.
        children_d, watch_d = self._client.get_children_and_watch(
            self._container_path)

        # Callback to set watches on children and notify membership changes.
        children_d.addCallback(self._cb_container_children)

        # After processing children, setup the container watch callback.
        children_d.addCallback(lambda result: watch_d.addCallback(
            self._cb_container_child_change))

        return children_d

    def _cb_unit_change(self, event):
        """Process a unit relation settings node change.
        """
        self._log.debug("relation watcher settings change %s", event)
        unit_id = basename(event.path)

        # Don't process deleted units or if we've been stopped.
        if self._stopped or not unit_id in self._units:
            return

        exists_d, watch_d = self._client.exists_and_watch(event.path)

        # We don't process settings deleted events here. We should get
        # membership changes from the container watch.
        if event.type_name != "deleted":

            # Resolve the unit id to a name before for the user callback
            exists_d.addCallback(
                lambda stat: self._resolve_unit_names([unit_id]))

            # Invoke the user callback after the exists deferred
            # fires, and defer on the user callback, we won't fire the
            # watch on this node till the callback has completed.
            exists_d.addCallback(
                lambda (unit_name,): maybeDeferred(
                    self._callback, modified=unit_name))

        # Restablish the child watch callback after the user callback completes
        exists_d.addCallback(
            lambda result: watch_d.addCallback(self._cb_unit_change))

    def _filter_units(self, units):
        """A utility method to filter the unit relations based on relation type
        """
        return units

    def stop(self):
        """Stops watch processing, and callback invocation.

        After this method is invoked, no additional watches will be
        established any existing watches will be ignored. The user
        callback will not be invoked.

        Start can be called after stop, however any modifications of
        existing nodes will not be detected, only membership changes
        from the stopped period will be sent after restarting.
        """
        self._stopped = True
        self._log.debug("relation watcher stop")

    def start(self):
        """Start watching membership and settings changes of relation units.

        Returns a deferred that fires, when the related unit container
        has a child watch established, or a watch has been created on
        the container existence. Individual watches on the children
        will not yet have been established, but that property is O(n)
        size of the container and requires as many communication
        roundtrips. So the watch started callback is a more limited
        guarantee that at least the container watch (children or
        exists if the container does not already exist) has been
        established.
        """
        assert self._stopped or self._stopped is None, "Already started"
        self._stopped = False

        watcher_started = Deferred()

        def on_container_watched(result):
            self._log.debug("relation watcher start")
            watcher_started.callback(True)
            return result

        self._watch_container(on_container_watched)

        return watcher_started


class ClientServerUnitWatcher(RelationUnitWatcherBase):
    pass


class PeerUnitWatcher(RelationUnitWatcherBase):

    def _filter_units(self, units):
        """Units in the peer relation type, ignore themselves.
        """
        return [unit_id for unit_id in units \
                if unit_id != self._watcher_unit.internal_unit_id]
