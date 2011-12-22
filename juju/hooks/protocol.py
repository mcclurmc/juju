"""
Protocol

Twisted AMP protocol used between the UnitAgent (via the
juju/hooks/invoker template) and client scripts invoked hooks on
behalf of charm authors.

Interactions with the server happen through an exchange of
commands. Each interaction with the UnitAgent is coordinated through
the use of a single command.

These commands have there concrete implementation relative to server
state in the UnitAgentServer class. The utility methods in
UnitAgentClient provide a synchronous interface for scripts derived
from juju.hooks.cli to expose to scripts.

To extend the system with additional command the following pattern is
used.

- Author a new BaseCommand subclass outlining the arguments and returns
the Command neeeds.

- Implement a responder for that command in UnitAgentServer returning
  a dict with the response agreed upon by the new Command

- Implement a client side callable in UnitAgentClient which handles
  any pre-wire data marshaling (with the goal of mapping to the
  Command objects contract) and return a result after waiting for any
  asynchronous actions to complete.

UnitAgentClient and UnitAgentServer act as the client and server sides
of an RPC interface. Due to this they have a number of arguments in
common which are documented here.

arguments:

`client_id` -- Client specifier identifying a client to the server
side thus connecting it with an juju.state.hook.HookContent (str)

`unit_name` -- String of the name of the unit being queried or
manipulated.


"""
import json
import logging

from twisted.internet import defer
from twisted.internet import protocol
from twisted.protocols import amp

from juju.errors import JujuError
from juju.state.errors import UnitRelationStateNotFound
from juju.state.hook import RelationHookContext


class NoSuchUnit(JujuError):
    """
    The requested Unit Name wasn't found
    """
    # Amp Currently cannot construct the 3 required arguments for
    # UnitRelationStateNotFound. This captures the error message in
    # a way that can pass over the wire
    pass


class NotRelationContext(JujuError):
    """Relation commands can only be used in relation hooks"""


class NoSuchKey(JujuError):
    """ The requested key did not exist.
    """


class BaseCommand(amp.Command):
    errors = {NoSuchUnit: "NoSuchUnit",
              NoSuchKey: "NoSuchKey",
              NotRelationContext: "NotRelationContext",
              UnitRelationStateNotFound: "UnitRelationStateNotFound"}


# All the commands below this point should be documented in the
# specification specifications/unit-agent-hooks
class RelationGetCommand(BaseCommand):
    commandName = "relation_get"
    arguments = [("client_id", amp.String(),),
                 ("unit_name", amp.String(),),
                 ("setting_name", amp.String())]
    response = [("data", amp.String())]


class RelationSetCommand(BaseCommand):
    commandName = "relation_set"
    arguments = [("client_id", amp.String(),),
                 ("json_blob", amp.String())]
    response = []


class ListRelationsCommand(BaseCommand):
    # comma delimited strings
    arguments = [("client_id", amp.String(),)]
    response = [("members", amp.String())]


class LogCommand(BaseCommand):
    arguments = [("level", amp.Integer()),
                 ("message", amp.String())]
    response = []


class ConfigGetCommand(BaseCommand):
    commandName = "config_get"
    arguments = [("client_id", amp.String()),
                 ("option_name", amp.String())]
    response = [("data", amp.String())]


class OpenPortCommand(BaseCommand):
    commandName = "open_port"
    arguments = [("client_id", amp.String()),
                 ("port", amp.Integer()),
                 ("proto", amp.String())]
    response = []


class ClosePortCommand(BaseCommand):
    commandName = "close_port"
    arguments = [("client_id", amp.String()),
                 ("port", amp.Integer()),
                 ("proto", amp.String())]
    response = []


class UnitGetCommand(BaseCommand):
    commandName = "get_unit_info"
    arguments = [("client_id", amp.String()),
                 ("setting_name", amp.String())]
    response = [("data", amp.String())]


def require_relation_context(context):
    """Is this a valid context for relation hook commands?

    A guard for relation methods ensuring they have the proper
    RelationHookContext. A NotRelationContext exception is raised when
    a non-RelationHookContext is provided.
    """
    if not isinstance(context, RelationHookContext):
        raise NotRelationContext(
            "Calling relation related method without relation context: %s" %
                type(context))


class UnitAgentServer(amp.AMP, object):
    """
    Protocol used by the UnitAgent to provide a server side to CLI
    tools
    """

    def connectionMade(self):
        """Inform the factory a connection was made.
        """
        super(UnitAgentServer, self).connectionMade()
        self.factory.connectionMade(self)

    @RelationGetCommand.responder
    @defer.inlineCallbacks
    def relation_get(self, client_id, unit_name, setting_name):
        """Get settings from a state.hook.RelationHookContext

        :param settings_name: optional setting_name (str) indicating that
        the client requested a single value only.

        """
        context = self.factory.get_context(client_id)
        require_relation_context(context)

        try:
            if setting_name:
                data = yield context.get_value(unit_name, setting_name)
            else:
                data = yield context.get(unit_name)
        except UnitRelationStateNotFound, e:
            raise NoSuchUnit(str(e))
        defer.returnValue(dict(data=json.dumps(data)))

    @RelationSetCommand.responder
    @defer.inlineCallbacks
    def relation_set(self, client_id, json_blob):
        """Set values into state.hook.RelationHookContext.

        :param json_blob: a JSON serialized string of a dict that will
        contain the delta of settings to be applied to a unit_name.
        """
        data = json.loads(json_blob)
        context = yield self.factory.get_context(client_id)
        require_relation_context(context)
        for k, v in data.items():
            if not v.strip():
                yield context.delete_value(k)
            else:
                yield context.set_value(k, v)
        defer.returnValue({})

    @ListRelationsCommand.responder
    @defer.inlineCallbacks
    def list_relations(self, client_id):
        """Lists the members of a Relation.
        """
        context = yield self.factory.get_context(client_id)
        require_relation_context(context)
        members = yield context.get_members()
        defer.returnValue(dict(members=" ".join(members)))

    @LogCommand.responder
    @defer.inlineCallbacks
    def log(self, level, message):
        """Log a message from the hook with the UnitAgent.

        :param level: A python logging module log level integer
        indicating the level the message should be logged at.

        :param message: A string containing the message to be logged.
        """
        yield self.factory.log(level, message)
        defer.returnValue({})

    @ConfigGetCommand.responder
    @defer.inlineCallbacks
    def config_get(self, client_id, option_name):
        """Retrieve one or more configuration options for a service.

        Service is implied in the hooks context.

        :client_id: hooks client id, used to define a context for a
        consistent view of state, as in the relation_<get|set>
        commands.

        :param option_name: Optional name of an option to fetch from
        the list.
        """
        context = self.factory.get_context(client_id)
        options = yield context.get_config()
        if option_name:
            options = options.get(option_name)
        else:
            options = dict(options)

        defer.returnValue(dict(data=json.dumps(options)))

    @OpenPortCommand.responder
    @defer.inlineCallbacks
    def open_port(self, client_id, port, proto):
        """Open `port` using `proto` for the service unit.

        The service unit is implied by the hook's context.

        `client_id` - hook's client id, used to define a context for a
           consistent view of state.

        `port` - port to be opened

        `proto` - protocol of the port to be opened
        """
        context = self.factory.get_context(client_id)
        service_unit_state = yield context.get_local_unit_state()
        yield service_unit_state.open_port(port, proto)
        yield self.factory.log(logging.DEBUG, "opened %s/%s" % (port, proto))
        defer.returnValue({})

    @ClosePortCommand.responder
    @defer.inlineCallbacks
    def close_port(self, client_id, port, proto):
        """Close `port` using `proto` for the service unit.

        The service unit is implied by the hook's context.

        `client_id` - hook's client id, used to define a context for a
           consistent view of state.

        `port` - port to be closed

        `proto` - protocol of the port to be closed
        """
        context = self.factory.get_context(client_id)
        service_unit_state = yield context.get_local_unit_state()
        yield service_unit_state.close_port(port, proto)
        yield self.factory.log(logging.DEBUG, "closed %s/%s" % (port, proto))
        defer.returnValue({})

    @UnitGetCommand.responder
    @defer.inlineCallbacks
    def get_unit_info(self, client_id, setting_name):
        """Retrieve a unit value with the given name.

        :param client_id: The hook's client id, used to define a context
            for a consitent view of state.
        :param setting_name: The name of the setting to be retrieved.
        """
        context = self.factory.get_context(client_id)
        unit_state = yield context.get_local_unit_state()
        yield self.factory.log(
            logging.DEBUG, "Get unit setting: %r" % setting_name)
        if setting_name == "private-address":
            value = yield unit_state.get_private_address()
        elif setting_name == "public-address":
            value = yield unit_state.get_public_address()
        else:
            raise NoSuchKey("Unit has no setting: %r" % setting_name)
        value = value or ""
        defer.returnValue({"data": value})


class UnitAgentClient(amp.AMP, object):
    """
    Helper used by the CLI tools to call the UnitAgentServer protocol run in
    the UnitAgent.
    """
    @defer.inlineCallbacks
    def relation_get(self, client_id, unit_name, setting_name):
        """ See UnitAgentServer.relation_get
        """
        if not setting_name:
            setting_name = ""
        result = yield self.callRemote(RelationGetCommand,
                                       client_id=client_id,
                                       unit_name=unit_name,
                                       setting_name=setting_name)
        defer.returnValue(json.loads(result["data"]))

    @defer.inlineCallbacks
    def relation_set(self, client_id, data):
        """Set relation settings for unit_name

        :param data: Python dict applied as a delta hook settings

        """
        json_blob = json.dumps(data)
        yield self.callRemote(RelationSetCommand,
                              client_id=client_id,
                              json_blob=json_blob)
        defer.returnValue(None)

    @defer.inlineCallbacks
    def list_relations(self, client_id):
        result = yield self.callRemote(ListRelationsCommand,
                                       client_id=client_id)
        members = result["members"].split()
        defer.returnValue(members)

    @defer.inlineCallbacks
    def log(self, level, message):
        if isinstance(message, (list, tuple)):
            message = " ".join(message)

        result = yield self.callRemote(LogCommand,
                                       level=level,
                                       message=message)
        defer.returnValue(result)

    @defer.inlineCallbacks
    def config_get(self, client_id, option_name=None):
        """See UnitAgentServer.config_get."""
        result = yield self.callRemote(ConfigGetCommand,
                                       client_id=client_id,
                                       option_name=option_name)
        # Unbundle and deserialize
        result = json.loads(result["data"])
        defer.returnValue(result)

    @defer.inlineCallbacks
    def open_port(self, client_id, port, proto):
        """Open `port` for `proto` for this unit identified by `client_id`."""
        yield self.callRemote(
            OpenPortCommand, client_id=client_id, port=port, proto=proto)
        defer.returnValue(None)

    @defer.inlineCallbacks
    def close_port(self, client_id, port, proto):
        """Close `port` for `proto` for this unit identified by `client_id`."""
        yield self.callRemote(
            ClosePortCommand, client_id=client_id, port=port, proto=proto)
        defer.returnValue(None)

    @defer.inlineCallbacks
    def get_unit_info(self, client_id, setting_name):
        result = yield self.callRemote(
            UnitGetCommand, client_id=client_id, setting_name=setting_name)
        defer.returnValue(result)


class UnitSettingsFactory(protocol.ServerFactory, object):
    protocol = UnitAgentServer

    def __init__(self, context_provider, logger=None):
        """ Factory to be used by the server for communications.

        :param context_provider: Callable(client_id) returning an
        juju.state.hook.RelationHookContext. A given `client_id`
        will map to a single HookContext.

        :param log: When not None a python.logging.Logger object. The
        log is usually managed by the UnitAgent and is passed through
        the factory.

        """
        self.context_provider = context_provider
        self._logger = logger
        self.onMade = defer.Deferred()

    def get_context(self, client_id):
        return self.context_provider(client_id)

    def log(self, level, message):
        if self._logger is not None:
            self._logger.log(level, message)

    def connectionMade(self, protocol):
        if self.onMade:
            self.onMade.callback(protocol)
            self.onMade = None
