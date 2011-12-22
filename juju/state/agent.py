import zookeeper


class AgentStateMixin(object):
    """A mixin for state objects that will have agents processes.

    Provides for the observation and connection of agent processes.
    Subclasses must implement M{_get_agent_path}.
    """

    def has_agent(self):
        """Does this domain object have an agent connected.

        Return boolean deferred informing whether an agent is
        connected.
        """
        d = self._client.exists(self._get_agent_path())
        d.addCallback(lambda result: bool(result))
        return d

    def _get_agent_path(self):
        raise NotImplementedError

    def watch_agent(self):
        """Observe changes to an agent's presence.

        Return two boolean deferreds informing whether an agent is
        connected, and whether a change happened. Both presence
        and content changes are encapsulated in the second deferred,
        callers interested in only presence need to perform event
        filtering as needed.
        """
        exists_d, watch_d = self._client.exists_and_watch(
            self._get_agent_path())
        exists_d.addCallback(lambda result: bool(result))
        return exists_d, watch_d

    def connect_agent(self):
        """Inform juju that this associated agent is alive.
        """
        return self._client.create(
            self._get_agent_path(), flags=zookeeper.EPHEMERAL)
