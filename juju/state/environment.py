import zookeeper
import yaml

from twisted.internet.defer import inlineCallbacks, returnValue

from txzookeeper.utils import retry_change

from juju.environment.config import EnvironmentsConfig
from juju.state.errors import EnvironmentStateNotFound
from juju.state.base import StateBase


SETTINGS_PATH = "/settings"


class EnvironmentStateManager(StateBase):

    @inlineCallbacks
    def set_config_state(self, config, environment_name):
        serialized_env = config.serialize(environment_name)

        def change_environment(old_content, stat):
            return serialized_env
        yield retry_change(self._client, "/environment",
                           change_environment)

    @inlineCallbacks
    def get_config(self):
        try:
            content, stat = yield self._client.get("/environment")
        except zookeeper.NoNodeException:
            raise EnvironmentStateNotFound()
        config = EnvironmentsConfig()
        config.parse(content)
        returnValue(config)

    @inlineCallbacks
    def wait_for_config(self):
        exists_d, watch_d = self._client.exists_and_watch("/environment")
        exists = yield exists_d
        if exists:
            try:
                result = yield self.get_config()
            except EnvironmentStateNotFound:
                result = yield self.wait_for_config()
        else:
            watch_d.addCallback(lambda result: self.get_config())
            result = yield watch_d
        returnValue(result)

    # TODO The environment waiting/watching logic in the
    #      provisioning agent should be moved here (#640726).


class GlobalSettingsStateManager(StateBase):
    """State for the the environment's runtime characterstics.

    This can't be stored directly in the environment, as that has access
    restrictions. The runtime state can be accessed from all connected
    juju clients.
    """

    def set_provider_type(self, provider_type):
        return self._set_value("provider-type", provider_type)

    def get_provider_type(self):
        return self._get_value("provider-type")

    def is_debug_log_enabled(self):
        """Find out if the debug log is enabled. Returns a boolean.
        """
        return self._get_value("debug-log", False)

    def set_debug_log(self, enabled):
        """Enable/Disable the debug log.

        :param enabled: Boolean denoting whether the log should be enabled.
        """
        return self._set_value("debug-log", bool(enabled))

    @inlineCallbacks
    def _get_value(self, key, default=None):
        try:
            content, stat = yield self._client.get(SETTINGS_PATH)
        except zookeeper.NoNodeException:
            returnValue(default)
        data = yaml.load(content)
        returnValue(data.get(key, default))

    def _set_value(self, key, value):

        def set_value(old_content, stat):
            if not old_content:
                data = {}
            else:
                data = yaml.load(old_content)
            data[key] = value
            return yaml.safe_dump(data)

        return retry_change(self._client, SETTINGS_PATH, set_value)

    def watch_settings_changes(self, callback, error_callback=None):
        """Register a callback to invoked when the runtime changes.

        This watch primarily serves to get a persistent watch of the
        existance and modifications to the global settings.

        The callback will be invoked the first time as soon as the
        settings are present. If the settings are already present, it
        will be invoked immediately. For initial presence the callback
        value will be the boolean value True.

        An error callback will be invoked if the callback raised an
        exception.  The watcher will be stopped, and the error
        consumed by the error callback.
        """
        assert callable(callback), "Invalid callback"
        watcher = _RuntimeWatcher(self._client, callback, error_callback)
        return watcher.start()


class _RuntimeWatcher(object):

    def __init__(self, client, callback, error_callback=None):
        self._client = client
        self._callback = callback
        self._watching = False
        self._error_callback = error_callback

    @property
    def is_running(self):
        return self._watching

    @inlineCallbacks
    def start(self):
        """Start watching the settings.

        The callback will receive notification of changes in addition
        to an initial presence message. No state is conveyed via
        the watch api only notifications.
        """
        assert not self._watching, "Already Watching"
        self._watching = True

        # This logic will break if the node is removed, and so will
        # the function below, but the internal logic never removes
        # it, so we do not handle this case.
        exists_d, watch_d = self._client.exists_and_watch(SETTINGS_PATH)
        exists = yield exists_d

        if exists:
            yield self._on_settings_changed()
        else:
            watch_d.addCallback(self._on_settings_changed)

        returnValue(self)

    def stop(self):
        """Stop the environment watcher, no more callbacks will be invoked."""
        self._watching = False

    @inlineCallbacks
    def _on_settings_changed(self, change_event=True):
        """Setup a perpetual watch till the watcher is stopped.
        """
        # Ensure the watch is active, and the client is connected.
        if not self._watching or not self._client.connected:
            returnValue(False)

        exists_d, watch_d = self._client.exists_and_watch(SETTINGS_PATH)

        try:
            yield self._callback(change_event)
        except Exception, e:
            self._watching = False
            if self._error_callback:
                self._error_callback(e)
            return

        watch_d.addCallback(self._on_settings_changed)
