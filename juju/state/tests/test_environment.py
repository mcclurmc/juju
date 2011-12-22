import yaml

from twisted.internet.defer import inlineCallbacks, Deferred

from juju.environment.tests.test_config import (
    EnvironmentsConfigTestBase, SAMPLE_ENV)
from juju.state.errors import EnvironmentStateNotFound
from juju.state.environment import (
    EnvironmentStateManager, GlobalSettingsStateManager, SETTINGS_PATH)
from juju.state.tests.common import StateTestBase

# Coverage dislikes dynamic imports, convert to static
from juju.providers import dummy


class EnvironmentStateManagerTest(StateTestBase, EnvironmentsConfigTestBase):

    @inlineCallbacks
    def setUp(self):
        yield super(EnvironmentStateManagerTest, self).setUp()
        self.environment_state_manager = EnvironmentStateManager(self.client)
        self.write_config(SAMPLE_ENV)
        self.config.load()

    @inlineCallbacks
    def tearDown(self):
        yield super(EnvironmentStateManagerTest, self).tearDown()

    @inlineCallbacks
    def test_set_config_state(self):
        """
        The simplest thing the manager can do is serialize a given
        environment and save it in zookeeper.
        """
        manager = self.environment_state_manager
        yield manager.set_config_state(self.config, "myfirstenv")

        serialized = self.config.serialize("myfirstenv")
        content, stat = yield self.client.get("/environment")
        self.assertEquals(yaml.load(content), yaml.load(serialized))

    @inlineCallbacks
    def test_set_config_state_replaces_environment(self):
        """
        Setting the environment should also work with an existing
        environment.
        """
        yield self.client.create("/environment", "Replace me!")

        manager = self.environment_state_manager
        yield manager.set_config_state(self.config, "myfirstenv")

        serialized = self.config.serialize("myfirstenv")
        content, stat = yield self.client.get("/environment")
        self.assertEquals(yaml.load(content), yaml.load(serialized))

    @inlineCallbacks
    def test_get_config(self):
        """
        We can also retrieve a loaded config from the environment.
        """
        manager = self.environment_state_manager
        yield manager.set_config_state(self.config, "myfirstenv")
        config = yield manager.get_config()
        serialized1 = self.config.serialize("myfirstenv")
        serialized2 = config.serialize("myfirstenv")
        self.assertEquals(yaml.load(serialized1), yaml.load(serialized2))

    def test_get_config_when_missing(self):
        """
        get_config should blow up politely if the environment config
        is missing.
        """
        d = self.environment_state_manager.get_config()
        return self.assertFailure(d, EnvironmentStateNotFound)

    @inlineCallbacks
    def test_wait_for_config_pre_existing(self):
        """
        wait_for_config() should return the environment immediately
        if it already exists.
        """
        manager = self.environment_state_manager
        yield manager.set_config_state(self.config, "myfirstenv")
        config = yield manager.wait_for_config()
        serialized1 = self.config.serialize("myfirstenv")
        serialized2 = config.serialize("myfirstenv")
        self.assertEquals(yaml.load(serialized1), yaml.load(serialized2))

    @inlineCallbacks
    def test_wait_for_config_pre_creation(self):
        """
        wait_for_config() should wait until the environment
        configuration is made available, and return it.
        """
        manager = self.environment_state_manager
        d = manager.wait_for_config()
        yield manager.set_config_state(self.config, "myfirstenv")
        config = yield d
        serialized1 = self.config.serialize("myfirstenv")
        serialized2 = config.serialize("myfirstenv")
        self.assertEquals(yaml.load(serialized1), yaml.load(serialized2))

    @inlineCallbacks
    def test_wait_for_config_retries_on_race(self):
        """
        If the config seems to exist, but then goes away, try again.
        """
        mock_manager = self.mocker.patch(self.environment_state_manager)
        mock_manager.get_config()
        self.mocker.throw(EnvironmentStateNotFound())
        mock_manager.get_config()
        self.mocker.passthrough()
        self.mocker.replay()

        manager = self.environment_state_manager
        yield manager.set_config_state(self.config, "myfirstenv")
        config = yield manager.wait_for_config()
        serialized1 = self.config.serialize("myfirstenv")
        serialized2 = config.serialize("myfirstenv")
        self.assertEquals(yaml.load(serialized1), yaml.load(serialized2))


class GlobalSettingsTest(StateTestBase):

    @inlineCallbacks
    def setUp(self):
        yield super(GlobalSettingsTest, self).setUp()
        self.manager = GlobalSettingsStateManager(self.client)

    @inlineCallbacks
    def test_get_set_provider_type(self):
        """Debug logging is off by default."""
        self.assertEqual((yield self.manager.get_provider_type()), None)
        yield self.manager.set_provider_type("ec2")
        self.assertEqual((yield self.manager.get_provider_type()), "ec2")
        content, stat = yield self.client.get("/settings")
        self.assertEqual(yaml.load(content),
                         {"provider-type": "ec2"})

    @inlineCallbacks
    def test_get_debug_log_enabled_no_settings_default(self):
        """Debug logging is off by default."""
        value = yield self.manager.is_debug_log_enabled()
        self.assertFalse(value)

    @inlineCallbacks
    def test_set_debug_log(self):
        """Debug logging can be (dis)enabled via the runtime manager."""
        yield self.manager.set_debug_log(True)
        value = yield self.manager.is_debug_log_enabled()
        self.assertTrue(value)
        yield self.manager.set_debug_log(False)
        value = yield self.manager.is_debug_log_enabled()
        self.assertFalse(value)

    @inlineCallbacks
    def test_watcher(self):
        """Use the watch facility of the settings manager to observer changes.
        """
        results = []
        callbacks = [Deferred() for i in range(5)]

        def watch(content):
            results.append(content)
            callbacks[len(results) - 1].callback(content)

        yield self.manager.set_debug_log(True)
        yield self.manager.watch_settings_changes(watch)
        self.assertTrue(results)

        yield self.manager.set_debug_log(False)
        yield self.manager.set_debug_log(True)
        yield callbacks[2]
        self.assertEqual(len(results), 3)

        self.assertEqual(
            map(lambda x: isinstance(x, bool) and x or x.type_name, results),
            [True, "changed", "changed"])
        data, stat = yield self.client.get(SETTINGS_PATH)
        self.assertEqual(
            (yield self.manager.is_debug_log_enabled()),
            True)

    @inlineCallbacks
    def test_watcher_start_stop(self):
        """Setings watcher observes changes till stopped.

        Additionally watching can be enabled on a setting node that doesn't
        exist yet.

        XXX For reasons unknown this fails under coverage outside of the test,
        at least for me (k.), but not for others.
        """
        results = []
        callbacks = [Deferred() for i in range(5)]

        def watch(content):
            results.append(content)
            callbacks[len(results) - 1].callback(content)

        watcher = yield self.manager.watch_settings_changes(watch)
        yield self.client.create(SETTINGS_PATH, "x")
        value = yield callbacks[0]
        self.assertEqual(value.type_name, "created")

        data = dict(x=1, y=2, z=3, moose=u"moon")
        yield self.client.set(
            SETTINGS_PATH, yaml.safe_dump(data))
        value = yield callbacks[1]
        self.assertEqual(value.type_name, "changed")

        watcher.stop()

        yield self.client.set(SETTINGS_PATH, "z")
        # Give a chance for things to go bad.
        yield self.sleep(0.1)
        self.assertFalse(callbacks[2].called)

    @inlineCallbacks
    def test_watcher_stops_on_callback_exception(self):
        """If a callback has an exception the watcher is stopped."""
        results = []
        callbacks = [Deferred(), Deferred()]

        def watch(content):
            results.append(content)
            callbacks[len(results) - 1].callback(content)
            raise AttributeError("foobar")

        def on_error(error):
            results.append(True)

        yield self.client.create(SETTINGS_PATH, "z")
        watcher = yield self.manager.watch_settings_changes(
            watch, on_error)
        yield callbacks[0]

        # The callback error should have disconnected the system.
        yield self.client.set(SETTINGS_PATH, "x")

        # Give a chance for things to go bad.
        yield self.sleep(0.1)

        # Verify nothing did go bad.
        self.assertFalse(watcher.is_running)
        self.assertFalse(callbacks[1].called)
        self.assertIdentical(results[1], True)
