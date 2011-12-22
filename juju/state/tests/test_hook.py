import yaml

from twisted.internet.defer import inlineCallbacks
from juju.lib.testing import TestCase

from juju.state.endpoint import RelationEndpoint
from juju.state.hook import (
    HookContext, RelationChange, RelationHookContext)
from juju.state.errors import UnitRelationStateNotFound
from juju.state.tests.test_relation import RelationTestBase
from juju.state.utils import AddedItem, DeletedItem, ModifiedItem


class RelationChangeTest(TestCase):

    def test_change_properties(self):

        change = RelationChange("db", "membership", "mysql/0")
        self.assertEqual(change.relation_name, "db")
        self.assertEqual(change.change_type, "membership")
        self.assertEqual(change.unit_name, "mysql/0")


class ExecutionContextTest(RelationTestBase):

    @inlineCallbacks
    def setUp(self):
        yield super(ExecutionContextTest, self).setUp()
        wordpress_ep = RelationEndpoint(
            "wordpress", "client-server", "", "client")
        mysql_ep = RelationEndpoint(
            "mysql", "client-server", "", "server")
        self.wordpress_states = yield self.\
            add_relation_service_unit_from_endpoints(
                wordpress_ep, mysql_ep)
        self.mysql_states = yield self.add_opposite_service_unit(
            self.wordpress_states)
        self.relation = self.mysql_states["relation"]

    def get_execution_context(self, states, change_type, unit_name):
        change = RelationChange(
            states["service_relation"].relation_name,
            change_type,
            unit_name)
        return RelationHookContext(self.client,
                                   states["unit_relation"],
                                   change,
                                   unit_name=unit_name)

    def get_config_execution_context(self, states):
        return HookContext(self.client, unit_name=states["unit"].unit_name)

    @inlineCallbacks
    def test_get(self):
        """Settings from a related unit can be retrieved as a blob."""
        yield self.mysql_states["unit_relation"].set_data({"hello": "world"})

        # use mocker to verify we only access the node once.
        mock_client = self.mocker.patch(self.client)
        mock_client.get(self.get_unit_settings_path(self.mysql_states))
        self.mocker.passthrough()
        self.mocker.replay()

        context = self.get_execution_context(
            self.wordpress_states, "modified", "mysql/0")

        data = yield context.get("mysql/0")
        self.assertEqual(data, {"hello": "world"})

    @inlineCallbacks
    def test_get_uses_copied_dict(self):
        """If we retrieve the settings with a get, modifying those
        values does not modify the underlying write buffer. They
        must be explicitly set.
        """
        context = self.get_execution_context(
            self.wordpress_states, "modified", "mysql/0")
        yield context.set_value("hello", u"world")
        data = yield context.get("wordpress/0")
        self.assertEqual(
            data,
            {"hello": "world", "private-address": "wordpress-0.example.com"})
        del data["hello"]
        current_data = yield context.get("wordpress/0")
        self.assertNotEqual(current_data, data)

        self.client.set(self.get_unit_settings_path(self.mysql_states),
                        yaml.dump({"hello": "world"}))
        data = yield context.get("mysql/0")
        data["abc"] = 1

        data = yield context.get("mysql/0")
        del data["hello"]

        current_data = yield context.get("mysql/0")
        self.assertEqual(current_data, {"hello": "world"})

    @inlineCallbacks
    def test_get_value(self):
        """Settings from a related unit can be retrieved by name."""

        # Getting a value from an existing empty unit is returns empty
        # strings for all keys.
        context = self.get_execution_context(
            self.wordpress_states, "modified", "mysql/0")
        port = yield context.get_value("mysql/0", "port")
        self.assertEqual(port, "")

        # Write some data to retrieve and refetch the context
        yield self.mysql_states["unit_relation"].set_data({
                "host": "xe.example.com", "port": 2222})

        context = self.get_execution_context(
            self.wordpress_states, "modified", "mysql/0")

        # use mocker to verify we only access the node once.
        mock_client = self.mocker.patch(self.client)
        mock_client.get(self.get_unit_settings_path(self.mysql_states))
        self.mocker.passthrough()
        self.mocker.replay()

        port = yield context.get_value("mysql/0", "port")
        self.assertEqual(port, 2222)

        host = yield context.get_value("mysql/0", "host")
        self.assertEqual(host, "xe.example.com")

        magic = yield context.get_value("mysql/0", "unknown")
        self.assertEqual(magic, "")

        # fetching from a value from a non existent unit raises an error.
        yield self.assertFailure(
            context.get_value("mysql/5", "zebra"),
            UnitRelationStateNotFound)

    @inlineCallbacks
    def test_get_self_value(self):
        """Settings from the unit associated to context can be retrieved.

        This is also holds true for values locally modified on the context.
        """
        context = self.get_execution_context(
            self.wordpress_states, "modified", "mysql/0")

        data = yield context.get_value("wordpress/0", "magic")
        self.assertEqual(data, "")

        yield context.set_value("magic", "room")
        data = yield context.get_value("wordpress/0", "magic")
        self.assertEqual(data, "room")

    @inlineCallbacks
    def test_set(self):
        """The unit relation settings can be done as a blob."""
        context = self.get_execution_context(
            self.wordpress_states, "modified", "mysql/0")
        yield self.assertFailure(context.set("abc"), TypeError)
        data = {"abc": 12, "bar": "21"}
        yield context.set(data)
        changes = yield context.flush()
        content, stat = yield self.client.get(
            self.get_unit_settings_path(self.wordpress_states))
        data["private-address"] = "wordpress-0.example.com"
        self.assertEqual(yaml.load(content), data)
        self.assertEqual(changes,
                         [AddedItem("abc", 12), AddedItem("bar", "21")])

    @inlineCallbacks
    def test_set_value(self):
        """Values can be set by name, and are written at flush time."""
        context = self.get_execution_context(
            self.wordpress_states, "modified", "mysql/0")

        yield context.set_value("zebra", 12)
        yield context.set_value("donkey", u"abc")
        data, stat = yield self.client.get(
            self.get_unit_settings_path(self.wordpress_states))
        self.assertEqual(yaml.load(data),
                         {"private-address": "wordpress-0.example.com"})

        changes = yield context.flush()
        data, stat = yield self.client.get(
            self.get_unit_settings_path(self.wordpress_states))

        self.assertEqual(
            yaml.load(data),
            {"zebra": 12, "donkey": "abc",
             "private-address": "wordpress-0.example.com"})

        self.assertEqual(
            changes,
            [AddedItem("donkey", u"abc"), AddedItem("zebra", 12)])

    @inlineCallbacks
    def test_delete_value(self):
        """A value can be deleted via key.
        """
        yield self.client.set(
            self.get_unit_settings_path(
                self.wordpress_states),
            yaml.dump({"key": "secret"}))

        context = self.get_execution_context(
            self.wordpress_states, "modified", "mysql/0")
        yield context.delete_value("key")
        changes = yield context.flush()
        data, stat = yield self.client.get(
            self.get_unit_settings_path(self.wordpress_states))

        self.assertNotIn("key", yaml.load(data))
        self.assertEqual(changes, [DeletedItem("key", "secret")])

    @inlineCallbacks
    def test_delete_nonexistent_value(self):
        """Deleting a non existent key is a no-op.
        """
        yield self.client.set(
            self.get_unit_settings_path(self.wordpress_states),
            yaml.dump({"lantern": "green"}))

        context = self.get_execution_context(
            self.wordpress_states, "modified", "mysql/0")
        yield context.delete_value("key")
        changes = yield context.flush()
        data, stat = yield self.client.get(
            self.get_unit_settings_path(self.wordpress_states))

        self.assertEqual(yaml.load(data), {"lantern": "green"})
        self.assertEqual(changes, [])

    @inlineCallbacks
    def test_empty_flush_maintains_value(self):
        """Flushing a context which has no writes is a noop."""
        yield self.client.set(
            self.get_unit_settings_path(self.wordpress_states),
            yaml.dump({"key": "secret"}))
        context = self.get_execution_context(
            self.wordpress_states, "modified", "mysql/0")
        changes = yield context.flush()
        data, stat = yield self.client.get(
            self.get_unit_settings_path(self.wordpress_states))

        self.assertEqual(yaml.load(data),
                         {"key": "secret"})
        self.assertEqual(changes, [])

    @inlineCallbacks
    def test_flush_merges_setting_values(self):
        """When flushing a context we merge the changes with the current
        value of the node. The goal is to allow external processes to
        modify, delete, and add new values and allow those changes to
        persist to the final state, IFF the context has not also modified
        that key. If the context has modified the key, then context change
        takes precendence over the external change.
        """
        data = {"key": "secret",
                "seed": "21",
                "castle": "keep",
                "tower": "moat",
                "db": "wordpress",
                "host": "xe1.example.com"}

        yield self.client.set(
            self.get_unit_settings_path(self.wordpress_states),
            yaml.dump(data))

        context = self.get_execution_context(
            self.wordpress_states, "modified", "mysql/0")

        # On the context:
        #  - add a new key
        #  - modify an existing key
        #  - delete an old key/value
        yield context.set_value("home", "good")
        yield context.set_value("db", 21)
        yield context.delete_value("seed")
        # Also test conflict on delete, modify, and add
        yield context.delete_value("castle")
        yield context.set_value("tower", "rock")
        yield context.set_value("zoo", "keeper")

        # Outside of the context:
        #  - add a new key/value.
        #  - modify an existing value
        #  - delete a key
        data["port"] = 22
        data["host"] = "xe2.example.com"
        del data["key"]
        # also test conflict on delete, modify, and add
        del data["castle"]
        data["zoo"] = "mammal"
        data["tower"] = "london"

        yield self.client.set(
            self.get_unit_settings_path(self.wordpress_states),
            yaml.dump(data))

        changes = yield context.flush()

        data, stat = yield self.client.get(
            self.get_unit_settings_path(self.wordpress_states))

        self.assertEqual(
            yaml.load(data),
            {"port": 22, "host": "xe2.example.com", "db": 21, "home": "good",
             "tower": "rock", "zoo": "keeper"})
        self.assertEqual(
            changes,
            [ModifiedItem("db", "wordpress", 21),
             AddedItem("zoo", "keeper"),
             DeletedItem("seed", "21"),
             AddedItem("home", "good"),
             ModifiedItem("tower", "moat", "rock")])

    @inlineCallbacks
    def test_set_value_existing_setting(self):
        """We can set a value even if we have existing saved settings."""
        yield self.client.set(
            self.get_unit_settings_path(self.wordpress_states),
            yaml.dump({"key": "secret"}))
        context = self.get_execution_context(
            self.wordpress_states, "modified", "mysql/0")
        yield context.set_value("magic", "room")
        value = yield context.get_value("wordpress/0", "key")
        self.assertEqual(value, "secret")

        value = yield context.get_value("wordpress/0", "magic")
        self.assertEqual(value, "room")

    @inlineCallbacks
    def test_get_members(self):
        """The related units of a  relation can be retrieved."""
        context = self.get_execution_context(
            self.wordpress_states, "modified", "mysql/0")
        members = yield context.get_members()
        self.assertEqual(members, ["mysql/0"])
        # Add a new member and refetch
        yield self.add_related_service_unit(self.mysql_states)
        members2 = yield context.get_members()
        # There should be no change in the retrieved members.
        self.assertEqual(members, members2)

    @inlineCallbacks
    def test_get_members_peer(self):
        """When retrieving members from a peer relation, the unit
        associated to the context is not included in the set.
        """
        riak1_states = yield self.add_relation_service_unit(
            "riak", "riak", "peer", "peer")
        riak2_states = yield self.add_related_service_unit(
            riak1_states)
        context = self.get_execution_context(
            riak1_states, "modified", "riak/1")
        members = yield context.get_members()
        self.assertEqual(members, [riak2_states["unit"].unit_name])

    @inlineCallbacks
    def test_tracking_read_nodes(self):
        """The context tracks which nodes it has read, this is used
        by external components to determine if the context has
        read a value that may have subsequently been modified, and
        act accordingly.
        """
        context = self.get_execution_context(
            self.wordpress_states, "modified", "mysql/0")

        # notify the context of a change
        self.assertFalse(context.has_read("mysql/0"))

        # read the node data
        yield context.get("mysql/0")

        # Now verify we've read it
        self.assertTrue(context.has_read("mysql/0"))

        # And only it.
        self.assertFalse(context.has_read("mysql/1"))

    @inlineCallbacks
    def test_hook_knows_service(self):
        """Verify that hooks can get their local service."""
        context = self.get_execution_context(
            self.wordpress_states, "modified", "wordpress/0")

        service = yield context.get_local_service()
        self.assertEqual(service.service_name, "wordpress")

    @inlineCallbacks
    def test_hook_knows_unit_state(self):
        """Verify that hook has access to its local unit state."""
        context = self.get_execution_context(
            self.wordpress_states, "modified", "wordpress/0")

        unit = yield context.get_local_unit_state()
        self.assertEqual(unit.unit_name, "wordpress/0")

    @inlineCallbacks
    def test_config_get(self):
        """Verify we can get config settings.

        This is a simple test that basic I/O works through the
        context.
        """
        config = yield self.wordpress_states["service"].get_config()
        config.update({"hello": "world"})
        yield config.write()

        context = self.get_config_execution_context(self.wordpress_states)

        data = yield context.get_config()
        self.assertEqual(data, {"hello": "world"})

        # Verify that context.flush triggers writes as well
        data["goodbye"] = "goodnight"
        yield context.flush()
        # get a new yamlstate from the service itself
        config = yield self.wordpress_states["service"].get_config()
        self.assertEqual(config["goodbye"], "goodnight")

    @inlineCallbacks
    def test_config_get_cache(self):
        """Verify we can get config settings.

        This is a simple test that basic I/O works through the
        context.
        """
        config = yield self.wordpress_states["service"].get_config()
        config.update({"hello": "world"})
        yield config.write()

        context = self.get_config_execution_context(self.wordpress_states)

        data = yield context.get_config()
        self.assertEqual(data, {"hello": "world"})

        d2 = yield context.get_config()
        self.assertIs(data, d2)

    @inlineCallbacks
    def test_config_get_with_relation_context(self):
        """Verify we can get config settings.

        This is a simple test that basic I/O works through the
        context. This variant tests this through the Relation based
        hook context.
        """
        config = yield self.wordpress_states["service"].get_config()
        config.update({"hello": "world"})
        yield config.write()

        context = self.get_execution_context(self.wordpress_states,
                                             "modified", "wordpress/0")

        data = yield context.get_config()
        self.assertEqual(data, {"hello": "world"})

        d2 = yield context.get_config()
        self.assertIs(data, d2)
