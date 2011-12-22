import yaml

from twisted.internet.defer import inlineCallbacks

from juju.control import main

from juju.charm.tests import local_charm_id
from .common import MachineControlToolTest


class ControlJujuGetTest(MachineControlToolTest):

    @inlineCallbacks
    def setUp(self):
        yield super(ControlJujuGetTest, self).setUp()
        config = {
            "environments": {"firstenv": {"type": "dummy"}}}
        self.write_config(yaml.dump(config))
        self.config.load()
        self.stderr = self.capture_stream("stderr")

    @inlineCallbacks
    def test_get_service_config(self):
        finished = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()

        self.service_state = yield self.add_service_from_charm("wordpress")
        config = yield self.service_state.get_config()
        # The value which isn't in the config won't be displayed.
        settings = {"blog-title": "Hello World", "world": 123}
        config.update(settings)
        yield config.write()

        output = self.capture_stream("stdout")
        main(["get", "wordpress"])

        yield finished
        data = yaml.load(output.getvalue())
        self.assertEqual(
            {"service": "wordpress",
             "charm": "local:series/wordpress-3",
             'settings': {'blog-title': {
                 'description': 'A descriptive title used for the blog.',
                 'type': 'string',
                 'value': 'Hello World'}}},
            data)

    @inlineCallbacks
    def test_get_service_config_with_no_value(self):
        finished = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()

        self.service_state = yield self.add_service_from_charm(
            "dummy", local_charm_id(self.charm))
        config = yield self.service_state.get_config()
        config["title"] = "hello movie"
        config["skill-level"] = 24
        yield config.write()

        output = self.capture_stream("stdout")
        main(["get", "dummy"])

        yield finished
        data = yaml.load(output.getvalue())

        self.assertEqual(
            {"service": "dummy",
             "charm": "local:series/dummy-1",
             "settings": {
                 'outlook': {
                     'description': 'No default outlook.',
                     'type': 'string',
                     'value': '-Not set-'},
                 'skill-level': {
                     'description': 'A number indicating skill.',
                     'value': 24,
                     'type': 'int'},
                 'title': {
                     'description': ('A descriptive title used '
                                    'for the service.'),
                     'value': 'hello movie',
                     'type': 'string'},
                 'username': {
                     'description': ('The name of the initial account (given '
                                     'admin permissions).'),
                     'default': 'admin001',
                     'value': '-Not set-',
                     'type': 'string'}}},
            data)

    @inlineCallbacks
    def test_set_invalid_service(self):
        finished = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()

        main(["get", "whatever"])

        yield finished

        self.assertIn("Service 'whatever' was not found",
                      self.stderr.getvalue())
