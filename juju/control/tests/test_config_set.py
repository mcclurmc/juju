from yaml import dump

from twisted.internet.defer import inlineCallbacks

from juju.control import main
from juju.control.config_set import config_set
from .common import MachineControlToolTest


class ControlJujuSetTest(MachineControlToolTest):

    @inlineCallbacks
    def setUp(self):
        yield super(ControlJujuSetTest, self).setUp()
        config = {
            "environments": {"firstenv": {"type": "dummy"}}}
        self.write_config(dump(config))
        self.config.load()
        self.service_state = yield self.add_service_from_charm("wordpress")
        self.service_unit = yield self.service_state.add_unit_state()
        self.environment = self.config.get_default()
        self.stderr = self.capture_stream("stderr")

    @inlineCallbacks
    def test_set_and_get(self):
        finished = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()

        main(["set",
              "wordpress",
              "blog-title=Hello Tribune?"])
        yield finished

        # Verify the state is accessible
        state = yield self.service_state.get_config()
        self.assertEqual(state, {"blog-title": "Hello Tribune?"})

    @inlineCallbacks
    def test_set_invalid_option(self):
        finished = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()
        main(["set",
              "wordpress",
              "blog-roll=What's a blog-roll?"])
        yield finished

        # Make sure we got an error message to the user
        self.assertIn("blog-roll is not a valid configuration option.",
                      self.stderr.getvalue())

    @inlineCallbacks
    def test_set_invalid_service(self):
        finished = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()

        main(["set",
              "whatever",
              "blog-roll=What's a blog-roll?"])
        yield finished

        self.assertIn("Service 'whatever' was not found",
                      self.stderr.getvalue())

    @inlineCallbacks
    def test_set_valid_option(self):
        finished = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()

        main(["set",
              "wordpress",
              "blog-title=My title"])
        yield finished

        # Verify the state is accessible
        state = yield self.service_state.get_config()
        self.assertEqual(state, {"blog-title": "My title"})

    @inlineCallbacks
    def test_multiple_calls_with_defaults(self):
        """Bug #873643

        Calling config set multiple times would result in the
        subsequent calls resetting values to defaults if the values
        were not explicitly set in each call. This verifies that each
        value need not be present in each call for proper functioning.
        """
        # apply all defaults as done through deploy
        self.service_state = yield self.add_service_from_charm("configtest")
        self.service_unit = yield self.service_state.add_unit_state()

        # Publish the defaults as deploy should have done
        charm = yield self.service_state.get_charm_state()
        config_options = yield charm.get_config()
        defaults = config_options.get_defaults()

        state = yield self.service_state.get_config()
        yield state.update(defaults)
        yield state.write()

        # Now perform two update in each case moving one value away
        # from their default and checking the end result is as expected
        yield config_set(self.environment, "configtest",
                         ["foo=new foo"])
        # force update
        yield state.read()
        self.assertEqual(state, {"foo": "new foo",
                                 "bar": "bar-default"})

        # Now perform two update in each case moving one value away
        # from their default and checking the end result is as expected
        yield config_set(self.environment, "configtest",
                         ["bar=new bar"])
        # force update
        yield state.read()
        self.assertEqual(state, {"foo": "new foo",
                                 "bar": "new bar"})
