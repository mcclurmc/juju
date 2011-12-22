import yaml
from twisted.internet.defer import inlineCallbacks

from juju.control import main
from juju.control.tests.common import ControlToolTest
from juju.state.tests.test_service import ServiceStateManagerTestBase


class ExposeControlTest(
    ServiceStateManagerTestBase, ControlToolTest):

    @inlineCallbacks
    def setUp(self):
        yield super(ExposeControlTest, self).setUp()
        config = {
            "environments": {"firstenv": {"type": "dummy"}}}

        self.write_config(yaml.dump(config))
        self.config.load()
        self.service_state = yield self.add_service_from_charm("wordpress")
        self.output = self.capture_logging()
        self.stderr = self.capture_stream("stderr")

    @inlineCallbacks
    def test_expose_service(self):
        """Test subcommand sets the exposed flag for service."""
        finished = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()
        main(["expose", "wordpress"])
        yield finished
        exposed_flag = yield self.service_state.get_exposed_flag()
        self.assertTrue(exposed_flag)
        self.assertIn("Service 'wordpress' was exposed.", self.output.getvalue())

    @inlineCallbacks
    def test_expose_service_twice(self):
        """Test subcommand can run multiple times, keeping service exposed."""
        yield self.service_state.set_exposed_flag()
        exposed_flag = yield self.service_state.get_exposed_flag()
        self.assertTrue(exposed_flag)

        finished = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()
        main(["expose", "wordpress"])
        yield finished
        exposed_flag = yield self.service_state.get_exposed_flag()
        self.assertTrue(exposed_flag)
        self.assertIn("Service 'wordpress' was already exposed.",
                      self.output.getvalue())

    # various errors

    def test_expose_with_no_args(self):
        """Test subcommand takes at least one service argument."""
        # in argparse, before reactor startup
        self.assertRaises(SystemExit, main, ["expose"])
        self.assertIn(
            "juju expose: error: too few arguments",
            self.stderr.getvalue())

    def test_expose_with_too_many_args(self):
        """Test subcommand takes at most one service argument."""
        self.assertRaises(
            SystemExit, main, ["expose", "foo", "fum"])
        self.assertIn(
            "juju: error: unrecognized arguments: fum",
            self.stderr.getvalue())

    @inlineCallbacks
    def test_expose_unknown_service(self):
        """Test subcommand fails if service does not exist."""
        finished = self.setup_cli_reactor()
        self.setup_exit(0) # XXX change when bug 697093 is fixed
        self.mocker.replay()
        main(["expose", "foobar"])
        yield finished
        self.assertIn(
            "Service 'foobar' was not found",
            self.stderr.getvalue())

    @inlineCallbacks
    def test_invalid_environment(self):
        """Test command with an environment that hasn't been set up."""
        finished = self.setup_cli_reactor()
        self.setup_exit(1)
        self.mocker.replay()
        main(["expose", "--environment", "roman-candle", "wordpress"])
        yield finished
        self.assertIn(
            "Invalid environment 'roman-candle'",
            self.stderr.getvalue())
