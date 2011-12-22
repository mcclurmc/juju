import yaml
from twisted.internet.defer import inlineCallbacks

from juju.control import main
from juju.control.tests.common import ControlToolTest
from juju.state.tests.test_service import ServiceStateManagerTestBase


class UnexposeControlTest(
    ServiceStateManagerTestBase, ControlToolTest):

    @inlineCallbacks
    def setUp(self):
        yield super(UnexposeControlTest, self).setUp()
        config = {
            "environments": {"firstenv": {"type": "dummy"}}}

        self.write_config(yaml.dump(config))
        self.config.load()
        self.service_state = yield self.add_service_from_charm("wordpress")
        self.output = self.capture_logging()
        self.stderr = self.capture_stream("stderr")

    @inlineCallbacks
    def test_unexpose_service(self):
        """Test subcommand clears exposed flag for service."""
        yield self.service_state.set_exposed_flag()
        exposed_flag = yield self.service_state.get_exposed_flag()
        self.assertTrue(exposed_flag)

        finished = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()
        main(["unexpose", "wordpress"])
        yield finished
        exposed_flag = yield self.service_state.get_exposed_flag()
        self.assertFalse(exposed_flag)
        self.assertIn("Service 'wordpress' was unexposed.", self.output.getvalue())

    @inlineCallbacks
    def test_unexpose_service_not_exposed(self):
        """Test subcommand keeps an unexposed service still unexposed."""
        finished = self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()
        main(["unexpose", "wordpress"])
        yield finished
        exposed_flag = yield self.service_state.get_exposed_flag()
        self.assertFalse(exposed_flag)
        self.assertIn("Service 'wordpress' was not exposed.",
                      self.output.getvalue())

    # various errors

    def test_unexpose_with_no_args(self):
        """Test subcommand takes at least one service argument."""
        # in argparse, before reactor startup
        self.assertRaises(SystemExit, main, ["unexpose"])
        self.assertIn(
            "juju unexpose: error: too few arguments",
            self.stderr.getvalue())

    def test_unexpose_with_too_many_args(self):
        """Test subcommand takes at most one service argument."""
        self.assertRaises(
            SystemExit, main, ["unexpose", "foo", "fum"])
        self.assertIn(
            "juju: error: unrecognized arguments: fum",
            self.stderr.getvalue())

    @inlineCallbacks
    def test_unexpose_unknown_service(self):
        """Test subcommand fails if service does not exist."""
        finished = self.setup_cli_reactor()
        self.setup_exit(0) # XXX change when bug 697093 is fixed
        self.mocker.replay()
        main(["unexpose", "foobar"])
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
        main(["unexpose", "--environment", "roman-candle", "wordpress"])
        yield finished
        self.assertIn(
            "Invalid environment 'roman-candle'",
            self.stderr.getvalue())
