from twisted.internet.defer import succeed, inlineCallbacks
from yaml import dump

from juju.lib.mocker import MATCH
from juju.providers.dummy import MachineProvider
from juju.control import main

from .common import ControlToolTest


class ControlDestroyEnvironmentTest(ControlToolTest):

    @inlineCallbacks
    def test_destroy_multiple_environments_no_default(self):
        """With multiple environments a default needs to be set or passed.
        """
        config = {
            "environments": {"firstenv": {"type": "dummy"},
                             "secondenv": {"type": "dummy"}}}
        self.write_config(dump(config))
        finished = self.setup_cli_reactor()
        self.setup_exit(1)
        self.mocker.replay()

        stderr = self.capture_stream('stderr')
        main(["destroy-environment"])
        yield finished
        self.assertIn(
            "There are multiple environments and no explicit default",
            stderr.getvalue())

    @inlineCallbacks
    def test_destroy_invalid_environment(self):
        """If an invalid environment is specified, an error message is given.
        """
        config = {
            "environments": {"firstenv": {"type": "dummy"},
                             "secondenv": {"type": "dummy"}}}
        self.write_config(dump(config))
        finished = self.setup_cli_reactor()
        self.setup_exit(1)
        self.mocker.replay()

        stderr = self.capture_stream('stderr')
        main(["destroy-environment", "-e", "thirdenv"])
        yield finished
        self.assertIn("Invalid environment 'thirdenv'", stderr.getvalue())

    @inlineCallbacks
    def test_destroy_environment_prompt_no(self):
        """If a user returns no to the prompt, destroy-environment is aborted.
        """
        config = {
            "environments": {"firstenv": {"type": "dummy"},
                             "secondenv": {"type": "dummy"}}}
        self.write_config(dump(config))
        finished = self.setup_cli_reactor()
        self.setup_exit(0)
        mock_raw = self.mocker.replace(raw_input)
        mock_raw(MATCH(lambda x: x.startswith(
                    "WARNING: this command will destroy the 'secondenv' "
                    "environment (type: dummy)")))
        self.mocker.result("n")
        self.mocker.replay()

        main(["destroy-environment", "-e", "secondenv"])
        yield finished
        self.assertIn(
            "Environment destruction aborted", self.log.getvalue())

    def test_destroy_environment(self):
        """Command will terminate instances in only one environment."""
        config = {
            "environments": {"firstenv": {"type": "dummy"},
                             "secondenv": {"type": "dummy"}}}
        self.write_config(dump(config))
        finished = self.setup_cli_reactor()

        envs = set(("firstenv", "secondenv"))

        def track_destroy_environment_call(self):
            envs.remove(self.environment_name)
            return succeed(True)

        provider = self.mocker.patch(MachineProvider)
        provider.destroy_environment()
        self.mocker.call(track_destroy_environment_call, with_object=True)

        self.setup_exit(0)
        mock_raw = self.mocker.replace(raw_input)
        mock_raw(MATCH(lambda x: x.startswith(
                    "WARNING: this command will destroy the 'secondenv' "
                    "environment (type: dummy)")))
        self.mocker.result("y")
        self.mocker.replay()

        main(["destroy-environment", "-e", "secondenv"])
        yield finished
        self.assertIn("Destroying environment 'secondenv' (type: dummy)...",
                      self.log.getvalue())
        self.assertEqual(envs, set(["firstenv"]))

    def test_destroy_default_environment(self):
        """Command works with default environment, if specified."""
        config = {
            "default": "thirdenv",
            "environments": {"firstenv": {"type": "dummy"},
                             "secondenv": {"type": "dummy"},
                             "thirdenv": {"type": "dummy"}}}
        self.write_config(dump(config))
        finished = self.setup_cli_reactor()

        envs = set(("firstenv", "secondenv", "thirdenv"))

        def track_destroy_environment_call(self):
            envs.remove(self.environment_name)
            return succeed(True)

        provider = self.mocker.patch(MachineProvider)
        provider.destroy_environment()
        self.mocker.call(track_destroy_environment_call, with_object=True)

        self.setup_exit(0)
        mock_raw = self.mocker.replace(raw_input)
        mock_raw(MATCH(lambda x: x.startswith(
                    "WARNING: this command will destroy the 'thirdenv' "
                    "environment (type: dummy)")))
        self.mocker.result("y")
        self.mocker.replay()

        main(["destroy-environment"])
        yield finished
        self.assertIn("Destroying environment 'thirdenv' (type: dummy)...",
                      self.log.getvalue())
        self.assertEqual(envs, set(["firstenv", "secondenv"]))
