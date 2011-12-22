from twisted.internet import reactor

from juju import control
from juju.control import setup_logging, admin, setup_parser

from juju.lib.mocker import ANY
from .common import ControlToolTest


class DummySubcommand(object):

    @staticmethod
    def configure_subparser(subparsers):
        subparser = subparsers.add_parser("dummy")
        subparser.add_argument("--opt1", default=1)
        return subparser

    @staticmethod
    def command(*args):
        """Doc String"""
        pass


class AdminCommandOptionTest(ControlToolTest):

    def test_admin_subcommand_execution(self):
        """
        Setup an admin subcommand, and verify that's it invoked.
        """
        self.setup_cli_reactor()
        self.setup_exit(0)

        self.patch(control, "ADMIN_SUBCOMMANDS", [DummySubcommand])
        setup_logging_mock = self.mocker.mock(setup_logging)
        setup_parser_mock = self.mocker.proxy(setup_parser)

        self.patch(control, "setup_logging", setup_logging_mock)
        self.patch(control, "setup_parser", setup_parser_mock)

        command_mock = self.mocker.proxy(DummySubcommand.command)
        self.patch(DummySubcommand, "command", command_mock)

        setup_parser_mock(
            subcommands=ANY,
            prog="juju-admin",
            description="juju cloud orchestration internal tools")
        self.mocker.passthrough()

        setup_logging_mock(ANY)
        command_mock(ANY)

        self.mocker.passthrough()
        self.mocker.replay()

        admin(["dummy"])
