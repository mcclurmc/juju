import json
import logging
import os
from argparse import ArgumentTypeError

from twisted.internet.defer import inlineCallbacks, returnValue

from juju.errors import JujuError
from juju.hooks.cli import (
    CommandLineClient, parse_keyvalue_pairs, parse_log_level,
    parse_port_protocol)
from juju.lib.testing import TestCase


class NoopCli(CommandLineClient):
    """
    do nothing client used to test options
    """
    manage_logging = True
    manage_connection = False

    def run(self):
        return self.options

    def format_special(self, result, stream):
        """
        render will lookup this method with the correct format
        option and make the output special!!
        """
        print >>stream, result + "!!"


class ErrorCli(CommandLineClient):
    """
    do nothing client used to test options
    """
    manage_logging = True
    manage_connection = False

    def run(self):
        self.exit_code = 1
        raise ValueError("Checking render error")


class GetCli(CommandLineClient):
    keyvalue_pairs = False

    def customize_parser(self):
        self.parser.add_argument("unit_name")
        self.parser.add_argument("settings_name", nargs="*")

    @inlineCallbacks
    def run(self):
        result = yield self.client.get(self.options.client_id,
                                       self.options.unit_name,
                                       self.options.settings_name)

        returnValue(result)


class SetCli(CommandLineClient):
    keyvalue_pairs = True

    def customize_parser(self):
        self.parser.add_argument("unit_name")

    @inlineCallbacks
    def run(self):
        result = yield self.client.set(self.options.client_id,
                                       self.options.unit_name,
                                       self.options.keyvalue_pairs)

        returnValue(result)


class TestCli(TestCase):
    """
    Verify the integration of the protocols with the cli tool helper.
    """
    def tearDown(self):
        # remove the logging handlers we installed
        root = logging.getLogger()
        root.handlers = []

    def setup_exit(self, code=0):
        mock_exit = self.mocker.replace("sys.exit")
        mock_exit(code)

    def setup_cli_reactor(self):
        """
        When executing the cli via tests, we need to mock out any reactor
        start or shutdown.
        """
        from twisted.internet import reactor

        mock_reactor = self.mocker.patch(reactor)
        mock_reactor.run()
        mock_reactor.stop()
        reactor.running = True

    def setup_environment(self):
        self.change_environment(JUJU_AGENT_SOCKET=self.makeFile(),
                                JUJU_CLIENT_ID="client_id")
        self.change_args(__file__)

    def test_empty_invocation(self):
        self.setup_cli_reactor()
        self.setup_environment()
        self.setup_exit(0)

        cli = CommandLineClient()
        cli.manage_connection = False
        self.mocker.replay()
        cli()

    def test_cli_get(self):
        self.setup_environment()
        self.setup_cli_reactor()
        self.setup_exit(0)

        cli = GetCli()
        cli.manage_connection = False
        obj = self.mocker.patch(cli)
        obj.client.get("client_id", "test_unit", ["foobar"])
        self.mocker.replay()

        cli("test_unit foobar".split())

    def test_cli_get_without_settings_name(self):
        self.setup_cli_reactor()
        self.setup_environment()
        self.setup_exit(0)

        cli = GetCli()
        cli.manage_connection = False
        obj = self.mocker.patch(cli)
        obj.client.get("client_id", "test_unit", [])

        self.mocker.replay()

        cli("test_unit".split())

    def test_cli_set(self):
        """
        verify the SetCli works
        """
        self.setup_environment()
        self.setup_cli_reactor()
        self.setup_exit(0)

        cli = SetCli()
        cli.manage_connection = False
        obj = self.mocker.patch(cli)
        obj.client.set("client_id", "test_unit",
                       {"foo": "bar", "sheep": "lamb"})
        self.mocker.replay()

        cli("test_unit foo=bar sheep=lamb".split())

    def test_cli_set_fileinput(self):
        """
        verify the SetCli works
        """
        self.setup_environment()
        self.setup_cli_reactor()
        self.setup_exit(0)

        contents = "this is a test"
        filename = self.makeFile(contents)

        cli = SetCli()
        cli.manage_connection = False
        obj = self.mocker.patch(cli)
        obj.client.set("client_id", "test_unit",
                       {"foo": "bar", "sheep": contents})
        self.mocker.replay()

        # verify that the @notation read the file
        cmdline = "test_unit foo=bar sheep=@%s" % (filename)
        cli(cmdline.split())

    def test_json_output(self):
        self.setup_environment()
        self.setup_cli_reactor()
        self.setup_exit(0)

        filename = self.makeFile()
        data = dict(a="b", c="d")

        cli = NoopCli()
        obj = self.mocker.patch(cli)
        obj.run()
        self.mocker.result(data)

        self.mocker.replay()

        cli(("--format json -o %s" % filename).split())
        with open(filename, "r") as fp:
            result = fp.read()
        self.assertEquals(json.loads(result), data)

    def test_special_format(self):
        self.setup_environment()
        self.setup_cli_reactor()
        self.setup_exit(0)

        filename = self.makeFile()
        data = "Base Value"

        cli = NoopCli()
        obj = self.mocker.patch(cli)
        obj.run()
        self.mocker.result(data)

        self.mocker.replay()

        cli(("--format special -o %s" % filename).split())
        with open(filename, "r") as fp:
            result = fp.read()
        self.assertEquals(result, data + "!!\n")

    def test_cli_no_socket(self):
        # don't set up the environment with a socket
        self.change_environment()
        self.change_args(__file__)

        cli = GetCli()
        cli.manage_connection = False
        cli.manage_logging = False

        self.mocker.replay()

        error_log = self.capture_stream("stderr")
        error = self.failUnlessRaises(SystemExit, cli,
                                      "test_unit foobar".split())
        self.assertEquals(error.code, 2)
        self.assertIn("No JUJU_AGENT_SOCKET", error_log.getvalue())

    def test_cli_no_client_id(self):
        # don't set up the environment with a socket
        self.setup_environment()
        del os.environ["JUJU_CLIENT_ID"]
        self.change_args(__file__)

        cli = GetCli()
        cli.manage_connection = False
        cli.manage_logging = False

        self.mocker.replay()

        error_log = self.capture_stream("stderr")
        error = self.failUnlessRaises(SystemExit, cli,
                                      "test_unit foobar".split())
        self.assertEquals(error.code, 2)
        self.assertIn("No JUJU_CLIENT_ID", error_log.getvalue())

    def test_log_level(self):
        self.setup_environment()
        self.change_args(__file__)

        cli = GetCli()
        cli.manage_connection = False

        self.mocker.replay()

        # bad log level
        log = self.capture_logging()
        cli.setup_parser()
        cli.parse_args("--log-level XYZZY test_unit".split())
        self.assertIn("Invalid log level", log.getvalue())
        # still get a default
        self.assertEqual(cli.options.log_level, logging.INFO)

        # good symbolic name
        cli.parse_args("--log-level CRITICAL test_unit".split())
        self.assertEqual(cli.options.log_level, logging.CRITICAL)

        # made up numeric level
        cli.parse_args("--log-level 42 test_unit".split())
        self.assertEqual(cli.options.log_level, 42)

    def test_log_format(self):
        self.setup_environment()
        self.change_args(__file__)

        cli = NoopCli()

        cli.setup_parser()
        cli.parse_args("--format smart".split())
        self.assertEqual(cli.options.format, "smart")

        cli.parse_args("--format json".split())
        self.assertEqual(cli.options.format, "json")

        out = self.capture_stream("stdout")
        err = self.capture_stream("stderr")
        self.setup_cli_reactor()
        self.setup_exit(0)
        self.mocker.replay()
        cli("--format missing".split())
        self.assertIn("missing", err.getvalue())
        self.assertIn("Namespace", out.getvalue())

    def test_render_error(self):
        self.setup_environment()
        self.change_args(__file__)

        cli = ErrorCli()
        # bad log level
        err = self.capture_stream("stderr")
        self.setup_cli_reactor()
        self.setup_exit(1)
        self.mocker.replay()
        cli("")
        # make sure we got a traceback on stderr
        self.assertIn("Checking render error", err.getvalue())

    def test_parse_log_level(self):
        self.assertEquals(parse_log_level("INFO"), logging.INFO)
        self.assertEquals(parse_log_level("ERROR"), logging.ERROR)
        self.assertEquals(parse_log_level(logging.INFO), logging.INFO)
        self.assertEquals(parse_log_level(logging.ERROR), logging.ERROR)

    def test_parse_keyvalue_pairs(self):
        sample = self.makeFile("INPUT DATA")

        # test various styles of options being read
        options = ["alpha=beta",
                   "content=@%s" % sample]

        data = parse_keyvalue_pairs(options)
        self.assertEquals(data["alpha"], "beta")
        self.assertEquals(data["content"], "INPUT DATA")

        # and check an error condition
        options = ["content=@missing"]
        error = self.assertRaises(JujuError, parse_keyvalue_pairs, options)
        self.assertEquals(
            str(error),
            "No such file or directory: missing (argument:content)")

        # and check when fed non-kvpairs the error makes sense
        options = ["foobar"]
        error = self.assertRaises(JujuError, parse_keyvalue_pairs, options)
        self.assertEquals(
            str(error), "Expected `option=value`. Found `foobar`")

    def test_parse_port_protocol(self):
        self.assertEqual(parse_port_protocol("80"), (80, "tcp"))
        self.assertEqual(parse_port_protocol("443/tcp"), (443, "tcp"))
        self.assertEqual(parse_port_protocol("53/udp"), (53, "udp"))
        self.assertEqual(parse_port_protocol("443/TCP"), (443, "tcp"))
        self.assertEqual(parse_port_protocol("53/UDP"), (53, "udp"))
        error = self.assertRaises(ArgumentTypeError,
            parse_port_protocol, "eighty")
        self.assertEqual(
            str(error),
            "Invalid port, must be an integer, got 'eighty'")
        error = self.assertRaises(ArgumentTypeError,
            parse_port_protocol, "fifty-three/udp")
        self.assertEqual(
            str(error),
            "Invalid port, must be an integer, got 'fifty-three'")
        error = self.assertRaises(ArgumentTypeError,
            parse_port_protocol, "53/udp/")
        self.assertEqual(
            str(error),
            "Invalid format for port/protocol, got '53/udp/'")
        error = self.assertRaises(ArgumentTypeError,
            parse_port_protocol, "53/udp/bad-format")
        self.assertEqual(
            str(error),
            "Invalid format for port/protocol, got '53/udp/bad-format'")
        error = self.assertRaises(ArgumentTypeError, parse_port_protocol, "0")
        self.assertEqual(
            str(error),
            "Invalid port, must be from 1 to 65535, got 0")
        error = self.assertRaises(
            ArgumentTypeError, parse_port_protocol, "65536")
        self.assertEqual(
            str(error),
            "Invalid port, must be from 1 to 65535, got 65536")
        error = self.assertRaises(ArgumentTypeError,
            parse_port_protocol, "53/not-a-valid-protocol")
        self.assertEqual(
            str(error),
            "Invalid protocol, must be 'tcp' or 'udp', "
            "got 'not-a-valid-protocol'")
