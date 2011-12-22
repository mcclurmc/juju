import logging
import os

from juju.errors import JujuError
from juju.hooks.cli import CommandLineClient
from juju.lib.testing import TestCase


class TestArguments(TestCase):
    """
    Test verifying the standard argument parsing and handling used
    by cli hook tools functions properly.
    """
    def setup_environment(self):
        self.change_environment(
            JUJU_AGENT_SOCKET="/tmp/juju_agent_socket",
            JUJU_CLIENT_ID="xyzzy")
        self.change_args("test-script")

    def test_usage(self):
        output = self.capture_stream("stdout")
        cli = CommandLineClient()
        cli.setup_parser()

        cli.parser.print_usage()

        # test for the existence of a std argument to
        # ensure function
        self.assertIn("-s SOCKET", output.getvalue())

    def test_default_socket_argument(self):
        """
        verify that the socket argument is accepted from a command
        line flag, or the environment or raises an error.
        """
        self.setup_environment()
        os.environ.pop("JUJU_AGENT_SOCKET", None)

        cli = CommandLineClient()
        cli.setup_parser()

        options = cli.parse_args("-s /tmp/socket".split())
        self.assertEquals(options.socket, "/tmp/socket")

        # now set the environment variable to a known state
        os.environ["JUJU_AGENT_SOCKET"] = "/tmp/socket2"

        options = cli.parse_args()
        self.assertEquals(options.socket, "/tmp/socket2")

        err = self.capture_stream("stderr")
        os.environ.pop("JUJU_AGENT_SOCKET", None)
        error = self.failUnlessRaises(SystemExit, cli.parse_args)
        self.assertEquals(str(error),
                          "No JUJU_AGENT_SOCKET/-s option found")
        self.assertIn("No JUJU_AGENT_SOCKET/-s option found",
                      err.getvalue())

    def test_single_keyvalue(self):
        """
        Verify that a single key/vaule setting can be properly read
        from the command line.
        """
        self.setup_environment()
        cli = CommandLineClient()
        cli.keyvalue_pairs = True
        cli.setup_parser()

        options = cli.parse_args(["foo=bar"])
        self.assertEqual(options.keyvalue_pairs["foo"], "bar")

        # need to verify this is akin to the sys.argv parsing that
        # will occur with single and double quoted strings around
        # foo's right hand side
        options = cli.parse_args(["foo=bar none"])
        self.assertEqual(options.keyvalue_pairs["foo"], "bar none")

    def test_multiple_keyvalue(self):
        self.setup_environment()
        cli = CommandLineClient()
        cli.keyvalue_pairs = True
        cli.setup_parser()

        options = cli.parse_args(["foo=bar", "baz=whatever"])
        self.assertIn(("foo", "bar"), options.keyvalue_pairs.items())
        self.assertIn(("baz", "whatever"), options.keyvalue_pairs.items())

    def test_without_keyvalue_flag(self):
        self.setup_environment()
        output = self.capture_stream("stderr")

        cli = CommandLineClient()
        cli.keyvalue_pairs = False
        cli.setup_parser()

        # exit with the proper error code and make sure a message
        # appears on stderr
        error = self.assertRaises(SystemExit, cli.parse_args, ["foo=bar"])
        self.assertEqual(error.code, 2)
        self.assertIn("unrecognized arguments: foo=bar",
                      output.getvalue())

    def test_bad_keyvalue_pair(self):
        self.setup_environment()
        cli = CommandLineClient()
        cli.keyvalue_pairs = True
        cli.setup_parser()

        options = cli.parse_args(["foo=bar", "baz=whatever", "xxx=",
        "yyy=", "zzz=zzz"])
        self.assertIn(("foo", "bar"), options.keyvalue_pairs.items())
        self.assertIn(("baz", "whatever"), options.keyvalue_pairs.items())

    def test_fileinput(self):
        self.setup_environment()
        filename = self.makeFile("""This is config""")

        # the @ sign maps to an argparse.File
        cli = CommandLineClient()
        cli.keyvalue_pairs = True
        cli.default_mode = "rb"
        cli.setup_parser()

        options = cli.parse_args(["foo=@%s" % filename])
        contents = options.keyvalue_pairs["foo"]

        self.assertEquals("This is config", contents)

    def test_fileinput_missing_file(self):
        self.setup_environment()
        filename = "missing"
        # the @ sign maps to an argparse.File
        cli = CommandLineClient()
        cli.keyvalue_pairs = True
        cli.default_mode = "rb"
        cli.setup_parser()

        # files in read-mode must exist at the time of the parse
        self.assertRaises(JujuError, cli.parse_args,
                          ["foo=@%s" % filename])

    def test_fileoutput(self):
        self.setup_environment()
        filename = self.makeFile()
        cli = CommandLineClient()
        cli.setup_parser()

        options = cli.parse_args(["-o", filename])
        # validate that the output file
        output = options.output
        self.assertInstance(output, file)
        self.assertEquals(output.mode, "wb")

    def test_logging(self):
        self.setup_environment()

        cli = CommandLineClient()
        cli.keyvalue_pairs = True
        cli.setup_parser()

        options = cli.parse_args(["foo=bar", "--log-level", "info"])
        cli.setup_logging()
        self.assertEquals(options.log_level, logging.INFO)
