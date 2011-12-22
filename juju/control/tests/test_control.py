import logging
import time
import os

from StringIO import StringIO
from argparse import Namespace
from yaml import dump

from twisted.internet.defer import inlineCallbacks

from juju.environment.errors import EnvironmentsConfigError
from juju.control import setup_logging, main, setup_parser
from juju.control.options import ensure_abs_path
from juju.control.command import Commander

from juju.lib.testing import TestCase

from .common import ControlToolTest


class ControlInitiailizationTest(ControlToolTest):

    # The EnvironmentTestBase will replace our $HOME, so that tests will
    # write properly to a temporary directory.
    def test_write_sample_config(self):
        """
        When juju-control is called without a valid environment
        configuration, it should write one down and raise an error to
        let the user know it should be edited.
        """
        try:
            main([])
        except EnvironmentsConfigError, error:
            self.assertTrue(error.sample_written)
        else:
            self.fail("EnvironmentsConfigError not raised")


class ControlOutputTest(ControlToolTest):

    def setUp(self):
        super(ControlOutputTest, self).setUp()
        config = {
            "environments": {
                "firstenv": {
                    "type": "dummy", "admin-secret": "homer"},
                "secondenv": {
                    "type": "dummy", "admin-secret": "marge"}}}
        self.write_config(dump(config))

    def test_sans_args_produces_help(self):
        """
        When juju-control is called without arguments, it
        produces a standard help message.
        """
        stderr = self.capture_stream("stderr")
        self.mocker.replay()
        try:
            main([])
        except SystemExit, e:
            self.assertEqual(e.args[0], 2)
        else:
            self.fail("Should have exited")

        output = stderr.getvalue()
        self.assertIn("add-relation", output)
        self.assertIn("destroy-environment", output)
        self.assertIn("juju cloud orchestration admin", output)

    def test_custom_parser_does_not_extend_to_subcommand(self):
        stderr = self.capture_stream("stderr")
        self.mocker.replay()
        try:
            main(["deploy"])
        except SystemExit, e:
            self.assertEqual(e.args[0], 2)
        else:
            self.fail("Should have exited")
        output = stderr.getvalue()
        self.assertIn("juju deploy: error: too few arguments", output)


class ControlCommandOptionTest(ControlToolTest):

    def test_command_suboption(self):
        """
        The argparser setup, invokes command module configure_subparser
        functions to allow the command to delineate additional cli
        options.
        """
        from juju.control import destroy_environment, bootstrap

        def configure_destroy_environment(subparsers):
            subparser = subparsers.add_parser("destroy-environment")
            subparser.add_argument("--opt1", default=1)
            return subparser

        def configure_bootstrap(subparsers):
            subparser = subparsers.add_parser("bootstrap")
            subparser.add_argument("--opt2", default=2)
            return subparser

        self.patch(destroy_environment, "configure_subparser",
                   configure_destroy_environment)
        self.patch(bootstrap, "configure_subparser", configure_bootstrap)

        parser = setup_parser(subcommands=[destroy_environment, bootstrap])

        shutdown_opts = parser.parse_args(["destroy-environment"])
        bootstrap_opts = parser.parse_args(["bootstrap"])

        missing = object()
        self.assertEquals(shutdown_opts.opt1, 1)
        self.assertEquals(getattr(shutdown_opts, "opt2", missing), missing)
        self.assertEquals(bootstrap_opts.opt2, 2)
        self.assertEquals(getattr(bootstrap_opts, "opt1", missing), missing)


class ControlUtilityTest(TestCase):

    def test_ensured_abs_path(self):
        parent_dir = self.makeDir()
        file_path = self.makeFile(dirname=parent_dir)
        os.rmdir(parent_dir)
        self.assertEqual(ensure_abs_path(file_path), file_path)
        self.assertTrue(os.path.exists(parent_dir))

    def test_ensure_abs_path_with_stdout_symbol(self):
        self.assertEqual(ensure_abs_path("-"), "-")

    def test_ensured_abs_path_with_existing(self):
        temp_dir = self.makeDir()
        self.assertTrue(os.path.exists(temp_dir))
        file_path = os.path.join(temp_dir, "zebra.txt")
        self.assertEqual(ensure_abs_path(file_path), file_path)
        self.assertTrue(os.path.exists(temp_dir))

    def test_ensure_abs_path_relative(self):
        current_dir = os.path.abspath(os.getcwd())
        self.addCleanup(os.chdir, current_dir)
        temp_dir = self.makeDir()
        os.chdir(temp_dir)
        file_path = ensure_abs_path("zebra.txt")
        self.assertEqual(file_path, os.path.join(temp_dir, "zebra.txt"))


class ControlLoggingTest(TestCase):

    def test_logging_format(self):
        self.log = StringIO()
        setup_logging(Namespace(verbose=False, log_file=self.log))

        # ensure that we use gtm regardless of system settings
        logging.getLogger().handlers[0].formatter.converter = time.gmtime

        record = logging.makeLogRecord(
            {"created": 0, "msecs": 0, "levelno": logging.INFO})
        logging.getLogger().handle(record)
        self.assertEqual(
            self.log.getvalue(),
            "1970-01-01 00:00:00,000 Level None \n")

    def test_default_logging(self):
        """
        Default log-level is informational.
        """
        self.log = self.capture_logging()
        setup_logging(Namespace(verbose=False, log_file=None))
        root = logging.getLogger()
        name = logging.getLevelName(root.getEffectiveLevel())
        self.assertEqual(name, "INFO")

    def test_verbose_logging(self):
        """
        When verbose logging is enabled, the log level is set to debugging.
        """
        setup_logging(Namespace(verbose=True, log_file=None))
        root = logging.getLogger()
        self.assertEqual(logging.getLevelName(root.level), "DEBUG")
        custom = logging.getLogger("custom")
        self.assertEqual(custom.getEffectiveLevel(), root.getEffectiveLevel())

    def test_default_loggers(self):
        """
        Verify that the default loggers are bound when the logging
        system is started.
        """
        root = logging.getLogger()
        self.assertEqual(root.handlers, [])
        setup_logging(Namespace(verbose=False, log_file=None))
        self.assertNotEqual(root.handlers, [])

    def tearDown(self):
        # remove the logging handlers we installed
        root = logging.getLogger()
        root.handlers = []


class AttrDict(dict):
    def __getattr__(self, key):
        return self[key]


class TestCommander(ControlToolTest):

    def get_sample_namespace(self):
        # Command expects these objects forming a non-obvious contract
        # with the runtime
        debugStream = StringIO()
        debugStream.__call__ = debugStream.write
        infoStream = StringIO()
        infoStream.__call__ = infoStream.write
        errorStream = StringIO()
        errorStream.__call__ = errorStream.write

        log = AttrDict(debug=debugStream, info=infoStream, error=errorStream)
        return Namespace(log=log,
                         verbose=False,
                         parser=AttrDict(prog="juju"))

    def test_invalid_callback(self):
        # non callable callback
        self.failUnlessRaises(ValueError, Commander, time.daylight)
        # valid callback
        Commander(time.time)

    def test_run_invalid_call(self):
        c = Commander(time.time)
        # called with invalid options
        self.failUnlessRaises(ValueError, c, None)

    def change_value(self, options):
        self.test_value = 42
        return self.test_value

    @inlineCallbacks
    def deferred_callback(self, options):
        self.test_value = 42
        yield self.test_value

    @inlineCallbacks
    def deferred_callback_with_exception(self, options):
        raise Exception("Some generic error condition")

    def test_call_without_deferred(self):
        self.test_value = None

        self.setup_cli_reactor()
        self.setup_exit(0)

        com = Commander(self.change_value)
        ns = self.get_sample_namespace()
        self.mocker.replay()
        com(ns)
        self.assertEqual(self.test_value, 42)

    def test_call_with_deferrred(self):
        self.test_value = None

        self.setup_cli_reactor()
        self.setup_exit(0)

        com = Commander(self.deferred_callback)
        ns = self.get_sample_namespace()
        self.mocker.replay()
        com(ns)
        self.assertEqual(self.test_value, 42)

    def test_call_with_deferrred_exception(self):
        self.test_value = None

        self.setup_cli_reactor()
        err = self.capture_stream("stderr")
        self.setup_exit(1)

        com = Commander(self.deferred_callback_with_exception)
        ns = self.get_sample_namespace()
        self.mocker.replay()
        com(ns)
        # verify that the exception message is all that comes out of stderr
        self.assertEqual(err.getvalue(),
                         "Some generic error condition\n")

    def test_verbose_successful(self):
        self.test_value = None

        self.setup_cli_reactor()
        self.setup_exit(0)

        com = Commander(self.deferred_callback)
        ns = self.get_sample_namespace()
        ns.verbose = True

        self.mocker.replay()
        com(ns)
        self.assertEqual(self.test_value, 42)

    def test_verbose_error_with_traceback(self):
        self.test_value = None

        self.setup_cli_reactor()
        err = self.capture_stream("stderr")
        self.setup_exit(1)

        com = Commander(self.deferred_callback_with_exception)
        ns = self.get_sample_namespace()
        ns.verbose = True

        self.mocker.replay()
        com(ns)

        self.assertIn("traceback", err.getvalue())
