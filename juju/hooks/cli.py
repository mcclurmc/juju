import argparse
import json
import logging
import os
import sys
from argparse import ArgumentTypeError

from twisted.internet import defer
from twisted.internet import protocol

from juju.errors import JujuError
from juju.hooks.protocol import UnitAgentClient

_marker = object()


class CommandLineClient(object):
    """Template for writing Command Line Clients. Used to implement
    the utility scripts available to hook authors. Provides a
    framework for utilities connected to the Unit Agent process via a
    UNIX Domain Socket. This provides facilities for standardized
    logging, error handling, output transformation and exit codes.

    There are a number of variables that can be set in a subclass to
    help configure its behavior.

    Instance Variables:

    `exit_code` -- Indicate an error to the caller. The default
    indicates no error. (default: 0)

    `keyvalue_pairs` -- Commands may process key-value pairs in the
    format 'alpha=a beta=b' as arguments. Setting this boolean to True
    enables the parsing of these options and supports the additional
    conventions described in the specifications/unit-agent-hooks
    document. (default: False)

    `require_cid` -- Does the command require the specification of a
      client_id. (default: True)

    """

    default_mode = "wb"
    keyvalue_pairs = False
    exit_code = 0
    require_cid = True

    manage_logging = True
    manage_connection = True

    def _setup_flags(self):
        parser = self.parser
        # Set up the default Arguments
        parser.add_argument("-o", "--output",
                            type=argparse.FileType(self.default_mode),
                            help="""Specify an output file""")

        parser.add_argument("-s", "--socket",
                            help="Unit Agent communicates with "
                            "tools over a socket. This value can be "
                            "overridden here or read from the "
                            "envionment variable JUJU_AGENT_SOCKET"
                            )

        parser.add_argument("--client-id",
                            help="A token used to connect the client "
                            "with an execution context and state "
                            "cache. This value can be overridden "
                            "here or read from the environment "
                            "variable JUJU_CLIENT_ID"

                            )

        # output rendering
        parser.add_argument("--format", default="smart")

        # logging
        parser.add_argument("--log-file", metavar="FILE",
                            default=sys.stderr,
                            type=argparse.FileType('a'),
                            help="Log output to file")
        parser.add_argument("--log-level",
                            metavar="CRITICAL|DEBUG|INFO|ERROR|WARNING",
                            help="Display messages starting at given level",
                            type=parse_log_level, default=logging.WARNING)

    def customize_parser(self):
        """Hook for subclasses to add special handling after the basic
        parser and standard flags have been added. This hook is called
        at such a time that if positional args are defined these will
        be added before any key-value pair handling.
        """
        pass

    def setup_parser(self):
        self.parser = argparse.ArgumentParser()
        self._setup_flags()
        self.customize_parser()

        if self.keyvalue_pairs:
            self.parser.add_argument("keyvalue_pairs", nargs="*")

        return self.parser

    def parse_args(self, arguments=None):
        """By default this processes command line arguments. However
        with arguments are passed they should be a list of arguments
        in the format normally provided by sys.argv.

        arguments:

        `arguments` -- optional list of arguments to parse in the
        sys.argv standard format. (default: None)

        """
        options = self.parser.parse_args(arguments)
        self.options = options

        if self.manage_logging:
            self.setup_logging()

        exit = False

        if not exit and not options.socket:
            options.socket = os.environ.get("JUJU_AGENT_SOCKET")
            if not options.socket:
                exit = SystemExit("No JUJU_AGENT_SOCKET/"
                                  "-s option found")
                # use argparse std error code for this error
                exit.code = 2

        if not exit and not options.client_id:
            options.client_id = os.environ.get("JUJU_CLIENT_ID")
            if not options.client_id and self.require_cid:
                exit = SystemExit("No JUJU_CLIENT_ID/"
                                  "--client_id option found")
                exit.code = 2

        if exit:
            self.parser.print_usage(sys.stderr)
            print >>sys.stderr, (str(exit))
            raise exit

        if self.keyvalue_pairs:
            self.parse_kvpairs(self.options.keyvalue_pairs)

        return options

    def setup_logging(self):
        logging.basicConfig(
            format="%(asctime)s %(levelname)s: %(message)s",
            level=self.options.log_level,
            stream=self.options.log_file)

    def parse_kvpairs(self, options):
        data = parse_keyvalue_pairs(options)
        # cache
        self.options.keyvalue_pairs = data
        return data

    def _connect_to_agent(self):
        from twisted.internet import reactor

        def onConnectionMade(p):
            self.client = p
            return p

        d = protocol.ClientCreator(
            reactor, UnitAgentClient).connectUNIX(self.options.socket)
        d.addCallback(onConnectionMade)
        return d

    def __call__(self, arguments=None):
        from twisted.internet import reactor
        self.setup_parser()
        self.parse_args(arguments=arguments)

        if self.manage_connection:
            self._connect_to_agent().addCallback(self._run)
        else:
            reactor.callWhenRunning(self._run)
        reactor.run()
        sys.exit(self.exit_code)

    def _run(self, result=None):
        from twisted.internet import reactor
        d = defer.maybeDeferred(self.run)
        d.addCallbacks(self.render, self.render_error)
        d.addBoth(lambda x: reactor.stop())
        return d

    def run(self):
        """Implemented by subclass. This method should implement any
        behavior specific to the command and return (or yield with
        inlineCallbacks) a value that will later be handed off to
        render for formatting and output.
        """
        pass

    def render_error(self, result):
        tb = result.getTraceback(elideFrameworkCode=True)
        sys.stderr.write(tb)
        logging.error(tb)
        logging.error(str(result))

    def render(self, result):
        options = self.options
        format = options.format

        if options.output:
            stream = options.output
        else:
            stream = sys.stdout

        formatter = getattr(self, "format_%s" % format, None)
        if formatter is not None:
            formatter(result, stream)
        else:
            print >>sys.stderr, "unknown output format: %s" % format
            if result:
                print >>stream, str(result)
        stream.flush()

    def format_json(self, result, stream):
        print >>stream, json.dumps(result)

    def format_smart(self, result, stream):
        if result:
            print >>stream, str(result)


def parse_log_level(level):
    """Level name/level number => level number"""
    if isinstance(level, basestring):
        level = level.upper()
        if level.isdigit():
            level = int(level)
        else:
            # converts the name INFO to level number
            level = logging.getLevelName(level)
    if not isinstance(level, int):
        logging.error("Invalid log level %s" % level)
        level = logging.INFO
    return level


def parse_keyvalue_pairs(options):
    data = {}
    for kv in options:
        if "=" not in kv:
            raise JujuError(
                "Expected `option=value`. Found `%s`" % kv)

        k, v = kv.split("=", 1)
        if v.startswith("@"):
            # handle fileinput per spec
            # XXX: todo -- sanitize input
            filename = v[1:]
            try:
                v = open(filename, "r").read()
            except IOError:
                raise JujuError(
                    "No such file or directory: %s (argument:%s)" % (
                        filename,
                        k))
        data[k] = v

    return data


def parse_port_protocol(port_protocol_string):
    """Returns (`port`, `protocol`) by converting `port_protocol_string`.

    `port` is an integer for a valid port (1 through 65535).

    `protocol` is restricted to TCP and UDP. TCP is the default.

    Otherwise raises ArgumentTypeError(msg).
    """
    split = port_protocol_string.split("/")
    if len(split) == 2:
        port_string, protocol = split
    elif len(split) == 1:
        port_string, protocol = split[0], "tcp"
    else:
        raise ArgumentTypeError(
            "Invalid format for port/protocol, got %r" % port_protocol_string)

    try:
        port = int(port_string)
    except ValueError:
        raise ArgumentTypeError(
            "Invalid port, must be an integer, got %r" % port_string)
        raise

    if port < 1 or port > 65535:
        raise ArgumentTypeError(
            "Invalid port, must be from 1 to 65535, got %r" % port)

    if protocol.lower() not in ("tcp", "udp"):
        raise ArgumentTypeError(
            "Invalid protocol, must be 'tcp' or 'udp', got %r" % protocol)

    return port, protocol.lower()
