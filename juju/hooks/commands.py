import logging
import os
import pipes
import re
import sys

from twisted.internet.defer import inlineCallbacks, returnValue

from juju.hooks.cli import (
    CommandLineClient, parse_log_level, parse_port_protocol)


BAD_CHARS = re.compile("[\-\./:()<>|?*]|(\\\)")


class RelationGetCli(CommandLineClient):
    keyvalue_pairs = False

    def customize_parser(self):
        remote_unit = os.environ.get("JUJU_REMOTE_UNIT")

        self.parser.add_argument("settings_name", default="", nargs="?")
        self.parser.add_argument("unit_name", default=remote_unit, nargs="?")

    @inlineCallbacks
    def run(self):
        # handle settings_name being explictly skipped on the cli
        if self.options.settings_name == "-":
            self.options.settings_name = ""
        result = yield self.client.relation_get(self.options.client_id,
                                                self.options.unit_name,
                                                self.options.settings_name)
        returnValue(result)

    def format_shell(self, result, stream):
        options = self.options
        settings_name = options.settings_name

        if settings_name and settings_name != "-":
            # result should be a single value
            result = {settings_name.upper(): result}

        if result:
            errs = []
            for k, v in sorted(os.environ.items()):
                if k.startswith("VAR_"):
                    print >>stream, "%s=" % (k.upper(), )
                    errs.append(k)

            for k, v in sorted(result.items()):
                k = BAD_CHARS.sub("_", k.upper())
                v = pipes.quote(v)
                print >>stream, 'VAR_%s=%s' % (k.upper(), v)

            # Order of output within streams is assured, but we output
            # on (commonly) two streams here and the ordering of those
            # messages is significant to the user. Make a best
            # effort. However, this cannot be guaranteed when these
            # streams are collected by `HookProtocol`.
            stream.flush()

            if errs:
                print >>sys.stderr, "The following were omitted from " \
                "the environment.  VAR_ prefixed variables indicate a " \
                "usage error."
                print >>sys.stderr, "".join(errs)


def relation_get():
    """Entry point for relation-get"""
    client = RelationGetCli()
    sys.exit(client())


class RelationSetCli(CommandLineClient):
    keyvalue_pairs = True

    def run(self):
        return self.client.relation_set(self.options.client_id,
                                        self.options.keyvalue_pairs)


def relation_set():
    """Entry point for relation-set."""
    client = RelationSetCli()
    sys.exit(client())


class ListCli(CommandLineClient):
    keyvalue_pairs = False

    def run(self):
        return self.client.list_relations(self.options.client_id)

    def format_eval(self, result, stream):
        """ eval `juju-list` """
        print >>stream, 'export JUJU_MEMBERS="%s"' % (" ".join(result))

    def format_smart(self, result, stream):
        for member in result:
            print >>stream, member


def relation_list():
    """Entry point for relation-list."""
    client = ListCli()
    sys.exit(client())


class LoggingCli(CommandLineClient):
    keyvalue_pairs = False
    require_cid = False

    def customize_parser(self):
        self.parser.add_argument("message", nargs="+")
        self.parser.add_argument("-l",
                                 metavar="CRITICAL|DEBUG|INFO|ERROR|WARNING",
                                 help="Send log message at the given level",
                                 type=parse_log_level, default=logging.INFO)

    def run(self, result=None):
        return self.client.log(self.options.l,
                               self.options.message)


def log():
    """Entry point for juju-log."""
    client = LoggingCli()
    sys.exit(client())


class ConfigGetCli(CommandLineClient):
    keyvalue_pairs = False

    def customize_parser(self):
        self.parser.add_argument("option_name", default="", nargs="?")

    @inlineCallbacks
    def run(self):
        # handle settings_name being explictly skipped on the cli
        result = yield self.client.config_get(self.options.client_id,
                                              self.options.option_name)
        returnValue(result)


def config_get():
    """Entry point for config-get"""
    client = ConfigGetCli()
    sys.exit(client())


class OpenPortCli(CommandLineClient):
    keyvalue_pairs = False

    def customize_parser(self):
        self.parser.add_argument(
            "port_protocol",
            metavar="PORT[/PROTOCOL]",
            help="The port to open. The protocol defaults to TCP.",
            type=parse_port_protocol)

    def run(self):
        return self.client.open_port(
            self.options.client_id, *self.options.port_protocol)


def open_port():
    """Entry point for open-port."""
    client = OpenPortCli()
    sys.exit(client())


class ClosePortCli(CommandLineClient):
    keyvalue_pairs = False

    def customize_parser(self):
        self.parser.add_argument(
            "port_protocol",
            metavar="PORT[/PROTOCOL]",
            help="The port to close. The protocol defaults to TCP.",
            type=parse_port_protocol)

    def run(self):
        return self.client.close_port(
            self.options.client_id, *self.options.port_protocol)


def close_port():
    """Entry point for close-port."""
    client = ClosePortCli()
    sys.exit(client())


class UnitGetCli(CommandLineClient):
    keyvalue_pairs = False

    def customize_parser(self):
        self.parser.add_argument("setting_name")

    @inlineCallbacks
    def run(self):
        result = yield self.client.get_unit_info(self.options.client_id,
                                                 self.options.setting_name)
        returnValue(result["data"])


def unit_get():
    """Entry point for config-get"""
    client = UnitGetCli()
    sys.exit(client())
