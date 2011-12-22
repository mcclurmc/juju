"""
Argparse implementation of twistd standard unix options.
"""

import os
import argparse

from twisted.python.util import uidFromString, gidFromString
from twisted.scripts._twistd_unix import _umask


def ensure_abs_path(path):
    """
    Ensure the parent directory to the given path exists. Returns
    the absolute file location to the given path
    """
    if path == "-":
        return path
    path = os.path.abspath(path)
    parent_dir = os.path.dirname(path)
    if not os.path.exists(parent_dir):
        os.makedirs(parent_dir)
    return path


def setup_twistd_options(parser, agent):
    """
    Mimic the standard twisted options with some sane defaults
    """
    # Standard twisted app options
    development_group = parser.add_argument_group("Development options")
    development_group.add_argument(
        "--debug", "-b", action="store_true",
        help="Run the application in the python debugger",
        )

    development_group.add_argument(
        "--profile", "-p", action="store_true",
        help="Run in profile mode, dumping results to specified file",
        )

    development_group.add_argument(
        "--savestats", "-s", action="store_true",
        help="Save the Stats object rather than text output of the profiler",
        )

    # Standard unix daemon options
    unix_group = parser.add_argument_group("Unix Daemon options")
    unix_group.add_argument(
        "--rundir", "-d", default=".",
        help="Change to supplied directory before running",
        type=os.path.abspath,
        )

    unix_group.add_argument(
        "--pidfile", default="%s.pid" % agent.name,
        help="Path to the pid file",
        type=ensure_abs_path,
        )

    unix_group.add_argument(
        "--logfile", default="%s.log" % agent.name,
        help="Log to a specified file, - for stdout",
        type=ensure_abs_path,
        )

    unix_group.add_argument(
        "--loglevel", default="DEBUG",
        choices=("DEBUG", "INFO", "ERROR", "WARNING", "CRITICAL"),
        help="Log level")

    unix_group.add_argument(
        "--chroot", default=None,
        help="Chroot to a supplied directory before running",
        type=os.path.abspath,
        )

    unix_group.add_argument(
        "--umask", default=None, type=_umask,
        help="The (octal) file creation mask to apply.",
        )

    unix_group.add_argument(
        "--uid", "-u", default=None, type=uidFromString,
        help="The uid to run as.",
        )

    unix_group.add_argument(
        "--gid", "-g", default=None, type=gidFromString,
        help="The gid to run as.",
        )

    unix_group.add_argument(
        "--daemon", "-n", default=True,
        dest="nodaemon", action="store_false",
        help="Daemonize the process",
        )

    unix_group.add_argument(
        "--syslog", default=False, action="store_true",
        help="Log to syslog, not to file",
        )

    unix_group.add_argument(
        "--sysprefix", dest="prefix", default=agent.name,
        help="Syslog prefix [default: %s]" % (agent.name),
        )

    # Hidden options expected by twistd, with sane defaults
    parser.add_argument(
        "--save", default=True, action="store_false",
        dest="no_save",
        help=argparse.SUPPRESS,
        )

    parser.add_argument(
        "--profiler", default="cprofile",
        help=argparse.SUPPRESS,
        )

    parser.add_argument(
        "--reactor", "-r", default="epoll",
        help=argparse.SUPPRESS,
        )

    parser.add_argument(
        "--originalname",
        help=argparse.SUPPRESS,
        )

    parser.add_argument(
        "--euid",
        help=argparse.SUPPRESS,
        )
