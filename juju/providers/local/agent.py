import errno
import os
import pipes
import subprocess
import sys

from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.threads import deferToThread

from juju.providers.common.cloudinit import get_default_origin, BRANCH


class ManagedMachineAgent(object):

    agent_module = "juju.agents.machine"

    def __init__(
        self, pid_file, zookeeper_hosts=None, machine_id="0",
        log_file=None, juju_directory="/var/lib/juju",
        juju_unit_namespace="", public_key=None, juju_origin="ppa"):
        """
        :param pid_file: Path to file used to store process id.
        :param machine_id: machine id for the local machine.
        :param zookeeper_hosts: Zookeeper hosts to connect.
        :param log_file: A file to use for the agent logs.
        :param juju_directory: The directory to use for all state and logs.
        :param juju_unit_namespace: The machine agent will create units with
               a known a prefix to allow for multiple users and multiple
               environments to create containers. The namespace should be
               unique per user and per environment.
        :param public_key: An SSH public key (string) that will be
               used in the container for access.
        """
        self._pid_file = pid_file
        self._machine_id = machine_id
        self._zookeeper_hosts = zookeeper_hosts
        self._juju_directory = juju_directory
        self._juju_unit_namespace = juju_unit_namespace
        self._log_file = log_file
        self._public_key = public_key
        self._juju_origin = juju_origin

        if self._juju_origin is None:
            origin, source = get_default_origin()
            if origin == BRANCH:
                origin = source
            self._juju_origin = origin

    @property
    def juju_origin(self):
        return self._juju_origin

    @inlineCallbacks
    def start(self):
        """Start the machine agent.
        """
        assert self._zookeeper_hosts and self._log_file

        if (yield self.is_running()):
            return

        # sudo even with -E will strip pythonpath, so pass it directly
        # to the command.
        args = ["sudo",
                "JUJU_ZOOKEEPER=%s" % self._zookeeper_hosts,
                "JUJU_ORIGIN=%s" % self._juju_origin,
                "JUJU_MACHINE_ID=%s" % self._machine_id,
                "JUJU_HOME=%s" % self._juju_directory,
                "JUJU_UNIT_NS=%s" % self._juju_unit_namespace,
                "PYTHONPATH=%s" % ":".join(sys.path),
                sys.executable, "-m", self.agent_module,
                "-n", "--pidfile", self._pid_file,
                "--logfile", self._log_file]

        if self._public_key:
            args.insert(
                1, "JUJU_PUBLIC_KEY=%s" % pipes.quote(self._public_key))

        yield deferToThread(subprocess.check_call, args)

    @inlineCallbacks
    def stop(self):
        """Stop the machine agent.
        """
        pid = yield self._get_pid()
        if pid is None:
            return

        # Verify the cmdline before attempting to kill.
        try:
            with open("/proc/%s/cmdline" % pid) as cmd_file:
                cmdline = cmd_file.read()
                if self.agent_module not in cmdline:
                    raise RuntimeError("Mismatch cmdline")
        except IOError, e:
            # Process already died.
            if e.errno == errno.ENOENT:
                return

        yield deferToThread(
            subprocess.check_call, ["sudo", "kill", str(pid)])

    @inlineCallbacks
    def _get_pid(self):
        """Return the agent process id or None.
        """
        # Default root pidfile mask is restrictive
        try:
            pid = yield deferToThread(
                subprocess.check_output,
                ["sudo", "cat", self._pid_file],
                stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError:
            return
        if not pid:
            return
        returnValue(int(pid.strip()))

    @inlineCallbacks
    def is_running(self):
        """Boolean value, true if the machine agent is running."""
        pid = yield self._get_pid()
        if pid is None:
            returnValue(False)
        returnValue(os.path.isdir("/proc/%s" % pid))
