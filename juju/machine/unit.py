import os
import errno
import signal
import shutil
import sys
import logging

import juju

from twisted.internet.defer import (
    Deferred, inlineCallbacks, returnValue, succeed, fail)
from twisted.internet.protocol import ProcessProtocol

from juju.charm.bundle import CharmBundle
from juju.lib.twistutils import get_module_directory
from juju.lib.lxc import LXCContainer, get_containers, LXCError

from .errors import UnitDeploymentError

log = logging.getLogger("unit.deploy")


def get_deploy_factory(provider_type):
    if provider_type == "local":
        return UnitContainerDeployment
    return UnitMachineDeployment


class AgentProcessProtocol(ProcessProtocol):

    def __init__(self, deferred):
        self.deferred = deferred
        self._error_buffer = []

    def errReceived(self, data):
        self._error_buffer.append(data)

    def processEnded(self, reason):
        if self._error_buffer:
            msg = "".join(self._error_buffer)
            msg.strip()
            self.deferred.errback(UnitDeploymentError(msg))
        else:
            self.deferred.callback(True)


class UnitMachineDeployment(object):
    """ Deploy a unit directly onto a machine.

    A service unit deployed directly to a machine has full access
    to the machine resources.

    Units deployed in such a manner have no isolation from other units
    on the machine, and may leave artifacts on the machine even upon
    service destruction.
    """

    unit_agent_module = "juju.agents.unit"

    def __init__(self, unit_name, juju_home):
        self.unit_name = unit_name

        assert ".." not in unit_name, "Invalid Unit Name"
        self.unit_path_name = unit_name.replace("/", "-")
        self.juju_home = juju_home

        self.directory = os.path.join(
            self.juju_home, "units", self.unit_path_name)

        self.pid_file = os.path.join(
            self.juju_home, "units", "%s.pid" % self.unit_path_name)

    def start(self, machine_id, zookeeper_hosts, bundle):
        """Start a service unit agent."""
        # Extract the charm into the unit directory.
        self.unpack_charm(bundle)

        # Start the service unit agent
        log_file = os.path.join(self.directory, "charm.log")
        environ = self.get_environment(machine_id, zookeeper_hosts)
        args = [sys.executable, "-m", self.unit_agent_module, "-n",
                "--pidfile", self.pid_file, "--logfile", log_file]

        from twisted.internet import reactor
        process_deferred = Deferred()
        protocol = AgentProcessProtocol(process_deferred)
        reactor.spawnProcess(protocol, sys.executable, args, environ)
        return process_deferred

    def get_environment(self, machine_id, zookeeper_hosts):
        environ = dict(os.environ)
        environ["JUJU_MACHINE_ID"] = str(machine_id)
        environ["JUJU_UNIT_NAME"] = self.unit_name
        environ["JUJU_HOME"] = self.juju_home
        environ["JUJU_ZOOKEEPER"] = zookeeper_hosts
        environ["PYTHONPATH"] = ":".join(
            filter(None, [
                os.path.dirname(get_module_directory(juju)),
                environ.get("PYTHONPATH")]))
        return environ

    @inlineCallbacks
    def destroy(self):
        """Forcibly terminate a service unit agent, and clean disk state.

        This will destroy/unmount any state on disk.
        """
        running = yield self.is_running()
        if running:
            pid = int(open(self.pid_file).read())
            try:
                os.kill(pid, signal.SIGKILL)
            except OSError, e:
                if e.errno != errno.ESRCH:
                    raise

        if os.path.exists(self.pid_file):
            os.remove(self.pid_file)

        if os.path.exists(self.directory):
            shutil.rmtree(self.directory)

    def is_running(self):
        """Is the service unit running."""
        try:
            with open(self.pid_file) as pid_fh:
                pid = int(pid_fh.read())
        except (IOError, ValueError):
            return succeed(False)

        # Attempt to send a signal to the process to verify its a valid process
        # From man 2 kill
        # "If sig is 0, then no signal is sent, but error checking is still
        # performed; this can be used to check for the existence of a process
        # ID or process group ID."
        try:
            os.kill(pid, 0)
        except OSError, e:
            if e.errno == errno.ESRCH:
                return succeed(False)
            return fail(e)
        return succeed(True)

    def unpack_charm(self, charm):
        """Unpack a charm to the service units directory."""
        if not isinstance(charm, CharmBundle):
            raise UnitDeploymentError(
                "Invalid charm for deployment: %s" % charm.path)

        charm.extract_to(os.path.join(self.directory, "charm"))


container_upstart_job_template = """\
description "Unit agent for %(JUJU_UNIT_NAME)s"
author "Juju Team <juju@lists.canonical.com>"

start on start on filesystem or runlevel [2345]
stop on runlevel [!2345]

respawn

env JUJU_MACHINE_ID="%(JUJU_MACHINE_ID)s"
env JUJU_HOME="%(JUJU_HOME)s"
env JUJU_ZOOKEEPER="%(JUJU_ZOOKEEPER)s"
env JUJU_UNIT_NAME="%(JUJU_UNIT_NAME)s"
env PYTHONPATH="%(PYTHONPATH)s"

exec /usr/bin/python -m juju.agents.unit \
     --logfile=/var/log/juju/unit-%(UNIT_PATH_NAME)s.log
"""


class UnitContainerDeployment(UnitMachineDeployment):
    """Deploy a service unit in a container.

    Units deployed in a container have strong isolation between
    others units deployed in a container on the same machine.

    From the perspective of the service unit, the container deployment
    should be indistinguishable from a machine deployment.

    Note, strong isolation properties are still fairly trivial
    to escape for a user with a root account within the container.
    This is an ongoing development topic for LXC.
    """

    def __init__(self, unit_name, juju_home):
        super(UnitContainerDeployment, self).__init__(unit_name, juju_home)

        self._unit_namespace = os.environ.get("JUJU_UNIT_NS")
        self._juju_origin = os.environ.get("JUJU_ORIGIN")
        assert self._unit_namespace is not None, "Required unit ns not found"

        self.pid_file = None
        self.container = LXCContainer(self.container_name, None, None, None)

    @property
    def container_name(self):
        """Get a qualfied name for the container.

        The units directory for the machine points to a path like::

           /var/lib/juju/units

        In the case of the local provider this directory is qualified
        to allow for multiple users with multiple environments::

          /var/lib/juju/username-envname

        This value is passed to the agent via the JUJU_HOME environment
        variable.

        This function extracts the name qualifier for the container from
        the JUJU_HOME value.
        """
        return "%s-%s" % (self._unit_namespace,
                          self.unit_name.replace("/", "-"))

    def setup_directories(self):
        # Create state directories for unit in the container
        # Move to juju-create script
        units_dir = os.path.join(
            self.directory, "var", "lib", "juju", "units")
        if not os.path.exists(units_dir):
            os.makedirs(units_dir)

        state_dir = os.path.join(
            self.directory, "var", "lib", "juju", "state")
        if not os.path.exists(state_dir):
            os.makedirs(state_dir)

        log_dir = os.path.join(
            self.directory, "var", "log", "juju")
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        unit_dir = os.path.join(units_dir, self.unit_path_name)
        if not os.path.exists(unit_dir):
            os.mkdir(unit_dir)

        host_unit_dir = os.path.join(
            self.juju_home, "units", self.unit_path_name)
        if not os.path.exists(host_unit_dir):
            os.makedirs(host_unit_dir)

    @inlineCallbacks
    def _get_master_template(self, machine_id, zookeeper_hosts, public_key):
        container_template_name = "%s-%s-template" % (
            self._unit_namespace, machine_id)

        master_template = LXCContainer(
            container_template_name, origin=self._juju_origin,
            public_key=public_key)

        # Debug log for the customize script, customize is only run on master.
        customize_log_path = os.path.join(
            self.juju_home, "units", "master-customize.log")
        master_template.customize_log = customize_log_path

        if not master_template.is_constructed():
            log.debug("Creating master container...")
            yield master_template.create()
            log.debug("Created master container %s" % container_template_name)

        # it wasn't constructed and we couldn't construct it
        if not master_template.is_constructed():
            raise LXCError("Unable to create master container")

        returnValue(master_template)

    @inlineCallbacks
    def _get_container(self, machine_id, zookeeper_hosts, bundle, public_key):
        master_template = yield self._get_master_template(
            machine_id, zookeeper_hosts, public_key)
        log.info(
            "Creating container %s...", os.path.basename(self.directory))

        container = yield master_template.clone(self.container_name)
        directory = container.rootfs
        log.info("Container created for %s" % self.unit_name)
        returnValue((container, directory))

    @inlineCallbacks
    def start(self, machine_id, zookeeper_hosts, bundle):
        """Start the unit.

        Creates and starts an lxc container for the unit.
        """
        # remove any quoting around the key
        public_key = os.environ.get("JUJU_PUBLIC_KEY", "")
        public_key = public_key.strip("'\"")

        # Build a template container that can be cloned in deploy
        # we leave the loosely initialized self.container in place for
        # the class as thats all we need for methods other than start.
        self.container, self.directory = yield self._get_container(machine_id,
                                                         zookeeper_hosts,
                                                         bundle,
                                                         public_key)
        # Create state directories for unit in the container
        self.setup_directories()

        # Extract the charm bundle
        charm_path = os.path.join(
            self.directory, "var", "lib", "juju", "units",
            self.unit_path_name, "charm")
        bundle.extract_to(charm_path)
        log.debug("Charm extracted into container")

        # Write upstart file for the agent into the container
        upstart_path = os.path.join(
            self.directory, "etc", "init",
            "%s-unit-agent.conf" % self.unit_path_name)
        with open(upstart_path, "w") as fh:
            fh.write(self.get_upstart_unit_job(machine_id, zookeeper_hosts))

        # Create a symlink on the host for easier access to the unit log file
        unit_log_path_host = os.path.join(
            self.juju_home, "units", self.unit_path_name, "unit.log")
        if not os.path.lexists(unit_log_path_host):
            os.symlink(
                os.path.join(self.directory, "var", "log", "juju",
                             "unit-%s.log" % self.unit_path_name),
                unit_log_path_host)

        # Debug log for the container
        container_log_path = os.path.join(
            self.juju_home, "units", self.unit_path_name, "container.log")
        self.container.debug_log = container_log_path

        log.debug("Starting container...")
        yield self.container.run()
        log.info("Started container for %s" % self.unit_name)

    @inlineCallbacks
    def destroy(self):
        """Destroy the unit container.
        """
        log.debug("Destroying container...")
        yield self.container.destroy()
        log.info("Destroyed container for %s" % self.unit_name)

    @inlineCallbacks
    def is_running(self):
        """Is the unit container running.
        """
        # TODO: container running may not imply agent running.  the
        # pid file has the pid from the container, we need a container
        # pid -> host pid mapping to query status from the machine agent.
        # alternatively querying zookeeper for the unit agent presence
        # node.
        if not self.container:
            returnValue(False)
        container_map = yield get_containers(
            prefix=self.container.container_name)
        returnValue(container_map.get(self.container.container_name, False))

    def get_upstart_unit_job(self, machine_id, zookeeper_hosts):
        """Return a string containing the  upstart job to start the unit agent.
        """
        environ = self.get_environment(machine_id, zookeeper_hosts)
        # Keep qualified locations within the container for colo support
        environ["JUJU_HOME"] = "/var/lib/juju"
        environ["UNIT_PATH_NAME"] = self.unit_path_name
        return container_upstart_job_template % environ
