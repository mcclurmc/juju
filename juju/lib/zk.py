import glob
import grp
import errno
import os
import pwd
import tempfile
import time
import shutil
import signal
import subprocess

from twisted.internet.threads import deferToThread


zookeeper_script_template = """\
#!/bin/bash
java \
  -cp "%(class_path)s" \
  -Dzookeeper.log.dir=%(log_dir)s \
  -Dzookeeper.root.logger=INFO,CONSOLE \
  -Dlog4j.configuration=file:%(log_config_path)s \
  "org.apache.zookeeper.server.quorum.QuorumPeerMain" \
  %(config_path)s &
/bin/echo -n $! > "%(pid_path)s"
"""

log4j_properties = """
# DEFAULT: console appender only
log4j.rootLogger=INFO, ROLLINGFILE
log4j.appender.ROLLINGFILE.layout=org.apache.log4j.PatternLayout
log4j.appender.ROLLINGFILE.layout.ConversionPattern=%d{ISO8601} - %-5p [%t:%C{1}@%L] - %m%n
log4j.appender.ROLLINGFILE=org.apache.log4j.RollingFileAppender
log4j.appender.ROLLINGFILE.Threshold=DEBUG
log4j.appender.ROLLINGFILE.File=/dev/null
"""

zookeeper_conf_template = """
tickTime=2000
dataDir=%s
clientPort=%s
maxClientCnxns=500
"""


def check_zookeeper():
    """Check for package installation of zookeeper."""
    return os.path.exists("/usr/share/java/zookeeper.jar")


class Zookeeper(object):

    def __init__(self, run_dir, port=None, host=None,
                 zk_location="system", user=None, group=None,
                 use_deferred=True):
        """
        :param run_dir: Directory to store all zookeeper instance related data.
        :param port: The port zookeeper should run on.
        :param zk_location: Directory to find zk jars or dev checkout,
               defaults to using 'system' indicating a package installation.
        :param use_deferred: For usage in a twisted application, this will
               cause subprocess calls to be executed in a separate thread.

        Specifying either of the following parameters, requires the
        process using the library to be running as root.

        :param user: The user name under which to run zookeeper as.
        :param group: The group under which to run zookeeper under
        """
        self._run_dir = run_dir
        self._host = host
        self._port = port
        self._user = user
        self._group = group
        self._zk_location = zk_location
        self._use_deferred = use_deferred

    def start(self):
        assert self._port is not None

        if self._use_deferred:
            return deferToThread(self._start)
        return self._start()

    def stop(self):
        if self._use_deferred:
            return deferToThread(self._stop)
        return self._stop()

    @property
    def address(self):
        host = self._host or "localhost"
        return "%s:%s" % (host, self._port)

    @property
    def running(self):
        pid_path = os.path.join(self._run_dir, "zk.pid")
        try:
            with open(pid_path) as pid_file:
                pid = int(pid_file.read().strip())
        except IOError:
            return False
        try:
            os.kill(pid, 0)
        except OSError, e:
            if e.errno == errno.ESRCH:  # No such process
                return False
            raise
        return True

    package_environment_file = "/etc/zookeeper/conf/environment"

    def get_class_path(self):
        """Get the java class path as a list of paths
        """
        class_path = None

        # Get class path for a package installation of zookeeper (default)
        if self._zk_location == "system":
            with open(self.package_environment_file) as package_environment:
                lines = package_environment.readlines()

            for l in lines:
                if not l.strip():
                    continue
                parts = l.split("=")
                if parts[0] == "CLASSPATH":
                    value = parts[1]
                    # On a system package, CLASSPATH comes in the form
                    # "$ZOOCFGDIR:dir1:dir2:dir2"\n

                    # First we strip off the leading and trailing "
                    class_path = [p for p in value[1:-2].split(":")]
                    # Next remove $ZOOCFGDIR, replaced by our run_dir
                    class_path.pop(0)
                    break

        elif self._zk_location and os.path.exists(self._zk_location):
            # Two additional possibilities, as seen in zkEnv.sh:
            # Either release binaries or a locally built version.
            # TODO: Needs to be verified against zookeeper - 3.4 builds
            software_dir = self._zk_location
            build_dir = os.path.join(software_dir, "build")
            if os.path.exists(build_dir):
                software_dir = build_dir
            class_path = glob.glob(
                os.path.join(software_dir, "zookeeper-*.jar"))
            class_path.extend(
                glob.glob(os.path.join(software_dir, "lib/*.jar")))

        if not class_path:
            raise RuntimeError(
                "Zookeeper libraries not found in location %s." % (
                    self._zk_location))
        # Add the managed dir containing log4j properties, which are retrieved
        # along classpath.
        class_path.insert(0, self._run_dir)
        return class_path

    def get_zookeeper_variables(self):
        """ Returns a dictionary containing variables for config templates
        """
        d = {}
        class_path = self.get_class_path()
        d["class_path"] = ":".join(class_path).replace('"', '')
        d["log_config_path"] = os.path.join(self._run_dir, "log4j.properties")
        d["config_path"] = os.path.join(self._run_dir, "zoo.cfg")
        d["log_dir"] = os.path.join(self._run_dir, "log")
        d["pid_path"] = os.path.join(self._run_dir, "zk.pid")
        d["data_dir"] = os.path.join(self._run_dir, "data")
        return d

    def _setup_data_dir(self, zk_vars):
        uid = self._user and pwd.getpwnam(self._user).pw_uid or None
        gid = self._group and grp.getgrnam(self._group).gr_gid or None

        if uid is not None or gid is not None:
            change_daemon_user = True
        else:
            change_daemon_user = False

        if not os.path.exists(self._run_dir):
            os.makedirs(self._run_dir)
            if change_daemon_user:
                os.chown(self._run_dir, uid, gid)

        if not os.path.exists(zk_vars["log_dir"]):
            os.makedirs(zk_vars["log_dir"])
            if change_daemon_user:
                os.chown(zk_vars["log_dir"], uid, gid)

        if not os.path.exists(zk_vars["data_dir"]):
            os.makedirs(zk_vars["data_dir"])
            if change_daemon_user:
                os.chown(zk_vars["data_dir"], uid, gid)

        with open(zk_vars["log_config_path"], "w") as config:
            config.write(log4j_properties)

        with open(zk_vars["config_path"], "w") as config:
            config.write(
                zookeeper_conf_template % (zk_vars["data_dir"], self._port))
            if self._host:
                config.write("\nclientPortAddress=%s" % self._host)

    def _start(self):
        zk_vars = self.get_zookeeper_variables()
        self._setup_data_dir(zk_vars)
        zookeeper_script = zookeeper_script_template % zk_vars

        fh = tempfile.NamedTemporaryFile(delete=False)
        fh.write(zookeeper_script)
        os.chmod(fh.name, 0700)
        # Can't execute open file on linux
        fh.close()

        # Start zookeeper
        subprocess.check_call([fh.name], shell=True)
        # Ensure the tempfile is removed.
        os.remove(fh.name)

    def _stop(self):
        pid_file_path = os.path.join(self._run_dir, "zk.pid")

        try:
            with open(pid_file_path) as pid_file:
                zookeeper_pid = int(pid_file.read().strip())
        except IOError:
            # No pid, move on
            return

        kill_grace_start = time.time()

        while True:
            try:
                os.kill(zookeeper_pid, 0)
            except OSError, e:
                if e.errno == errno.ESRCH:  # No such process, already dead.
                    break
                raise
            if time.time() - kill_grace_start > 5:
                # Hard kill if we've been trying this for a few seconds
                os.kill(zookeeper_pid, signal.SIGKILL)
                break
            else:
                # Graceful kill up to 5s
                os.kill(zookeeper_pid, signal.SIGTERM)
                # Give a moment for shutdown
                time.sleep(0.5)

        shutil.rmtree(self._run_dir)
