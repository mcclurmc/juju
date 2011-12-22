import os
import pipes
import subprocess
import sys
import tempfile

from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.threads import deferToThread

from juju.errors import JujuError

DATA_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "data"))

CONTAINER_OPTIONS_DOC = """
The following options are expected.

JUJU_CONTAINER_NAME: Applied as the hostname of the machine.

JUJU_ORIGIN: Where to obtain the containers version of juju from.
             (ppa, distro or branch). When 'branch' JUJU_SOURCE should
             be set to the location of a bzr(1) accessible branch.

JUJU_PUBLIC_KEY: An SSH public key used by the ubuntu account for
             interaction with the container.

"""


class LXCError(JujuError):
    """Indicates a low level error with an LXC container"""


def _cmd(args):
    p = subprocess.Popen(
        args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    stdout_data, _ = p.communicate()
    r = p.returncode
    if  r != 0:
        # read the stdout/err streams and show the user
        print >>sys.stderr, stdout_data
        raise LXCError(stdout_data)
    return (r, stdout_data)


# Wrapped lxc cli primitives
def _lxc_create(
    container_name, template="ubuntu", config_file=None, release="oneiric"):
    # the -- argument indicates the last parameters are passed
    # to the template and not handled by lxc-create
    args = ["sudo", "lxc-create",
            "-n", container_name,
            "-t", template,
            "-f", config_file,
            "--",
            "-r", release]
    return _cmd(args)


def _lxc_start(container_name, debug_log=None, console_log=None):
    args = ["sudo", "lxc-start", "--daemon", "-n", container_name]
    if console_log:
        args.extend(["-c", console_log])
    if debug_log:
        args.extend(["-l", "DEBUG", "-o", debug_log])
    return _cmd(args)


def _lxc_stop(container_name):
    _cmd(["sudo", "lxc-stop", "-n", container_name])


def _lxc_destroy(container_name):
    return _cmd(["sudo", "lxc-destroy", "-n", container_name])


def _lxc_ls():
    _, output = _cmd(["lxc-ls"])
    output = output.replace("\n", " ")
    return set([c for c in output.split(" ") if c])


def _lxc_wait(container_name, state="RUNNING"):
    """Wait for container to be in a given state RUNNING|STOPPED."""

    def wait(container_name):
        rc, _ = _cmd(["sudo", "lxc-wait",
                   "-n", container_name,
                   "-s", state])
        return rc == 0

    return deferToThread(wait, container_name)


def _lxc_clone(existing_container_name, new_container_name):
    return _cmd(["sudo", "lxc-clone", "-o", existing_container_name, "-n",
                 new_container_name])


def _customize_container(customize_script, container_root):
    if not os.path.isdir(container_root):
        raise LXCError("Expect container root directory: %s" %
                       container_root)

    # write the container scripts into the container
    fd, in_path = tempfile.mkstemp(prefix=os.path.basename(customize_script),
                                   dir=os.path.join(container_root, "tmp"))

    os.write(fd, open(customize_script, "r").read())
    os.close(fd)
    os.chmod(in_path, 0755)

    args = ["sudo", "chroot", container_root,
            os.path.join("/tmp", os.path.basename(in_path))]
    return _cmd(args)


def validate_path(pathname):
    if not os.access(pathname, os.R_OK):
        raise LXCError("Invalid or unreadable file: %s" % pathname)


@inlineCallbacks
def get_containers(prefix):
    """Return a dictionary of containers key names to runtime boolean value.

    :param prefix: Optionally specify a prefix that the container should
    match any returned containers.
    """
    _, output = yield deferToThread(_cmd, ["lxc-ls"])

    containers = {}
    for i in filter(None, output.split("\n")):
        if i in containers:
            containers[i] = True
        else:
            containers[i] = False

    if prefix:
        remove = [k for k in containers.keys() if not
                  k.startswith(prefix)]
        map(containers.pop, remove)

    returnValue(containers)


class LXCContainer(object):
    def __init__(self,
                 container_name,
                 public_key,
                 origin,
                 origin_source=None,
                 network_name="virbr0",
                 customize_script=None,
                 debug_log=None,
                 console_log=None,
                 customize_log=None):
        """Create an LXCContainer

        :param container_name: should be unique within the system

        :param public_key: SSH public key

        :param origin: distro|ppa|branch

        :param origin_source: when origin is branch supply a valid bzr branch

        :param network_name: name of network link

        :param customize_script: script used inside container to make
        it juju ready

        See :data CONFIG_OPTIONS_DOC: explain how these values map
        into the container in more detail.
        """
        self.container_name = container_name
        self.debug_log = debug_log
        self.console_log = console_log
        self.customize_log = customize_log
        if customize_script is None:
            customize_script = os.path.join(DATA_PATH,
                                            "juju-create")
        self.customize_script = customize_script
        validate_path(self.customize_script)

        self.public_key = public_key
        self.origin = origin
        self.source = origin_source
        self.network_name = network_name

    @property
    def rootfs(self):
        return "/var/lib/lxc/%s/rootfs/" % self.container_name

    def _p(self, path):
        if path[0] == "/":
            path = path[1:]
        return os.path.join(self.rootfs, path)

    def is_constructed(self):
        """Does the lxc image exist and has juju-create been run in it."""
        return os.path.exists(self.rootfs) and \
            os.path.exists(self._p("/etc/juju/juju.conf"))

    @inlineCallbacks
    def is_running(self):
        """Is the lxc image running."""
        state = yield get_containers(None)
        returnValue(state.get(self.container_name))

    def _make_lxc_config(self, network_name):
        lxc_config = os.path.join(DATA_PATH, "lxc.conf")
        with open(lxc_config, "r") as fh:
            template = fh.read()
            fd, output_fn = tempfile.mkstemp(suffix=".conf")
            output_config = open(output_fn, "w")
            output_config.write(template % {"network_name": network_name})
            output_config.close()

            validate_path(output_fn)
            return output_fn

    def _customize_container(self):

        config_dir = self._p("etc/juju")
        if not os.path.exists(config_dir):
            _cmd(["sudo", "mkdir", config_dir])

        _, fn = tempfile.mkstemp()

        def escape(key, value):
            return "%s=%s" % (key, pipes.quote(value))

        with open(fn, "w") as fp:
            target = self._p("etc/juju/juju.conf")
            docstring = "\n#".join([""] + CONTAINER_OPTIONS_DOC.splitlines())
            print >>fp, docstring

            print >>fp, escape("JUJU_CONTAINER_NAME", self.container_name)
            print >>fp, escape("JUJU_PUBLIC_KEY", self.public_key)
            print >>fp, escape("JUJU_ORIGIN", self.origin)
            if self.source:
                print >>fp, escape("JUJU_SOURCE", self.source)

        _cmd(["sudo", "mv", fn, target])
        _, output = _customize_container(self.customize_script, self.rootfs)

        if self.customize_log:
            with open(self.customize_log, "w") as fh:
                fh.write(output)

    def execute(self, args):
        if not isinstance(args, (list, tuple)):
            args = [args, ]

        args = ["sudo", "chroot", self.rootfs] + args
        return _cmd(args)

    def _create_wait(self):
        """Create the container synchronously."""
        if not self.is_constructed():
            lxc_config = self._make_lxc_config(self.network_name)
            _lxc_create(self.container_name, config_file=lxc_config)
            os.unlink(lxc_config)
            self._customize_container()

    @inlineCallbacks
    def create(self):
        # open the template file and create a new temp processed
        yield deferToThread(self._create_wait)

    def _clone_wait(self, container_name):
        """Return a cloned LXCContainer with a the new container name.

        This method is synchronous and will provision the new image
        blocking till done.
        """
        if not self.is_constructed():
            raise LXCError("Attempted to clone container "
                           "that hasn't been had create() called")

        container = LXCContainer(container_name,
                                 public_key=self.public_key,
                                 origin=self.origin,
                                 origin_source=self.source,
                                 debug_log=self.debug_log,
                                 console_log=self.console_log,
                                 customize_script=self.customize_script,
                                 network_name=self.network_name)

        if not container.is_constructed():
            _lxc_clone(self.container_name, container_name)
        return container

    def clone(self, container_name):
        return deferToThread(self._clone_wait, container_name)

    @inlineCallbacks
    def run(self):
        if not self.is_constructed():
            raise LXCError("Attempting to run a container that "
                           "hasn't been created or cloned.")

        yield deferToThread(
            _lxc_start, self.container_name,
            debug_log=self.debug_log, console_log=self.console_log)
        yield _lxc_wait(self.container_name, "RUNNING")

    @inlineCallbacks
    def stop(self):
        yield deferToThread(_lxc_stop, self.container_name)
        yield _lxc_wait(self.container_name, "STOPPED")

    @inlineCallbacks
    def destroy(self):
        yield self.stop()
        yield deferToThread(_lxc_destroy, self.container_name)
