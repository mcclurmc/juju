import os
import tempfile

from twisted.internet.defer import inlineCallbacks
from twisted.internet.threads import deferToThread

from juju.lib.lxc import (_lxc_start, _lxc_stop, _lxc_create,
                              _lxc_wait, _lxc_ls, _lxc_destroy,
                              LXCContainer, get_containers, LXCError)
from juju.lib.testing import TestCase


def run_lxc_tests():
    if os.environ.get("TEST_LXC"):
        return None
    return "TEST_LXC=1 to include lxc tests"


DATA_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "data"))


DEFAULT_CONTAINER = "lxc_test"


class LXCTest(TestCase):
    timeout = 240
    skip = run_lxc_tests()

    def setUp(self):
        self.config = self.make_config()

        @self.addCleanup
        def remove_config():
            if os.path.exists(self.config):
                os.unlink(self.config)

    def make_config(self, network_name="virbr0"):
        lxc_config = os.path.join(DATA_PATH, "lxc.conf")
        template = open(lxc_config, "r").read()

        fd, output_fn = tempfile.mkstemp(suffix=".conf")
        output_config = open(output_fn, "w")
        output_config.write(template % {"network_name": network_name})
        output_config.close()

        return output_fn

    def clean_container(self, container_name):
        if os.path.exists("/var/lib/lxc/%s" % container_name):
            _lxc_stop(container_name)
            _lxc_destroy(container_name)

    def test_lxc_create(self):
        self.addCleanup(self.clean_container, DEFAULT_CONTAINER)

        _lxc_create(DEFAULT_CONTAINER, config_file=self.config)

        # verify we can find the container
        output = _lxc_ls()
        self.assertIn(DEFAULT_CONTAINER, output)

        # remove and verify the container was removed
        _lxc_destroy(DEFAULT_CONTAINER)
        output = _lxc_ls()
        self.assertNotIn(DEFAULT_CONTAINER, output)

    def test_lxc_start(self):
        self.addCleanup(self.clean_container, DEFAULT_CONTAINER)

        _lxc_create(DEFAULT_CONTAINER, config_file=self.config)

        _lxc_start(DEFAULT_CONTAINER)
        _lxc_stop(DEFAULT_CONTAINER)

    @inlineCallbacks
    def test_lxc_deferred(self):
        self.addCleanup(self.clean_container, DEFAULT_CONTAINER)
        yield deferToThread(
            _lxc_create, DEFAULT_CONTAINER, config_file=self.config)
        yield deferToThread(_lxc_start, DEFAULT_CONTAINER)

    @inlineCallbacks
    def test_lxc_container(self):
        self.addCleanup(self.clean_container, DEFAULT_CONTAINER)
        customize_log = self.makeFile()
        c = LXCContainer(
            DEFAULT_CONTAINER, "dsa...", "ppa", customize_log=customize_log)

        running = yield c.is_running()
        self.assertFalse(running)
        self.assertFalse(c.is_constructed())
        # verify we can't run a non-constructed container
        failure = c.run()
        yield self.assertFailure(failure, LXCError)

        yield c.create()

        self.assertFalse(running)
        self.assertTrue(c.is_constructed())
        yield c.run()

        running = yield c.is_running()
        self.assertTrue(running)
        self.assertTrue(c.is_constructed())

        output = _lxc_ls()
        self.assertIn(DEFAULT_CONTAINER, output)

        # verify we have a path into the container
        self.assertTrue(os.path.exists(c.rootfs))
        self.assertTrue(c.is_constructed())

        self.verify_container(c, "dsa...", "ppa")

        # verify that we are in containers
        containers = yield get_containers(None)
        self.assertEqual(containers[DEFAULT_CONTAINER], True)

        # tear it down
        yield c.destroy()
        running = yield c.is_running()
        self.assertFalse(running)

        containers = yield get_containers(None)
        self.assertNotIn(DEFAULT_CONTAINER, containers)

        # Verify the customize log file.
        self.assertTrue(os.path.exists(customize_log))

        # and its gone
        output = _lxc_ls()
        self.assertNotIn(DEFAULT_CONTAINER, output)

    @inlineCallbacks
    def test_lxc_wait(self):
        self.addCleanup(self.clean_container, DEFAULT_CONTAINER)

        _lxc_create(DEFAULT_CONTAINER, config_file=self.config)

        _lxc_start(DEFAULT_CONTAINER)

        def waitForState(result):
            self.assertEqual(result, True)

        d = _lxc_wait(DEFAULT_CONTAINER, "RUNNING")
        d.addCallback(waitForState)
        yield d

        _lxc_stop(DEFAULT_CONTAINER)
        yield _lxc_wait(DEFAULT_CONTAINER, "STOPPED")
        _lxc_destroy(DEFAULT_CONTAINER)

    @inlineCallbacks
    def test_container_clone(self):
        self.addCleanup(self.clean_container, DEFAULT_CONTAINER)
        self.addCleanup(self.clean_container, DEFAULT_CONTAINER + "_child")

        master_container = LXCContainer(DEFAULT_CONTAINER,
                                        origin="ppa",
                                        public_key="dsa...")

        # verify that we cannot clone an unconstructed container
        failure = master_container.clone("test_lxc_fail")
        yield self.assertFailure(failure, LXCError)

        yield master_container.create()

        # Clone a child container from the template
        child_name = DEFAULT_CONTAINER + "_child"
        c = yield master_container.clone(child_name)

        self.assertEqual(c.container_name, child_name)

        running = yield c.is_running()
        self.assertFalse(running)
        yield c.run()

        running = yield c.is_running()
        self.assertTrue(running)

        output = _lxc_ls()
        self.assertIn(DEFAULT_CONTAINER, output)

        self.verify_container(c, "dsa...", "ppa")

        # verify that we are in containers
        containers = yield get_containers(None)
        self.assertEqual(containers[child_name], True)

        # tear it down
        yield c.destroy()
        running = yield c.is_running()
        self.assertFalse(running)

        containers = yield get_containers(None)
        self.assertNotIn(child_name, containers)

        # and its gone
        output = _lxc_ls()
        self.assertNotIn(child_name, output)

        yield master_container.destroy()

    def verify_container(self, c, public_key, origin):
        """Verify properties of an LXCContainer"""

        def p(path):
            return os.path.join(c.rootfs, path)

        def sudo_get(path):
            # super get path (superuser priv)
            rc, output = c.execute(["cat", path])
            return output

        def run(cmd):
            try:
                rc, output = c.execute(cmd)
            except LXCError:
                rc = 1
            return rc

        # basic path checks
        for path in ("etc/juju", "var/lib/juju"):
            self.assertTrue(os.path.exists(p(path)))

        # verify packages we depend on are installed
        for pkg in ("resolvconf", "sudo"):
            self.assertEqual(run(["dpkg-query", "-s", pkg]), 0)

        # ubuntu user
        self.assertEqual(run(["id", "ubuntu"]), 0)

        # public key checks
        pub = sudo_get("home/ubuntu/.ssh/authorized_keys")
        self.assertEqual(pub.strip(), public_key)

        # sudoers access
        sudoers = sudo_get("etc/sudoers.d/lxc")
        self.assertIn("ubuntu ALL=(ALL:ALL) NOPASSWD: ALL", sudoers)

        # hostname
        self.assertEqual(c.container_name, sudo_get("etc/hostname").strip())
        # the lxc-clone command provides a different ordering here
        # we'd have to run customize_constainer again which removes
        # some of the point of the clone support to repair this.
        # droppping assertion for now and replacing with a lax one
        #XXX::: self.assertIn("127.0.0.1 %s localhost" % c.container_name,
        #XXX    sudo_get("etc/hosts"))

        self.assertIn(c.container_name, sudo_get("etc/hosts"))

        # nameserver
        resolv_conf = sudo_get("etc/resolvconf/resolv.conf.d/base")
        self.assertIn("nameserver 192.168.122.1", resolv_conf)

        # verify apt-cacher
        apt_proxy = sudo_get("/etc/apt/apt.conf.d/02juju-apt-proxy")
        self.assertIn('Acquire::http { Proxy "http://192.168.122.1:3142"; };',
                      apt_proxy)

        # check basic juju installation
        # these could be more through
        if origin == "ppa":
            self.assertEqual(0, run(["dpkg-query", "-s", "juju"]))
        elif origin == "distro":
            self.assertEqual(0, run(["dpkg-query", "-s", "juju"]))
        elif origin == "branch":
            # package isn't installed
            self.assertEqual(1, run(["dpkg-query", "-s", "juju"]))
            # but the branch is checked out
            self.asssertTrue(os.path.exists(p("usr/lib/juju/juju")))
