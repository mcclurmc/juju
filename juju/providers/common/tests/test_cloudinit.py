import os
import stat

import yaml

from juju.errors import CloudInitError
from juju.lib.testing import TestCase
from juju.providers.common.cloudinit import (
    CloudInit, parse_juju_origin, get_default_origin)
from juju.providers.dummy import DummyMachine
import juju


DATA_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), "data")


class CloudInitTest(TestCase):

    def construct_normal(self):
        cloud_init = CloudInit()
        cloud_init.add_ssh_key("chubb")
        cloud_init.set_machine_id("passport")
        cloud_init.set_provider_type("dummy")
        cloud_init.set_zookeeper_machines([
            DummyMachine("blah", "blah", "cotswold"),
            DummyMachine("blah", "blah", "longleat")])
        return cloud_init

    def construct_bootstrap(self, with_zookeepers=False):
        cloud_init = CloudInit()
        cloud_init.enable_bootstrap()
        cloud_init.add_ssh_key("chubb")
        cloud_init.set_machine_id("passport")
        cloud_init.set_provider_type("dummy")
        cloud_init.set_instance_id_accessor("token")
        cloud_init.set_zookeeper_secret("seekrit")
        cloud_init.set_juju_source(distro=True)
        if with_zookeepers:
            cloud_init.set_zookeeper_machines([
                DummyMachine("blah", "blah", "cotswold"),
                DummyMachine("blah", "blah", "longleat")])
        return cloud_init

    def assert_render(self, cloud_init, name):
        with open(os.path.join(DATA_DIR, name)) as f:
            expected = yaml.load(f.read())
        obtained = yaml.load(cloud_init.render())
        self.assertEquals(obtained, expected)

    def test_render_validate_normal(self):
        cloud_init = CloudInit()
        error = self.assertRaises(CloudInitError, cloud_init.render)
        self.assertEquals(
            str(error),
            "Incomplete cloud-init: you need to call add_ssh_key, "
            "set_machine_id, set_provider_type, set_zookeeper_machines")

    def test_render_validate_bootstrap(self):
        cloud_init = CloudInit()
        cloud_init.enable_bootstrap()
        error = self.assertRaises(CloudInitError, cloud_init.render)
        self.assertEquals(
            str(error),
            "Incomplete cloud-init: you need to call add_ssh_key, "
            "set_machine_id, set_provider_type, set_instance_id_accessor, "
            "set_zookeeper_secret")

    def test_source_validate(self):
        bad_choices = (
            (None,      False, False),
            (None,      True,  True),
            ("lp:blah", True,  True),
            ("lp:blah", False, True),
            ("lp:blah", True,  False))
        cloud_init = CloudInit()
        for choice in bad_choices:
            error = self.assertRaises(
                CloudInitError, cloud_init.set_juju_source, *choice)
            self.assertEquals(str(error), "Please specify one source")

    def test_render_normal(self):
        path = os.environ.get("PATH", "")
        alt_apt_cache_path = self.makeDir()
        filename = os.path.join(alt_apt_cache_path, "apt-cache")
        with open(filename, "w") as f:
            f.write(
                "#!/bin/bash\n"
                "cat <<EOF\n"
                "juju:\n"
                "  Installed: good-magic-1.0\n"
                "  Candidate: good-magic-1.0\n"
                "  Version table:\n"
                " *** good-magic-1.0\n"
                "        500 http://us.archive.ubuntu.com/ubuntu/ "
                "natty/main amd64 Packages\n"
                "        100 /var/lib/dpkg/status\n"
                "EOF\n")
        os.chmod(filename, stat.S_IEXEC | stat.S_IREAD)
        updated_path = alt_apt_cache_path + ":" + path
        self.change_environment(PATH=updated_path)
        import juju
        self.patch(juju, "__file__",
                       "/usr/lib/pymodules/python2.7/juju/__init__.pyc")
        self.assert_render(self.construct_normal(), "cloud_init_distro")

    def test_render_ppa_source(self):
        cloud_init = self.construct_normal()
        cloud_init.set_juju_source(ppa=True)
        self.assert_render(cloud_init, "cloud_init_ppa")

    def test_render_distro_source(self):
        cloud_init = self.construct_normal()
        cloud_init.set_juju_source(distro=True)
        self.assert_render(cloud_init, "cloud_init_distro")

    def test_render_branch_source(self):
        cloud_init = self.construct_normal()
        cloud_init.set_juju_source(branch="lp:blah/juju/blah-blah")
        self.assert_render(cloud_init, "cloud_init_branch")

    def test_render_branch_source_if_not_installed(self):
        import juju
        self.patch(juju, "__file__", "/not/installed/under/usr")
        cloud_init = self.construct_normal()
        self.assert_render(cloud_init, "cloud_init_branch_trunk")

    def test_render_bootstrap(self):
        self.assert_render(self.construct_bootstrap(), "cloud_init_bootstrap")

    def test_render_bootstrap_with_zookeepers(self):
        self.assert_render(
            self.construct_bootstrap(True), "cloud_init_bootstrap_zookeepers")


class ParseJujuOriginTest(TestCase):

    def test_distro_installed(self):
        data = (
            "juju:\n"
            "  Installed: good-magic-1.0\n"
            "  Candidate: good-magic-1.0\n"
            "  Version table:\n"
            "     0.5+bzr366-1juju1~natty1 0\n"
            "        500 http://ppa.launchpad.net/juju/pkgs/ubuntu/ "
            "natty/main amd64 Packages\n"
            " *** good-magic-1.0 0\n"
            "        500 http://us.archive.ubuntu.com/ubuntu/ "
            "natty/main amd64 Packages\n"
            "        100 /var/lib/dpkg/status\n")

        origin, source = parse_juju_origin(data)
        self.assertEqual(origin, "distro")
        self.assertEqual(source, None)

    def test_multiple_versions_available(self):
        data = (
            "juju:\n"
            "  Installed: 0.5+bzr366-1juju1~natty1\n"
            "  Candidate: 0.5+bzr366-1juju1~natty1\n"
            "  Version table:\n"
            "     bad-magic-0.5 0\n"
            "        500 http://us.archive.ubuntu.com/ubuntu/ "
            "natty/main amd64 Packages\n"
            " *** 0.5+bzr366-1juju1~natty1 0\n"
            "        100 /var/lib/dpkg/status\n"
            "        500 http://ppa.launchpad.net/juju/pkgs/ubuntu/ "
            "natty/main amd64 Packages\n"
            "     0.5+bzr356-1juju1~natty1 0\n"
            "        500 http://us.archive.ubuntu.com/ubuntu/ "
            "natty/main amd64 Packages\n")

        origin, source = parse_juju_origin(data)
        self.assertEqual(origin, "ppa")
        self.assertEqual(source, None)

    def test_distro_not_installed(self):
        data = (
            "juju:\n"
            "  Installed: (none)\n"
            "  Candidate: good-magic-1.0\n"
            "  Version table:\n"
            "     0.5+bzr366-1juju1~natty1 0\n"
            "        100 /var/lib/dpkg/status\n"
            "        500 http://ppa.launchpad.net/juju/pkgs/ubuntu/ "
            "natty/main amd64 Packages\n"
            "     good-magic-1.0 0\n"
            "        500 http://us.archive.ubuntu.com/ubuntu/ "
            "natty/main amd64 Packages\n"
            "        100 /var/lib/dpkg/status\n")

        origin, source = parse_juju_origin(data)
        self.assertEqual(origin, "branch")
        self.assertEqual(source, "lp:juju")

    def test_ppa_installed(self):
        data = (
            "juju:\n"
            "  Installed: 0.5+bzr356-1juju1~natty1\n"
            "  Candidate: 0.5+bzr356-1juju1~natty1\n"
            "  Version table:\n"
            "     good-magic-1.0 0\n"
            "        500 http://us.archive.ubuntu.com/ubuntu/ "
            "natty/main amd64 Packages\n"
            " *** 0.5+bzr356-1juju1~natty1 0\n"
            "        500 http://ppa.launchpad.net/juju/pkgs/ubuntu/ "
            "natty/main amd64 Packages\n"
            "        100 /var/lib/dpkg/status\n")

        origin, source = parse_juju_origin(data)
        self.assertEqual(origin, "ppa")
        self.assertEqual(source, None)

    def test_juju_package_is_unknown(self):
        data = "N: Unable to locate package juju"
        origin, source = parse_juju_origin(data)
        self.assertEqual(origin, "branch")
        self.assertEqual(source, "lp:juju")

    def test_partial_input_goes_to_distro(self):
        # Broken input should be handled fine
        data = (
            "juju:\n"
            "  Installed: whatever\n"
            "  Candidate: whatever-else\n"
            "  Nothing interesting here:\n")

        origin, source = parse_juju_origin(data)
        self.assertEqual(origin, "distro")
        self.assertEqual(source, None)

    def test_entirely_unknown_input_goes_to_distro(self):
        data = "N: VAT GEEV?"
        origin, source = parse_juju_origin(data)
        self.assertEqual(origin, "distro")
        self.assertEqual(source, None)

    def test_get_default_origin(self):
        bin_dir = self.makeDir()
        filename = os.path.join(bin_dir, "apt-cache")
        with open(filename, "w") as f:
            f.write(
                "#!/bin/bash\n"
                "cat <<EOF\n"
                "juju:\n"
                "  Installed: good-magic-1.0\n"
                "  Candidate: good-magic-1.0\n"
                "  Version table:\n"
                " *** good-magic-1.0 0\n"
                "        500 http://ppa.launchpad.net/juju/pkgs/ubuntu/ "
                "natty/main amd64 Packages\n"
                "        100 /var/lib/dpkg/status\n"
                "EOF\n")
        os.chmod(filename, stat.S_IEXEC | stat.S_IREAD)
        path = os.environ.get("PATH", "")
        self.change_environment(PATH=bin_dir+":/bin")
        self.patch(juju, "__file__",
                       "/usr/lib/pymodules/python2.7/juju/__init__.pyc")
        self.assertEquals(get_default_origin(), ("ppa", None))

    def test_get_default_origin_not_from_system(self):
        """
        If the running logic isn't in /usr, installed version doesn't matter.
        """
        bin_dir = self.makeDir()
        filename = os.path.join(bin_dir, "apt-cache")
        with open(filename, "w") as f:
            f.write(
                "#!/bin/bash\n"
                "cat <<EOF\n"
                "juju:\n"
                "  Installed: good-magic-1.0\n"
                "  Candidate: good-magic-1.0\n"
                "  Version table:\n"
                " *** good-magic-1.0 0\n"
                "        500 http://ppa.launchpad.net/juju/pkgs/ubuntu/ "
                "natty/main amd64 Packages\n"
                "        100 /var/lib/dpkg/status\n"
                "EOF\n")
        os.chmod(filename, stat.S_IEXEC | stat.S_IREAD)
        path = os.environ.get("PATH", "")
        self.change_environment(PATH=bin_dir+":/bin")
        self.patch(juju, "__file__", "/home/joe/juju/__init__.pyc")
        self.assertEquals(get_default_origin(), ("branch", "lp:juju"))

    def test_get_default_origin_apt_cache_not_found(self):
        """
        If the running logic isn't in /usr, installed version doesn't matter.
        """
        bin_dir = self.makeDir()
        path = os.environ.get("PATH", "")
        self.change_environment(PATH=bin_dir+":/bin")
        self.patch(juju, "__file__",
                       "/usr/lib/pymodules/python2.7/juju/__init__.pyc")
        self.assertEquals(get_default_origin(), ("distro", None))
