import subprocess

from twisted.internet.defer import inlineCallbacks

from juju.providers.local.network import Network
from juju.lib.testing import TestCase
from juju.lib.mocker import MATCH

# Captured raw output from virsh net-list
network_list_default_output = """\
Name                 State      Autostart
-----------------------------------------
default              active     yes       

"""

# Parameterized output for test case
network_list_output_template = """\
Name                 State      Autostart
-----------------------------------------
default              active     yes
%(name)s             %(state)s  %(autostart)s

"""

network_dump_xml_default_output = """\
<network>
  <name>default</name>
  <uuid>56fb682c-26a8-3645-5e47-b77ce33eefc6</uuid>
  <forward mode='nat'/>
  <bridge name='virbr0' stp='on' delay='0' />
  <ip address='192.168.122.1' netmask='255.255.255.0'>
    <dhcp>
      <range start='192.168.122.2' end='192.168.122.254' />
    </dhcp>
  </ip>
</network>
"""


class NetworkTestCase(TestCase):

    # Mocked tests against the network class

    @inlineCallbacks
    def test_get_network_attributes(self):
        network_dumpxml_mock = self.mocker.mock()
        self.patch(subprocess,  "check_output", network_dumpxml_mock)
        network_dumpxml_mock(["virsh", "net-dumpxml", "default"],
                             env={"LC_ALL": "C"})
        self.mocker.result(network_dump_xml_default_output)
        self.mocker.replay()

        network = Network("default")
        attributes = yield network.get_attributes()
        self.assertEqual(
            dict(name="default",
                 ip=dict(address="192.168.122.1", netmask="255.255.255.0"),
                 bridge="virbr0"),
            attributes)

    @inlineCallbacks
    def test_start(self):
        network_list_mock = self.mocker.mock()
        network_start_mock = self.mocker.mock()

        def check_definition(args):
            cmd, subcommand, template_path = args
            network_definition = open(template_path).read()
            self.assertIn(
                '<name>foobar</name>', network_definition)
            self.assertIn(
                '<bridge name="vbr-foobar-%d" />', network_definition)
            self.assertIn(
                '<range start="192.168.131.2" end="192.168.131.254" />',
                network_definition)
            self.assertIn(
                '<ip address="192.168.131.1" netmask="255.255.255.0">',
                network_definition)
            return True

        self.patch(subprocess,  "check_output", network_list_mock)
        self.patch(subprocess, "check_call", network_start_mock)

        network_list_mock(["virsh", "net-list", "--all"], env={"LC_ALL": "C"})
        self.mocker.result(network_list_default_output)

        network_start_mock(MATCH(check_definition))
        network_start_mock(["virsh", "net-start", "foobar"])
        self.mocker.replay()

        network = Network("foobar")
        yield network.start()

    @inlineCallbacks
    def test_start_stoppped_network(self):
        network_list_mock = self.mocker.mock()
        self.patch(subprocess, "check_output", network_list_mock)
        network_list_mock(["virsh", "net-list", "--all"], env={"LC_ALL": "C"})

        self.mocker.result(
            network_list_output_template % dict(
                name="juju", state="inactive", autostart="no"))

        network_start_mock = self.mocker.mock()
        self.patch(subprocess, "check_call", network_start_mock)
        network_start_mock(["virsh", "net-start", "juju"])
        self.mocker.replay()

        network = Network("juju")
        yield network.start()

    @inlineCallbacks
    def test_start_started_network(self):
        network_list_mock = self.mocker.mock()
        self.patch(subprocess, "check_output", network_list_mock)
        network_list_mock(["virsh", "net-list", "--all"], env={"LC_ALL": "C"})

        self.mocker.result(
            network_list_output_template % dict(
                name="zoo", state="active", autostart="no"))

        # patch to catch any errant calls
        network_start_mock = self.mocker.mock()
        self.patch(subprocess, "check_call", network_start_mock)
        self.mocker.replay()
        network = Network("zoo")
        yield network.start()

    @inlineCallbacks
    def test_stop(self):
        network_list_mock = self.mocker.mock()
        network_stop_mock = self.mocker.mock()
        self.patch(subprocess, "check_output", network_list_mock)
        self.patch(subprocess, "check_call", network_stop_mock)

        network_list_mock(["virsh", "net-list", "--all"], env={"LC_ALL": "C"})
        self.mocker.result(
            network_list_output_template % dict(
                name="zoo", state="active", autostart="no"))

        network_stop_mock(["virsh", "net-stop", "zoo"])
        self.mocker.replay()

        network = Network("zoo")
        yield network.stop()

    @inlineCallbacks
    def test_stop_stopped_network(self):
        network_list_mock = self.mocker.mock()
        self.patch(subprocess, "check_output", network_list_mock)
        network_list_mock(["virsh", "net-list", "--all"], env={"LC_ALL": "C"})

        self.mocker.result(
            network_list_output_template % dict(
                name="zoo", state="inactive", autostart="no"))

        # patch to catch any errant calls
        network_stop_mock = self.mocker.mock()
        self.patch(subprocess, "check_call", network_stop_mock)
        self.mocker.replay()

        network = Network("zoo")
        yield network.stop()

    @inlineCallbacks
    def test_stop_nonexistent_network(self):
        network_list_mock = self.mocker.mock()
        self.patch(subprocess, "check_output", network_list_mock)
        network_list_mock(["virsh", "net-list", "--all"], env={"LC_ALL": "C"})

        self.mocker.result(
            network_list_output_template % dict(
                name="zoo", state="inactive", autostart="no"))

        # patch to catch any errant calls
        network_stop_mock = self.mocker.mock()
        self.patch(subprocess, "check_call", network_stop_mock)
        self.mocker.replay()

        network = Network("zebra")
        yield network.stop()

    @inlineCallbacks
    def test_is_running(self):
        network_list_mock = self.mocker.mock()
        self.patch(subprocess,  "check_output", network_list_mock)

        # Network on
        network_list_mock(["virsh", "net-list", "--all"],
                          env={"LC_ALL": "C"})
        self.mocker.result(
            network_list_output_template % (
                dict(name="foobar", state="active", autostart="off")))

        # Network off
        network_list_mock(["virsh", "net-list", "--all"],
                          env={"LC_ALL": "C"})
        self.mocker.result(
            network_list_output_template % (
                dict(name="foobar", state="inactive", autostart="off")))

        # Non existent
        network_list_mock(["virsh", "net-list", "--all"],
                          env={"LC_ALL": "C"})
        self.mocker.result(
            network_list_output_template % (
                dict(name="magic", state="inactive", autostart="off")))

        self.mocker.replay()

        network = Network("foobar")
        self.assertTrue((yield network.is_running()))
        self.assertFalse((yield network.is_running()))
        self.assertFalse((yield network.is_running()))
