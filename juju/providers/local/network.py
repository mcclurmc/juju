
import subprocess
import tempfile
from xml.etree import ElementTree


from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.threads import deferToThread


libvirt_network_template = """\
<network>
  <name>%(name)s</name>
  <bridge name="vbr-%(name)s-%%d" />
  <forward/>
  <ip address="192.168.%(subnet)d.1" netmask="255.255.255.0">
    <dhcp>
      <range start="192.168.%(subnet)d.2" end="192.168.%(subnet)d.254" />
    </dhcp>
  </ip>
</network>
"""


class Network(object):
    """ Setup a bridge network with forwarding and dnsmasq for the environment.

    Utilizes libvirt's networking subsystem to actualize configuration.
    """

    def __init__(self, name, subnet=131):
        """
        :param name: Name of the libvirt network
        :param subnet: 192.168.x subnet to utilize
        """
        self._name = name
        self._subnet = subnet

    def start(self):
        """Start the network.
        """
        return deferToThread(start_network, self._name, self._subnet)

    def stop(self):
        """Stop the network.
        """
        return deferToThread(stop_network, self._name)

    @inlineCallbacks
    def is_running(self):
        """Returns True if the network is currently active, False otherwise.
        """
        networks = yield deferToThread(list_networks)
        returnValue(bool(networks.get(self._name, False)))

    def get_attributes(self):
        """Return attributes of the network as a dictionary.

        The network name, starting ip address, and bridge name are returned
        """
        return deferToThread(get_network_attributes, self._name)


def start_network(name, subnet):
    """Install a libvirt network for the environment.

    Alternatively do the nat and bridge by hand ala
    http://blog.mudy.info/2010/07/linux-container-on-amazon-ec2-server/
    """
    networks = list_networks()
    if name in networks:
        if networks[name]:
            return
        else:
            subprocess.check_call(["virsh", "net-start", name])
            return

    libvirt_network = libvirt_network_template % (
        dict(name=name, subnet=subnet))
    network_file = tempfile.NamedTemporaryFile()
    network_file.write(libvirt_network)
    network_file.flush()

    subprocess.check_call(["virsh", "net-define", network_file.name])
    network_file.close()
    subprocess.check_call(["virsh", "net-start", name])


def list_networks():
    """Return a dictionary of network name to active status bools.

    Sample virsh net-list output::

Name                 State      Autostart
-----------------------------------------
default              active     yes
juju-test        inactive   no
foobar               inactive   no

    Parsing the above would return::
    {"default": True, "juju-test": False, "foobar": False}
    """
    output = subprocess.check_output(
        ["virsh", "net-list", "--all"], env={"LC_ALL": "C"})
    networks = {}
    # Take the header off and normalize whitespace.
    net_lines = [n.strip() for n in output.splitlines()[2:]]
    for line in net_lines:
        if not line:
            continue
        name, state, auto = line.split()
        networks[name] = state == "active"
    return networks


def get_network_attributes(name):
    """Return core attributes of a libvirt network.

    :param name: Name of the libvirt network.

    returns a dictionary containing the name, ip,, and bridge name.

    Sample virsh net-dumpxml output::
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
    output = subprocess.check_output(
        ["virsh", "net-dumpxml", name], env={"LC_ALL": "C"})
    attrs = {}
    etree = ElementTree.fromstring(output)
    attrs["name"] = etree.findtext("name")
    attrs["ip"] = etree.find("ip").attrib
    attrs["bridge"] = etree.find("bridge").attrib["name"]
    return attrs


def stop_network(name):
    """Stop the named libvirt network.
    """
    networks = list_networks()
    status = networks.get(name)
    if not status:
        return
    subprocess.check_call(["virsh", "net-stop", name])
