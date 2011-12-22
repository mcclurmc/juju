from juju.lib.testing import TestCase
from juju.providers.orchestra.machine import machine_from_dict


class MachineTest(TestCase):

    def test_create(self):
        info = {
            "name": "billy-bob",
            "uid": "furblewurbleburble",
            "netboot_enabled": True}
        machine = machine_from_dict(info)

        self.assertEquals(machine.instance_id, "furblewurbleburble")
        self.assertEquals(machine.dns_name, "billy-bob")
        self.assertEquals(machine.private_dns_name, "billy-bob")
        self.assertEquals(machine.state, "pending")

    def test_provisioned_state(self):
        info = {
            "name": "billy-bob",
            "uid": "furblewurbleburble",
            "netboot_enabled": False}
        machine = machine_from_dict(info)
        self.assertEquals(machine.state, "provisioned")
