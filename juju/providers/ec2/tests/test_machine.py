from txaws.ec2.model import Instance

from juju.lib.testing import TestCase
from juju.providers.ec2.machine import (
        EC2ProviderMachine, machine_from_instance)


class EC2ProviderMachineTest(TestCase):

    def test_machine_from_instance(self):
        instance = Instance(
            "i-foobar", "oscillating",
            dns_name="public",
            private_dns_name="private")

        machine = machine_from_instance(instance)
        self.assertTrue(isinstance(machine, EC2ProviderMachine))
        self.assertEquals(machine.instance_id, "i-foobar")
        self.assertEquals(machine.dns_name, "public")
        self.assertEquals(machine.private_dns_name, "private")
        self.assertEquals(machine.state, "oscillating")
