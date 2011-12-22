from juju.lib.testing import TestCase
from juju.machine import ProviderMachine


class ProviderMachineTest(TestCase):

    def test_minimal(self):
        machine = ProviderMachine("i-abc")
        self.assertEqual(machine.instance_id, "i-abc")
        self.assertEqual(machine.dns_name, None)
        self.assertEqual(machine.private_dns_name, None)
        self.assertEqual(machine.state, "unknown")

    def test_all_attrs(self):
        machine = ProviderMachine(
            "i-abc", "xe.example.com", "foo.local", "borken")
        self.assertEqual(machine.instance_id, "i-abc")
        self.assertEqual(machine.dns_name, "xe.example.com")
        self.assertEqual(machine.private_dns_name, "foo.local")
        self.assertEqual(machine.state, "borken")
