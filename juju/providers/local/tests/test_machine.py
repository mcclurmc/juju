from juju.providers.local.machine import LocalMachine
from juju.lib.testing import TestCase


class LocalMachineTest(TestCase):

    def test_machine_attributes(self):

        machine = LocalMachine()
        self.assertEqual(machine.instance_id, "local")
        self.assertEqual(machine.dns_name, "localhost")
        self.assertEqual(machine.private_dns_name, "localhost")
