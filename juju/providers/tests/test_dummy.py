from cStringIO import StringIO

import zookeeper

from twisted.internet.defer import inlineCallbacks

from juju.errors import ProviderError
from juju.machine import ProviderMachine
from juju.providers.dummy import MachineProvider, DummyMachine
from juju.state.placement import UNASSIGNED_POLICY
from juju.lib.testing import TestCase


class DummyProviderTest(TestCase):

    def setUp(self):
        super(DummyProviderTest, self).setUp()
        self.provider = MachineProvider("foo", {"peter": "rabbit"})
        zookeeper.set_debug_level(0)

    def test_environment_name(self):
        self.assertEqual(self.provider.environment_name, "foo")

    def test_provider_type(self):
        self.assertEqual(self.provider.provider_type, "dummy")

    def test_get_placement_policy(self):
        self.assertEqual(
            self.provider.get_placement_policy(), UNASSIGNED_POLICY)
        self.provider = MachineProvider("foo", {"placement": "local"})
        self.assertEqual(
            self.provider.get_placement_policy(), "local")

    @inlineCallbacks
    def test_bootstrap(self):
        machines = yield self.provider.bootstrap()
        self.assertTrue(machines)
        for m in machines:
            self.assertTrue(isinstance(m, ProviderMachine))

    @inlineCallbacks
    def test_start_machine(self):
        machines = yield self.provider.start_machine({"machine-id": 0})
        self.assertTrue(machines)
        for m in machines:
            self.assertTrue(isinstance(m, ProviderMachine))

    @inlineCallbacks
    def test_start_machine_with_dns_name(self):
        # This ability is for testing purposes.
        machines = yield self.provider.start_machine(
            {"machine-id": 0, "dns-name": "xe.example.com"})
        self.assertEqual(len(machines), 1)
        machine = machines.pop()
        self.assertTrue(isinstance(machine, ProviderMachine))
        self.assertTrue(machine.dns_name, "xe.example.com")

    @inlineCallbacks
    def test_get_machine(self):
        machines = yield self.provider.start_machine(
            {"machine-id": 0})
        machine = machines.pop()

        result = yield self.provider.get_machine(machine.instance_id)
        self.assertTrue(isinstance(result, ProviderMachine))
        self.assertEqual(machine.instance_id, result.instance_id)

    @inlineCallbacks
    def test_start_machine_accepts_machine_data(self):
        machines = yield self.provider.start_machine(
            {"machine-id": 0, "a": 1})
        self.assertEqual(len(machines), 1)
        self.assertTrue(isinstance(machines[0], ProviderMachine))

    def test_start_machine_requires_machine_id(self):
        d = self.provider.start_machine({"a": 1})
        return self.assertFailure(d, ProviderError)

    @inlineCallbacks
    def test_destroy_environment(self):
        result = yield self.provider.destroy_environment()
        self.assertEqual(result, [])

    @inlineCallbacks
    def test_destroy_environment_returns_machines(self):
        yield self.provider.bootstrap()
        result = yield self.provider.destroy_environment()
        self.assertEqual(len(result), 1)
        self.assertTrue(isinstance(result[0], ProviderMachine))

    @inlineCallbacks
    def test_connect(self):
        client = yield self.provider.connect()
        self.assertTrue(client.connected)

    @inlineCallbacks
    def test_connect_with_sharing(self):
        # Ensure the sharing option is simply ignored for dummy.
        client = yield self.provider.connect(share=True)
        self.assertTrue(client.connected)

    @inlineCallbacks
    def test_get_machines(self):
        machines = yield self.provider.get_machines()
        self.assertEqual(machines, [])

    @inlineCallbacks
    def test_shutdown_machine(self):
        result = yield self.provider.bootstrap()
        machine = result[0]

        machines = yield self.provider.get_machines()
        self.assertTrue(machines)

        yield self.provider.shutdown_machine(machine)
        machines = yield self.provider.get_machines()
        self.assertFalse(machines)

    def test_shutdown_rejects_invalid_machine(self):
        machine = ProviderMachine("a-value")
        d = self.provider.shutdown_machine(machine)
        self.assertFailure(d, ProviderError)
        return d

    def test_shutdown_rejects_unknown_machine(self):
        machine = DummyMachine(1)
        d = self.provider.shutdown_machine(machine)
        self.assertFailure(d, ProviderError)
        return d

    @inlineCallbacks
    def test_save_state(self):
        yield self.provider.save_state(dict(a=1))
        state = yield self.provider.load_state()
        self.assertTrue("a" in state)
        self.assertEqual(state["a"], 1)

    @inlineCallbacks
    def test_load_state(self):
        state = yield self.provider.load_state()
        self.assertEqual(state, {})

    def test_get_serialization_data(self):
        data = self.provider.get_serialization_data()
        self.assertEqual(
            data,
            {"peter": "rabbit",
             "dynamicduck": "magic"})

    @inlineCallbacks
    def test_port_exposing(self):
        """Verifies dummy provider properly works with ports."""
        machines = yield self.provider.start_machine({"machine-id": 0})
        machine = machines[0]
        yield self.provider.open_port(machine, 0, 25, "tcp")
        yield self.provider.open_port(machine, 0, 80)
        yield self.provider.open_port(machine, 0, 53, "udp")
        yield self.provider.open_port(machine, 0, 443, "tcp")
        yield self.provider.close_port(machine, 0, 25)
        yield self.provider.close_port(machine, 0, 25)  # ignored
        exposed_ports = yield self.provider.get_opened_ports(machine, 0)
        self.assertEqual(exposed_ports,
                         set([(53, 'udp'), (80, 'tcp'), (443, 'tcp')]))

    @inlineCallbacks
    def test_file_storage_returns_same_storage(self):
        """Multiple invocations of MachineProvider.get_file_storage use the
        same path.
        """
        file_obj = StringIO("rabbits")
        storage = self.provider.get_file_storage()
        yield storage.put("/magic/beans.txt", file_obj)
        storage2 = self.provider.get_file_storage()
        fh = yield storage2.get("/magic/beans.txt")
        self.assertEqual(fh.read(), "rabbits")
