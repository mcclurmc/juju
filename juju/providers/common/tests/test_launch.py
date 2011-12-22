import tempfile

from twisted.internet.defer import fail, succeed

from juju.errors import EnvironmentNotFound
from juju.lib.testing import TestCase
from juju.providers.common.base import MachineProviderBase
from juju.providers.common.launch import LaunchMachine
from juju.providers.dummy import DummyMachine, FileStorage


def launch_machine(test, master, zookeeper):

    class DummyProvider(MachineProviderBase):

        def __init__(self):
            self.config = {"admin-secret": "BLAH"}
            self._file_storage = FileStorage(tempfile.mkdtemp())

        def get_file_storage(self):
            return self._file_storage

        def get_zookeeper_machines(self):
            if zookeeper:
                return succeed([zookeeper])
            return fail(EnvironmentNotFound())

    class DummyLaunchMachine(LaunchMachine):

        def start_machine(self, machine_id, zookeepers):
            test.assertEquals(machine_id, "1234")
            test.assertEquals(zookeepers, filter(None, [zookeeper]))
            test.assertEquals(self._master, master)
            test.assertEquals(self._constraints, {})
            return succeed([DummyMachine("i-malive")])

    provider = DummyProvider()
    d = DummyLaunchMachine(provider, master).run("1234")
    return provider, d


class LaunchMachineTest(TestCase):

    def assert_success(self, master, zookeeper):
        provider, d = launch_machine(self, master, zookeeper)

        def verify(machines):
            (machine,) = machines
            self.assertTrue(isinstance(machine, DummyMachine))
            self.assertEquals(machine.instance_id, "i-malive")
            return provider
        d.addCallback(verify)
        return d

    def test_start_nonzookeeper_no_zookeepers(self):
        """Starting a non-zookeeper without a zookeeper is an error"""
        unused, d = launch_machine(self, False, None)
        self.assertFailure(d, EnvironmentNotFound)
        return d

    def test_start_zookeeper_no_zookeepers(self):
        """A new zookeeper should be recorded in provider state"""
        d = self.assert_success(True, None)

        def verify(provider):
            provider_state = yield provider.load_state()
            self.assertEquals(
                provider_state, {"zookeeper-instances": ["i-malive"]})
        d.addCallback(verify)
        return d

    def test_works_with_zookeeper(self):
        """Creating a non-zookeeper machine should not alter provider state"""
        d = self.assert_success(False, DummyMachine("i-keepzoos"))

        def verify(provider):
            provider_state = yield provider.load_state()
            self.assertEquals(provider_state, False)
        d.addCallback(verify)
        return d
