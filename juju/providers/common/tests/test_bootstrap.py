import logging
import tempfile

from twisted.internet.defer import fail, succeed

from juju.errors import EnvironmentNotFound, ProviderError
from juju.lib.testing import TestCase
from juju.providers.common.base import MachineProviderBase
from juju.providers.dummy import DummyMachine, FileStorage


class SomeError(Exception):
    pass


class WorkingFileStorage(FileStorage):

    def __init__(self):
        super(WorkingFileStorage, self).__init__(tempfile.mkdtemp())


class UnwritableFileStorage(object):

    def put(self, name, f):
        return fail(Exception("oh noes"))


class DummyProvider(MachineProviderBase):

    def __init__(self, file_storage, zookeeper):
        self._file_storage = file_storage
        self._zookeeper = zookeeper

    def get_file_storage(self):
        return self._file_storage

    def get_zookeeper_machines(self):
        if isinstance(self._zookeeper, Exception):
            return fail(self._zookeeper)
        if self._zookeeper:
            return succeed([self._zookeeper])
        return fail(EnvironmentNotFound())

    def start_machine(self, machine_id, master=False):
        assert master is True
        return [DummyMachine("i-keepzoos")]


class BootstrapTest(TestCase):

    def test_unknown_error(self):
        provider = DummyProvider(None, SomeError())
        d = provider.bootstrap()
        self.assertFailure(d, SomeError)
        return d

    def test_zookeeper_exists(self):
        log = self.capture_logging("juju.common", level=logging.DEBUG)
        provider = DummyProvider(
            WorkingFileStorage(), DummyMachine("i-alreadykeepzoos"))
        d = provider.bootstrap()

        def verify(machines):
            (machine,) = machines
            self.assertTrue(isinstance(machine, DummyMachine))
            self.assertEquals(machine.instance_id, "i-alreadykeepzoos")

            log_text = log.getvalue()
            self.assertIn(
                "juju environment previously bootstrapped", log_text)
            self.assertNotIn("Launching", log_text)
        d.addCallback(verify)
        return d

    def test_bad_storage(self):
        provider = DummyProvider(UnwritableFileStorage(), None)
        d = provider.bootstrap()
        self.assertFailure(d, ProviderError)

        def verify(error):
            self.assertEquals(
                str(error),
                "Bootstrap aborted because file storage is not writable: "
                "oh noes")
        d.addCallback(verify)
        return d

    def test_create_zookeeper(self):
        log = self.capture_logging("juju.common", level=logging.DEBUG)
        provider = DummyProvider(WorkingFileStorage(), None)
        d = provider.bootstrap()

        def verify(machines):
            (machine,) = machines
            self.assertTrue(isinstance(machine, DummyMachine))
            self.assertEquals(machine.instance_id, "i-keepzoos")

            log_text = log.getvalue()
            self.assertIn("Launching juju bootstrap instance", log_text)
            self.assertNotIn("previously bootstrapped", log_text)
        d.addCallback(verify)
        return d
