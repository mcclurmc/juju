from cStringIO import StringIO
from yaml import dump

from twisted.internet.defer import fail, succeed

from juju.errors import EnvironmentNotFound, MachinesNotFound
from juju.lib.testing import TestCase
from juju.providers.common.base import MachineProviderBase


class SomeError(Exception):
    pass


class FindZookeepersTest(TestCase):

    def get_provider(self, state):
        test = self

        class DummyStorage(object):

            def get(self, path):
                test.assertEquals(path, "provider-state")
                return succeed(StringIO(dump(state)))

        class DummyProvider(MachineProviderBase):

            def __init__(self):
                self.get_machines = test.mocker.mock()

            def get_file_storage(self):
                return DummyStorage()

        return DummyProvider()

    def test_no_state(self):
        provider = self.get_provider(False)
        d = provider.get_zookeeper_machines()
        self.assertFailure(d, EnvironmentNotFound)

        def check(error):
            self.assertEquals(
                str(error),
                "juju environment not found: is the environment "
                "bootstrapped?")
        d.addCallback(check)
        return d

    def test_empty_state(self):
        provider = self.get_provider({})
        d = provider.get_zookeeper_machines()
        self.assertFailure(d, EnvironmentNotFound)

        def check(error):
            self.assertEquals(
                str(error),
                "juju environment not found: is the environment "
                "bootstrapped?")
        d.addCallback(check)
        return d
        return d

    def test_get_machine_error_aborts(self):
        provider = self.get_provider(
            {"zookeeper-instances": ["porter", "carter"]})
        provider.get_machines(["porter"])
        self.mocker.result(fail(SomeError()))
        self.mocker.replay()

        d = provider.get_zookeeper_machines()
        self.assertFailure(d, SomeError)
        return d

    def test_bad_machines(self):
        provider = self.get_provider({
            "zookeeper-instances": ["porter", "carter"]})
        provider.get_machines(["porter"])
        self.mocker.result(fail(MachinesNotFound(["porter"])))
        provider.get_machines(["carter"])
        self.mocker.result(fail(MachinesNotFound(["carter"])))
        self.mocker.replay()

        d = provider.get_zookeeper_machines()
        self.assertFailure(d, EnvironmentNotFound)

        def check(error):
            self.assertEquals(
                str(error),
                "juju environment not found: machines are not running "
                "(porter, carter)")
        d.addCallback(check)
        return d

    def test_good_machine(self):
        provider = self.get_provider({"zookeeper-instances": ["porter"]})
        provider.get_machines(["porter"])
        machine = object()
        self.mocker.result(succeed([machine]))
        self.mocker.replay()

        d = provider.get_zookeeper_machines()

        def verify_machine(result):
            self.assertEquals(result, [machine])
        d.addCallback(verify_machine)
        return d

    def test_gets_all_good_machines(self):
        provider = self.get_provider(
            {"zookeeper-instances": ["porter", "carter", "miller", "baker"]})
        provider.get_machines(["porter"])
        self.mocker.result(fail(MachinesNotFound(["porter"])))
        provider.get_machines(["carter"])
        carter = object()
        self.mocker.result(succeed([carter]))
        provider.get_machines(["miller"])
        self.mocker.result(fail(MachinesNotFound(["miller"])))
        provider.get_machines(["baker"])
        baker = object()
        self.mocker.result(succeed([baker]))
        self.mocker.replay()

        d = provider.get_zookeeper_machines()

        def verify_machine(result):
            self.assertEquals(result, [carter, baker])
        d.addCallback(verify_machine)
        return d
