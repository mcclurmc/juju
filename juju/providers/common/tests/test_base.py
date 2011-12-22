from twisted.internet.defer import fail, succeed

from juju.environment.errors import EnvironmentsConfigError
from juju.lib.testing import TestCase
from juju.machine import ProviderMachine
from juju.providers.common.base import MachineProviderBase
from juju.state.placement import UNASSIGNED_POLICY


class SomeError(Exception):
    pass


class DummyLaunchMachine(object):

    def __init__(self, master=False):
        self._master = master

    def run(self, machine_data):
        return succeed([ProviderMachine(machine_data["machine-id"])])


class DummyProvider(MachineProviderBase):

    def __init__(self, config=None):
        super(DummyProvider, self).__init__("venus", config or {})


class MachineProviderBaseTest(TestCase):

    def test_init(self):
        provider = DummyProvider({"some": "config"})
        self.assertEquals(provider.environment_name, "venus")
        self.assertEquals(provider.config, {"some": "config"})

    def test_bad_config(self):
        try:
            DummyProvider({"authorized-keys": "foo",
                           "authorized-keys-path": "bar"})
        except EnvironmentsConfigError as error:
            expect = ("Environment config cannot define both authorized-keys "
                      "and authorized-keys-path. Pick one!")
            self.assertEquals(str(error), expect)
        else:
            self.fail("Failed to detect bad config")

    def test_get_serialization_data(self):
        keys_path = self.makeFile("some-key")
        provider = DummyProvider({"foo": {"bar": "baz"},
                                  "authorized-keys-path": keys_path})
        data = provider.get_serialization_data()
        self.assertEquals(data, {"foo": {"bar": "baz"},
                                 "authorized-keys": "some-key"})
        data["foo"]["bar"] = "qux"
        self.assertEquals(provider.config, {"foo": {"bar": "baz"},
                                            "authorized-keys-path": keys_path})

    def test_get_provider_placement(self):
        provider = DummyProvider()
        self.assertEqual(
            provider.get_placement_policy(), UNASSIGNED_POLICY)
        provider = DummyProvider({"placement": "local"})
        self.assertEqual(
            provider.get_placement_policy(), "local")

    def test_get_machine_error(self):
        provider = DummyProvider()
        provider.get_machines = self.mocker.mock()
        provider.get_machines(["piffle"])
        self.mocker.result(fail(SomeError()))
        self.mocker.replay()

        d = provider.get_machine("piffle")
        self.assertFailure(d, SomeError)
        return d

    def test_get_machine_success(self):
        provider = DummyProvider()
        provider.get_machines = self.mocker.mock()
        provider.get_machines(["piffle"])
        machine = object()
        self.mocker.result(succeed([machine]))
        self.mocker.replay()

        d = provider.get_machine("piffle")

        def verify(result):
            self.assertEquals(result, machine)
        d.addCallback(verify)
        return d

    def test_shutdown_machine_error(self):
        provider = DummyProvider()
        provider.shutdown_machines = self.mocker.mock()
        machine = object()
        provider.shutdown_machines([machine])
        self.mocker.result(fail(SomeError()))
        self.mocker.replay()

        d = provider.shutdown_machine(machine)
        self.assertFailure(d, SomeError)
        return d

    def test_shutdown_machine_success(self):
        provider = DummyProvider()
        provider.shutdown_machines = self.mocker.mock()
        machine = object()
        provider.shutdown_machines([machine])
        probably_the_same_machine = object()
        self.mocker.result(succeed([probably_the_same_machine]))
        self.mocker.replay()

        d = provider.shutdown_machine(machine)

        def verify(result):
            self.assertEquals(result, probably_the_same_machine)
        d.addCallback(verify)
        return d

    def test_destroy_environment_error(self):
        provider = DummyProvider()
        provider.get_machines = self.mocker.mock()
        provider.get_machines()
        machines = [object(), object()]
        self.mocker.result(succeed(machines))
        provider.shutdown_machines = self.mocker.mock()
        provider.shutdown_machines(machines)
        self.mocker.result(fail(SomeError()))
        self.mocker.replay()

        d = provider.destroy_environment()
        self.assertFailure(d, SomeError)
        return d

    def test_destroy_environment_success(self):
        provider = DummyProvider()
        provider.get_machines = self.mocker.mock()
        provider.get_machines()
        machines = [object(), object()]
        self.mocker.result(succeed(machines))
        provider.shutdown_machines = self.mocker.mock()
        provider.shutdown_machines(machines)
        self.mocker.result(succeed(machines))
        self.mocker.replay()

        d = provider.destroy_environment()

        def verify(result):
            self.assertEquals(result, machines)
        d.addCallback(verify)
        return d
