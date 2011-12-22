from yaml import dump

from juju.lib.testing import TestCase
from juju.providers.orchestra import MachineProvider

from .common import OrchestraTestMixin


def get_provider():
    config = {"orchestra-server": "somewhe.re",
              "orchestra-user": "user",
              "orchestra-pass": "pass",
              "acquired-mgmt-class": "acquired",
              "available-mgmt-class": "available"}
    return MachineProvider("tetrascape", config)


class StateTest(TestCase, OrchestraTestMixin):

    def test_save(self):
        self.setup_mocks()
        state = {"foo": "blah blah"}
        self.mock_fs_put(
            "http://somewhe.re/webdav/provider-state", dump(state))
        self.mocker.replay()

        provider = get_provider()
        d = provider.save_state(state)

        def verify(result):
            self.assertEquals(result, True)
        d.addCallback(verify)
        return d

    def test_load(self):
        self.setup_mocks()
        expect_state = {"foo": "blah blah"}
        self.mock_fs_get(
            "http://somewhe.re/webdav/provider-state", 200, dump(expect_state))
        self.mocker.replay()

        provider = get_provider()
        d = provider.load_state()

        def verify(state):
            self.assertEquals(state, expect_state)
        d.addCallback(verify)
        return d
