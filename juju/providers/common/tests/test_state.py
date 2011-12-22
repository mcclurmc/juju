from cStringIO import StringIO
from yaml import dump

from twisted.internet.defer import fail, succeed

from juju.errors import FileNotFound
from juju.lib.mocker import MATCH
from juju.lib.testing import TestCase
from juju.providers.common.base import MachineProviderBase
from juju.providers.common.state import LoadState, SaveState


class DummyProvider(MachineProviderBase):

    def __init__(self, get=None, put=None):
        self._get = get
        self._put = put

    def get_file_storage(self):
        return DummyFileStorage(self._get, self._put)


class DummyFileStorage(object):

    def __init__(self, get, put):
        self.get = get
        self.put = put


class LoadStatetest(TestCase):

    def mock_get(self, result):
        get = self.mocker.mock()
        get("provider-state")
        self.mocker.result(result)
        self.mocker.replay()
        provider = DummyProvider(get=get)
        load_state = LoadState(provider)
        return load_state.run()

    def test_load_state(self):
        d = self.mock_get(succeed(StringIO(dump({"some": "thing"}))))

        def verify(result):
            self.assertEquals(result, {"some": "thing"})
        d.addCallback(verify)
        return d

    def test_load_empty_state(self):
        d = self.mock_get(succeed(StringIO("")))

        def verify(result):
            self.assertEquals(result, False)
        d.addCallback(verify)
        return d

    def test_load_no_file(self):
        d = self.mock_get(fail(FileNotFound("blah")))

        def verify(result):
            self.assertEquals(result, False)
        d.addCallback(verify)
        return d

    def test_load_unknown_error(self):

        class SomeError(Exception):
            pass
        d = self.mock_get(fail(SomeError("blah")))
        self.assertFailure(d, SomeError)
        return d


class SaveStateTest(TestCase):

    def is_expected_yaml(self, result):
        return result.read() == dump({"some": "thing"})

    def test_save(self):
        put = self.mocker.mock()
        put("provider-state", MATCH(self.is_expected_yaml))
        self.mocker.result(succeed(True))
        self.mocker.replay()

        provider = DummyProvider(put=put)
        save_state = SaveState(provider)
        d = save_state.run({"some": "thing"})

        def verify_true(result):
            self.assertEquals(result, True)
        d.addCallback(verify_true)
        return d

    def test_save_error(self):

        class SomeError(Exception):
            pass
        put = self.mocker.mock()
        put("provider-state", MATCH(self.is_expected_yaml))
        self.mocker.result(fail(SomeError("blah")))
        self.mocker.replay()

        provider = DummyProvider(put=put)
        save_state = SaveState(provider)
        d = save_state.run({"some": "thing"})
        self.assertFailure(d, SomeError)
        return d
