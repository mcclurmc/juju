from twisted.internet.defer import inlineCallbacks

from juju.lib.testing import TestCase
from juju.lib.lxc import get_containers
from juju.lib import lxc
lxc_output_sample = "\
calendarserver\ncaprica\ngemini\nreconnoiter\nvirgo\ncalendarserver\n"


class GetContainersTest(TestCase):

    @inlineCallbacks
    def test_get_containers(self):
        lxc_ls_mock = self.mocker.mock()
        self.patch(lxc, "_cmd", lxc_ls_mock)
        lxc_ls_mock(["lxc-ls"])
        self.mocker.result((0, lxc_output_sample))
        self.mocker.replay()

        container_map = yield get_containers(None)
        self.assertEqual(
            dict(caprica=False, gemini=False, reconnoiter=False, virgo=False,
                 calendarserver=True),
            container_map)

    @inlineCallbacks
    def test_get_containers_with_prefix(self):
        lxc_ls_mock = self.mocker.mock()
        self.patch(lxc, "_cmd", lxc_ls_mock)
        lxc_ls_mock(["lxc-ls"])
        self.mocker.result((0, lxc_output_sample))
        self.mocker.replay()

        container_map = yield get_containers("ca")
        self.assertEqual(
            dict(calendarserver=True, caprica=False), container_map)
