import subprocess
import zookeeper

from twisted.internet.defer import inlineCallbacks, succeed, returnValue
from twisted.web import client

from juju.errors import JujuError
from juju.lib.testing import TestCase
from juju.unit.address import (
    EC2UnitAddress, LocalUnitAddress, OrchestraUnitAddress, DummyUnitAddress,
    get_unit_address)
from juju.state.environment import GlobalSettingsStateManager


class AddressTest(TestCase):

    def setUp(self):
        zookeeper.set_debug_level(0)
        self.client = self.get_zookeeper_client()
        return self.client.connect()

    @inlineCallbacks
    def get_address_for(self, provider_type):
        settings = GlobalSettingsStateManager(self.client)
        yield settings.set_provider_type(provider_type)
        address = yield get_unit_address(self.client)
        returnValue(address)

    @inlineCallbacks
    def test_get_ec2_address(self):
        address = yield self.get_address_for("ec2")
        self.assertTrue(isinstance(address, EC2UnitAddress))

    @inlineCallbacks
    def test_get_local_address(self):
        address = yield self.get_address_for("local")
        self.assertTrue(isinstance(address, LocalUnitAddress))

    @inlineCallbacks
    def test_get_orchestra_address(self):
        address = yield self.get_address_for("orchestra")
        self.assertTrue(isinstance(address, OrchestraUnitAddress))

    @inlineCallbacks
    def test_get_dummy_address(self):
        address = yield self.get_address_for("dummy")
        self.assertTrue(isinstance(address, DummyUnitAddress))

    def test_get_unknown_address(self):
        return self.assertFailure(self.get_address_for("foobar"), JujuError)


class DummyAddressTest(TestCase):

    def setUp(self):
        self.address = DummyUnitAddress()

    def test_get_address(self):

        self.assertEqual(
            (yield self.address.get_public_address()),
            "localhost")

        self.assertEqual(
            (yield self.address.get_private_address()),
            "localhost")


class EC2AddressTest(TestCase):

    def setUp(self):
        self.address = EC2UnitAddress()

    @inlineCallbacks
    def test_get_address(self):
        urls = [
            "http://169.254.169.254/latest/meta-data/local-hostname",
            "http://169.254.169.254/latest/meta-data/public-hostname"]

        def verify_args(url):
            self.assertEqual(urls.pop(0), url)
            return succeed("foobar\n")

        self.patch(client, "getPage", verify_args)
        self.assertEqual(
            (yield self.address.get_private_address()), "foobar")
        self.assertEqual(
            (yield self.address.get_public_address()), "foobar")


class LocalAddressTest(TestCase):

    def setUp(self):
        self.address = LocalUnitAddress()

    @inlineCallbacks
    def test_get_address(self):
        self.patch(
            subprocess, "check_output",
            lambda args: "192.168.1.122 127.0.0.1\n")
        self.assertEqual(
            (yield self.address.get_public_address()),
            "192.168.1.122")
        self.assertEqual(
            (yield self.address.get_private_address()),
            "192.168.1.122")


class OrchestraAddressTest(TestCase):

    def setUp(self):
        self.address = OrchestraUnitAddress()

    @inlineCallbacks
    def test_get_address(self):
        self.patch(
            subprocess, "check_output",
            lambda args: "slice.foobar.domain.net\n")

        self.assertEqual(
            (yield self.address.get_public_address()),
            "slice.foobar.domain.net")

        self.assertEqual(
            (yield self.address.get_private_address()),
            "slice.foobar.domain.net")
