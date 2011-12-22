"""Service units have both a public and private address.
"""
import subprocess

from twisted.internet.defer import inlineCallbacks, returnValue, succeed
from twisted.internet.threads import deferToThread
from twisted.web import client

from juju.errors import JujuError
from juju.state.environment import GlobalSettingsStateManager


@inlineCallbacks
def get_unit_address(client):
    settings = GlobalSettingsStateManager(client)
    provider_type = yield settings.get_provider_type()
    if provider_type == "ec2":
        returnValue(EC2UnitAddress())
    elif provider_type == "local":
        returnValue(LocalUnitAddress())
    elif provider_type == "orchestra":
        returnValue(OrchestraUnitAddress())
    elif provider_type == "dummy":
        returnValue(DummyUnitAddress())

    raise JujuError(
        "Unknown provider type: %r, unit addresses unknown." % provider_type)


class UnitAddress(object):

    def get_private_address(self):
        raise NotImplemented()

    def get_public_address(self):
        raise NotImplemented()


class DummyUnitAddress(UnitAddress):

    def get_private_address(self):
        return succeed("localhost")

    def get_public_address(self):
        return succeed("localhost")


class EC2UnitAddress(UnitAddress):

    @inlineCallbacks
    def get_private_address(self):
        content = yield client.getPage(
            "http://169.254.169.254/latest/meta-data/local-hostname")
        returnValue(content.strip())

    @inlineCallbacks
    def get_public_address(self):
        content = yield client.getPage(
            "http://169.254.169.254/latest/meta-data/public-hostname")
        returnValue(content.strip())


class LocalUnitAddress(UnitAddress):

    def get_private_address(self):
        return deferToThread(self._get_address)

    def get_public_address(self):
        return deferToThread(self._get_address)

    def _get_address(self):
        output = subprocess.check_output(["hostname", "-I"])
        return output.strip().split()[0]


class OrchestraUnitAddress(UnitAddress):

    def get_private_address(self):
        return deferToThread(self._get_address)

    def get_public_address(self):
        return deferToThread(self._get_address)

    def _get_address(self):
        output = subprocess.check_output(["hostname", "-f"])
        return output.strip()
