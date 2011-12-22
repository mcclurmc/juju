import logging

from twisted.internet.defer import inlineCallbacks
from txzookeeper.client import ZOO_OPEN_ACL_UNSAFE

from .auth import make_ace
from .environment import GlobalSettingsStateManager
from .machine import MachineStateManager


log = logging.getLogger("juju.state.init")


class StateHierarchy(object):
    """
    An initializer to the juju zookeeper hierarchy.
    """

    def __init__(
        self, client, admin_identity, instance_id, provider_type):
        """
        :param client: A zookeeper client
        :param admin_identity: A zookeeper auth identity for the admin.
        :param instance_id: The boostrap node machine id.
        :param provider_type: The type of the environnment machine provider.
        """
        self.client = client
        self.admin_identity = admin_identity
        self.instance_id = instance_id
        self.provider_type = provider_type

    @inlineCallbacks
    def initialize(self):
        log.info("Initializing zookeeper hierarchy")

        acls = [make_ace(self.admin_identity, all=True),
                # XXX till we have roles throughout
                ZOO_OPEN_ACL_UNSAFE]

        yield self.client.create("/charms", acls=acls)
        yield self.client.create("/services", acls=acls)
        yield self.client.create("/machines", acls=acls)
        yield self.client.create("/units", acls=acls)
        yield self.client.create("/relations", acls=acls)

        # Create the node representing the bootstrap machine (ourself)
        manager = MachineStateManager(self.client)
        machine_state = yield manager.add_machine_state()
        yield machine_state.set_instance_id(self.instance_id)

        # Setup default global settings information.
        settings = GlobalSettingsStateManager(self.client)
        yield settings.set_provider_type(self.provider_type)

        # This must come last, since clients will wait on it.
        yield self.client.create("/initialized", acls=acls)

        # DON'T WRITE ANYTHING HERE.  See line above.
