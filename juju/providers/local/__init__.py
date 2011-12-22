import os
import logging

from twisted.internet.defer import succeed, fail, inlineCallbacks, returnValue

from txzookeeper import ZookeeperClient

from juju.errors import ProviderError, EnvironmentNotFound
from juju.lib.lxc import LXCContainer, get_containers
from juju.lib.zk import Zookeeper
from juju.providers.common.base import MachineProviderBase
from juju.providers.common.connect import ZookeeperConnect
from juju.providers.common.utils import get_user_authorized_keys

from juju.providers.local.agent import ManagedMachineAgent
from juju.providers.local.files import StorageServer, LocalStorage
from juju.providers.local.machine import LocalMachine
from juju.providers.local.network import Network
from juju.providers.local.pkg import check_packages
from juju.state.auth import make_identity
from juju.state.initialize import StateHierarchy
from juju.state.placement import LOCAL_POLICY
from juju.state.utils import get_open_port


log = logging.getLogger("juju.local-dev")

REQUIRED_PACKAGES = ["zookeeper",
                     "libvirt-bin",
                     "lxc",
                     "apt-cacher-ng"]


class MachineProvider(MachineProviderBase):
    """LXC/Ubuntu local provider.

    Only the host machine is utilized.
    """

    def __init__(self, environment_name, config):
        super(MachineProvider, self).__init__(environment_name, config)
        self._qualified_name = self._get_qualified_name()
        self._directory = os.path.join(
            self.config["data-dir"], self._qualified_name)

    def get_placement_policy(self):
        """Local dev supports only one unit placement policy."""
        if self.config.get("placement", LOCAL_POLICY) != LOCAL_POLICY:
            raise ProviderError(
                "Unsupported placement policy for local provider")
        return LOCAL_POLICY

    @property
    def provider_type(self):
        return "local"

    @inlineCallbacks
    def bootstrap(self):
        """Bootstrap a local development environment.
        """
        # Check for existing environment
        state = yield self.load_state()
        if state is not False:
            raise ProviderError("Environment already bootstrapped")

        # Check for required packages
        log.info("Checking for required packages...")
        missing = check_packages(*REQUIRED_PACKAGES)
        if missing:
            raise ProviderError("Missing packages %s" % (
                ", ".join(sorted(list(missing)))))

        # Get/create directory for zookeeper and files
        zookeeper_dir = os.path.join(self._directory, "zookeeper")
        if not os.path.exists(zookeeper_dir):
            os.makedirs(zookeeper_dir)

        # Start networking, and get an open port.
        log.info("Starting networking...")
        net = Network("default", subnet=122)

        # Start is a noop if its already started, which it is by default,
        # per libvirt-bin package installation
        yield net.start()
        net_attributes = yield net.get_attributes()
        port = get_open_port(net_attributes["ip"]["address"])

        # Start zookeeper
        log.info("Starting zookeeper...")
        # Run zookeeper as the current user, unless we're being run as root
        # in which case run zookeeper as the 'zookeeper' user.
        zookeeper_user = None
        if os.geteuid() == 0:
            zookeeper_user = "zookeeper"
        zookeeper = Zookeeper(zookeeper_dir,
                              port=port,
                              host=net_attributes["ip"]["address"],
                              user=zookeeper_user, group=zookeeper_user)

        yield zookeeper.start()

        # Starting provider storage server
        log.info("Starting storage server...")
        storage_server = StorageServer(
            pid_file=os.path.join(self._directory, "storage-server.pid"),
            storage_dir=os.path.join(self._directory, "files"),
            host=net_attributes["ip"]["address"],
            port=get_open_port(net_attributes["ip"]["address"]),
            log_file=os.path.join(self._directory, "storage-server.log"))
        yield storage_server.start()

        # Save the zookeeper start to provider storage.
        yield self.save_state({"zookeeper-instances": ["local"],
                               "zookeeper-address": zookeeper.address})

        # Initialize the zookeeper state
        log.debug("Initializing state...")
        admin_identity = make_identity(
            "admin:%s" % self.config["admin-secret"])
        client = ZookeeperClient(zookeeper.address)
        yield client.connect()
        hierarchy = StateHierarchy(client, admin_identity, "local", "local")
        yield hierarchy.initialize()

        # Store user credentials from the running user
        try:
            public_key = get_user_authorized_keys(self.config)
            public_key = public_key.strip()
        except LookupError, e:
            raise ProviderError(str(e))

        # Startup the machine agent
        pid_file = os.path.join(self._directory, "machine-agent.pid")
        log_file = os.path.join(self._directory, "machine-agent.log")

        juju_origin = self.config.get("juju-origin")
        agent = ManagedMachineAgent(pid_file,
                                    zookeeper_hosts=zookeeper.address,
                                    machine_id="0",
                                    juju_directory=self._directory,
                                    log_file=log_file,
                                    juju_origin=juju_origin,
                                    juju_unit_namespace=self._qualified_name,
                                    public_key=public_key)
        log.info(
            "Starting machine agent (origin: %s)... ", agent.juju_origin)
        yield agent.start()

        log.info("Environment bootstrapped")

    @inlineCallbacks
    def destroy_environment(self):
        """Shutdown the machine environment.
        """
        # Stop all the containers
        log.info("Destroying unit containers...")
        yield self._destroy_containers()

        # Stop the machine agent
        log.debug("Stopping machine agent...")
        pid_file = os.path.join(self._directory, "machine-agent.pid")
        agent = ManagedMachineAgent(pid_file)
        yield agent.stop()

        # Stop the storage server
        log.debug("Stopping storage server...")
        pid_file = os.path.join(self._directory, "storage-server.pid")
        storage_server = StorageServer(pid_file)
        yield storage_server.stop()

        # Stop zookeeper
        log.debug("Stopping zookeeper...")
        zookeeper_dir = os.path.join(self._directory, "zookeeper")
        zookeeper = Zookeeper(zookeeper_dir, None)
        yield zookeeper.stop()

        # Clean out local state
        yield self.save_state(False)

        # Don't stop the network since we're using the default libvirt
        log.debug("Environment destroyed.")

    @inlineCallbacks
    def _destroy_containers(self):
        container_map = yield get_containers(self._qualified_name)
        for container_name in container_map:
            container = LXCContainer(container_name, None, None, None)
            if container_map[container.container_name]:
                yield container.stop()
            yield container.destroy()

    @inlineCallbacks
    def connect(self, share=False):
        """Connect to juju's zookeeper.
        """
        state = yield self.load_state()
        if not state:
            raise EnvironmentNotFound()
        client = yield ZookeeperClient(state["zookeeper-address"]).connect()
        yield ZookeeperConnect(self).wait_for_initialization(client)
        returnValue(client)

    def get_file_storage(self):
        """Retrieve the provider C{FileStorage} abstraction.
        """
        storage_path = self.config.get(
            "storage-dir", os.path.join(self._directory, "files"))
        if not os.path.exists(storage_path):
            try:
                os.makedirs(storage_path)
            except OSError:
                raise ProviderError(
                    "Unable to create file storage for environment")
        return LocalStorage(storage_path)

    def start_machine(self, machine_data, master=False):
        """Start a machine in the provider.

        @param machine_data: a dictionary of data to pass along to the newly
           launched machine.

        @param master: if True, machine will initialize the juju admin
            and run a provisioning agent.
        """
        return fail(ProviderError("Only local machine available"))

    def shutdown_machine(self, machine_id):
        return fail(ProviderError(
            "Not enabled for local dev, use remove-unit"))

    def get_machines(self, instance_ids=()):
        """List machines running in the provider.

        @param instance_ids: ids of instances you want to get. Leave blank
            to list all machines owned by juju.

        @return: a list of LocalMachine, always contains one item.

        @raise: MachinesNotFound
        """
        if instance_ids and instance_ids != ["local"]:
            raise ProviderError("Only local machine available")
        return succeed([LocalMachine()])

    def _get_qualified_name(self):
        """Get a qualified environment name for use by local dev resources
        """
        # Ensure we namespace resources by user.
        user = os.environ.get("USER")

        # We need sudo access for lxc (till user namespaces), use the actual
        # user.
        if user == "root":
            sudo_user = os.environ.get("SUDO_USER")
            if sudo_user:
                user = sudo_user
        return "%s-%s" % (user, self.environment_name)
