import os
import re

from twisted.internet.defer import fail, inlineCallbacks, returnValue

from txaws.ec2.exception import EC2Error
from txaws.service import AWSServiceRegion

from juju.errors import (
    MachinesNotFound, ProviderError, ProviderInteractionError)
from juju.providers.common.base import MachineProviderBase

from .files import FileStorage
from .launch import EC2LaunchMachine
from .machine import EC2ProviderMachine, machine_from_instance
from .securitygroup import (
    open_provider_port, close_provider_port, get_provider_opened_ports,
    remove_security_groups, destroy_environment_security_group)
from .utils import get_region_uri


class MachineProvider(MachineProviderBase):
    """MachineProvider for use in an EC2/S3 environment"""

    def __init__(self, environment_name, config):
        super(MachineProvider, self).__init__(environment_name, config)

        if not config.get("ec2-uri"):
            ec2_uri = get_region_uri(config.get("region", "us-east-1"))
        else:
            ec2_uri = config.get("ec2-uri")

        self._service = AWSServiceRegion(
            access_key=config.get("access-key", ""),
            secret_key=config.get("secret-key", ""),
            ec2_uri=ec2_uri,
            s3_uri=config.get("s3-uri", ""))
        self.s3 = self._service.get_s3_client()
        self.ec2 = self._service.get_ec2_client()

    @property
    def provider_type(self):
        return "ec2"

    def get_serialization_data(self):
        """Get provider configuration suitable for serialization.

        Also extracts credential information from the environment.
        """
        data = super(MachineProvider, self).get_serialization_data()
        data.setdefault("access-key", os.environ.get("AWS_ACCESS_KEY_ID"))
        data.setdefault("secret-key", os.environ.get("AWS_SECRET_ACCESS_KEY"))
        return data

    def get_file_storage(self):
        """Retrieve an S3-backed :class:`FileStorage`."""
        return FileStorage(self.s3, self.config["control-bucket"])

    def start_machine(self, machine_data, master=False):
        """Start an EC2 machine.

        :param dict machine_data: desired characteristics of the new machine;
            it must include a "machine-id" key, and may include a "constraints"
            key to specify the underlying OS and hardware.

        :param bool master: if True, machine will initialize the juju admin
            and run a provisioning agent, in addition to running a machine
            agent.
        """
        if "machine-id" not in machine_data:
            return fail(ProviderError(
                "Cannot launch a machine without specifying a machine-id"))
        machine_id = machine_data["machine-id"]
        constraints = machine_data.get("constraints", {})
        return EC2LaunchMachine(self, master, constraints).run(machine_id)

    @inlineCallbacks
    def get_machines(self, instance_ids=()):
        """List machines running in the provider.

        :param list instance_ids: ids of instances you want to get. Leave empty
            to list every
            :class:`juju.providers.ec2.machine.EC2ProviderMachine` owned by
            this provider.

        :return: a list of
            :class:`juju.providers.ec2.machine.EC2ProviderMachine`
            instances
        :rtype: :class:`twisted.internet.defer.Deferred`

        :raises: :exc:`juju.errors.MachinesNotFound`
        """
        group_name = "juju-%s" % self.environment_name
        try:
            instances = yield self.ec2.describe_instances(*instance_ids)
        except EC2Error as error:
            code = error.get_error_codes()
            message = error.get_error_messages()
            if code == "InvalidInstanceID.NotFound":
                message = error.get_error_messages()
                raise MachinesNotFound(
                    re.findall(r"\bi-[0-9a-f]{3,15}\b", message))
            raise ProviderInteractionError(
                "Unexpected EC2Error getting machines %s: %s"
                % (", ".join(instance_ids), message))

        machines = []
        for instance in instances:
            if instance.instance_state not in ("running", "pending"):
                continue
            if group_name not in instance.reservation.groups:
                continue
            machines.append(machine_from_instance(instance))

        if instance_ids:
            # We were asked for a specific list of machines, and if we can't
            # completely fulfil that request we should blow up.
            found_instance_ids = set(m.instance_id for m in machines)
            missing = set(instance_ids) - found_instance_ids
            if missing:
                raise MachinesNotFound(missing)
        returnValue(machines)

    @inlineCallbacks
    def destroy_environment(self):
        """Terminate all associated machines and security groups.

        The super defintion of this method terminates each machine in
        the environment; this needs to be augmented here by also
        removing the security group for the environment.

        :rtype: :class:`twisted.internet.defer.Deferred`
        """
        try:
            killed_machines = yield super(MachineProvider, self).\
                destroy_environment()
            returnValue(killed_machines)
        finally:
            yield destroy_environment_security_group(self)

    @inlineCallbacks
    def shutdown_machines(self, machines):
        """Terminate machines associated with this provider.

        :param machines: machines to shut down
        :type machines: list of
            :class:`juju.providers.ec2.machine.EC2ProviderMachine`

        :return: list of terminated
            :class:`juju.providers.ec2.machine.EC2ProviderMachine`
            instances
        :rtype: :class:`twisted.internet.defer.Deferred`
        """
        if not machines:
            returnValue([])

        for machine in machines:
            if not isinstance(machine, EC2ProviderMachine):
                raise ProviderError("Can only shut down EC2ProviderMachines; "
                                    "got a %r" % type(machine))

        ids = [m.instance_id for m in machines]
        killable_machines = yield self.get_machines(ids)
        if not killable_machines:
            returnValue([])  # Nothing to do

        killable_ids = [m.instance_id for m in killable_machines]
        terminated = yield self.ec2.terminate_instances(*killable_ids)

        # Pass on what was actually terminated, in the case the
        # machine has somehow disappeared since get_machines
        # above. This is to avoid getting EC2Error: Error Message:
        # Invalid id when running ec2.describe_instances in
        # remove_security_groups
        terminated_ids = [info[0] for info in terminated]
        yield remove_security_groups(self, terminated_ids)
        returnValue(killable_machines)

    def open_port(self, machine, machine_id, port, protocol="tcp"):
        """Authorizes `port` using `protocol` on EC2 for `machine`."""
        return open_provider_port(self, machine, machine_id, port, protocol)

    def close_port(self, machine, machine_id, port, protocol="tcp"):
        """Revokes `port` using `protocol` on EC2 for `machine`."""
        return close_provider_port(self, machine, machine_id, port, protocol)

    def get_opened_ports(self, machine, machine_id):
        """Returns a set of open (port, proto) pairs for `machine`."""
        return get_provider_opened_ports(self, machine, machine_id)
