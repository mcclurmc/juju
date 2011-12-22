from twisted.internet.defer import inlineCallbacks, returnValue

from txaws.ec2.exception import EC2Error

from juju.errors import ProviderInteractionError
from juju.providers.common.launch import LaunchMachine

from .machine import machine_from_instance
from .utils import get_image_id, log


class EC2LaunchMachine(LaunchMachine):
    """Amazon EC2 operation for launching an instance"""

    @inlineCallbacks
    def start_machine(self, machine_id, zookeepers):
        """Actually launch an instance on EC2.

        :param str machine_id: the juju machine ID to assign

        :param zookeepers: the machines currently running zookeeper, to which
            the new machine will need to connect
        :type zookeepers: list of
            :class:`juju.providers.ec2.machine.EC2ProviderMachine`

        :return: a singe-entry list containing a
            :class:`juju.providers.ec2.machine.EC2ProviderMachine`
            representing the newly-launched machine
        :rtype: :class:`twisted.internet.defer.Deferred`
        """
        cloud_init = self._create_cloud_init(machine_id, zookeepers)
        cloud_init.set_provider_type("ec2")
        cloud_init.set_instance_id_accessor(
            "$(curl http://169.254.169.254/1.0/meta-data/instance-id)")
        user_data = cloud_init.render()

        instance_type = self._provider.config.get(
            "default-instance-type", "m1.small")
        image_id = yield get_image_id(self._provider.config, self._constraints)
        security_groups = yield self._ensure_groups(machine_id)

        instances = yield self._provider.ec2.run_instances(
            image_id=image_id,
            instance_type=instance_type,
            min_count=1,
            max_count=1,
            security_groups=security_groups,
            user_data=user_data)

        returnValue([machine_from_instance(i) for i in instances])

    @inlineCallbacks
    def _ensure_groups(self, machine_id):
        """Ensure the juju group is the machine launch groups.

        Machines launched by juju are tagged with a group so they
        can be distinguished from other machines that might be running
        on an EC2 account. This group can be specified explicitly or
        implicitly defined by the environment name. In addition, a
        specific machine security group is created for each machine,
        so that its firewall rules can be configured per machine.

        :param machine_id: The juju machine ID of the new machine
        """
        juju_group = "juju-%s" % self._provider.environment_name
        juju_machine_group = "juju-%s-%s" % (
            self._provider.environment_name, machine_id)

        security_groups = yield self._provider.ec2.describe_security_groups()
        group_ids = [group.name for group in security_groups]

        # Create the provider group if doesn't exist.
        if not juju_group in group_ids:
            log.debug("Creating juju provider group %s", juju_group)
            yield self._provider.ec2.create_security_group(
                juju_group,
                "juju group for %s" % self._provider.environment_name)

            # Authorize SSH.
            yield self._provider.ec2.authorize_security_group(
                juju_group,
                ip_protocol="tcp",
                from_port="22", to_port="22",
                cidr_ip="0.0.0.0/0")

            # We need to describe the group to pickup the owner_id for auth.
            groups_info = yield self._provider.ec2.describe_security_groups(
                juju_group)

            # Authorize Internal ZK Traffic
            yield self._provider.ec2.authorize_security_group(
                juju_group,
                source_group_name=juju_group,
                source_group_owner_id=groups_info.pop().owner_id)

        # Create the machine-specific group, but first see if there's
        # one already existing from a previous machine launch;
        # if so, delete it, since it can have the wrong firewall setup
        if juju_machine_group in group_ids:
            try:
                yield self._provider.ec2.delete_security_group(
                    juju_machine_group)
                log.debug("Deleted existing machine group %s, will replace",
                          juju_machine_group)
            except EC2Error, e:
                log.debug("Cannot delete security group %s: %s",
                          juju_machine_group, e)
                raise ProviderInteractionError(
                        "Unexpected EC2Error deleting security group %s: %s"
                    % (juju_machine_group, e.get_error_messages()))
        log.debug("Creating juju machine security group %s",
                  juju_machine_group)
        yield self._provider.ec2.create_security_group(
            juju_machine_group,
            "juju group for %s machine %s" % (
                self._provider.environment_name, machine_id))

        returnValue([juju_group, juju_machine_group])
