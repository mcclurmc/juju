from twisted.internet.defer import Deferred, inlineCallbacks, returnValue
from txaws.ec2.exception import EC2Error

from juju.errors import ProviderInteractionError
from juju.lib.twistutils import gather_results

from .utils import log


def _get_juju_security_group(provider):
    """Get EC2 security group name for environment of `provider`."""
    return "juju-%s" % provider.environment_name


def _get_machine_group_name(provider, machine_id):
    """Get EC2 security group name associated just with `machine_id`."""
    return "juju-%s-%s" % (provider.environment_name, machine_id)


# TODO These security group functions do not handle the eventual
# consistency seen with EC2. A future branch will add support for
# retry so that using code doesn't have to be aware of this issue.
#
# In addition, the functions work with respect to the machine id,
# since they manipulate a security group permanently associated with
# the EC2 provided machine, and the machine must be launched into this
# security group. This security group, per the above
# `_get_machine_group_name`, embeds the machine id, eg
# juju-moon-42. Ideally, this would not be the case. See the
# comments associated with the merge proposal of
# https://code.launchpad.net/~jimbaker/juju/expose-provider-ec2/


@inlineCallbacks
def open_provider_port(provider, machine, machine_id, port, protocol):
    """Authorize `port`/`proto` for the machine security group."""
    try:
        yield provider.ec2.authorize_security_group(
            _get_machine_group_name(provider, machine_id),
            ip_protocol=protocol,
            from_port=str(port), to_port=str(port),
            cidr_ip="0.0.0.0/0")
        log.debug("Opened %s/%s on provider machine %r",
                  port, protocol, machine.instance_id)
    except EC2Error, e:
        raise ProviderInteractionError(
            "Unexpected EC2Error opening %s/%s on machine %s: %s"
            % (port, protocol, machine.instance_id, e.get_error_messages()))


@inlineCallbacks
def close_provider_port(provider, machine, machine_id, port, protocol):
    """Revoke `port`/`proto` for the machine security group."""
    try:
        yield provider.ec2.revoke_security_group(
            _get_machine_group_name(provider, machine_id),
            ip_protocol=protocol,
            from_port=str(port), to_port=str(port),
            cidr_ip="0.0.0.0/0")
        log.debug("Closed %s/%s on provider machine %r",
                  port, protocol, machine.instance_id)
    except EC2Error, e:
        raise ProviderInteractionError(
            "Unexpected EC2Error closing %s/%s on machine %s: %s"
            % (port, protocol, machine.instance_id, e.get_error_messages()))


@inlineCallbacks
def get_provider_opened_ports(provider, machine, machine_id):
    """Gets the opened ports for `machine`.

    Retrieves the IP permissions associated with the machine
    security group, then parses them to return a set of (port,
    proto) pairs.
    """
    try:
        security_groups = yield provider.ec2.describe_security_groups(
            _get_machine_group_name(provider, machine_id))
    except EC2Error, e:
        raise ProviderInteractionError(
            "Unexpected EC2Error getting open ports on machine %s: %s"
            % (machine.instance_id, e.get_error_messages()))

    opened_ports = set()  # made up of (port, protocol) pairs
    for ip_permission in security_groups[0].allowed_ips:
        if ip_permission.cidr_ip != "0.0.0.0/0":
            continue
        from_port = int(ip_permission.from_port)
        to_port = int(ip_permission.to_port)
        if from_port == to_port:
            # Only return ports that are individually opened. We
            # ignore multi-port ranges, since they are set outside of
            # juju (at this time at least)
            opened_ports.add((from_port, ip_permission.ip_protocol))
    returnValue(opened_ports)


def _get_machine_security_group_from_instance(provider, instance):
    """Parses the `reservation` of `instance` to get assoc machine group."""
    juju_security_group = _get_juju_security_group(provider)
    for group in instance.reservation.groups:
        if group != juju_security_group:
            return group

    # Ignore if no such group exists; this allows some limited
    # backwards compatibility with old setups without machine
    # security group
    log.info("Ignoring missing machine security group for instance %r",
             instance.instance_id)
    return None


@inlineCallbacks
def _delete_security_group(provider, group):
    """Wrap EC2 delete_security_group."""
    try:
        yield provider.ec2.delete_security_group(group)
        log.debug("Deleted security group %r", group)
    except EC2Error, e:
        raise ProviderInteractionError(
            "EC2 error when attempting to delete group %s: %s" % (group, e))


@inlineCallbacks
def remove_security_groups(provider, instance_ids):
    """Remove security groups associated with `instance_ids` for `provider`"""
    log.info(
        "Waiting on %d EC2 instances to transition to terminated state, "
        "this may take a while", len(instance_ids))

    # Repeatedly poll EC2 until instances are in terminated state;
    # upon reaching that state, delete associated machine security
    # groups. The limit of 200 polls is arbitrary and could be
    # specified by a command line option (and/or an overall
    # timeout). It's based on an observed ~500 ms roundtrip time per
    # call of the describe_instances web service, along with typically
    # taking about 40s to move all instances to a terminated state.
    wait_on = set(instance_ids)
    pending_deletions = []
    for i in xrange(200):
        if not wait_on:
            break
        instances = yield provider.ec2.describe_instances(*wait_on)
        for instance in instances:
            if instance.instance_state == "terminated":
                log.debug("Instance %r was terminated",
                          instance.instance_id)
                wait_on.discard(instance.instance_id)
                group = _get_machine_security_group_from_instance(
                    provider, instance)
                if group:
                    pending_deletions.append(
                        _delete_security_group(provider, group))
    if wait_on:
        outstanding = [
            _get_machine_security_group_from_instance(provider, instance)
                for instance in instances]
        log.error("Instance shutdown taking too long, "
                  "could not delete groups %s",
                 ", ".join(sorted(outstanding)))

    # Wait for all pending deletions to complete
    yield gather_results(pending_deletions)


@inlineCallbacks
def destroy_environment_security_group(provider):
    """Delete the security group for the environment of `provider`"""
    group = _get_juju_security_group(provider)
    try:
        yield provider.ec2.delete_security_group(group)
        log.debug("Deleted environment security group %r", group)
        returnValue(True)
    except EC2Error, e:
        # Ignore, since this is only attempting to cleanup
        log.debug(
            "Ignoring EC2 error when attempting to delete group %s: %s" % (
                group, e))
        returnValue(False)
