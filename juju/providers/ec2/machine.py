from juju.machine import ProviderMachine


class EC2ProviderMachine(ProviderMachine):
    """EC2-specific ProviderMachine implementation.

    Not really interesting right now, besides for "tagging" purposes.
    """


def machine_from_instance(instance):
    """Create an :class:`EC2ProviderMachine` from a txaws :class:`Instance`

    :param instance: the EC2 Instance

    :return: a matching :class:`EC2ProviderMachine`
    """
    return EC2ProviderMachine(
        instance.instance_id,
        instance.dns_name,
        instance.private_dns_name,
        instance.instance_state)
