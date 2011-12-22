

class ProviderMachine(object):
    """
    Representative of a machine resource created by a
    :class:`MachineProvider`. The object is typically annotated by the
    machine provider, such that the provider can perform subsequent
    actions upon it, using the additional metadata for identification,
    without leaking these details to consumers of the
    :class:`MachineProvider` api.
    """

    def __init__(self, instance_id, dns_name=None, private_dns_name=None,
                 state="unknown"):
        self.instance_id = instance_id
        # ideally this would be ip_address, but txaws doesn't expose it.
        self.dns_name = dns_name
        self.private_dns_name = private_dns_name
        self.state = state
