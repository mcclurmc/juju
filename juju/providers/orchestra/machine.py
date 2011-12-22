from juju.machine import ProviderMachine


class OrchestraMachine(ProviderMachine):
    """Orchestra-specific provider machine implementation"""


def machine_from_dict(d):
    """Convert a `dict` into a :class:`OrchestraMachine`.

    :param dict d: a dict as returned (in a list) by
        :meth:`juju.providers.orchestra.cobbler.CobblerClient.get_systems`

    :rtype: :class:`OrchestraMachine`
    """
    state = "pending" if d["netboot_enabled"] else "provisioned"
    return OrchestraMachine(d["uid"], d["name"], d["name"], state)
