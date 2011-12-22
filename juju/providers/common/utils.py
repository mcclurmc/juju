import logging
import os
from yaml import safe_dump

from twisted.python.failure import Failure

from juju.errors import JujuError, ProviderInteractionError

log = logging.getLogger("juju.common")


def convert_unknown_error(failure):
    """Convert any non-juju errors to a provider interaction error.

    Supports both usage from within an except clause, and as an
    errback handler ie. both the following forms are supported.

    ...
        try:
           something()
        except Exception, e:
           convert_unknown_errors(e)

    ...
        d.addErrback(convert_unknown_errors)

    """
    if isinstance(failure, Failure):
        error = failure.value
    else:
        error = failure

    if not isinstance(error, JujuError):
        message = ("Unexpected %s interacting with provider: %s"
                   % (type(error).__name__, str(error)))
        error = ProviderInteractionError(message)

    if isinstance(failure, Failure):
        return Failure(error)
    raise error


# XXX There's some inconsistency in the handling of authorized_keys
#     here. While it's fine and correct for this function to read the
#     list of keys in authorized_keys format (text with key-per-line),
#     cloud-init itself expects a _list_ of keys, and what we end up
#     doing is passing the whole blob of data as a single key. This
#     should be fixed to *return* a list of keys by splitting the
#     lines in the data obtained, and then the call site can add each
#     individual key to CloudInit.
#
def get_user_authorized_keys(config):
    """Locate a public key for the user.

    If neither "authorized-keys" nor "authorized-keys-path" is
    present in config, will look in the user's .ssh directory.

    The name of this method is "authorized_keys", plural, because
    it returns the *text* (not list) to be inserted into the
    ~/.ssh/authorized_keys file. Multiple keys may be returned,
    in the same format expected by that file.

    :return: an SSH public key
    :rtype: str

    :raises: :exc:`LookupError` if an SSH public key is not found.
    """
    key_names = ["id_dsa.pub", "id_rsa.pub", "identity.pub"]
    if config.get("authorized-keys"):
        return config["authorized-keys"]

    if config.get("authorized-keys-path"):
        key_names[:] = [config.get("authorized-keys-path")]

    for key_path in key_names:
        path_candidate = os.path.expanduser(key_path)
        if not os.path.exists(path_candidate):
            path_candidate = "~/.ssh/%s" % key_path
            path_candidate = os.path.expanduser(path_candidate)

        if not os.path.exists(path_candidate):
            continue
        return open(path_candidate).read()
    raise LookupError("SSH authorized/public key not found.")


def format_cloud_init(
    authorized_keys, packages=(), repositories=None, scripts=None, data=None):
    """Format a user-data cloud-init file.

    This will enable package installation, and ssh access, and script
    execution on launch, and passing data values to an instance.

    Its important to note that no sensistive data (credentials) should be
    conveyed via cloud-config, as the values are accessible from the any
    process by default on the instance.

    Further documentation on the capabilities of cloud-init
    https://help.ubuntu.com/community/CloudInit

    :param authorized_keys: The authorized SSH key to be used when
        populating the newly launched machines.
    :type authorized_keys: list of strings

    :param packages: The packages to be installed on a machine.
    :type packages: list of strings

    :param repositories: Debian repostiories to be used as apt sources on the
        machine. 'ppa:' syntax can be used.
    :type repositories: list of strings

    :param scripts: Scripts to be executed (in order) on machine start.
    :type scripts: list of strings

    :param dict data: Optional additional data to be passed via cloud config.
        It will be accessible via the key 'machine-data' from the yaml data
        structure.
    """
    cloud_config = {
        "apt-update": True,
        "apt-upgrade": True,
        "ssh_authorized_keys": authorized_keys,
        "packages": [],
        "output": {"all": "| tee -a /var/log/cloud-init-output.log"}}

    if data:
        cloud_config["machine-data"] = data

    if packages:
        cloud_config["packages"].extend(packages)

    if repositories:
        sources = [dict(source=r) for r in repositories]
        cloud_config["apt_sources"] = sources

    if scripts:
        cloud_config["runcmd"] = scripts

    output = safe_dump(cloud_config)
    output = "#cloud-config\n%s" % (output)
    return output
