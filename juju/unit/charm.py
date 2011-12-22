
import os
import shutil

from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.web.client import downloadPage
from twisted.web.error import Error

from juju.errors import FileNotFound
from juju.charm.bundle import CharmBundle
from juju.lib import under
from juju.state.charm import CharmStateManager


@inlineCallbacks
def download_charm(client, charm_id, charms_directory):
    charm_state_manager = CharmStateManager(client)
    charm_state = yield charm_state_manager.get_charm_state(charm_id)

    # Calculate local charm path
    checksum = yield charm_state.get_sha256()
    charm_key = under.quote("%s:%s" % (charm_state.id, checksum))
    local_charm_path = os.path.join(
        charms_directory, charm_key)

    # Retrieve charm from provider storage link
    if charm_state.bundle_url.startswith("file://"):
        file_path = charm_state.bundle_url[len("file://"):]
        if not os.path.exists(file_path):
            raise FileNotFound(charm_state.bundle_url)
        shutil.copyfileobj(open(file_path), open(local_charm_path, "w"))
    else:
        try:
            yield downloadPage(charm_state.bundle_url, local_charm_path)
        except Error:
            raise FileNotFound(charm_state.bundle_url)
    returnValue(CharmBundle(local_charm_path))
