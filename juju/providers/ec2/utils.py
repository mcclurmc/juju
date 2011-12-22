import logging
import csv
import StringIO

from twisted.web.client import getPage
from twisted.web.error import Error
from twisted.internet.defer import succeed

log = logging.getLogger("juju.ec2")

_CURRENT_IMAGE_URI_TEMPLATE = (
    "http://uec-images.ubuntu.com/query/"
    "%(ubuntu_release_name)s/%(variant)s/%(version)s.current.txt")


def get_region_uri(region):
    """Get the URL endpoint for the region."""
    return "https://ec2.%s.amazonaws.com" % region


# XXX ideally should come from latest available or client release name.
def get_current_ami(ubuntu_release="oneiric", architecture="i386",
                    persistent_storage=True, region="us-east-1", daily=False,
                    desktop=False, url_fetch=None):
    """Get the latest ami for the last release of ubuntu."""
    data = {}
    data["ubuntu_release_name"] = ubuntu_release
    data["version"] = daily and "daily" or "released"
    data["variant"] = desktop and "desktop" or "server"
    ebs_match = persistent_storage and "ebs" or "instance-store"

    url = _CURRENT_IMAGE_URI_TEMPLATE % data
    url_fetch = url_fetch or getPage

    def failed():
        raise LookupError((ubuntu_release, architecture, region,
                           data["version"], data["variant"]))

    d = url_fetch(url)

    def handle_404(failure):
        failure.trap(Error)
        if failure.value.status == "404":
            failed()
        return failure

    def extract_ami(current_data):
        data_stream = StringIO.StringIO(current_data)
        for tokens in csv.reader(data_stream, "excel-tab"):
            if tokens[4] != ebs_match:
                continue
            if tokens[5] == architecture and tokens[6] == region:
                return tokens[7]
        failed()

    d.addErrback(handle_404)
    d.addCallback(extract_ami)
    return d


def get_image_id(config, constraints):
    """Get an EC2 image ID.

    :param dict config: environment configuration (which can override
        `constraints`)
    :param dict constraints: specific requirements for requested machine; TODO
        improve documentation once requirements firm up.

    :return: An AMI ID
    :rtype: :class:`twisted.internet.defer.Deferred`
    """
    image_id = config.get("default-image-id", None)
    if image_id:
        return succeed(image_id)
    region = config.get("region", "us-east-1")
    constraints = constraints.copy()
    if "ubuntu_release" not in constraints:
        if "default-series" in config:
            constraints["ubuntu_release"] = config["default-series"]
    return get_current_ami(region=region, **constraints)
