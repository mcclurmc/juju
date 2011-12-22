from cStringIO import StringIO
import urllib
import urlparse

from twisted.web.client import getPage
from twisted.web.error import Error

from juju.errors import FileNotFound, ProviderError
from juju.providers.common.utils import convert_unknown_error
from juju.providers.orchestra.digestauth import (
    DigestAuthenticator, get_page_auth)


def _convert_error(failure, method, url, errors):
    if failure.check(Error):
        status = failure.value.status
        error = errors.get(int(status))
        if error:
            raise error
        raise ProviderError(
            "Unexpected HTTP %s trying to %s %s" % (status, method, url))
    return convert_unknown_error(failure)


class FileStorage(object):
    """A WebDAV-backed :class:`FileStorage` abstraction"""

    def __init__(self, config):
        fallback_url = "http://%(orchestra-server)s/webdav" % config
        self._base_url = config.get("storage-url", fallback_url)
        self._auth = DigestAuthenticator(
            config.get("storage-user", config["orchestra-user"]),
            config.get("storage-pass", config["orchestra-pass"]))

    def get_url(self, name):
        """Return a URL that can be used to access a stored file.

        :param unicode name: the file path for which to provide a URL

        :return: a URL
        :rtype: str
        """
        url = u"/".join((self._base_url, name))
        # query and fragment are irrelevant to our purposes
        scheme, netloc, path = urlparse.urlsplit(url)[:3]
        return urlparse.urlunsplit((
            str(scheme),
            netloc.encode("idna"),
            urllib.quote(path.encode("utf-8")),
            "", ""))

    def get(self, name):
        """Get a file object from the Orchestra WebDAV server.

        :param unicode name: path to for the desired file

        :return: an open file object
        :rtype: :class:`twisted.internet.defer.Deferred`

        :raises: :exc:`juju.errors.FileNotFound` if the file doesn't exist
        """
        url = self.get_url(name)
        d = getPage(url)
        d.addCallback(StringIO)
        d.addErrback(_convert_error, "GET", url, {404: FileNotFound(url)})
        return d

    def put(self, name, file_object):
        """Upload a file to WebDAV.

        :param unicode remote_path: path on which to store the content

        :param file_object: open file object containing the content

        :rtype: :class:`twisted.internet.defer.Deferred`
        """
        url = self.get_url(name)
        postdata = file_object.read()
        d = get_page_auth(url, self._auth, method="PUT", postdata=postdata)
        d.addCallback(lambda _: True)
        d.addErrback(_convert_error, "PUT", url, {401: ProviderError(
            "The supplied storage credentials were not accepted by the "
            "server")})
        return d
