"""
Ec2 Provider File Storage on S3
"""

from base64 import b64encode

import hmac
import sha
import urllib
import time

from cStringIO import StringIO

from twisted.internet.defer import fail
from twisted.web.error import Error
from txaws.s3.client import URLContext

from juju.errors import FileNotFound

_FILENOTFOUND_CODES = ("NoSuchKey", "NoSuchBucket")


def _safe_string(s):
    if isinstance(s, unicode):
        s = s.encode('utf8')
    return s


class FileStorage(object):
    """S3-backed :class:`FileStorage` abstraction"""

    def __init__(self, s3, bucket):
        self._s3 = s3
        self._bucket = bucket

    def get_url(self, name):
        """Return a URL that can be used to access a stored file.

        S3 time authenticated URL reference:
        http://s3.amazonaws.com/doc/s3-developer-guide/RESTAuthentication.html

        :param unicode name: the S3 key for which to provide a URL

        :return: a signed URL, expiring 10 years from now
        :rtype: str
        """
        # URLs are good for 10 years.
        expires = int(time.time()) + 365 * 24 * 3600 * 10
        name = _safe_string(name)
        path = "%s/%s" % (self._bucket, urllib.quote(name))
        signed = hmac.new(self._s3.creds.secret_key, digestmod=sha)
        signed.update("GET\n\n\n%s\n/%s" % (expires, path))
        signature = urllib.quote_plus(b64encode(signed.digest()).strip())

        url_context = URLContext(
            self._s3.endpoint, urllib.quote(self._bucket), urllib.quote(name))
        url = url_context.get_url()

        url += "?Signature=%s&Expires=%s&AWSAccessKeyId=%s" % (
            signature, expires, self._s3.creds.access_key)
        return url

    def get(self, name):
        """Get a file object from S3.

        :param unicode name: S3 key for the desired file

        :return: an open file object
        :rtype: :class:`twisted.internet.defer.Deferred`

        :raises: :exc:`juju.errors.FileNotFound` if the file doesn't exist
        """
        # for now we do the simplest thing and just fetch the file
        # in a single call, s3 limits this to some mb, as we grow
        # to have charms that might grow to this size, we can
        # revisit fetching in batches (head request for size) and
        # streaming to disk.
        d = self._s3.get_object(self._bucket, _safe_string(name))

        def on_content_retrieved(content):
            return StringIO(content)
        d.addCallback(on_content_retrieved)

        def on_no_file(failure):
            # Trap file not found errors and wrap them in an application error.
            failure.trap(Error)
            if str(failure.value.status) != "404":
                return failure
            return fail(FileNotFound("s3://%s/%s" % (self._bucket, name)))
        d.addErrback(on_no_file)

        return d

    def put(self, remote_path, file_object):
        """Upload a file to S3.

        :param unicode remote_path: key on which to store the content

        :param file_object: open file object containing the content

        :rtype: :class:`twisted.internet.defer.Deferred`
        """
        content = file_object.read()
        path = _safe_string(remote_path)

        def put(ignored):
            return self._s3.put_object(self._bucket, path, content)

        def create_retry(failure):
            failure.trap(Error)
            if str(failure.value.status) != "404":
                return failure
            d = self._s3.create_bucket(self._bucket)
            d.addCallback(put)
            return d

        d = put(None)
        d.addErrback(create_retry)
        return d
