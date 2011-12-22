from cStringIO import StringIO
import time

from twisted.internet.defer import succeed, fail
from twisted.web.error import Error
from txaws.s3.exception import S3Error

from juju.lib.mocker import MATCH
from juju.lib.testing import TestCase
from juju.errors import FileNotFound
from juju.providers.ec2.tests.common import EC2TestMixin


class FileStorageTestCase(EC2TestMixin, TestCase):

    def get_storage(self):
        provider = self.get_provider()
        storage = provider.get_file_storage()
        return storage

    def test_put_file(self):
        """
        A file can be put in the storage.
        """
        content = "blah blah"
        control_bucket = self.get_config()["control-bucket"]
        self.s3.put_object(
            control_bucket, "pirates/content.txt", content)
        self.mocker.result(succeed(""))
        self.mocker.replay()

        storage = self.get_storage()
        d = storage.put("pirates/content.txt", StringIO(content))

        def validate_result(result):
            self.assertIdentical(result, "")

        d.addCallback(validate_result)
        return d

    def test_put_file_unicode(self):
        """
        A file can be put in the storage with a unicode key, will
        be implicitly converted to a string. The reason for this
        conversion is that the txaws will raise an exception on
        unicode strings passed as keys.
        """
        content = "blah blah"
        control_bucket = self.get_config()["control-bucket"]
        self.s3.put_object(
            control_bucket,
            "\xe2\x99\xa3\xe2\x99\xa6\xe2\x99\xa5\xe2\x99\xa0.txt",
            content)
        self.mocker.result(succeed(""))
        self.mocker.replay()

        storage = self.get_storage()
        d = storage.put(u"\u2663\u2666\u2665\u2660.txt", StringIO(content))

        def validate_result(result):
            self.assertIdentical(result, "")

        d.addCallback(validate_result)
        return d

    def test_put_file_no_bucket(self):
        """The buket will be created if it doesn't exist yet"""
        content = "blah blah"
        control_bucket = self.get_config()["control-bucket"]
        self.s3.put_object(
            control_bucket, "pirates/content.txt", content)
        error = Error("404", "Not Found")
        self.mocker.result(fail(error))
        self.s3.create_bucket(control_bucket)
        self.mocker.result(succeed(None))
        self.s3.put_object(
            control_bucket, "pirates/content.txt", content)
        self.mocker.result(succeed(""))
        self.mocker.replay()

        storage = self.get_storage()
        d = storage.put(u"pirates/content.txt", StringIO(content))

        def validate_result(result):
            self.assertIdentical(result, "")

        d.addCallback(validate_result)
        return d

    def verify_strange_put_error(self, error):
        """Weird errors? don't even try"""
        content = "blah blah"
        control_bucket = self.get_config()["control-bucket"]
        self.s3.put_object(
            control_bucket, "pirates/content.txt", content)
        self.mocker.result(fail(error))
        self.mocker.replay()

        storage = self.get_storage()
        d = storage.put(u"pirates/content.txt", StringIO(content))
        self.assertFailure(d, type(error))
        return d

    def test_put_file_unknown_error(self):
        return self.verify_strange_put_error(Exception("cellosticks"))

    def test_get_url(self):
        """A url can be generated for any stored file."""
        self.mocker.reset()

        # Freeze time for the hmac comparison
        self.patch(time, "time", lambda: 1313469969.311376)

        storage = self.get_storage()
        url = storage.get_url("pirates/content.txt")
        self.assertTrue(url.startswith(
            "https://s3.amazonaws.com/moon/pirates/content.txt?"))
        params = url[url.index("?") + 1:].split("&")
        self.assertEqual(
            sorted(params),
            ["AWSAccessKeyId=0f62e973d5f8",
             "Expires=1628829969",
             "Signature=8A%2BF4sk48OmJ8xfPoOY7U0%2FacvM%3D"])

    def test_get_url_unicode(self):
        """A url can be generated for *any* stored file."""
        self.mocker.reset()

        # Freeze time for the hmac comparison
        self.patch(time, "time", lambda: 1315469969.311376)

        storage = self.get_storage()
        url = storage.get_url(u"\u2663\u2666\u2665\u2660.txt")
        self.assertTrue(url.startswith(
            "https://s3.amazonaws.com/moon/"
            "%E2%99%A3%E2%99%A6%E2%99%A5%E2%99%A0.txt"))
        params = url[url.index("?") + 1:].split("&")
        self.assertEqual(
            sorted(params),
            ["AWSAccessKeyId=0f62e973d5f8",
             "Expires=1630829969",
             "Signature=bbmdpkLqmrY4ebc2eoCJgt95ojg%3D"])

    def test_get_file(self):
        """Retrieving a file from storage returns a temporary file."""
        content = "blah blah"
        control_bucket = self.get_config()["control-bucket"]
        self.s3.get_object(
            control_bucket, "pirates/content.txt")
        self.mocker.result(succeed(content))
        self.mocker.replay()

        storage = self.get_storage()
        d = storage.get("pirates/content.txt")

        def validate_result(result):
            self.assertEqual(result.read(), content)

        d.addCallback(validate_result)
        return d

    def test_get_file_unicode(self):
        """Retrieving a file with a unicode object, will refetch with
        a utf8 interpretation."""
        content = "blah blah"
        control_bucket = self.get_config()["control-bucket"]

        def match_string(s):
            self.assertEqual(
                s, "\xe2\x99\xa3\xe2\x99\xa6\xe2\x99\xa5\xe2\x99\xa0.txt")
            self.assertFalse(isinstance(s, unicode))
            return True

        self.s3.get_object(control_bucket, MATCH(match_string))
        self.mocker.result(succeed(content))
        self.mocker.replay()

        storage = self.get_storage()
        d = storage.get(u"\u2663\u2666\u2665\u2660.txt")

        def validate_result(result):
            self.assertEqual(result.read(), content)

        d.addCallback(validate_result)
        return d

    def test_get_file_nonexistant(self):
        """Retrieving a nonexistant file raises a file not found error."""
        control_bucket = self.get_config()["control-bucket"]
        file_name = "pirates/ship.txt"
        error = Error("404", "Not Found")
        self.s3.get_object(control_bucket, file_name)
        self.mocker.result(fail(error))
        self.mocker.replay()

        storage = self.get_storage()
        d = storage.get(file_name)
        self.failUnlessFailure(d, FileNotFound)

        def validate_error_message(result):
            self.assertEqual(
                result.path, "s3://%s/%s" % (control_bucket, file_name))
        d.addCallback(validate_error_message)
        return d

    def test_get_file_error(self):
        """
        An unexpected error from s3 on file retrieval is exposed via the api.
        """
        control_bucket = self.get_config()["control-bucket"]
        file_name = "pirates/ship.txt"
        self.s3.get_object(
            control_bucket, file_name)
        self.mocker.result(fail(S3Error("<error/>", 503)))
        self.mocker.replay()

        storage = self.get_storage()
        d = storage.get(file_name)
        self.failUnlessFailure(d, S3Error)
        return d
