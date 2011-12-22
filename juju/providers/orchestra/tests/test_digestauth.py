import os

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from twisted.internet.ssl import DefaultOpenSSLContextFactory
from twisted.protocols.policies import WrappingFactory
from twisted.web.error import Error
from twisted.web.resource import Resource
from twisted.web.server import Site

from juju.errors import ProviderError
from juju.lib.testing import TestCase
from juju.providers.orchestra.digestauth import (
    DigestAuthenticator, get_page_auth)


def data_file(name):
    return os.path.join(os.path.dirname(__file__), "data", name)


class DigestAuthenticatorTest(TestCase):

    def setUp(self):
        super(DigestAuthenticatorTest, self).setUp()
        self.uuid4_m = self.mocker.replace("uuid.uuid4")
        self.auth = DigestAuthenticator("jiminy", "cricket")

    def challenge(self, method="Digest", **kwargs):
        kwargs.setdefault("qop", "auth")
        details = ", ".join("%s=%s" % item for item in kwargs.items())
        return ("%s realm=loathing, nonce=blah, %s" % (method, details))

    def default_response(self):
        return ('Digest username="jiminy", realm="loathing", nonce="blah", '
                'uri="http://somewhe.re/", algorithm="MD5", '
                'response="8891952040a96ba62cce585972bd3360", qop="auth", '
                'nc="00000001", cnonce="twiddle"')

    def assert_response(self, challenge, response):
        self.assertEquals(
            self.auth.authenticate("METH", "http://somewhe.re/", challenge),
            response)

    def assert_error(self, challenge, err_type, err_message):
        error = self.assertRaises(
            err_type,
            self.auth.authenticate, "METH", "http://somewhe.re/", challenge)
        self.assertEquals(str(error), err_message)

    def assert_missing_key(self, challenge, key):
        self.mocker.replay()
        message = "Authentication request missing required key: %r" % key
        self.assert_error(challenge, ProviderError, message)

    def test_normal(self):
        self.uuid4_m()
        self.mocker.result("twiddle")
        self.mocker.replay()
        self.assert_response(self.challenge(), self.default_response())

    def test_nc_increments(self):
        self.uuid4_m()
        self.mocker.result("twiddle")
        self.uuid4_m()
        self.mocker.result("twaddle")
        self.mocker.replay()
        self.assert_response(self.challenge(), self.default_response())
        self.assert_response(
            self.challenge(),
            'Digest username="jiminy", realm="loathing", nonce="blah", '
            'uri="http://somewhe.re/", algorithm="MD5", '
            'response="c12e9ec2e0569d896b2f26b91c0e74c3", qop="auth", '
            'nc="00000002", cnonce="twaddle"')

    def test_qop_choices(self):
        self.uuid4_m()
        self.mocker.result("twiddle")
        self.mocker.replay()
        self.assert_response(self.challenge(qop='"auth,auth-int"'),
                             self.default_response())

    def test_specify_algorithm(self):
        self.uuid4_m()
        self.mocker.result("twiddle")
        self.mocker.replay()
        self.assert_response(self.challenge(algorithm="MD5"),
                             self.default_response())

    def test_bad_method(self):
        self.mocker.replay()
        self.assert_error(self.challenge("Masticate"), ProviderError,
                          "Unknown authentication method: Masticate")

    def test_bad_algorithm(self):
        self.mocker.replay()
        self.assert_error(self.challenge(algorithm="ROT13"), ProviderError,
                          "Unsupported digest algorithm: ROT13")

    def test_bad_qop(self):
        self.mocker.replay()
        self.assert_error(self.challenge(qop="auth-int"), ProviderError,
                          "Unsupported quality-of-protection: auth-int")

    def test_missing_keys(self):
        self.assert_missing_key("Digest realm=x, nonce=y", "qop")
        self.assert_missing_key("Digest realm=x, qop=y", "nonce")
        self.assert_missing_key("Digest qop=x, nonce=y", "realm")

class PlainResource(Resource):

    def __init__(self, test, method, content, expect_content=None, status=200):
        Resource.__init__(self)
        self._test = test
        self._method = method
        self._content = content
        self._expect_content = expect_content
        self._status = status

    def render(self, request):
        self._test.assertEquals(request.method, self._method)
        if self._expect_content:
            self._test.assertEquals(
                request.content.read(), self._expect_content)
        request.setResponseCode(self._status)
        return self._content


class AuthResource(Resource):

    def __init__(self, test, method, content, challenge, check_response,
                 expect_content=None, status=200):
        Resource.__init__(self)
        self._test = test
        self._method = method
        self._content = content
        self._challenge = challenge
        self._check_response = check_response
        self._expect_content = expect_content
        self._status = status
        self._rendered = False

    def render(self, request):
        if self._expect_content:
            self._test.assertEquals(
                request.content.read(), self._expect_content)
        if not self._rendered:
            self._rendered = True
            request.setResponseCode(401)
            request.setHeader("www-authenticate", self._challenge)
            return ""
        else:
            self._check_response(request.getHeader("authorization"))
            request.setResponseCode(self._status)
            return self._content


class GetPageAuthTestCase(TestCase):

    scheme = "http"

    def _listen(self, site):
        if self.scheme == "http":
            return reactor.listenTCP(0, site, interface="127.0.0.1")
        elif self.scheme == "https":
            sslFactory = DefaultOpenSSLContextFactory(
                data_file("server.key"), data_file("server.crt"))
            return reactor.listenSSL(
                0, site, sslFactory, interface="127.0.0.1")
        else:
            self.fail("unknown scheme: %s" % self.scheme)

    def setUp(self):
        super(GetPageAuthTestCase, self).setUp()
        self.root = Resource()
        self.wrapper = WrappingFactory(Site(self.root, timeout=None))
        self.port = self._listen(self.wrapper)
        self.portno = self.port.getHost().port

    def tearDown(self):
        super(GetPageAuthTestCase, self).tearDown()
        if self.wrapper.protocols.keys():
            print "CONNECTION(S) ALIVE! buggy test?"
        return self.port.stopListening()

    def get_url(self, path):
        return "%s/%s" % (self.get_base_url(), path)

    def get_base_url(self):
        return "%s://127.0.0.1:%s" % (self.scheme, self.portno)

    def add_plain(self, path, method, content,
                  expect_content=None, status=200):
        self.root.putChild(path, PlainResource(
            self, method, content,
            expect_content=expect_content, status=status))

    def add_auth(self, path, method, content, challenge, check_response,
                 expect_content=None, status=200):
        self.root.putChild(path, AuthResource(
            self, method, content, challenge, check_response,
            expect_content=expect_content, status=status))


class GetPageAuthTestsMixin(object):

    def setup_mock(self):
        self.uuid4_m = self.mocker.replace("uuid.uuid4")

    @inlineCallbacks
    def test_404(self):
        # verify that usual mechanism is in place
        d = get_page_auth(self.get_url("missing"), None)
        error = yield self.assertFailure(d, Error)
        self.assertEquals(error.status, "404")

    def test_default_method(self):
        self.add_plain("blah", "GET", "cheese")
        d = get_page_auth(self.get_url("blah"), None)
        d.addCallback(self.assertEquals, "cheese")
        return d

    def test_other_method(self):
        self.add_plain("blob", "TWIDDLE", "pickle")
        d = get_page_auth(self.get_url("blob"), None, method="TWIDDLE")
        d.addCallback(self.assertEquals, "pickle")
        return d

    def test_authenticate(self):
        self.setup_mock()
        self.uuid4_m()
        self.mocker.result("baguette")
        self.mocker.replay()

        url = self.get_url("blip")
        auth = DigestAuthenticator("sandwich", "earl")

        def check(response):
            self.assertTrue(response.startswith(
                'Digest username="sandwich", realm="x", nonce="y", uri="%s"'
                % url))
            self.assertIn(
                'qop="auth", nc="00000001", cnonce="baguette"', response)

        self.add_auth(
            "blip", "PROD", "ham", "Digest realm=x, nonce=y, qop=auth", check)
        d = get_page_auth(url, auth, method="PROD")
        d.addCallback(self.assertEquals, "ham")
        return d

    def test_authenticate_postdata_201(self):
        self.setup_mock()
        self.uuid4_m()
        self.mocker.result("ciabatta")
        self.mocker.replay()

        url = self.get_url("blam")
        auth = DigestAuthenticator("pizza", "principessa")

        def check(response):
            self.assertTrue(response.startswith(
                'Digest username="pizza", realm="a", nonce="b", uri="%s"'
                % url))
            self.assertIn(
                'qop="auth", nc="00000001", cnonce="ciabatta"', response)

        self.add_auth(
            "blam", "FLING", "tomato", "Digest realm=a, nonce=b, qop=auth",
            check, expect_content="oven", status=201)
        d = get_page_auth(url, auth, method="FLING", postdata="oven")
        d.addCallback(self.assertEquals, "tomato")
        return d

    def test_authenticate_postdata_204(self):
        self.setup_mock()
        self.uuid4_m()
        self.mocker.result("focaccia")
        self.mocker.replay()

        url = self.get_url("blur")
        auth = DigestAuthenticator("quesadilla", "king")

        def check(response):
            self.assertTrue(response.startswith(
                'Digest username="quesadilla", realm="p", nonce="q", uri="%s"'
                % url))
            self.assertIn(
                'qop="auth", nc="00000001", cnonce="focaccia"', response)

        self.add_auth(
            "blur", "HURL", "", "Digest realm=p, nonce=q, qop=auth", check,
            expect_content="bbq", status=204)
        d = get_page_auth(url, auth, method="HURL", postdata="bbq")
        d.addCallback(self.assertEquals, "")
        return d


class GetPageAuthHttpTest(GetPageAuthTestCase, GetPageAuthTestsMixin):
   scheme = "http"


class GetPageAuthHttpsTest(GetPageAuthTestCase, GetPageAuthTestsMixin):
   scheme = "https"
