from hashlib import md5
from urllib2 import parse_http_list, parse_keqv_list
from uuid import uuid4

from twisted.internet import reactor
from twisted.internet.ssl import ClientContextFactory
from twisted.python.failure import Failure
from twisted.web.client import HTTPClientFactory, HTTPPageGetter
from twisted.web.error import Error

from juju.errors import ProviderError


def _parse_auth_info(auth_info):
    method, info_str = auth_info.split(' ', 1)
    if method != "Digest":
        raise ProviderError("Unknown authentication method: %s" % method)
    items = parse_http_list(info_str)
    info = parse_keqv_list(items)

    try:
        qop = info["qop"]
        realm = info["realm"]
        nonce = info["nonce"]
    except KeyError as e:
        raise ProviderError(
            "Authentication request missing required key: %s" % e)
    algorithm = info.get("algorithm", "MD5")
    if algorithm != "MD5":
        raise ProviderError("Unsupported digest algorithm: %s" % algorithm)
    if "auth" not in qop.split(","):
        raise ProviderError("Unsupported quality-of-protection: %s" % qop)
    return realm, nonce, "auth", algorithm


def _digest_squish(*fields):
    return md5(":".join(fields)).hexdigest()


class DigestAuthenticator(object):

    def __init__(self, username, password):
        self._user = username
        self._pass = password
        self._nonce_count = 0

    def authenticate(self, method, url, auth_info):
        realm, nonce, qop, algorithm = _parse_auth_info(auth_info)
        ha1 = _digest_squish(self._user, realm, self._pass)
        ha2 = _digest_squish(method, url)
        cnonce = str(uuid4())
        self._nonce_count += 1
        nc = "%08x" % self._nonce_count
        response = _digest_squish(ha1, nonce, nc, cnonce, qop, ha2)
        return (
            'Digest username="%s", realm="%s", nonce="%s", uri="%s", '
            'algorithm="%s", response="%s", qop="%s", nc="%s", cnonce="%s"'
            % (self._user, realm, nonce, url, algorithm, response, qop,
               nc, cnonce))


def _connect(factory):
    if factory.scheme == 'https':
        reactor.connectSSL(
            factory.host, factory.port, factory, ClientContextFactory())
    else:
        reactor.connectTCP(factory.host, factory.port, factory)


class _AuthPageGetter(HTTPPageGetter):

    handleStatus_204 = lambda self: self.handleStatus_200()

    def handleStatus_401(self):
        if not self.factory.authenticated:
            (auth_info,) = self.headers["www-authenticate"]
            self.factory.authenticate(auth_info)
            _connect(self.factory)
        else:
            self.handleStatusDefault()
            self.factory.noPage(Failure(Error(self.status, self.message)))
        self.quietLoss = True
        self.transport.loseConnection()


class _AuthClientFactory(HTTPClientFactory):

    protocol = _AuthPageGetter
    authenticated = False

    def __init__(self, url, authenticator, **kwargs):
        HTTPClientFactory.__init__(self, url, **kwargs)
        self._authenticator = authenticator

    def authenticate(self, auth_info):
        self.headers["authorization"] = self._authenticator.authenticate(
            self.method, self.url, auth_info)
        self.authenticated = True


def get_page_auth(url, authenticator, method="GET", postdata=None):
    factory = _AuthClientFactory(
        url, authenticator, method=method, postdata=postdata)
    _connect(factory)
    return factory.deferred
