import json
import logging
import os
import tempfile
import urllib
import yaml

from twisted.internet.defer import fail, inlineCallbacks, returnValue, succeed
from twisted.web.client import downloadPage, getPage
from twisted.web.error import Error

from juju.charm.errors import MetaDataError, ServiceConfigError
from juju.charm.provider import get_charm_from_path
from juju.charm.url import CharmURL
from juju.errors import FileNotFound
from juju.lib import under

from .errors import CharmNotFound, CharmError, RepositoryNotFound

log = logging.getLogger("juju.charm")


def _makedirs(path):
    try:
        os.makedirs(path)
    except OSError:
        pass


def _cache_key(charm_url):
    charm_url.assert_revision()
    return under.quote("%s.charm" % charm_url)


class LocalCharmRepository(object):
    """Charm repository in a local directory."""

    def __init__(self, path):
        if path is None or not os.path.isdir(path):
            raise RepositoryNotFound(path)
        self.path = path

    def _collection(self, collection):
        path = os.path.join(self.path, collection.series)
        if not os.path.exists(path):
            return

        for dentry in os.listdir(path):
            dentry_path = os.path.join(path, dentry)
            try:
                yield get_charm_from_path(dentry_path)
            except FileNotFound:
                continue
            # There is a broken charm in the repo, but that
            # shouldn't stop us from continuing
            except yaml.YAMLError, e:
                # Log yaml errors for feedback to developers.
                log.warning("Charm %r has a YAML error: %s", dentry, e)
                continue
            except (ServiceConfigError, MetaDataError), e:
                # Log invalid config.yaml and metadata.yaml semantic errors
                log.warning("Charm %r has an error: %r %s", dentry, e, e)
                continue
            except CharmError:
                # This could just be a random directory/file in the repo
                continue

    def find(self, charm_url):
        """Find a charm with the given name.

        If multiple charms are found with different versions, the most
        recent one (greatest revision) will be returned.
        """
        assert charm_url.collection.schema == "local", "schema mismatch"
        latest = None
        for charm in self._collection(charm_url.collection):
            if charm.metadata.name == charm_url.name:
                if charm.get_revision() == charm_url.revision:
                    return succeed(charm)
                if (latest is None or
                    latest.get_revision() < charm.get_revision()):
                    latest = charm

        if latest is None or charm_url.revision is not None:
            return fail(CharmNotFound(self.path, charm_url))

        return succeed(latest)

    def latest(self, charm_url):
        d = self.find(charm_url.with_revision(None))
        d.addCallback(lambda c: c.get_revision())
        return d


class RemoteCharmRepository(object):

    cache_path = os.path.expanduser("~/.juju/cache")

    def __init__(self, url_base, cache_path=None):
        self.url_base = url_base
        if cache_path is not None:
            self.cache_path = cache_path

    @inlineCallbacks
    def _get_info(self, charm_url):
        charm_id = str(charm_url)
        url = "%s/charm-info?charms=%s" % (
            self.url_base, urllib.quote(charm_id))
        try:
            all_info = json.loads((yield getPage(url)))
            charm_info = all_info[charm_id]
            for warning in charm_info.get("warnings", []):
                log.warning("%s: %s", charm_id, warning)
            errors = charm_info.get("errors", [])
            if errors:
                raise CharmError(charm_id, "; ".join(errors))
            returnValue(charm_info)
        except Error:
            raise CharmNotFound(self.url_base, charm_url)

    @inlineCallbacks
    def _download(self, charm_url, cache_path):
        url = "%s/charm/%s" % (self.url_base, urllib.quote(charm_url.path))
        downloads = os.path.join(self.cache_path, "downloads")
        _makedirs(downloads)
        f = tempfile.NamedTemporaryFile(
            prefix=_cache_key(charm_url), suffix=".part", dir=downloads,
            delete=False)
        f.close()
        downloading_path = f.name
        try:
            yield downloadPage(url, downloading_path)
        except Error:
            raise CharmNotFound(self.url_base, charm_url)
        os.rename(downloading_path, cache_path)

    @inlineCallbacks
    def find(self, charm_url):
        info = yield self._get_info(charm_url)
        revision = info["revision"]
        if charm_url.revision is None:
            charm_url = charm_url.with_revision(revision)
        else:
            assert revision == charm_url.revision, "bad url revision"

        cache_path = os.path.join(self.cache_path, _cache_key(charm_url))
        cached = os.path.exists(cache_path)
        if not cached:
            yield self._download(charm_url, cache_path)
        charm = get_charm_from_path(cache_path)

        assert charm.get_revision() == revision, "bad charm revision"
        if charm.get_sha256() != info["sha256"]:
            os.remove(cache_path)
            name = "%s (%s)" % (
                charm_url, "cached" if cached else "downloaded")
            raise CharmError(name, "SHA256 mismatch")
        returnValue(charm)

    @inlineCallbacks
    def latest(self, charm_url):
        info = yield self._get_info(charm_url.with_revision(None))
        returnValue(info["revision"])


def resolve(vague_name, repository_path, default_series):
    """Get a Charm and associated identifying information

    :param str vague_name: a lazily specified charm name, suitable for use with
        :meth:`CharmURL.infer`

    :param repository_path: where on the local filesystem to find a repository
        (only currently meaningful when `charm_name` is specified with
        `"local:"`)
    :type repository_path: str or None

    :param str default_series: the Ubuntu series to insert when `charm_name` is
        inadequately specified.

    :return: a tuple of a :class:`juju.charm.url.CharmURL` and a
        :class:`juju.charm.base.CharmBase` subclass, which together contain
        both the charm's data and all information necessary to specify its
        source.
    """
    url = CharmURL.infer(vague_name, default_series)
    if url.collection.schema == "local":
        repo = LocalCharmRepository(repository_path)
    elif url.collection.schema == "cs":
        repo = RemoteCharmRepository("https://store.juju.ubuntu.com")
    return repo, url
