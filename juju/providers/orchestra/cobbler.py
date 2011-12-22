from base64 import b64encode
from xmlrpclib import Fault

from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.web.xmlrpc import Proxy

from juju.errors import MachinesNotFound, ProviderError


class CobblerCaller(object):
    """Handles the details of communicating with a Cobbler server"""

    def __init__(self, config):
        self._user = config["orchestra-user"]
        self._pass = config["orchestra-pass"]
        self._token = ""
        self._proxy = Proxy("http://%(orchestra-server)s/cobbler_api" % config)

    def _login(self):
        login = self.call("login", (self._user, self._pass))
        login.addCallback(self._set_token)

        def bad_credentials(failure):
            failure.trap(Fault)
            if "login failed" not in failure.getErrorMessage():
                return failure
            raise ProviderError("Cobbler server rejected credentials.")
        login.addErrback(bad_credentials)
        return login

    def _set_token(self, token):
        self._token = token

    def call(self, name, args=(), auth=False):

        def call():
            call_args = args
            if auth:
                call_args += (self._token,)
            return self._proxy.callRemote(name, *call_args)

        def login_retry(failure):
            # Login tokens expire after an hour: it seems more sensible
            # to assume we always have a valid one, and to relogin and
            # retry if it fails, than to try to maintain validity state.
            # NOTE: some methods, such as get_system_handle, expect
            # tokens but appear not to check validity.
            failure.trap(Fault)
            if "invalid token" not in failure.getErrorMessage():
                return failure
            login = self._login()
            login.addCallback(lambda unused: call())
            return login

        result = call()
        if auth:
            result.addErrback(login_retry)
        return result

    def check_call(self, name, args=(), auth=False, expect=None):

        def check(actual):
            if actual != expect:
                raise ProviderError(
                    "Bad result from call to %s with %s: got %r, expected %r"
                    % (name, args, actual, expect))
            return actual

        call = self.call(name, args, auth=auth)
        call.addCallback(check)
        return call


class CobblerClient(object):
    """Convenient interface to a Cobbler server"""

    def __init__(self, config):
        self._caller = CobblerCaller(config)
        self._acquired_class = config["acquired-mgmt-class"]
        self._available_class = config["available-mgmt-class"]

    def _get_name(self, instance_id):
        d = self._caller.call("find_system", ({"uid": instance_id},))

        def extract_name(systems):
            if len(systems) > 1:
                raise ProviderError(
                    "Got multiple names for machine %s: %s"
                    % (instance_id, ", ".join(systems)))
            if not systems:
                raise MachinesNotFound([instance_id])
            return systems[0]
        d.addCallback(extract_name)
        return d

    def _power_call(self, operation, names):
        # note: cobbler immediately returns something looking like a timestamp
        # that we don't know how to interpret; we can't tell if this fails.
        return self._caller.call("background_power_system",
                                 ({"power": operation, "systems": names},),
                                 auth=True)

    def _class_swapper(self, class_):
        if class_ == self._available_class:
            return self._acquired_class
        if class_ == self._acquired_class:
            return self._available_class
        return class_

    @inlineCallbacks
    def _get_available_system(self):
        names = yield self._caller.call(
            "find_system", ({"mgmt_classes": self._available_class,
                             "netboot_enabled": "true"},))
        if not names:
            raise ProviderError("Could not find any Cobbler systems marked as "
                                "available and configured for network boot.")

        # It's possible that some systems could be marked both available and
        # acquired, so we check each one for sanity (but only complain when
        # the problem becomes critical: ie, we can't find a system in a sane
        # state).
        inconsistent_instance_ids = []
        for name in names:
            info = yield self._caller.call("get_system", (name,))
            if info == "~":
                continue
            classes = info["mgmt_classes"]
            if self._acquired_class in classes:
                inconsistent_instance_ids.append(info["uid"])
                continue
            returnValue((info["uid"], map(self._class_swapper, classes)))
        raise ProviderError(
            "All available Cobbler systems were also marked as acquired "
            "(instances: %s)."
            % ", ".join(inconsistent_instance_ids))

    @inlineCallbacks
    def _update_system(self, instance_id, info):
        """Set an attribute on a Cobbler system.

        :param str instance_id: the Cobbler uid of the system

        :param dict info: names and desired values of system attributes to set

        :raises: :exc:`juju.errors.ProviderError` on invalid cobbler state
        :raises: :exc:`juju.errors.MachinesNotFound` when `instance_id` is
            not acquired
        """
        name = yield self._get_name(instance_id)
        try:
            handle = yield self._caller.call(
                "get_system_handle", (name,), auth=True)
        except Fault as e:
            if "unknown system name" in str(e):
                raise MachinesNotFound([instance_id])
            raise
        for (key, value) in sorted(info.items()):
            yield self._caller.check_call(
                "modify_system", (handle, key, value), auth=True, expect=True)
        yield self._caller.check_call(
            "save_system", (handle,), auth=True, expect=True)
        returnValue(name)

    @inlineCallbacks
    def describe_systems(self, *instance_ids):
        """Get all available information about systems.

        :param instance_ids: Cobbler uids of requested systems; leave blank to
            return information about all acquired systems.

        :return: a list of dictionaries describing acquired systems
        :rtype: :class:`twisted.internet.defer.Deferred`

        :raises: :exc:`juju.errors.MachinesNotFound` if any requested
            instance_id doesn't exist
        """
        all_systems = yield self._caller.call("get_systems")
        acquired_systems = [s for s in all_systems
                            if self._acquired_class in s["mgmt_classes"]]
        if not instance_ids:
            returnValue(acquired_systems)

        keyed_systems = dict(((system["uid"], system)
                              for system in acquired_systems))
        result_systems = []
        missing_instance_ids = []
        for instance_id in instance_ids:
            if instance_id in keyed_systems:
                result_systems.append(keyed_systems[instance_id])
            else:
                missing_instance_ids.append(instance_id)
        if missing_instance_ids:
            raise MachinesNotFound(missing_instance_ids)
        returnValue(result_systems)

    @inlineCallbacks
    def acquire_system(self):
        """Find a system marked as available and mark it as acquired.

        :return: the instance id (Cobbler uid) str of the acquired system.
        :rtype: :class:`twisted.internet.defer.Deferred`

        :raises: :exc:`juju.errors.ProviderError` if no suitable system can
            be found.
        """
        instance_id, new_classes = yield self._get_available_system()
        yield self._update_system(instance_id, {"mgmt_classes": new_classes})
        returnValue(instance_id)

    @inlineCallbacks
    def start_system(self, instance_id, machine_id, user_data):
        """Launch a cobbler system.

        :param str instance_id: The Cobbler uid of the desired system.

        :param str ks_meta: kickstart metadata with which to launch system.

        :return: dict of instance data
        :rtype: :class:`twisted.internet.defer.Deferred`
        """
        (system,) = yield self.describe_systems(instance_id)
        ks_meta = system["ks_meta"]
        ks_meta["MACHINE_ID"] = machine_id
        ks_meta["USER_DATA_BASE64"] = b64encode(user_data)
        name = yield self._update_system(instance_id, {
            "netboot_enabled": True, "ks_meta": ks_meta})
        yield self._power_call("on", [name])
        (info,) = yield self.describe_systems(instance_id)
        returnValue(info)

    @inlineCallbacks
    def shutdown_system(self, instance_id):
        """Take a system marked as acquired, and make it available again

        :rtype: :class:`twisted.internet.defer.Deferred`
        """
        (system,) = yield self.describe_systems(instance_id)
        ks_meta = system["ks_meta"]
        ks_meta.pop("MACHINE_ID", None)
        ks_meta.pop("USER_DATA_BASE64", None)
        new_classes = map(self._class_swapper, system["mgmt_classes"])
        name = yield self._update_system(
            instance_id, {"netboot_enabled": True,
                          "mgmt_classes": new_classes,
                          "ks_meta": ks_meta})
        yield self._power_call("off", [name])
        returnValue(True)
