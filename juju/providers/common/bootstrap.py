from cStringIO import StringIO

from juju.errors import EnvironmentNotFound, ProviderError

from .utils import log

_VERIFY_PATH = "bootstrap-verify"


class Bootstrap(object):
    """Generic bootstrap operation class."""

    def __init__(self, provider):
        self._provider = provider

    def run(self):
        """Get an existing zookeeper, or launch a new one.

        :return: a single-element list containing an appropriate
            :class:`juju.machine.ProviderMachine` instance, set up to run
            zookeeper and a provisioning agent.
        :rtype: :class:`twisted.internet.defer.Deferred`
        """
        machines = self._provider.get_zookeeper_machines()
        machines.addCallback(self._on_machines_found)
        machines.addErrback(self._on_no_machines_found)
        return machines

    def _on_machines_found(self, machines):
        log.info("juju environment previously bootstrapped.")
        return machines

    def _on_no_machines_found(self, failure):
        failure.trap(EnvironmentNotFound)
        d = self._verify_file_storage()
        d.addErrback(self._cannot_write)
        d.addCallback(self._launch_machine)
        return d

    def _verify_file_storage(self):
        log.debug("Verifying writable storage")
        storage = self._provider.get_file_storage()
        return storage.put(_VERIFY_PATH, StringIO("storage is writable"))

    def _cannot_write(self, failure):
        raise ProviderError(
            "Bootstrap aborted because file storage is not writable: %s"
            % str(failure.value))

    def _launch_machine(self, unused):
        log.debug("Launching juju bootstrap instance.")
        return self._provider.start_machine({"machine-id": "0"}, master=True)
