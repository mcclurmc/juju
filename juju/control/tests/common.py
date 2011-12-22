from twisted.internet import reactor
from twisted.internet.defer import Deferred, inlineCallbacks

from juju.environment.tests.test_config import EnvironmentsConfigTestBase
from juju.charm.tests.test_repository import RepositoryTestBase
from juju.state.tests.test_service import ServiceStateManagerTestBase


class ControlToolTest(EnvironmentsConfigTestBase):

    def setup_cli_reactor(self):
        """Mock mock out reactor start and stop.

        This is necessary when executing the CLI via tests since
        commands will run a reactor as part of their execution, then
        shut it down. Obviously this would cause issues with running
        multiple tests under Twisted Trial.

        Returns a a `Deferred` that a test can wait on until the
        reactor is mocked stopped. This means that code running in the
        context of a mock reactor run is in fact complete, and
        assertions and tearDown can now be done.
        """
        mock_reactor = self.mocker.patch(reactor)
        mock_reactor.run()
        mock_reactor.stop()
        wait_on_stopped = Deferred()

        def f():
            wait_on_stopped.callback("reactor has stopped")
        self.mocker.call(f)
        reactor.running = True
        return wait_on_stopped

    def setUp(self):
        self.log = self.capture_logging()
        return super(ControlToolTest, self).setUp()

    def setup_exit(self, code=0):
        mock_exit = self.mocker.replace("sys.exit")
        mock_exit(code)


class MachineControlToolTest(
    ServiceStateManagerTestBase, ControlToolTest, RepositoryTestBase):

    @inlineCallbacks
    def setUp(self):
        yield super(MachineControlToolTest, self).setUp()
        # Dummy out the construction of our root machine (id=0), this
        # will go away in a later release. Right now, there's no
        # service unit holding it, so we have to special case.
        yield self.machine_state_manager.add_machine_state()

    @inlineCallbacks
    def destroy_service(self, service_name):
        """Destroys the service equivalently to destroy-service subcommand."""
        service_state = yield self.service_state_manager.get_service_state(
            service_name)
        yield self.service_state_manager.remove_service_state(service_state)
