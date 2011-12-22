import logging

import zookeeper

from twisted.internet.defer import inlineCallbacks, fail, succeed
from twisted.internet import reactor

from txzookeeper.client import ZOO_OPEN_ACL_UNSAFE

from juju.agents.provision import ProvisioningAgent
from juju.environment.environment import Environment
from juju.environment.config import EnvironmentsConfig
from juju.environment.errors import EnvironmentsConfigError
from juju.environment.tests.test_config import SAMPLE_ENV

from juju.errors import ProviderInteractionError
from juju.lib.mocker import MATCH
from juju.providers.dummy import DummyMachine
from juju.state.errors import StopWatcher
from juju.state.machine import MachineState, MachineStateManager
from juju.state.tests.test_service import ServiceStateManagerTestBase

from .common import AgentTestBase


MATCH_MACHINE = MATCH(lambda x: isinstance(x, DummyMachine))
MATCH_MACHINE_STATE = MATCH(lambda x: isinstance(x, MachineState))
MATCH_SET = MATCH(lambda x: isinstance(x, set))


class ProvisioningTestBase(AgentTestBase):

    agent_class = ProvisioningAgent

    def get_serialized_environment(self):
        config = EnvironmentsConfig()
        config.parse(SAMPLE_ENV)
        return config.serialize("myfirstenv")


class ProvisioningAgentStartupTest(ProvisioningTestBase):

    @inlineCallbacks
    def setUp(self):
        yield super(ProvisioningAgentStartupTest, self).setUp()
        yield self.agent.connect()

    def test_agent_waits_for_environment(self):
        """
        When the agent starts it waits for the /environment node to exist.
        As soon as it does, the agent will fetch the environment, and
        deserialize it into an environment object.
        """
        env_loaded_deferred = self.agent.configure_environment()

        def verify_environment(result):
            self.assertTrue(isinstance(result, Environment))
            self.assertEqual(result.name, "myfirstenv")

        env_loaded_deferred.addCallback(verify_environment)

        def create_environment_node():
            self.assertFalse(env_loaded_deferred.called)
            return self.client.create(
                "/environment", self.get_serialized_environment())

        reactor.callLater(0.3, create_environment_node)
        return env_loaded_deferred

    @inlineCallbacks
    def test_agent_with_existing_environment(self):
        """An agent should load an existing environment to configure itself."""

        yield self.client.create(
            "/environment", self.get_serialized_environment())

        def verify_environment(result):
            self.assertTrue(isinstance(result, Environment))
            self.assertEqual(result.name, "myfirstenv")

        d = self.agent.configure_environment()
        d.addCallback(verify_environment)
        yield d

    @inlineCallbacks
    def test_agent_with_invalid_environment(self):
        yield self.client.create("/environment", "WAHOO!")
        d = self.agent.configure_environment()
        yield self.assertFailure(d, EnvironmentsConfigError)

    def test_agent_with_nonexistent_environment_created_concurrently(self):
        """
        If the environment node does not initially exist but it is created
        while the agent is processing the NoNodeException, it should detect
        this and configure normally.
        """
        data = self.get_serialized_environment()
        exists_and_watch = self.agent.client.exists_and_watch

        mock_client = self.mocker.patch(self.agent.client)
        mock_client.exists_and_watch("/environment")

        def inject_creation(path):
            zookeeper.create(
                self.agent.client.handle, path, data, [ZOO_OPEN_ACL_UNSAFE])
            return exists_and_watch(path)

        self.mocker.call(inject_creation)
        self.mocker.replay()

        def verify_configured(result):
            self.assertTrue(isinstance(result, Environment))
            self.assertEqual(result.type, "dummy")
        # mocker magic test
        d = self.agent.configure_environment()
        d.addCallback(verify_configured)
        return d


class ProvisioningAgentTest(ProvisioningTestBase):

    @inlineCallbacks
    def setUp(self):
        yield super(ProvisioningAgentTest, self).setUp()
        yield self.client.create(
            "/environment", self.get_serialized_environment())
        self.agent.set_watch_enabled(False)
        yield self.agent.startService()
        self.output = self.capture_logging("juju.agents.provision",
                                           logging.DEBUG)

    def test_get_agent_name(self):
        self.assertEqual(self.agent.get_agent_name(), "provision:dummy")

    @inlineCallbacks
    def test_watch_machine_changes_processes_new_machine_id(self):
        """The agent should process a new machine id by creating it"""
        manager = MachineStateManager(self.client)
        machine_state0 = yield manager.add_machine_state()
        machine_state1 = yield manager.add_machine_state()

        yield self.agent.watch_machine_changes(
            None, [machine_state0.id, machine_state1.id])

        self.assertIn(
            "Machines changed old:None new:[0, 1]", self.output.getvalue())
        self.assertIn("Starting machine id:0", self.output.getvalue())

        machines = yield self.agent.provider.get_machines()
        self.assertEquals(len(machines), 2)

        instance_id = yield machine_state0.get_instance_id()
        self.assertEqual(instance_id, 0)

        instance_id = yield machine_state1.get_instance_id()
        self.assertEqual(instance_id, 1)

    @inlineCallbacks
    def test_watch_machine_changes_ignores_running_machine(self):
        """
        If there is an existing machine instance and state, when a
        new machine state is added, the existing instance is preserved,
        and a new instance is created.
        """
        manager = MachineStateManager(self.client)
        machine_state0 = yield manager.add_machine_state()
        machines = yield self.agent.provider.start_machine(
            {"machine-id": machine_state0.id})
        machine = machines.pop()
        yield machine_state0.set_instance_id(machine.instance_id)

        machine_state1 = yield manager.add_machine_state()

        machines = yield self.agent.provider.get_machines()
        self.assertEquals(len(machines), 1)

        yield self.agent.watch_machine_changes(
            None, [machine_state0.id, machine_state1.id])

        machines = yield self.agent.provider.get_machines()
        self.assertEquals(len(machines), 2)

        instance_id = yield machine_state1.get_instance_id()
        self.assertEqual(instance_id, 1)

    @inlineCallbacks
    def test_watch_machine_changes_terminates_unused(self):
        """
        Any running provider machine instances without corresponding
        machine states are terminated.
        """
        # start an unused machine within the dummy provider instance
        yield self.agent.provider.start_machine({"machine-id": "machine-1"})
        yield self.agent.watch_machine_changes(None, [])
        self.assertIn("Shutting down machine id:0", self.output.getvalue())
        machines = yield self.agent.provider.get_machines()
        self.assertFalse(machines)

    @inlineCallbacks
    def test_watch_machine_changes_stop_watches(self):
        """Verify that the watches stops once the agent stops."""
        yield self.agent.start()
        yield self.agent.stop()
        yield self.assertFailure(
            self.agent.watch_machine_changes(None, []),
            StopWatcher)

    @inlineCallbacks
    def test_new_machine_state_removed_while_processing(self):
        """
        If the machine state is removed while the event is processing the
        state, the watch function should process it normally.
        """
        yield self.agent.watch_machine_changes(
            None, [0])
        machines = yield self.agent.provider.get_machines()
        self.assertEquals(len(machines), 0)

    @inlineCallbacks
    def test_process_machines_non_concurrency(self):
        """
        Process machines should only be executed serially by an
        agent.
        """
        manager = MachineStateManager(self.client)
        machine_state0 = yield manager.add_machine_state()
        machine_state1 = yield manager.add_machine_state()

        call_1 = self.agent.process_machines([machine_state0.id])

        # The second call should return immediately due to the
        # instance attribute guard.
        call_2 = self.agent.process_machines([machine_state1.id])
        self.assertEqual(call_2.called, True)
        self.assertEqual(call_2.result, False)

        # The first call should have started a provider machine
        yield call_1

        machines = yield self.agent.provider.get_machines()
        self.assertEquals(len(machines), 1)

        instance_id_0 = yield machine_state0.get_instance_id()
        self.assertEqual(instance_id_0, 0)

        instance_id_1 = yield machine_state1.get_instance_id()
        self.assertEqual(instance_id_1, None)

    def test_new_machine_state_removed_while_processing_get_provider_id(self):
        """
        If the machine state is removed while the event is processing the
        state, the watch function should process it normally.
        """
        yield self.agent.watch_machine_changes(
            None, [0])
        machines = yield self.agent.provider.get_machines()
        self.assertEquals(len(machines), 0)

    @inlineCallbacks
    def test_on_environment_change_agent_reconfigures(self):
        """
        If the environment changes the agent reconfigures itself
        """
        provider = self.agent.provider
        data = self.get_serialized_environment()
        yield self.client.set("/environment", data)
        yield self.sleep(0.2)
        self.assertNotIdentical(provider, self.agent.provider)

    @inlineCallbacks
    def test_machine_state_reflects_invalid_provider_state(self):
        """
        If a machine state has an invalid instance_id, it should be detected,
        and a new machine started and the machine state updated with the
        new instance_id.
        """
        machine_manager = MachineStateManager(self.client)
        m1 = yield machine_manager.add_machine_state()
        yield m1.set_instance_id("zebra")

        m2 = yield machine_manager.add_machine_state()
        yield self.agent.watch_machine_changes(None, [m1.id, m2.id])

        m1_instance_id = yield m1.get_instance_id()
        self.assertEqual(m1_instance_id, 0)

        m2_instance_id = yield m2.get_instance_id()
        self.assertEqual(m2_instance_id, 1)

    def test_periodic_task(self):
        """
        The agent schedules period checks that execute the process machines
        call.
        """
        mock_reactor = self.mocker.patch(reactor)
        mock_reactor.callLater(self.agent.machine_check_period,
                               self.agent.periodic_machine_check)
        mock_agent = self.mocker.patch(self.agent)
        mock_agent.process_machines(())
        self.mocker.result(succeed(None))
        self.mocker.replay()

        # mocker magic test
        self.agent.periodic_machine_check()

    @inlineCallbacks
    def test_transient_provider_error_on_start_machine(self):
        """
        If there's an error when processing changes, the agent should log
        the error and continue.
        """
        manager = MachineStateManager(self.client)
        machine_state0 = yield manager.add_machine_state()
        machine_state1 = yield manager.add_machine_state()

        mock_provider = self.mocker.patch(self.agent.provider)
        mock_provider.start_machine({"machine-id": 0})
        self.mocker.result(fail(ProviderInteractionError()))

        mock_provider.start_machine({"machine-id": 1})
        self.mocker.passthrough()
        self.mocker.replay()

        yield self.agent.watch_machine_changes(
            [], [machine_state0.id, machine_state1.id])

        machine1_instance_id = yield machine_state1.get_instance_id()
        self.assertEqual(machine1_instance_id, 0)
        self.assertIn(
            "Cannot process machine 0",
            self.output.getvalue())

    @inlineCallbacks
    def test_transient_provider_error_on_shutdown_machine(self):
        """
        A transient provider error on shutdown will be ignored
        and the shutdown will be reattempted (assuming similiar
        state conditions) on the next execution of process machines.
        """
        yield self.agent.provider.start_machine({"machine-id": 1})
        mock_provider = self.mocker.patch(self.agent.provider)

        mock_provider.shutdown_machine(MATCH_MACHINE)
        self.mocker.result(fail(ProviderInteractionError()))

        mock_provider.shutdown_machine(MATCH_MACHINE)
        self.mocker.passthrough()

        self.mocker.replay()
        try:
            yield self.agent.process_machines([])
        except:
            self.fail("Should not raise")

        machines = yield self.agent.provider.get_machines()
        self.assertTrue(machines)

        yield self.agent.process_machines([])
        machines = yield self.agent.provider.get_machines()
        self.assertFalse(machines)

        self.assertIn(
            "Cannot shutdown machine 0",
            self.output.getvalue())

    @inlineCallbacks
    def test_transient_provider_error_on_get_machines(self):
        manager = MachineStateManager(self.client)
        machine_state0 = yield manager.add_machine_state()

        mock_provider = self.mocker.patch(self.agent.provider)
        mock_provider.get_machines()
        self.mocker.result(fail(ProviderInteractionError()))

        mock_provider.get_machines()
        self.mocker.passthrough()

        self.mocker.replay()
        try:
            yield self.agent.process_machines([machine_state0.id])
        except:
            self.fail("Should not raise")

        instance_id = yield machine_state0.get_instance_id()
        self.assertEqual(instance_id, None)

        yield self.agent.process_machines(
            [machine_state0.id])

        instance_id = yield machine_state0.get_instance_id()
        self.assertEqual(instance_id, 0)
        self.assertIn(
            "Cannot get machine list",
            self.output.getvalue())

    @inlineCallbacks
    def test_transient_unhandled_error_in_process_machines(self):
        """Verify that watch_machine_changes handles the exception.

        Provider implementations may use libraries like txaws that do
        not handle every error. However, this should not stop the
        watch from re-establishing itself, as will be the case if the
        exception is not caught.
        """
        manager = MachineStateManager(self.client)
        machine_state0 = yield manager.add_machine_state()
        machine_state1 = yield manager.add_machine_state()

        # Simulate a failure scenario seen occasionally when working
        # with OpenStack and txaws
        mock_agent = self.mocker.patch(self.agent)

        # Simulate transient error
        mock_agent.process_machines([machine_state0.id])
        self.mocker.result(fail(
                TypeError("'NoneType' object is not iterable")))

        # Let it succeed on second try. In this case, the scenario is
        # that the watch triggered before the periodic_machine_check
        # was run again
        mock_agent.process_machines([machine_state0.id, machine_state1.id])
        self.mocker.passthrough()
        self.mocker.replay()

        # Verify that watch_machine_changes does not fail even in the case of
        # the transient error, although no work was done
        try:
            yield self.agent.watch_machine_changes([], [machine_state0.id])
        except:
            self.fail("Should not raise")

        instance_id = yield machine_state0.get_instance_id()
        self.assertEqual(instance_id, None)

        # Second attempt, verifiy it did in fact process the machine
        yield self.agent.watch_machine_changes(
            [machine_state0.id], [machine_state0.id, machine_state1.id])
        self.assertEqual((yield machine_state0.get_instance_id()), 0)
        self.assertEqual((yield machine_state1.get_instance_id()), 1)

        # But only after attempting and failing the first time
        self.assertIn(
            "Got unexpected exception in processing machines, will retry",
            self.output.getvalue())
        self.assertIn(
            "'NoneType' object is not iterable",
            self.output.getvalue())

    @inlineCallbacks
    def test_start_agent_with_watch(self):
        mock_reactor = self.mocker.patch(reactor)
        mock_reactor.callLater(
            self.agent.machine_check_period,
            self.agent.periodic_machine_check)
        self.mocker.replay()

        self.agent.set_watch_enabled(True)
        yield self.agent.start()

        manager = MachineStateManager(self.client)
        machine_state0 = yield manager.add_machine_state()
        exists_d, watch_d = self.client.exists_and_watch(
            "/machines/%s" % machine_state0.internal_id)
        yield exists_d
        # Wait for the provisioning agent to wake and modify
        # the machine id.
        yield watch_d
        instance_id = yield machine_state0.get_instance_id()
        self.assertEqual(instance_id, 0)


class FirewallManagerTest(
        ProvisioningTestBase, ServiceStateManagerTestBase):

    @inlineCallbacks
    def setUp(self):
        yield super(FirewallManagerTest, self).setUp()
        yield self.client.create(
            "/environment", self.get_serialized_environment())
        self.agent.set_watch_enabled(False)
        yield self.agent.startService()

    @inlineCallbacks
    def test_watch_service_changes_is_called(self):
        """Verify FirewallManager is called when services change"""
        from juju.state.firewall import FirewallManager
        mock_manager = self.mocker.patch(FirewallManager)

        seen = []

        def record_watch_changes(old_services, new_services):
            seen.append((old_services, new_services))
            return succeed(True)

        mock_manager.watch_service_changes(MATCH_SET, MATCH_SET)
        self.mocker.count(3, 3)
        self.mocker.call(record_watch_changes)

        mock_reactor = self.mocker.patch(reactor)
        mock_reactor.callLater(
            self.agent.machine_check_period,
            self.agent.periodic_machine_check)
        self.mocker.replay()

        self.agent.set_watch_enabled(True)
        yield self.agent.start()

        # Modify services, while subsequently poking to ensure service
        # watch is processed on each modification
        yield self.add_service("wordpress")
        yield self.poke_zk()
        mysql = yield self.add_service("mysql")
        yield self.poke_zk()
        yield self.service_state_manager.remove_service_state(mysql)
        yield self.poke_zk()

        self.assertEqual(
            seen,
            [(set(), set(["wordpress"])),
             (set(["wordpress"]), set(["mysql", "wordpress"])),
             (set(["mysql", "wordpress"]), set(["wordpress"]))])

    @inlineCallbacks
    def test_process_machine_is_called(self):
        """Verify FirewallManager is called when machines are processed"""
        from juju.state.firewall import FirewallManager
        mock_manager = self.mocker.patch(FirewallManager)

        seen = []

        def record_machine(machine):
            seen.append(machine)
            return succeed(True)

        mock_manager.process_machine(MATCH_MACHINE_STATE)
        self.mocker.call(record_machine)
        self.mocker.replay()

        machine_manager = MachineStateManager(self.client)
        machine_state = yield machine_manager.add_machine_state()
        yield self.agent.process_machines([machine_state.id])
        self.assertEqual(seen, [machine_state])
