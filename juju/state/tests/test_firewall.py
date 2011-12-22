import logging

from twisted.internet.defer import (
    Deferred, inlineCallbacks, fail, returnValue, succeed)

from juju.environment.config import EnvironmentsConfig
from juju.errors import ProviderInteractionError
from juju.lib.mocker import MATCH
from juju.providers.dummy import DummyMachine, MachineProvider
from juju.state.errors import StopWatcher
from juju.state.firewall import FirewallManager
from juju.state.machine import MachineStateManager
from juju.state.service import ServiceStateManager
from juju.state.tests.test_service import ServiceStateManagerTestBase


MATCH_MACHINE = MATCH(lambda x: isinstance(x, DummyMachine))

SAMPLE_ENV = """\
environments:
  myfirstenv:
    type: dummy
    foo: bar
    storage-directory: %s
"""


class FirewallTestBase(ServiceStateManagerTestBase):

    @inlineCallbacks
    def setUp(self):
        yield super(FirewallTestBase, self).setUp()
        self._running = True
        self.environment = yield self.get_test_environment()
        self.provider = self.environment.get_machine_provider()
        self.firewall_manager = FirewallManager(self.client, self.is_running, self.provider)
        self.service_state_manager = ServiceStateManager(self.client)
        self.output = self.capture_logging(level=logging.DEBUG)

    def get_test_environment_config(self):
        sample_config = SAMPLE_ENV % self.makeDir()
        config = EnvironmentsConfig()
        config.parse(sample_config)
        return config

    def get_test_environment(self):
        return self.get_test_environment_config().get_default()

    # The following methods are used to provide the scaffolding given
    # by the provisioning agent, which normally runs the FirewallManager

    def is_running(self):
        return self._running

    def start(self):
        self.service_state_manager.watch_service_states(
            self.firewall_manager.watch_service_changes)

    def stop(self):
        self._running = False

    @inlineCallbacks
    def provide_machine(self, machine_state):
        machines = yield self.provider.start_machine(
            {"machine-id": machine_state.id})
        instance_id = machines[0].instance_id
        yield machine_state.set_instance_id(instance_id)

    def wait_on_expected_units(self, expected):
        """Returns deferred for waiting on `expected` unit names.

        These unit names may require the firewall to have ports opened
        and/or closed.
        """
        condition_met = Deferred()
        seen = set()

        def observer(unit_state):
            unit_name = unit_state.unit_name
            seen.add(unit_name)
            if seen >= expected:
                # Call the callback just once, since it is possible
                # for this condition to be satisfied multiple times in
                # using tests because of background activity
                if not condition_met.called:
                    condition_met.callback(True)
            return succeed(True)

        self.firewall_manager.add_open_close_ports_observer(observer)
        return condition_met

    def wait_on_expected_machines(self, expected):
        """Returns deferred for waiting on `expected` machine IDs.

        These machines may require the firewall to have ports opened
        and/or closed.
        """
        condition_met = Deferred()
        seen = set()

        def observer(machine_id):
            seen.add(machine_id)
            if seen >= expected:
                # Call the callback just once, since it is possible
                # for this condition to be satisfied multiple times in
                # using tests because of background activity
                if not condition_met.called:
                    condition_met.callback(True)
            return succeed(True)

        self.firewall_manager.add_open_close_ports_on_machine_observer(
            observer)
        return condition_met


class FirewallServiceTest(FirewallTestBase):

    @inlineCallbacks
    def test_service_exposed_flag_changes(self):
        """Verify that a service unit is checked whenever a change
        occurs such that ports may need to be opened and/or closed
        for the machine corresponding to a given service unit.
        """
        self.start()
        expected_units = self.wait_on_expected_units(
            set(["wordpress/0"]))
        wordpress = yield self.add_service("wordpress")
        yield wordpress.add_unit_state()
        yield wordpress.set_exposed_flag()
        self.assertTrue((yield expected_units))

        # Then clear the flag, see that it triggers on the expected units
        expected_units = self.wait_on_expected_units(
            set(["wordpress/0"]))
        yield wordpress.clear_exposed_flag()
        self.assertTrue((yield expected_units))

        # Re-expose wordpress: set the flag again, verify that it
        # triggers on the expected units
        expected_units = self.wait_on_expected_units(
            set(["wordpress/0"]))
        yield wordpress.set_exposed_flag()
        self.assertTrue((yield expected_units))
        self.stop()

    @inlineCallbacks
    def test_add_remove_service_units_for_exposed_service(self):
        """Verify that adding/removing service units for an exposed
        service triggers the appropriate firewall management of
        opening/closing ports on the machines for the corresponding
        service units.
        """
        self.start()
        wordpress = yield self.add_service("wordpress")
        yield wordpress.set_exposed_flag()

        # Adding service units to this exposed service will trigger
        expected_units = self.wait_on_expected_units(
            set(["wordpress/0", "wordpress/1"]))
        wordpress_0 = yield wordpress.add_unit_state()
        yield wordpress.add_unit_state()
        self.assertTrue((yield expected_units))

        # Removing service units will also trigger
        expected_units = self.wait_on_expected_units(
            set(["wordpress/2"]))
        yield wordpress.remove_unit_state(wordpress_0)
        yield wordpress.add_unit_state()
        self.assertTrue((yield expected_units))
        self.stop()

    @inlineCallbacks
    def test_open_close_ports(self):
        """Verify that opening/closing ports triggers the appropriate
        firewall management for the corresponding service units.
        """
        self.start()
        expected_units = self.wait_on_expected_units(
            set(["wordpress/0"]))
        wordpress = yield self.add_service("wordpress")
        yield wordpress.set_exposed_flag()
        wordpress_0 = yield wordpress.add_unit_state()
        wordpress_1 = yield wordpress.add_unit_state()
        yield wordpress.add_unit_state()
        yield wordpress_0.open_port(443, "tcp")
        yield wordpress_0.open_port(80, "tcp")
        yield wordpress_0.close_port(443, "tcp")
        self.assertTrue((yield expected_units))

        expected_units = self.wait_on_expected_units(
            set(["wordpress/1", "wordpress/3"]))
        wordpress_3 = yield wordpress.add_unit_state()
        yield wordpress_1.open_port(53, "udp")
        yield wordpress_3.open_port(80, "tcp")
        self.assertTrue((yield expected_units))

        expected_units = self.wait_on_expected_units(
            set(["wordpress/0", "wordpress/1", "wordpress/3"]))
        yield wordpress.clear_exposed_flag()
        self.assertTrue((yield expected_units))
        self.stop()

    @inlineCallbacks
    def test_remove_service_state(self):
        """Verify that firewall mgmt for corresponding service units
        is triggered upon the service's removal.
        """
        self.start()
        expected_units = self.wait_on_expected_units(
            set(["wordpress/0", "wordpress/1"]))
        wordpress = yield self.add_service("wordpress")
        yield wordpress.add_unit_state()
        yield wordpress.add_unit_state()
        yield wordpress.set_exposed_flag()
        self.assertTrue((yield expected_units))

        # Do not clear the exposed flag prior to removal, triggering
        # should still occur as expected
        yield self.service_state_manager.remove_service_state(wordpress)
        self.stop()

    @inlineCallbacks
    def test_port_mgmt_for_unexposed_service_is_a_nop(self):
        """Verify that activity on an unexposed service does NOT
        trigger firewall mgmt for the corresponding service unit."""
        self.start()
        expected_units = self.wait_on_expected_units(
            set(["not-called"]))
        wordpress = yield self.add_service("wordpress")
        wordpress_0 = yield wordpress.add_unit_state()
        yield wordpress_0.open_port(53, "tcp")
        # The observer should not be called in this case
        self.assertFalse(expected_units.called)
        self.stop()

    @inlineCallbacks
    def test_provisioning_agent_restart(self):
        """Verify that firewall management is correct if the agent restarts.

        In particular, this test verifies that all state relevant for
        firewall management is stored in ZK and not in the agent
        itself.
        """
        # Store into ZK relevant state, this might have been observed
        # in a scenario in which the agent has previously been
        # running.
        wordpress = yield self.add_service("wordpress")
        wordpress_0 = yield wordpress.add_unit_state()
        wordpress_1 = yield wordpress.add_unit_state()
        yield wordpress_1.open_port(443, "tcp")
        yield wordpress_1.open_port(80, "tcp")
        yield wordpress.set_exposed_flag()

        # Now simulate agent start
        self.start()

        # Verify the expected service units are observed as needing
        # firewall mgmt
        expected_units = self.wait_on_expected_units(
            set(["wordpress/0", "wordpress/1"]))
        yield wordpress_0.open_port(53, "udp")
        yield wordpress_1.close_port(443, "tcp")
        self.assertTrue((yield expected_units))

        # Also verify that opening/closing ports work as expected
        expected_units = self.wait_on_expected_units(
            set(["wordpress/1"]))
        yield wordpress_1.close_port(80, "tcp")

        expected_units = self.wait_on_expected_units(
            set(["wordpress/0", "wordpress/1"]))
        yield wordpress.clear_exposed_flag()
        self.assertTrue((yield expected_units))
        self.stop()


class FirewallMachineTest(FirewallTestBase):

    @inlineCallbacks
    def get_provider_ports(self, machine):
        instance_id = yield machine.get_instance_id()
        machine_provider = yield self.provider.get_machine(instance_id)
        provider_ports = yield self.provider.get_opened_ports(
            machine_provider, machine.id)
        returnValue(provider_ports)

    def test_open_close_ports_on_machine(self):
        """Verify opening/closing ports on a machine works properly.

        In particular this is done without watch support."""
        manager = MachineStateManager(self.client)
        machine = yield manager.add_machine_state()
        yield self.firewall_manager.process_machine(machine)

        # Expose a service
        wordpress = yield self.add_service("wordpress")
        yield wordpress.set_exposed_flag()
        wordpress_0 = yield wordpress.add_unit_state()
        yield wordpress_0.open_port(80, "tcp")
        yield wordpress_0.open_port(443, "tcp")
        yield wordpress_0.assign_to_machine(machine)
        yield self.firewall_manager.open_close_ports_on_machine(machine.id)
        self.assertEqual((yield self.get_provider_ports(machine)),
                         set([(80, "tcp"), (443, "tcp")]))
        self.assertIn("Opened 80/tcp on provider machine 0",
                      self.output.getvalue())
        self.assertIn("Opened 443/tcp on provider machine 0",
                      self.output.getvalue())

        # Now change port setup
        yield wordpress_0.open_port(8080, "tcp")
        yield wordpress_0.close_port(443, "tcp")
        yield self.firewall_manager.open_close_ports_on_machine(machine.id)
        self.assertEqual((yield self.get_provider_ports(machine)),
                         set([(80, "tcp"), (8080, "tcp")]))
        self.assertIn("Opened 8080/tcp on provider machine 0",
                      self.output.getvalue())
        self.assertIn("Closed 443/tcp on provider machine 0",
                      self.output.getvalue())

    @inlineCallbacks
    def test_open_close_ports_on_unassigned_machine(self):
        """Verify corner case that nothing happens on an unassigned machine."""
        manager = MachineStateManager(self.client)
        machine = yield manager.add_machine_state()
        yield self.provide_machine(machine)
        yield self.firewall_manager.process_machine(machine)
        yield self.firewall_manager.open_close_ports_on_machine(machine.id)
        self.assertEqual((yield self.get_provider_ports(machine)),
                         set())

    @inlineCallbacks
    def test_open_close_ports_on_machine_unexposed_service(self):
        """Verify opening/closing ports on a machine works properly.

        In particular this is done without watch support."""
        manager = MachineStateManager(self.client)
        machine = yield manager.add_machine_state()
        yield self.provide_machine(machine)
        wordpress = yield self.add_service("wordpress")
        wordpress_0 = yield wordpress.add_unit_state()

        # Port activity, but service is not exposed
        yield wordpress_0.open_port(80, "tcp")
        yield wordpress_0.open_port(443, "tcp")
        yield wordpress_0.assign_to_machine(machine)
        yield self.firewall_manager.open_close_ports_on_machine(machine.id)
        self.assertEqual((yield self.get_provider_ports(machine)),
                         set())

        # Now expose it
        yield wordpress.set_exposed_flag()
        yield self.firewall_manager.open_close_ports_on_machine(machine.id)
        self.assertEqual((yield self.get_provider_ports(machine)),
                         set([(80, "tcp"), (443, "tcp")]))

    @inlineCallbacks
    def test_open_close_ports_on_machine_not_yet_provided(self):
        """Verify that opening/closing ports will eventually succeed
        once a machine is provided.
        """
        manager = MachineStateManager(self.client)
        machine = yield manager.add_machine_state()
        wordpress = yield self.add_service("wordpress")
        yield wordpress.set_exposed_flag()
        wordpress_0 = yield wordpress.add_unit_state()
        yield wordpress_0.open_port(80, "tcp")
        yield wordpress_0.open_port(443, "tcp")
        yield wordpress_0.assign_to_machine(machine)

        # First attempt to open ports quietly fails (except for
        # logging) because the machine has not yet been provisioned
        yield self.firewall_manager.open_close_ports_on_machine(machine.id)
        self.assertIn("No provisioned machine for machine 0",
                      self.output.getvalue())

        yield self.provide_machine(machine)
        # Machine is now provisioned (normally visible in the
        # provisioning agent through periodic rescan and corresponding
        # watches)
        yield self.firewall_manager.open_close_ports_on_machine(machine.id)
        self.assertEqual((yield self.get_provider_ports(machine)),
                         set([(80, "tcp"), (443, "tcp")]))

    @inlineCallbacks
    def test_open_close_ports_in_stopped_agent_stops_watch(self):
        """Verify code called by watches properly stops when agent stops."""
        self.stop()
        yield self.assertFailure(
            self.firewall_manager.open_close_ports_on_machine(0),
            StopWatcher)

    @inlineCallbacks
    def test_watches_trigger_port_mgmt(self):
        """Verify that watches properly trigger firewall management
        for the corresponding service units on the corresponding
        machines.
        """
        self.start()
        manager = MachineStateManager(self.client)

        # Immediately expose
        drupal = yield self.add_service("drupal")
        wordpress = yield self.add_service("wordpress")
        yield drupal.set_exposed_flag()
        yield wordpress.set_exposed_flag()

        # Then add these units
        drupal_0 = yield drupal.add_unit_state()
        wordpress_0 = yield wordpress.add_unit_state()
        wordpress_1 = yield wordpress.add_unit_state()
        wordpress_2 = yield wordpress.add_unit_state()

        # Assign some machines; in particular verify that multiple
        # service units on one machine works properly with opening
        # firewall
        machine_0 = yield manager.add_machine_state()
        machine_1 = yield manager.add_machine_state()
        machine_2 = yield manager.add_machine_state()
        yield self.provide_machine(machine_0)
        yield self.provide_machine(machine_1)
        yield self.provide_machine(machine_2)

        yield drupal_0.assign_to_machine(machine_0)
        yield wordpress_0.assign_to_machine(machine_0)
        yield wordpress_1.assign_to_machine(machine_1)
        yield wordpress_2.assign_to_machine(machine_2)

        # Simulate service units opening ports
        expected_machines = self.wait_on_expected_machines(set([0, 1]))
        expected_units = self.wait_on_expected_units(
            set(["wordpress/0", "wordpress/1", "drupal/0"]))
        yield drupal_0.open_port(8080, "tcp")
        yield drupal_0.open_port(443, "tcp")
        yield wordpress_0.open_port(80, "tcp")
        yield wordpress_1.open_port(80, "tcp")
        self.assertTrue((yield expected_units))
        self.assertTrue((yield expected_machines))
        self.assertEqual((yield self.get_provider_ports(machine_0)),
                         set([(80, "tcp"), (443, "tcp"), (8080, "tcp")]))
        self.assertEqual((yield self.get_provider_ports(machine_1)),
                         set([(80, "tcp")]))

        # Simulate service units close port
        expected_machines = self.wait_on_expected_machines(set([1, 2]))
        yield wordpress_1.close_port(80, "tcp")
        yield wordpress_2.open_port(80, "tcp")
        self.assertTrue((yield expected_machines))
        self.assertEqual((yield self.get_provider_ports(machine_1)), set())

        # Simulate service units open port
        expected_machines = self.wait_on_expected_machines(set([0]))
        yield wordpress_0.open_port(53, "udp")
        self.assertTrue((yield expected_machines))
        self.assertEqual((yield self.get_provider_ports(machine_0)),
                         set([(53, "udp"), (80, "tcp"),
                              (443, "tcp"), (8080, "tcp")]))
        self.stop()

    @inlineCallbacks
    def test_late_expose_properly_triggers(self):
        """Verify that an expose flag properly cascades the
        corresponding watches to perform the desired firewall mgmt.
        """
        self.start()
        manager = MachineStateManager(self.client)
        drupal = yield self.add_service("drupal")
        wordpress = yield self.add_service("wordpress")

        # Then add these units
        drupal_0 = yield drupal.add_unit_state()
        wordpress_0 = yield wordpress.add_unit_state()
        wordpress_1 = yield wordpress.add_unit_state()

        machine_0 = yield manager.add_machine_state()
        machine_1 = yield manager.add_machine_state()
        yield self.provide_machine(machine_0)
        yield self.provide_machine(machine_1)

        yield drupal_0.assign_to_machine(machine_0)
        yield wordpress_0.assign_to_machine(machine_0)
        yield wordpress_1.assign_to_machine(machine_1)

        # Simulate service units opening ports
        expected_machines = self.wait_on_expected_machines(set([0, 1]))
        expected_units = self.wait_on_expected_units(
            set(["wordpress/0", "wordpress/1"]))
        yield drupal_0.open_port(8080, "tcp")
        yield drupal_0.open_port(443, "tcp")
        yield wordpress_0.open_port(80, "tcp")
        yield wordpress_1.open_port(80, "tcp")
        yield wordpress.set_exposed_flag()
        self.assertTrue((yield expected_units))
        self.assertTrue((yield expected_machines))
        self.assertEqual((yield self.get_provider_ports(machine_0)),
                         set([(80, "tcp")]))
        self.assertEqual((yield self.get_provider_ports(machine_1)),
                         set([(80, "tcp")]))

        # Expose drupal service, verify ports are opened on provider
        expected_machines = self.wait_on_expected_machines(set([0]))
        expected_units = self.wait_on_expected_units(set(["drupal/0"]))
        yield drupal.set_exposed_flag()
        self.assertTrue((yield expected_machines))
        self.assertTrue((yield expected_units))
        self.assertEqual((yield self.get_provider_ports(machine_0)),
                         set([(80, "tcp"), (443, "tcp"), (8080, "tcp")]))

        # Unexpose drupal service, verify only wordpress ports are now opened
        expected_machines = self.wait_on_expected_machines(set([0]))
        expected_units = self.wait_on_expected_units(set(["drupal/0"]))
        yield drupal.clear_exposed_flag()
        self.assertTrue((yield expected_machines))
        self.assertTrue((yield expected_units))
        self.assertEqual((yield self.get_provider_ports(machine_0)),
                         set([(80, "tcp")]))

        # Re-expose drupal service, verify ports are once again opened
        expected_machines = self.wait_on_expected_machines(set([0]))
        expected_units = self.wait_on_expected_units(set(["drupal/0"]))
        yield drupal.set_exposed_flag()
        self.assertTrue((yield expected_machines))
        self.assertTrue((yield expected_units))
        self.assertEqual((yield self.get_provider_ports(machine_0)),
                         set([(80, "tcp"), (443, "tcp"), (8080, "tcp")]))
        self.stop()

    @inlineCallbacks
    def test_open_close_ports_on_machine_will_retry(self):
        """Verify port mgmt for a machine will retry if there's a failure."""
        mock_provider = self.mocker.patch(MachineProvider)
        mock_provider.open_port(MATCH_MACHINE, 0, 80, "tcp")
        self.mocker.result(fail(
                TypeError("'NoneType' object is not iterable")))
        mock_provider.open_port(MATCH_MACHINE, 0, 80, "tcp")
        self.mocker.result(fail(
                ProviderInteractionError("Some sort of EC2 problem")))
        mock_provider.open_port(MATCH_MACHINE, 0, 80, "tcp")
        self.mocker.passthrough()
        self.mocker.replay()

        manager = MachineStateManager(self.client)
        machine = yield manager.add_machine_state()
        yield self.provide_machine(machine)

        # Expose a service and attempt to open/close ports. The first
        # attempt will see the simulated failure.
        wordpress = yield self.add_service("wordpress")
        yield wordpress.set_exposed_flag()
        wordpress_0 = yield wordpress.add_unit_state()
        yield wordpress_0.assign_to_machine(machine)
        yield self.firewall_manager.process_machine(machine)

        yield wordpress_0.open_port(80, "tcp")
        yield self.firewall_manager.open_close_ports_on_machine(machine.id)
        self.assertEqual((yield self.get_provider_ports(machine)),
                         set())
        self.assertIn(
            "Got exception in opening/closing ports, will retry",
            self.output.getvalue())
        self.assertIn("TypeError: 'NoneType' object is not iterable",
                      self.output.getvalue())

        # Retries will now happen in the periodic recheck. First one
        # still fails due to simulated error.
        yield self.firewall_manager.process_machine(machine)
        self.assertEqual((yield self.get_provider_ports(machine)),
                         set())
        self.assertIn("ProviderInteractionError: Some sort of EC2 problem",
                      self.output.getvalue())

        # Third time is the charm in the mock setup, the recheck succeeds
        yield self.firewall_manager.process_machine(machine)
        self.assertEqual((yield self.get_provider_ports(machine)),
                         set([(80, "tcp")]))
        self.assertIn("Opened 80/tcp on provider machine 0",
                      self.output.getvalue())

    @inlineCallbacks
    def test_process_machine_ignores_stop_watcher(self):
        """Verify that process machine catches `StopWatcher`.

        `process_machine` calls `open_close_ports_on_machine`, which
        as verified in an earlier test, raises a `StopWatcher`
        exception to shutdown watches that use it in the event of
        agent shutdown. Verify this dual usage does not cause issues
        while the agent is being stopped for this usage.
        """
        mock_provider = self.mocker.patch(MachineProvider)
        mock_provider.open_port(MATCH_MACHINE, 0, 80, "tcp")
        self.mocker.result(fail(
                TypeError("'NoneType' object is not iterable")))
        self.mocker.replay()

        manager = MachineStateManager(self.client)
        machine = yield manager.add_machine_state()
        yield self.provide_machine(machine)

        # Expose a service and attempt to open/close ports. The first
        # attempt will see the simulated failure.
        wordpress = yield self.add_service("wordpress")
        yield wordpress.set_exposed_flag()
        wordpress_0 = yield wordpress.add_unit_state()
        yield wordpress_0.assign_to_machine(machine)
        yield self.firewall_manager.process_machine(machine)

        yield wordpress_0.open_port(80, "tcp")
        yield self.firewall_manager.open_close_ports_on_machine(machine.id)
        self.assertEqual((yield self.get_provider_ports(machine)),
                         set())
        self.assertIn(
            "Got exception in opening/closing ports, will retry",
            self.output.getvalue())
        self.assertIn("TypeError: 'NoneType' object is not iterable",
                      self.output.getvalue())

        # Stop the provisioning agent
        self.stop()

        # But retries can potentially still happening anyway, just
        # make certain nothing bad happens.
        yield self.firewall_manager.process_machine(machine)


class Observer(object):

    def __init__(self, calls, name):
        self.calls = calls
        self.name = name

    def __call__(self, obj):
        self.calls[0].add((self.name, obj))


class FirewallObserversTest(FirewallTestBase):

    @inlineCallbacks
    def test_observe_open_close_ports(self):
        """Verify one or more observers can be established on action."""
        wordpress = yield self.add_service("wordpress")
        wordpress_0 = yield wordpress.add_unit_state()
        yield wordpress.set_exposed_flag()

        # Add one observer, verify it gets called
        calls = [set()]
        self.firewall_manager.add_open_close_ports_observer(
            Observer(calls, "a"))
        yield self.firewall_manager.open_close_ports(wordpress_0)
        self.assertEqual(calls[0], set([("a", wordpress_0)]))

        # Reset records of calls, and then add a second observer.
        # Verify both get called.
        calls[0] = set()
        self.firewall_manager.add_open_close_ports_observer(
            Observer(calls, "b"))
        yield self.firewall_manager.open_close_ports(wordpress_0)
        self.assertEqual(
            calls[0],
            set([("a", wordpress_0), ("b", wordpress_0)]))

    @inlineCallbacks
    def test_observe_open_close_ports_on_machine(self):
        """Verify one or more observers can be established on action."""
        manager = MachineStateManager(self.client)
        machine = yield manager.add_machine_state()

        # Add one observer, verify it gets called
        calls = [set()]
        self.firewall_manager.add_open_close_ports_on_machine_observer(
            Observer(calls, "a"))
        yield self.firewall_manager.open_close_ports_on_machine(machine.id)
        self.assertEqual(calls[0], set([("a", machine.id)]))

        # Reset records of calls, and then add a second observer.
        # Verify both get called.
        calls[0] = set()
        self.firewall_manager.add_open_close_ports_on_machine_observer(
            Observer(calls, "b"))
        yield self.firewall_manager.open_close_ports_on_machine(machine.id)
        self.assertEqual(
            calls[0],
            set([("a", machine.id), ("b", machine.id)]))
