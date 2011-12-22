
from twisted.internet.defer import inlineCallbacks

from juju.errors import InvalidPlacementPolicy
from juju.state.placement import place_unit, pick_policy
from juju.state.tests.test_service import ServiceStateManagerTestBase


class TestPlacement(ServiceStateManagerTestBase):

    @inlineCallbacks
    def setUp(self):
        yield super(TestPlacement, self).setUp()

        self.service = yield self.add_service_from_charm("mysql")
        self.unit_state = yield self.service.add_unit_state()

    def test_pick_policy(self):
        mock_provider = self.mocker.mock()
        mock_provider.get_placement_policies()
        self.mocker.result(["unassigned", "local", "new"])
        self.mocker.count(3)
        mock_provider.provider_type
        self.mocker.result("dummy")
        self.mocker.replay()

        # No selection gets first listed provider policy
        self.assertEqual(
            pick_policy(None, mock_provider), "unassigned")

        # If the user selection doesn't match we get an error
        self.assertRaises(
            InvalidPlacementPolicy,
            pick_policy, "smart", mock_provider)

        # The user choice is respected if its available
        self.assertEqual(
            pick_policy("new", mock_provider), "new")

    @inlineCallbacks
    def test_unassign_placement(self):
        machine1 = yield self.machine_state_manager.add_machine_state()
        machine2 = yield self.machine_state_manager.add_machine_state()

        unit2 = yield self.service.add_unit_state()

        # Take machine 1 manually
        yield self.unit_state.assign_to_machine(machine1)

        ms2 = yield place_unit(self.client, "unassigned", unit2)
        self.assertEqual(ms2.id, machine2.id)

        # and placing a new unit creates a new machine state
        unit3 = yield self.service.add_unit_state()
        ms3 = yield place_unit(self.client, "unassigned", unit3)
        self.assertEqual(ms3.id, machine2.id + 1)

    @inlineCallbacks
    def test_local_placement(self):
        ms0 = yield self.machine_state_manager.add_machine_state()
        self.assertEqual(ms0.id, 0)

        # These shouldn't be used with local (but should be available
        # to prove a different policy is at work)
        yield self.machine_state_manager.add_machine_state()
        yield self.machine_state_manager.add_machine_state()
        unit2 = yield self.service.add_unit_state()

        ms1 = yield place_unit(self.client, "local", self.unit_state)
        ms2 = yield place_unit(self.client, "local", unit2)

        # Everything should end up on machine 0 with local placement
        # even though other machines are available
        self.assertEqual(ms0.id, ms1.id)
        self.assertEqual(ms0.id, ms2.id)
