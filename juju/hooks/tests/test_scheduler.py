import logging

from twisted.internet.defer import inlineCallbacks

from juju.hooks.scheduler import HookScheduler
from juju.state.hook import RelationChange
from juju.state.tests.test_service import ServiceStateManagerTestBase

class HookSchedulerTest(ServiceStateManagerTestBase):

    @inlineCallbacks
    def setUp(self):
        yield super(HookSchedulerTest, self).setUp()
        self.client = self.get_zookeeper_client()
        self.unit_relation = self.mocker.mock()
        self.executions = []
        self.service = yield self.add_service_from_charm("wordpress")
        self.scheduler = HookScheduler(self.client,
                                       self.collect_executor,
                                       self.unit_relation, "",
                                       unit_name="wordpress/0")
        self.log_stream = self.capture_logging(
            "hook.scheduler", level=logging.DEBUG)

    def collect_executor(self, context, change):
        self.executions.append((context, change))

    # Event reduction/coalescing cases
    def test_reduce_removed_added(self):
        """ A remove event for a node followed by an add event,
        results in a modify event.
        """
        self.scheduler.notify_change(old_units=["u-1"], new_units=[])
        self.scheduler.notify_change(old_units=[], new_units=["u-1"])
        self.scheduler.run()
        self.assertEqual(len(self.executions), 1)
        self.assertEqual(self.executions[0][1].change_type, "modified")

        output = ("relation change old:['u-1'], new:[], modified:()",
                  "relation change old:[], new:['u-1'], modified:()",
                  "start",
                  "executing hook for u-1:modified\n")
        self.assertEqual(self.log_stream.getvalue(), "\n".join(output))

    def test_reduce_modify_remove_add(self):
        """A modify, remove, add event for a node results in a modify.
        An extra validation of the previous test.
        """
        self.scheduler.notify_change(modified=["u-1"])
        self.scheduler.notify_change(old_units=["u-1"], new_units=[])
        self.scheduler.notify_change(old_units=[], new_units=["u-1"])
        self.scheduler.run()
        self.assertEqual(len(self.executions), 1)
        self.assertEqual(self.executions[0][1].change_type, "modified")

    def test_reduce_add_modify(self):
        """An add and modify event for a node are coalesced to an add."""
        self.scheduler.notify_change(old_units=[], new_units=["u-1"])
        self.scheduler.notify_change(modified=["u-1"])
        self.scheduler.run()
        self.assertEqual(len(self.executions), 1)
        self.assertEqual(self.executions[0][1].change_type, "joined")

    def test_reduce_add_remove(self):
        """an add followed by a removal results in a noop."""
        self.scheduler.notify_change(old_units=[], new_units=["u-1"])
        self.scheduler.notify_change(old_units=["u-1"], new_units=[])
        self.scheduler.run()
        self.assertEqual(len(self.executions), 0)

    def test_reduce_modify_remove(self):
        """Modifying and then removing a node, results in just the removal."""
        self.scheduler.notify_change(old_units=["u-1"],
                                     new_units=["u-1"],
                                     modified=["u-1"])
        self.scheduler.notify_change(old_units=["u-1"], new_units=[])
        self.scheduler.run()
        self.assertEqual(len(self.executions), 1)
        self.assertEqual(self.executions[0][1].change_type, "departed")

    def test_reduce_modify_modify(self):
        """Multiple modifies get coalesced to a single modify."""
        # simulate normal startup, the first notify will always be the existing
        # membership set.
        self.scheduler.notify_change(old_units=[], new_units=["u-1"])
        self.scheduler.run()
        self.scheduler.stop()
        self.assertEqual(len(self.executions), 1)

        # Now continue the modify/modify reduction.
        self.scheduler.notify_change(modified=["u-1"])
        self.scheduler.notify_change(modified=["u-1"])
        self.scheduler.notify_change(modified=["u-1"])
        self.scheduler.run()

        self.assertEqual(len(self.executions), 2)
        self.assertEqual(self.executions[1][1].change_type, "modified")

    # Other stuff.
    @inlineCallbacks
    def test_start_stop(self):
        d = self.scheduler.run()
        # starting multiple times results in an error
        self.assertFailure(self.scheduler.run(), AssertionError)
        self.scheduler.stop()
        yield d
        # stopping multiple times is not an error
        yield self.scheduler.stop()

    @inlineCallbacks
    def test_membership_visibility_per_change(self):
        """Hooks are executed against changes, those changes are
        associated to a temporal timestamp, however the changes
        are scheduled for execution, and the state/time of the
        world may have advanced, to present a logically consistent
        view, we try to gaurantee at a minimum, that hooks will
        always see the membership of a relations it was at the
        time of their associated change.
        """
        self.scheduler.notify_change(
            old_units=[], new_units=["u-1", "u-2"])
        self.scheduler.notify_change(
            old_units=["u-1", "u-2"], new_units=["u-2", "u-3"])
        self.scheduler.notify_change(modified=["u-2"])

        self.scheduler.run()
        self.scheduler.stop()
        # only two reduced events, u-2, u-3 add
        self.assertEqual(len(self.executions), 2)

        # Now the first execution (u-2 add) should only see members
        # from the time of its change, not the current members. However
        # since u-1 has been subsequently removed, it no longer retains
        # an entry in the membership list.
        change_members = yield self.executions[0][0].get_members()
        self.assertEqual(change_members, ["u-2"])

        self.scheduler.notify_change(modified=["u-2"])
        self.scheduler.notify_change(
            old_units=["u-2", "u-3"], new_units=["u-2"])
        self.scheduler.run()

        self.assertEqual(len(self.executions), 4)
        self.assertEqual(self.executions[2][1].change_type, "modified")
        # Verify modify events see the correct membership.
        change_members = yield self.executions[2][0].get_members()
        self.assertEqual(change_members, ["u-2", "u-3"])

    @inlineCallbacks
    def test_membership_visibility_with_change(self):
        """We express a stronger guarantee of the above, namely that
        a hook wont see any 'active' members in a membership list, that
        it hasn't previously been given a notify of before.
        """
        self.scheduler.notify_change(
            old_units=["u-1", "u-2"],
            new_units=["u-2", "u-3", "u-4"],
            modified=["u-2"])

        self.scheduler.run()
        self.scheduler.stop()

        # add for u-3, u-4, remove for u-1, modify for u-2
        self.assertEqual(len(self.executions), 4)

        # Verify members for each change.
        self.assertEqual(self.executions[0][1].change_type, "joined")
        members = yield self.executions[0][0].get_members()
        self.assertEqual(members, ["u-1", "u-2", "u-3"])

        self.assertEqual(self.executions[1][1].change_type, "joined")
        members = yield self.executions[1][0].get_members()
        self.assertEqual(members, ["u-1", "u-2", "u-3", "u-4"])

        self.assertEqual(self.executions[2][1].change_type, "departed")
        members = yield self.executions[2][0].get_members()
        self.assertEqual(members, ["u-2", "u-3", "u-4"])

        self.assertEqual(self.executions[3][1].change_type, "modified")
        members = yield self.executions[2][0].get_members()
        self.assertEqual(members, ["u-2", "u-3", "u-4"])

        context = yield self.scheduler.get_hook_context(
            RelationChange("", "", ""))
        self.assertEqual((yield context.get_members()),
                         ["u-2", "u-3", "u-4"])

    @inlineCallbacks
    def test_get_relation_change_empty(self):
        """Retrieving a hook context, is possible even if no
        no hooks has previously been fired (Empty membership)."""
        context = yield self.scheduler.get_hook_context(
            RelationChange("", "", ""))
        members = yield context.get_members()
        self.assertEqual(members, [])
