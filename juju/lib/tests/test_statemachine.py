import logging

from twisted.internet.defer import succeed, fail, inlineCallbacks

from juju.lib.testing import TestCase
from juju.lib.statemachine import (
    Workflow, Transition, WorkflowState, InvalidStateError,
    InvalidTransitionError, TransitionError)


class AttributeWorkflowState(WorkflowState):

    _workflow_state = None

    # required workflow state implementations
    def _store(self, state_dict):
        self._workflow_state = state_dict
        return succeed(True)

    def _load(self):
        return self._workflow_state

    # transition handlers.
    def do_jump_puddle(self):
        self._jumped = True

    def do_error_transition(self):
        self._error_handler_invoked = True
        return dict(error=True)

    def do_transition_variables(self):
        return dict(hello="world")

    def do_error_unknown(self):
        raise AttributeError("unknown")

    def do_error_deferred(self):
        return fail(AttributeError("unknown"))

    def do_raises_transition_error(self):
        raise TransitionError("eek")


class StateMachineTest(TestCase):

    def setUp(self):
        super(StateMachineTest, self).setUp()
        self.log_stream = self.capture_logging(
            "statemachine", level=logging.DEBUG)

    def test_transition_constructor(self):
        t = Transition("id", "label", "source_state", "destination_state")
        self.assertEqual(t.transition_id, "id")
        self.assertEqual(t.label, "label")
        self.assertEqual(t.source, "source_state")
        self.assertEqual(t.destination, "destination_state")

    def test_workflow_get_transitions(self):
        workflow = Workflow(
            Transition("init_workflow", "", None, "initialized"),
            Transition("start", "", "initialized", "started"))
        self.assertRaises(InvalidStateError,
                          workflow.get_transitions,
                          "magic")
        self.assertEqual(
            workflow.get_transitions("initialized"),
            [workflow.get_transition("start")])

    def test_workflow_get_transition(self):
        transition = Transition("init_workflow", "", None, "initialized")
        workflow = Workflow(
            Transition("init_workflow", "", None, "initialized"),
            Transition("start", "", "initialized", "started"))
        self.assertRaises(KeyError, workflow.get_transition, "rabid")
        self.assertEqual(
            workflow.get_transition("init_workflow").transition_id,
            transition.transition_id)

    @inlineCallbacks
    def test_state_get_available_transitions(self):
        workflow = Workflow(
            Transition("init_workflow", "", None, "initialized"),
            Transition("start", "", "initialized", "started"))
        workflow_state = AttributeWorkflowState(workflow)
        transitions = yield workflow_state.get_available_transitions()
        yield self.assertEqual(
            transitions, [workflow.get_transition("init_workflow")])

    def test_fire_transition_alias_multiple(self):
        workflow = Workflow(
            Transition("init", "", None, "initialized", alias="init"),
            Transition("init_start", "", None, "started", alias="init"))
        workflow_state = AttributeWorkflowState(workflow)
        return self.assertFailure(
            workflow_state.fire_transition_alias("init"),
            InvalidTransitionError)

    def test_fire_transition_alias_none(self):
        workflow = Workflow(
            Transition("init_workflow", "", None, "initialized"),
            Transition("start", "", "initialized", "started"))
        workflow_state = AttributeWorkflowState(workflow)
        return self.assertFailure(
            workflow_state.fire_transition_alias("dog"),
            InvalidTransitionError)

    @inlineCallbacks
    def test_fire_transition_alias(self):
        workflow = Workflow(
            Transition("init_magic", "", None, "initialized", alias="init"))
        workflow_state = AttributeWorkflowState(workflow)
        value = yield workflow_state.fire_transition_alias("init")
        self.assertEqual(value, True)

    @inlineCallbacks
    def test_state_get_set(self):
        workflow = Workflow(
            Transition("init_workflow", "", None, "initialized"),
            Transition("start", "", "initialized", "started"))

        workflow_state = AttributeWorkflowState(workflow)
        current_state = yield workflow_state.get_state()
        self.assertEqual(current_state, None)

        yield workflow_state.set_state("started")
        current_state = yield workflow_state.get_state()
        self.assertEqual(current_state, "started")

    @inlineCallbacks
    def test_state_fire_transition(self):
        workflow = Workflow(
            Transition("init_workflow", "", None, "initialized"),
            Transition("start", "", "initialized", "started"))
        workflow_state = AttributeWorkflowState(workflow)
        yield workflow_state.fire_transition("init_workflow")
        current_state = yield workflow_state.get_state()
        self.assertEqual(current_state, "initialized")
        yield workflow_state.fire_transition("start")
        current_state = yield workflow_state.get_state()
        self.assertEqual(current_state, "started")
        yield self.assertFailure(workflow_state.fire_transition("stop"),
                                 InvalidTransitionError)

        name = "attributeworkflowstate"
        output = (
            "%s: transition init_workflow (None -> initialized) {}",
            "%s: transition complete init_workflow (state initialized) {}",
            "%s: transition start (initialized -> started) {}",
            "%s: transition complete start (state started) {}\n")
        self.assertEqual(self.log_stream.getvalue(),
                         "\n".join([line % name for line in output]))

    @inlineCallbacks
    def test_state_transition_callback(self):
        """If the workflow state, defines an action callback for a transition,
        its invoked when the transition is fired.
        """
        workflow = Workflow(
            Transition("jump_puddle", "", None, "dry"))

        workflow_state = AttributeWorkflowState(workflow)
        yield workflow_state.fire_transition("jump_puddle")
        current_state = yield workflow_state.get_state()
        self.assertEqual(current_state, "dry")
        self.assertEqual(
            getattr(workflow_state, "_jumped", None),
            True)

    @inlineCallbacks
    def test_transition_action_workflow_error(self):
        """If a transition action callback raises a transitionerror, the
        transition does not complete, and the state remains the same.
        The fire_transition method in this cae returns False.
        """
        workflow = Workflow(
            Transition("raises_transition_error", "", None, "next-state"))
        workflow_state = AttributeWorkflowState(workflow)
        result = yield workflow_state.fire_transition(
            "raises_transition_error")
        self.assertEqual(result, False)
        current_state = yield workflow_state.get_state()
        self.assertEqual(current_state, None)

        name = "attributeworkflowstate"
        output = (
            "%s: transition raises_transition_error (None -> next-state) {}",
            "%s:  execute action do_raises_transition_error",
            "%s:  transition raises_transition_error failed eek\n")
        self.assertEqual(self.log_stream.getvalue(),
                         "\n".join([line % name for line in output]))

    def test_transition_action_unknown_error(self):
        """If an unknown error is raised by a transition action, it
        is raised from the fire transition method.
        """
        workflow = Workflow(
            Transition("error_unknown", "", None, "next-state"))

        workflow_state = AttributeWorkflowState(workflow)
        return self.assertFailure(
            workflow_state.fire_transition("error_unknown"),
            AttributeError)

    @inlineCallbacks
    def test_transition_resets_state_variables(self):
        """State variables are only stored, while the associated state is
        current.
        """
        workflow = Workflow(
            Transition("transition_variables", "", None, "next-state"),
            Transition("some_transition", "", "next-state", "final-state"))

        workflow_state = AttributeWorkflowState(workflow)
        state_variables = yield workflow_state.get_state_variables()
        self.assertEqual(state_variables, {})

        yield workflow_state.fire_transition("transition_variables")
        current_state = yield workflow_state.get_state()
        self.assertEqual(current_state, "next-state")
        state_variables = yield workflow_state.get_state_variables()
        self.assertEqual(state_variables, {"hello": "world"})

        yield workflow_state.fire_transition("some_transition")
        current_state = yield workflow_state.get_state()
        self.assertEqual(current_state, "final-state")
        state_variables = yield workflow_state.get_state_variables()
        self.assertEqual(state_variables, {})

    @inlineCallbacks
    def test_transition_success_transition(self):
        """If a transition specifies a success transition, and its action
        handler completes successfully, the success transistion and associated
        action handler are executed.
        """
        workflow = Workflow(
            Transition("initialized", "", None, "next-state",
                       success_transition_id="markup"),
            Transition("markup", "", "next-state", "final-state"),
            )
        workflow_state = AttributeWorkflowState(workflow)
        yield workflow_state.fire_transition("initialized")
        self.assertEqual((yield workflow_state.get_state()), "final-state")
        self.assertIn(
            "initiating success transition: markup",
            self.log_stream.getvalue())

    @inlineCallbacks
    def test_transition_error_transition(self):
        """If a transition specifies an error transition, and its action
        handler raises a transition error, the error transition and associated
        hooks are executed.
        """
        workflow = Workflow(
            Transition("raises_transition_error", "", None, "next-state",
                       error_transition_id="error_transition"),
            Transition("error_transition", "", None, "error-state"))

        workflow_state = AttributeWorkflowState(workflow)
        yield workflow_state.fire_transition("raises_transition_error")

        current_state = yield workflow_state.get_state()
        self.assertEqual(current_state, "error-state")
        state_variables = yield workflow_state.get_state_variables()
        self.assertEqual(state_variables, {"error": True})

    @inlineCallbacks
    def test_state_machine_observer(self):
        """A state machine observer can be registered for tests visibility
        of async state transitions."""

        results = []

        def observer(state, variables):
            results.append((state, variables))

        workflow = Workflow(
            Transition("begin", "", None, "next-state"),
            Transition("continue", "", "next-state", "final-state"))

        workflow_state = AttributeWorkflowState(workflow)
        workflow_state.set_observer(observer)

        yield workflow_state.fire_transition("begin")
        yield workflow_state.fire_transition("continue")

        self.assertEqual(results,
                         [("next-state", {}), ("final-state", {})])

    @inlineCallbacks
    def test_state_variables_via_transition(self):
        """Per state variables can be passed into the transition.
        """
        workflow = Workflow(
            Transition("begin", "", None, "next-state"),
            Transition("continue", "", "next-state", "final-state"))

        workflow_state = AttributeWorkflowState(workflow)

        yield workflow_state.fire_transition(
            "begin", rabbit="moon", hello=True)
        current_state = yield workflow_state.get_state()
        self.assertEqual(current_state, "next-state")
        variables = yield workflow_state.get_state_variables()
        self.assertEqual({"rabbit": "moon", "hello": True}, variables)

        yield workflow_state.fire_transition("continue")
        current_state = yield workflow_state.get_state()
        self.assertEqual(current_state, "final-state")
        variables = yield workflow_state.get_state_variables()
        self.assertEqual({}, variables)

    @inlineCallbacks
    def test_transition_state(self):
        """Transitions can be specified by the desired state.
        """
        workflow = Workflow(
            Transition("begin", "", None, "trail"),
            Transition("to_cabin", "", "trail", "cabin"),
            Transition("to_house", "", "trail", "house"))

        workflow_state = AttributeWorkflowState(workflow)

        result = yield workflow_state.transition_state("trail")
        self.assertEqual(result, True)
        current_state = yield workflow_state.get_state()
        self.assertEqual(current_state, "trail")

        result = yield workflow_state.transition_state("cabin")
        self.assertEqual(result, True)

        result = yield workflow_state.transition_state("house")
        self.assertEqual(result, False)

        self.assertFailure(workflow_state.transition_state("unknown"),
                           InvalidStateError)
