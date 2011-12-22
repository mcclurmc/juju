Resolving errors
================

juju internally tracks the state of units and their relations.
It moves them through a simple state machine to ensure the correct
sequencing of hooks and associated unit agent behavior. Typically
this means that all hooks are executed as part of a workflow transition.

If a hook fails, then juju notes this failure, and transitions
either the unit or the unit relation (depending on the hook) to a
failure state.

If a hook for a unit relation fails, only that unit relation is
considered to be in an error state, the unit and other relations of
the unit continue to operate normally.

If a hook for the unit fails (install, start, stop, etc), the unit
is considered not running, and its unit relations will stop responding
to changes.

As a means to recover from hook errors, juju offers the
``juju resolved`` command.

This command will operate on either a unit or unit relation and
schedules a transition from the error state back to its original
destination state. For example given a unit mysql/0 whose start
hook had failed, and the following command line::

  $ juju resolved mysql/0

After being resolved the unit would be in the started state. It
important to note that by default ``juju resolved`` does
not fire hooks and the ``start`` hook would not be invoked again
as a result of the above.

If a unit's relation-change/joined/departed hook had failed, then
juju resolved can also be utilized to resolve the error on
the unit relation::

  $ juju resolved mysql/0 db

This would re-enable change watching, and hook execution for the
``db`` relation after a hook failure.

It's expected that an admin will typically have a look at the system to
determine or correct the issue before using this command line.
``juju resolved`` is meant primarily as a mechanism to remove the
error block after correction of the original issue.

However ``juju resolved`` can optionally re-invoke the failed hook.
This feature is particular beneficial during charm development, when
iterating over an in development hook. Assuming a mysql unit with
with a start hook error upon executing the following command::

 $ juju resolved --retry mysql/0

juju will examine the mysql/0 unit, and will re-execute its start
hook before marking it as running. If the start hook fails again,
then the unit will remain in the same state.

