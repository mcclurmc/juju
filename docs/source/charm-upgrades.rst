Charm Upgrades
================


Upgrading a charm
-------------------

A charm_ can be upgraded via the command line using the following
syntax::

  $ juju upgrade-charm <service-name>

In the case of a local charm the sytax would be::

  $ juju upgrade-charm --repository=principia <service-name>

This will examine the named service, determine its charm, and check the
charm's originating repository for a newer version of the charm.
If a newer charm version is found, it will be uploaded to the juju
environment, and downloaded to all the running units of the service.
The unit agent will switch over to executing hooks from the new charm,
after executing the `upgrade-charm` hook.

.. _charm: ../charm.html


Charm upgrade support
-----------------------

A charm author can add charm specific support for upgrades by
providing an additional hook that can customize its upgrade behavior.

The hook ``upgrade-charm`` is executed with the new charm version
in place on the unit. juju guarantees this hook will be the first
executed hook from the new charm.

The hook is intended to allow the charm to process any upgrade
concerns it may have with regard to upgrading databases, software, etc
before its new version of hooks are executed.

After the ``upgrade-charm`` hook is executed, new hooks of the
charm will be utilized to respond to any system changes.

Futures
-------

The ``upgrade-charm`` hook will likely need access to a new cli-api
to access all relations of the unit, in addition to the standard hook
api commands like ``relation-list``, ``relation-get``,
``relation-set``, to perform per unit relation upgrades.

The new hook-cli api name is open, but possible suggestions are
``unit-relations`` or  ``query-relations`` and would list
all the relations a unit is a member of.

Most `server` services have multiple instances of a named relation.
Else name iteration of the charm defined relations would suffice.
It's an open question on how these effectively anonymous instances
of a named relation would be addressed.

The existing relation-* cli would also need to be extended to take
a relation parameter, or documented usage of environment variables
when doing relation iteration during upgrades.

Internals
---------

The upgrade cli updates the service with its new unit, and sets
an upgrade flag on each of its units. The unit agent then processes
the upgrade using the workflow machinery to execute hooks and
track upgrades across service units.

A unit whose upgrade-charm hook fails will be left running
but won't process any additional hooks. The hooks will continue
to be queued for execution.

The upgrade cli command is responsible for

 - Finding the named service.

 - Determining its charm.

 - Determining if a newer version of the charm exists in the
   origin repository.

 - Uploading the new version of the charm to the environment's machine
   provider storage.

 - Updating the service state with a reference to the new charm.

 - Marking the associated unit states as needing an upgrade.

As far as determining newer versions, the cli will assume the same charm
name with the max version number that is greater than the installed to
be an upgrade.

The unit agent is responsible for

 - Watching the unit state for upgrade changes.

 - Clearing the upgrade setting on the unit state.

 - Downloading the new charm version.

 - Stopping hook execution, hooks will continue to queue while
   the execution is stopped.

 - Extracting the charm into the unit container.

 - Updating the unit charm reference.

 - Running the upgrade workflow transition which will run the
   upgrade-charm hook, and restart normal hook execution.

Only the charm directory within a unit container/directory is
replaced on upgrade, any existing peristent data within the unit
container is maintained.
