Hook debugging
==============

Introduction
------------

An important facility in any distributed system is the ability to
introspect the running system, and to debug it. Within juju the
actions performed by the system are executing charm defined
hooks. The ``debug-log`` cli provides for inspecting the total state of
the system via capturing the logs of all agents and output of all
hooks run by the system.

To facilitate better debugging of hooks, the ``debug-hooks`` cli
provides for interactive shell usage as a substitute for running a
hook. This allows a charm author or system adminstrator the ability
to interact with the system in a live environment and either develop
or debug a hook.

How it works
------------

When the juju user utilizes the hook debug command like so::

  juju debug-hooks unit_name [hook_name]

juju is instructed to replace the execution of the hook from the
charm of the respective service unit, and instead to execute it in a
shell associated to a tmux session. If no hook name is given, then all
hooks will be debugged in this fashion. Multiple hook names can also be
specified on the command line. Shell regular expressions can also be
utilized to specify hook names.

The ``debug-hooks`` command line invocation will immediately connect
to the remote machine of the remote unit and start a named shell
connected to the same tmux session there.

The native capabilities of tmux can be exploited to construct a full
debug/development environment on the remote machine.

When a debugged hook is executed a new named window will pop up in the
tmux session with the hook shell started.  The new window's title will
match the hook name, and the shell environment will have all the
juju environment variables in place, and all of the hook cli API
may be utilized (relation-get, relation-set, relation-list, etc.).

It's important to note that juju serializes hook execution, so
while the shell is active, no other hooks will be executed on the 
unit.  Once the experimentation is done, the user must stop the hook
by exiting the shell session.  At this point the system is then
free to execute additional hooks.

It's important to note that any state changes performed while in the
hook window via relation-set are buffered till the hook is done
executing, in the same way performed for all the relation hooks when
running outside of a debug session.

The debug-hooks can be used to debug the same hook being invoked
multiple times as long as the user has not closed the debug screen
session.

The user can exit debug mode by exiting the tmux session (e.g.
exiting all shells). The unit will then resume its normal
processing.


Limitations
-----------

Note that right now one can only query relation information when
debugging a running relation hook.  This means commands such as
relation-get, relation-set, etc, will not work on a hook like
'install' or 'upgrade'.

This problem will be solved once the following bug is fixed:

    https://bugs.launchpad.net/juju/+bug/767195


Internals
---------

Internally the ``debug-hooks`` cli begins by verifying its arguments,
namely the unit exists, and the named hook is valid for the charm.
After that it modifies the zookeeper state of the unit node, setting
a flag noting the hook to debug. It then establishes an ssh 
connection to the machine and executes the tmux command.

The unit-agent will establish a watch on its own debug settings, on
changes introspecting the debug flag, and pass any named hook values 
down to the hook executor, which will construct debug hook scripts on 
the fly for matching hooks. These debug hook scripts are responsible
for connecting to tmux and monitoring the execution of the hook
therein.

Special care will be taken to ensure the viability of the tmux 
session and that debug mode is active before creating the interactive
hook window in tmux.


Screen vs. tmux
---------------

Initially juju used GNU screen for the debugging sessions rather
than tmux, but tmux turned out to make it easier to avoid race
conditions when starting the same session concurrently, as done for
the debugging system.  This was the main motivation that prompted
the change to tmux.  They both worked very similarly otherwise.
