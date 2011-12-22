Charms
======

Introduction
------------

Charms define how services integrate and how their service units
react to events in the distributed environment, as orchestrated by
juju.

This specification describes how charms are defined, including their
metadata and hooks. It also describes the resources available to hooks
in working with the juju environment.


The metadata file
-----------------

The `metadata.yaml` file, at the root of the charm directory,
describes the charm. The following fields are supported:

  * **name:** - The charm name itself. Charm names are formed by
    lowercase letters, digits, and dashes, and must necessarily
    begin with a letter and have no digits alone in a dashed
    section.

  * **summary:** - A one-line description of the charm.

  * **description:** - Long explanation of the charm and its
    features.

  * **provides:** - The deployed service unit must have the given
    relations established with another service unit whose charm
    requires them for the service to work properly. See below for how
    to define a relation.

  * **requires:** - The deployed service unit must have the given
    relations established with another service unit whose charm
    provides them for the service to work properly. See below for how
    to define a relation.

  * **peers:** - Relations that are established with P2P semantics
    instead of a provides/requires (or client/server) style.  When the
    charm is deployed as a service unit, all the units from the
    given service will automatically be made part of the relation.
    See below for how to define a relation.


Relations available in `provides`, `requires`, and `peers` are defined
as follows:

  * **provides|requires|peers:**

    * **<relation name>:** - This name is a user-provided value which
      identifies the relation uniquely within the given charm.
      Examples include "database", "cache", "proxy", and "appserver".

      Each relation may have the following fields defined:

      * **interface:** - This field defines the type of the
        relation. The relation will only be established with service
        units that define a compatible relation with the same
        interface. Examples include "http", "mysql", and
        "backup-schedule".

      * **limit:** - The maximum number of relations of this kind
        which may be established to other service units.  Defaults to
        1 for `requires` relations, and to "none" (no limit) for
        `provides` and `peers` relations. While you may define it,
        this field is not yet enforced by juju.

      * **optional:** - Whether this relation is required for the
        service unit to function or not.  Defaults to `false`, which
        means the relation is required. While you may define it, this
        field is not yet enforced by juju.

      As a shortcut, if these properties are not defined, and instead
      a single string value is provided next to the relation name, the
      string is taken as the interface value, as seen in this
      example::

          requires:
            db: mysql

Some sample charm definitions are provided at the end of this
specification.


Hooks
-----

juju uses hooks to notify a service unit about changes happening
in its lifecycle or the larger distributed environment. A hook running
for a service unit can query this environment, make any desired local
changes on its underlying machine, and change the relation
settings.

Each hook for a charm is implemented by placing an executable with
the desired hook name under the ``hooks/`` directory of the charm
directory.  juju will execute the hook based on its file name when
the corresponding event occurs.

All hooks are optional. Not including a corresponding executable in
the charm is treated by juju as if the hook executed and then
exited with an exit code of 0.

All hooks are executed in the charm directory on the service unit.

The following hooks are with respect to the lifecycle of a service unit:

  * **install** - Runs just once during the life time of a service
    unit. Currently this hook is the right place to ensure any package
    dependencies are met. However, in the future juju will use the
    charm metadata to perform this role instead.

  * **start** - Runs when the service unit is started. This happens
    before any relation hooks are called. The purpose of this hook is
    to get the service unit ready for relations to be established.

  * **stop** - Runs when the service unit is stopped. If relations
    exist, they will be broken and the respective hooks called before
    this hook is called.

The following hooks are called on each service unit as the membership
of an established relation changes:

  * **<relation name>-relation-joined** - Runs upon each time a remote
    service unit joins the relation.

  * **<relation name>-relation-changed** - Runs upon each time the
    following events occur:

    1. A remote service unit joins the relation, right after the
       **<relation name>-relation-joined** hook was called.

    2. A remote service unit changes its relation settings.

    This hook enables the charm to modify the service unit state
    (configuration, running processes, or anything else) to adapt to
    the relation settings of remote units.

    An example usage is that HAProxy needs to be aware of web servers
    as they become available, including details like its IP
    address. Web server service units can publish their availability
    by making the appropriate relation settings in the hook that makes
    the most sense. Assume the HAProxy uses the relation name of
    ``server``. Then upon that happening, the HAProxy in its
    ``server-relation-changed hook`` can then change its own
    configuration as to what is available to be proxied.

  * **<relation name>-relation-departed** - Runs upon each time a
    remote service unit leaves a relation. This could happen because
    the service unit has been removed, its service has been destroyed,
    or the relation between this service and the remote service has
    been removed.

    An example usage is that HAProxy needs to be aware of web servers
    when they are no longer available. It can remove each web server
    its configuration as the corresponding service unit departs the
    relation.

This relation hook is with respect to the relation itself:

  * **<relation name>-relation-broken** - Runs when a relation which
    had at least one other relation hook run for it (successfully or
    not) is now unavailable. The service unit can then clean up any
    established state.

    An example might be cleaning up the configuration changes which
    were performed when HAProxy was asked to load-balance for another
    service unit.

Note that the coupling between charms is defined by which settings
are required and made available to them through the relation hooks and
how these settings are used. Those conventions then define what the
relation interface really is, and the **interface** name in the
`metadata.yaml` file is simply a way to refer to them and avoid the
attempting of incompatible conversations.  Keep that in mind when
designing your charms and relations, since it is a good idea to
allow the implementation of the charm to change and be replaced with
alternative versions without changing the relation conventions in a
backwards incompatible way.


Hook environment
----------------

Hooks can expect to be invoked with a standard environment and
context. The following environment variables are set:

  * **$JUJU_UNIT_NAME** - The name of the local unit executing,
    in the form ``<service name>/<unit sequence>``. E.g. ``myblog/3``.

Hooks called for relation changes will have the follow additional
environment variables set:

  * **$JUJU_RELATION** - The relation name this hook is running
    for.  It's redundant with the hook name, but is necessary for
    the command line tools to know the current context.

  * **$JUJU_REMOTE_UNIT** - The unit name of the remote unit
    which has triggered the hook execution.


Hook commands for working with relations
----------------------------------------

In implementing their functionality, hooks can leverage a set of
command tools provided by juju for working with relations.  These
utilities enable the hook to collaborate on their relation settings,
and to inquire about the peers the service unit has relations with.

The following command line tools are made available:

  * **relation-get** - Queries a setting from an established relation
    with one or more service units.  This command will read some
    context information from environment variables (e.g.
    $JUJU_RELATION_NAME).

    Examples:

    Get the IP address from the remote unit which triggered the hook
    execution::

        relation-get ip

    Get all the settings from the remote unit which triggered the hook
    execution::

        relation-get

    Get the port information from the `wordpress/3` unit::

        relation-get port wordpress/3

    Get all the settings from the `wordpress/3` unit, in JSON format::

        relation-get - wordpress/3

  * **relation-set** - Changes a setting in an established relation.

    Examples:

    Set this unit's port number for other peers to use::

        relation-set port=8080

    Change two settings at once::

        relation-set dbname=wordpress dbpass="super secur3"

    Change several settings at once, with a JSON file::

        cat settings.json | relation-set

    Delete a setting::

        relation-set name=

  * **relation-list** - List all service units participating in the
    established relation.  This list excludes the local service unit
    which is executing the command. For `provides` and `requires`
    relations, this command will always return a single service unit.

    Example::

        MEMBERS=$(relation-list)

Changes to relation settings are only committed if the hook exited
with an exit code of 0. Such changes will then trigger further hook
execution in the remote unit(s), through the **<relation
name>-relation-changed** hook. This mechanism enables a general
communication mechanism for service units to coordinate.


Hook commands for opening and closing ports
-------------------------------------------

Service exposing determines which ports to expose by using the
``open-port`` and ``close-port`` commands in hooks. They may be
executed within any charm hook. The commands take the same
arguments::

    open-port port[/protocol]

    close-port port[/protocol]

These commands are executed immediately; they do not depend on the
exit status of the hook.

As an example, consider the WordPress charm, which has been deployed
as ``my-wordpress``. After completing the setup and restart of Apache,
the ``wordpress`` charm can then publish the available port in its
``start`` hook for a given service unit::

    open-port 80

External access to the service unit is only allowed when both
``open-port`` is executed within any hook and the administrator has
exposed its service.  The order in which these happen is not
important, however.

.. note::

    Being able to use any hook may be important for your charm.
    Ideally, the service does not have ports that are vulnerable if
    exposed prior to the service being fully ready. But if that's the
    case, you can solve this problem by only opening the port in the
    appropriate hook and when the desired conditions are met.

Alternatively, you may need to expose more than one port, or expose
ports that don't use the TCP protocol. To expose ports for
HTTP and HTTPS, your charm could instead make these settings::

    open-port 80
    open-port 443

Or if you are writing a charm for a DNS server that you would like
to expose, then specify the protocol to be UDP::

    open-port 53/udp

When the service unit is removed or stopped for any reason, the
firewall will again be changed to block traffic which was previously
allowed to reach the exposed service. Your charm can also do this to
close the port::

    close-port 80

To be precise, the firewall is only open for the exposed ports during
the time both these conditions hold:

    * A service has been exposed.
    * A corresponding ``open-port`` command has been run (without a
      subsequent ``close-port``).


Sample metadata.yaml files
--------------------------

Below are presented some sample metadata files.


MySQL::

  name: mysql
  revision: 1
  summary: "A pretty popular database"

  provides:
    db: mysql


Wordpress::

  name: wordpress
  revision: 3
  summary: "A pretty popular blog engine"
  provides:
    url:
      interface: http

  requires:
    db:
     interface: mysql


Riak::

  name: riak
  revision: 7
  summary: "Scalable K/V Store in Erlang with Clocks :-)"
  provides:
    endpoint:
      interface: http

  peers:
    ring:
      interface: riak
