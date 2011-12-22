Unit Agent hooks
================

Introduction
------------

The Unit Agent (**UA**) in juju is responsible for managing and
maintaining the per-machine service units. By calling life-cycle and
state change hooks the UA is able to allow the Service Unit to respond
to changes in the state of the environment. This is done through the
invocation of hooks provided by the charm author.

This specification outlines the interaction between the UA, the
running software being managed by the UA and the hooks invoked in
response to state or process level changes in the runtime.

Hooks_ are defined in another document. This specification only
captures how they are invoked and managed, not why.

.. _Hooks: ../charm.html#hooks

When the Machine Agent (**MA**) spawns a UA it does so in order to
manage the smallest managed unit of service deployment and
management. The process managed by the UA will be called the **UAP**
later in the documentation.

The UAP does not directly communicate with the UA, that is the
responsibility of the hooks and is handled by the provided command
line tools. The means through which that communication occurs and the
semantics of it are described in this document.


Hooks access to settings
------------------------

Hooks have access to two kinds of settings. The first is the
*"service settings"*, which cover configuration details for the
all units of the given service.  These are usually provided
manually by the user, are global to the service, and will not
be written to by service units themselves.  This is the
principal way through which an administrator configures the
software running inside an juju service unit.

The second kind is known as *"relation settings"*, and are
made available to service units whenever they are participating in
a relation with one or more service units.  In these cases, each
participating unit will have its own set of settings specific to
that relation, and will be able to query both its local settings
and the remote settings from any of the participating units.
That's the main mechanism used by juju to allow service units
to communicate with each other.

Using the example of a blog deployment we might include information
such as the theme used by the blog engine and the title of the blog in
the "service settings". The "relation settings" might contain specific
information about the blog engines connection to a database deployed
on its behalf, for example and IP address and port.

There is a single ZK node for the "service settings" and another for
the "relation settings". Within this node we store an dictionary
mapping string keys to opaque blobs of information which are managed
by the service hooks and the juju administrator.

Hooks are presented with a synchronous view of the state of these
nodes. When a request is made for a particular setting in a particular
node the cache will present a view of that node that is consistent to
the client for the lifetime of the hook invocation. For example,
assume a settings node with settings 'a' and 'b'. When the hook
requests the value of 'a' from the a relation settings node we would
present a consistent view of those settings should they request 'a' or
'b' from that same relation settings during the lifetime of the
hook. If however they were to attempt to request value 'a' from a
different relation settings node this new nodes setting would be
cached at the time of its first interaction with the hook. Repeated
reads of data from the same settings node will continue to yield the
clients view of that data.

When manipulating data, even if the initial interaction with the data
is a set, the settings are first read into the UA cache and the cache
is updated with the current value.


Service Unit name
-----------------

A service unit name in juju is formed by including both the name
of the service and a monotonically increasing number that uniquely
specifies the service unit for the life time of an juju
deployment::

      <service_name>/<service_unit_number>

This results in name like "wordpress/1" and "mysql/1". The numbers
themselves are not significant but do obey the rule that they will not
be reused during the lifetime of a service. This means that if a UA
goes away the number that represented it is retired from the
deployment.

For additional details see juju/state/service.py.


Client Id
---------

Because of the way in which settings state is presented through the
command line utilities within hooks clients are provided a string
token through a calling environmental variable,
*JUJU_CLIENT_ID*. Using this variable all command line tools will
connect with a shared common state when used from a single hook
invocation.

The few command line utilities, such as juju-log, which could be
called outside the context of a hook need not pass a client id. At the
time of this writing its expected that cli tools which don't need hook
context either don't make an effort to present a stable view of
settings between calls (and thus run with a completely pass-through
cache proxy) or don't interact directly with the state.

However as indicated below the *--client_id* flag can be passed
directly to any tool indicating the caching context which should be
used. This facilitates testing as well as allowing some flexibility in
the future.

Passing a client_id which the UA is unaware of (or which has expired
through some other means) will result in an error and an exit code
being returned to the client.


Hook invocation and communication
---------------------------------

Twisted (which is used to handle networking and asynchronous
interactions throughout the codebase) defines a key-value oriented
binary protocol called AMP which is used to communicate between the UA
and the hooks executed on behalf of the charm. To facilitate this
the filename of a Unix Domain socket is provided through the process
environment. This socket is shared among all hook invocations and can
even be used by tools outside the context of a particular hook
invocation. Because of this providing a 'client id'_ to calls will
establish a connection to an internal data-cache offering a consistent
view of settings on a per-node, per-client basis.

Communication over this socket takes place using an abstraction
provided by AMP called Commands. Hooks trigger, through the invocation
of utility commands, these commands to the provided socket. These
commands in turn schedule interactions with the settings available in
ZK.

Because of the policy used for scheduling changes to settings the
actions of hooks are not applied directly to ZK (and this are not
visible outside the particular UA invoking the hook) until the hook
terminates with a success code.

Here are the commands the initial revision will support and a bit about
there characteristics:

    * **get(client_id, unit_name, setting_name)** - This command will return the
        value for a given key name or return a KeyError. A key error
        can be mapped through to the cli as null with a failed exit
        code. **unit_name** is processed using the standard `Service
        Unit Name`_ policy.

    * **set(client_id, unit_name, json_blob)** - This command will enqueue a
        state change to ZK pending successful termination of the
        hook. **unit_name** is processed using the standard `Service
        Unit Name`_ policy. The json_blob is a JSON string
        serialization of a dict which will be applied as a set of
        updates to the keys and values stored in the existing
        settings. Because the cache object contains the updated state
        (but is not visiable outside the hook until successful
        completion) subsequent reads of settings would return the
        values provided by the set call.

    * **list_relations(client_id)** - Returns a list of all relations
        associated with a hook at the time of invocation. The values
        of this call will typically also be exposed as a environment
        variable, **JUJU_MEMBERS**.

    * **flush(client_id)** - reserved

    * **sync(client_id)** - reserved

    * **wait(client_id, keyname)** - reserved


Unit Agent internal state
-------------------------

This is a list of internal state which the UA maintains for the proper
management of hook invocations.

    * which hooks have fired (and the expected result state).
    * the UNIX domain socket passed to hooks for AMP communication
    * the path to the container in which the Service Unit is executing
      (passed in environment to hooks).
    * the cached state of relationship nodes and settings relative to
      particular hook invocations.


Command line interface
----------------------

While the command line utilities provided use the underlying AMP
commands to enact their work they provide a standard set of utilities
for passing data between files and ZK state.

Hooks have access to many commands provided by juju for
interfacing with settings. These provide a set of standard command
line options and conventions.

     * Command line tools like *relation-set* will check stdin
       processing the provided input as a JSON dict of values that
       should be handled as though they were command line
       arguments. Using this convention its possible to easily set
       many values at once without any thought to escaping values for
       the shell.

     * Similar to *curl(1)* if you start the data with the letter @,
       the rest should be a file name to read the data from, or - if
       you want to read the data from stdin.

    * Command line tools responsible for returning data to the user,
      such as **relation-get**, will output JSON by default when
      returning more than a single value or **--format=json** is
      present in the command line. Requests for a single value default
      to returning the value without JSON serialisation unless the
      --format=json flag is passed.

    * Output from command lines tools default to stdout. If the **-o**
      option is provided any tool will write its output to a file
      named after that flag. ex. **relation-get -o /tmp/output.json**
      will create or replace a file called /tmp/output.json with the
      data existent in the relation.


Logging
-------

Command line hooks communicate with the user/admin by means three
primary channels.

    * **Hook exit code** Zero is success, anything is regarded as hook
      failure and will cause the hook to be run at a later time.

    * **Stdout/Stderr** Messages printed, echoed or otherwise emitted
        from the hooks on stdout or stderr are converted to log
        messages of levels INFO and ERROR respectively. These messages
        will then be emitted by the UA as they occur and are not
        buffered like global state changes.

    * **juju-logger** (reserved) An additional command line tool
        provided to communicate more complex logging messages to the
        UA and help them be made available to the user.


Calling environment
-------------------

Hooks can expect to be invoked with a standard environment and
context. The following be included:

    * `$JUJU_SOCKET` - Path to a UNIX Domain socket which will be
      made available to the command line tools in order to communicate
      with the UA.

    * `$JUJU_CLIENT_ID` - A unique identifier passed to a hook
      invocation used to populate the --client_id flag to cli
      tools. This is describe in the section, `Client Id`_.

   * `$JUJU_LOCAL_UNIT` - The unit name of the unit this hook is
     being invoked in. (ex: myblog/0)

   * `$JUJU_SERVICE` - The name of the service for which this hook
     is running. (ex: myblog)

   * `$JUJU_CHARM` - The name of the charm which deployed the
     unit the hook is running in. (ex: wordpress)


Hooks called for relationships will have the follow additional
environment variables available to them.

    * `$JUJU_MEMBERS` - A space-delimited list of qualified
      relationship ids uniquely specifying all the UAs participating in
      a given relationship. (ex. "wordpress/1 worpress/2")

    * `$JUJU_RELATION` - The relation name this hook is running
      for.  It's redundant with the hook name, but is necessary for
      the command line tools to know the current context.

    * `$JUJU_REMOTE_UNIT` - The unit name of the remote unit
      which has triggered the hook execution.


Open issues
-----------

There are still a number of open issues with this specification. There
is still open debate if the UA runs inside the same process
space/container and how this will play out with security. This has
ramifications to this specification as well as we'd take steps to make sure
client code cannot violate the ZK juju by connection with their
own copy of the code on a known port.

There specification doesn't define 100% which command line tools will
get which environment settings.

