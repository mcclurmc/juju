Service exposing implementation details
=======================================


Not in scope
------------

It is not in the scope of this specification to determine mapping to a
public DNS or other directory service.


Implementation of ``expose`` and ``unexpose`` subcommands
---------------------------------------------------------

Two new user commands were added::

    juju expose <service name>

    juju unexpose <service name>

These commands set and remove a flag znode, **/services/<internal
service id>/exposed**, respectively.


Hook command additions
----------------------

Two new hook commands were added for opening and closing ports. They
may be executed within any charm hook::

    open-port port[/protocol]

    close-port port[/protocol]

These commands store in the ZK tree, under **/units/<internal unit
id>/ports**, the desired opened port information as serialized to
JSON. For example, executing ``open-port 80`` would be serialized as
follows::

    {"open": [{"port": 80, "proto": "tcp"}, ...]}

This format accommodates tracking other ancillary information for
exposing services.

These commands are executed immediately within the hook.


New ``exposed`` and ``unexposed`` service hooks
-----------------------------------------------

The ``exposed`` service hook runs upon a service being exposed with
the ``juju expose`` command. As part of the unit workflow, it is
scheduled to run upon the existence of **/services/<internal service
id>/exposed** and the service unit being in the ``started`` state.

Likewise, the ``unexposed`` service hook runs upon the removal of a
**/services/<internal service id>/exposed** flag znode.

These hooks will be implemented at a future time.


``juju status`` display of opened ports
-------------------------------------------

If a service has been exposed, then the juju status output is
augmented. For the YAML serialization, for each exposed service, the
``exposed`` key is added, with the value of ``true``. (It is not
displayed otherwise.) For each service unit of an exposed service with
opened ports, the ``open-ports`` key is added, with its value a
sequence of ``port/proto`` strings. If no ports are opened, its value
is an empty list.
  

Provisioning agent implementation
---------------------------------

The provisioning agent currently is the only place within juju
that can take global actions with respect to the provider. Consequently,
provisioning is currently responsible for the current, if simple EC2
security group management (with the policy of open all ports, seen in
the code `juju.providers.ec2.launch.EC2LaunchMachine`).

The provisioning agent watches for the existence of
**/services/<internal service id>/exposed**, and if so watches the
service units settings **/units/<internal unit id>/ports** and makes
changes in the firewall settings through the provider.

For the EC2 provider, this is done through security groups (see
below). Later we will revisit to let a machine agent do this in the
context of iptables, so as to get out of the 500 security group limit
for EC2, enable multiple service units per machine, be generic with
other providers, and to provide future support for internal firewall
config.


EC2 provider implementation
---------------------------

Prior to the launch of a new machine instance, a unique EC2 security
group is added. The machine instance is then assigned to this group at
launch. Likewise, terminating the machine will result in the EC2
provider deleting the security group for the machine. (This cleanup
will be implemented in a future branch.)

Given this model of a security group per machine, with one service
unit per machine, exposing and unexposing ports for a service unit
corresponds to EC2's support for authorization and revocation of ports
per security group. In particular, EC2 supports a source address of
``0.0.0.0/0`` that corresponds to exposing the port to the world.

To make this concrete, consider the example of exposing the
``my-wordpress`` service. Once the command ``open-port 80`` has been
run on a given service unit of ``my-wordpress``, then for the
corresponding machine instance, the equivalent of this EC2 command is
run::

    ec2-authorize $MACHINE_SECURITY_GROUP -P tcp -p 80 -s 0.0.0.0/0

``$MACHINE_SECURITY_GROUP`` is named ``juju-ENVIRONMENT-MACHINE_ID``,
eg. something like ``juju-prod-2``.

Any additional service units of ``my-wordpress``, if they run
``open-port 80``, will likewise invoke the equivalent of the above
command, for the corresponding machine security groups.

If ``my-wordpress`` is unexposed, a ``my-wordpress`` service unit is
removed, the ``my-wordpress`` service is destroyed, or the
``close-port`` command is run for a service unit, then the equivalent
of the following EC2 command is run, for all applicable machines::

    ec2-revoke $MACHINE_SECURITY_GROUP -P tcp -p 80 -s 0.0.0.0/0

Although this section showed the equivalent EC2 commands for
simplicity, txaws is used for the actual implementation.


Implementation plan
-------------------

The following functionality needs to be added. This should divisible
into separate, small branches:

    * Implement exposed and unexposed hooks.
