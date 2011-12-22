ZooKeeper
=========

This document describes the reasoning behind juju's use of ZooKeeper,
and also the structure and semantics used by juju in the ZooKeeper
filesystem.

juju & ZooKeeper
--------------------

ZooKeeper offers a virtual filesystem with so called *znodes* (we'll
refer to them simply as *nodes* in this document).  The state stored in
the filesystem is fully introspectable and observable, and the changes
performed on it are atomic and globally ordered.  These features are
used by juju to maintain its distributed runtime state in a reliable
and fault tolerant fashion.

When some part of juju wants to modify the runtime state anyhow,
rather than enqueuing a message to a specific agent, it should instead
perform the modification in the ZooKeeper representation of the state,
and the agents responsible for enforcing the requested modification
should be watching the given nodes, so that they can realize the changes
performed.

When compared to traditional message queueing, this kind of behavior
enables easier global analysis, fault tolerance (through redundant
agents which watch the same states), introspection, and so on.


Filesystem Organization
-----------------------

The semantics and structures of all nodes used by juju in its
ZooKeeper filesystem usage are described below.  Each entry here maps
to a node, and the semantics of the given node are described right
below it.

Note that, unlike a traditional filesystem, nodes in ZooKeeper may
hold data, while still being a parent of other nodes.  In some cases,
information is stored as content for the node itself, in YAML format.
These are noted in the tree below under a bulleted list and *italics*.
In other cases, data is stored inside a child node, noted in the tree
below as indented **/bold**.  The decision around whether to use a
child node or content in the parent node revolves around use cases.


.. Not for now:

        .. _/files:

        **/files**
          Holds information about files stored in the machine provider.  Each
          file stored in the machine provider's storage location must have a
          entry here with metadata about the file.

          **/<filename>:<sha256>**
            The name of nodes here is composed by a plain filename, a colon, and
            the file content's sha256.  As of today these nodes are empty, since
            the node name itself is enough to locate it in the storage, and to
            assess its validity.

**/topology**
  Describes the current topology of machines, services, and service units.  Nodes
  under ``/machines``, ``/services``, and ``/units``, should not be considered
  as valid unless they are described in this file.  The precise format of this
  file is an implementation detail.

**/charms**
  Each charm used in this environment must have one entry inside this
  node.

  :Readable by: Everyone

  **/<namespace>:<name>-<revision>**
    Represents a charm available in this environment.  The node name
    includes the charm namespace (ubuntu, ~user, etc), the charm name,
    and the charm revision.

    - *sha256*: This option contains the sha256 of a file in the file
      storage, which contains the charm bundle itself.

    - *metadata*: Contains the metadata for the charm itself.

    - *schema*: The settings accepted by this charm. The precise details
      of this are still unspecified.

**/services**
  Each charm to be deployed must be included under an entry in
  this tree.

  :Readable by: Everyone

  **/service-<0..N>**
    Node with details about the configuration for one charm, which can
    be used to deploy one or more charm instances for this specific
    charm.

    - *charm*: The charm to be deployed.  The value of this option should
      be the name of a child node under the ``/charms`` parent.

    **/settings**
      Options for the charm provided by the user, stored internally in
      YAML format.

      :Readable by: Charm Agent
      :Writable by: Admin tools

**/units**
  Each node under this parent reflects an actual service agent which should
  be running to manage a charm.

  **/unit-<0..N>**
    One running service.

    :Readable by: Charm Agent
    :Writable by: Charm Agent

    **/machine**
      Contains the internal machine id this service is assigned to.

    **/charm-agent-connected**
      Ephemeral node which exists when a charm agent is handling
      this instance.


**/machines**

  **/machine-<0..N>**

    **/provisioning-lock**
      The Machine Provisioning Agent

    **/machine-agent-connected**
      Ephemeral node created when the Machine Agent is connected.

    **/info**
      Basic information about this machine.

      - *public-dns-name*: The public DNS name of this machine.
      - *machine-provider-id*: ie. EC2 instance id. 


Provisioning a new machine
--------------------------

When the need for a new machine is determined, the following sequence of
events happen inside the ZooKeeper filesystem to deploy the new machine:

1. A new node is created at ``/machines/instances/<N>``.
2. Machine Provisioning Agent has a watcher on ``/machines/instances/``, and
   gets notified about the new node.
3. Agent acquires a provisioning lock at
   ``/machines/instances/<N>/provisioning-lock``
4. Agent checks if the machine still has to be provisioned by verifying
   if ``/machines/instances/<N>/info`` exists.
5. If the machine has provider launch information, than the agent schedules
   to come back to the machine after ``<MachineBootstrapMaxTime>``.
6. If not, the agent fires the machine via the provider and stores the
   provider launch info (ie. EC2 machine id, etc.) and schedules the
   to come back to the machine after ``<MachineBootstrapMaxTime>``.
7. As a result of a schedule call the machine provider verifies the
   existence of a ``/machines/instance/<N>/machine-agent-connected`` node
   and if it does sets a watch on it.
8. If the agent node doesn't exist after the <MachineBootstrapMaxTime> then
   the agent acquires the ``/machines/instances/<N>/provisioning-lock``, 
   terminates the instance, and goes to step 6. 


Bootstrap Notes
~~~~~~~~~~~~~~~

This verification of the connected machine agent helps us guard against any
transient errors that may exist on a given virtual node due to provider 
vagaries.

When a machine provisioning agent comes up, it must scan the entire instance
tree to verify all nodes are running. We need to keep some state to distinguish
a node that has never come up from a node that has had its machine agent connection
die so that a new provisioning agent can distinguish between a new machine bootstrap
failure and an running machine failure.

use a one time password (otp) via user data to guard the machine agent 
permanent principal credentials.

TODO... we should track a counter to keep track of how many times we've
attempt to launch a single instance.


Connecting a Machine
--------------------

When a machine is launched, we utilize cloud-init to install the requisite
packages to run a machine agent (libzookeeper, twisted) and launch the 
machine agent.

The machine agent reads its one time password from ec2 user-data and connects
to zookeeper and reads its permanent principal info and role information which
it adds to its connection.

The machine agent reads and sets a watch on 
``/machines/instances/<N>/services/``. When a service is placed there the agent
resolve its charm, downloads the charm, creates an lxc container, and launches
a charm agent within the container passing the charm path.

Starting a Charm
------------------

The charm agent connects to zookeeper using principal information provided
by the machine agent. The charm agent reads the charm metadata, and 
installs any package dependencies, and then starts invoking charm hooks.

The charm agent creates the ephemeral node 
``/services/<service name>/instances/<N>/charm-agent-connected``.

The charm is running when....
