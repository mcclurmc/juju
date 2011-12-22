Unit Agent startup
==================

Introduction
------------

The unit agent manages a state machine workflow for the unit. For each
transition the agent records the current state of the unit and stores
that information as defined below. If the agent dies, or is restarted
for any reason, the agent will resume the workflow from its last known
state.

The available workflow states and transitions are::

    "new" -> "ready" [label="install"]
    "new" -> "install-error" [label="error-install"]
    "ready" -> "running" [label="start"]
    "ready" -> "start-error" [label="error-start"]
    "running" -> "ready" [label="stop"]
    "running" -> "stop-error" [label="error-stop"]

The agent does not have any insight into external processes that the
unit's charm may be managing, it's sole responsibility is executing
hooks in deterministic fashion as a consequence of state changes.

Charm hook execution (excepting relation hooks), corresponds to
invoking a transition on the unit workflow state. Any errors during a
transition, will prevent a state change. All state changes are
recorded persistently on the unit state. If a state change fails, it
will be reattempted until a max number of retries, after which the
unit workflow will be transitioned to failure state specific to the
current state and attempted transition, and administrator intervention
will be required to resolve.

On startup the agent will, establish its presence node (as per the
agent state spec), and read the state of the unit. If the unit is not
running it will have its transition hooks executed to place it in the
running state.

The persistent state of the unit as per this state machine is stored
locally on disk of the unit. This allows for the continuation of long
running tasks in the face of transient communication failures with zk.
For example if a long running install task is kicked off then it may
complete and record the transition to persistent state even if the zk
connection is not available when the install hook has completed.

The persistent workflow state of the unit is also replicated to
zookeeper for introspectability, and communication of local failures
to the global coordination space. The zk state for this workflow is
considered non-authoritative by the unit-agent if its operating in a
disconnected mode.


Startup sequence
----------------

The following outlines the set of steps a unit agent executes when
starting up on a machine resource.
 
 - Unit agent process starts, inspects its configuration and
   environment.
 
 - A zookeeper client handle is obtained.
 
 - The agent retrieves its unit state, via the service state manager.
 
 - The agent retrieves its service relations, via the relation state
   manager.
 
At deployment time, a service is deployed with its dependencies. Those
dependencies are actualized in relations between the services that are
being deployed. There are several times of relations that can be
established. The most common is a client/server relationship, like a
client application and a database server. Each of the services in such
a relation performs a role within that relation. In this case the
database performs the 'server' role, and the client application
performs the 'client' role. When actualizing the service relations,
the physical layout within the coordination space (zookeeper) takes
these roles into account.
 
For example in the client server relation, the service performing the
'server' role has its units under a service-role container named
'server' denoting the role of its units in the relation.

For each service relation, the agent will
 
 - Creates its ``/relations/relation-1/settings/unit-X`` relation
   local data node, if it doesn't exist.
 
 - Creates its ``/relations/relation-1/<service-role>/unit-X`` if it
   doesn't exist. The node is not considered 'established' for the
   purposes of hook execution on other units till this node exists.
 
 - Establish watches as outlined below.
 

Unit relation observation
-------------------------

Based on the relation type and the unit's service role, the unit agent
will establish will retrieve and establish watches on the other units
in the relation.
 
The relation type determines which service role container the
container will get and observe children of. In a client server
relation there would be both::
 
    /relations/relation-1/server
    /relations/relation-1/client
 
And a client unit would observe and process the unit children of the
server node which functions as the service-role representing the
endpoint of the relation. In a peer relation there would be a
service-role container with the path ``/relations/relation-1/peer``
which would be observed and processed.
 
 - The unit agent will get the children and establish a watch (w-1) on
   the service role container in the relationship.
 
 - For each unit found, the relation local data node
   ``/relations/relation-X/settings/unit-X`` will have a get watch
   (w-2) established .
 
 - the agent stores a process local variable noting which children its
   seen (v-1)
 
Finally after processing the children.
 
 - if the unit agent is completing its startup, and another
   'established' unit was found, the agent should fire the its
   relation-changed hook (type joined).
 
 
Watch behavior
--------------
 
 - (w-1) if the service-role child watch fires with a delete event,
   reestablish the watch, and execute the relation-changed hook (type
   departed), update variable (v-1)
 
 - (w-1) if the service-role child watch fires with a created event,
   reestablish the watch, and execute the relation-changed hook (type
   joined), update variable (v-1)

 - (w-1) if the service-role node child watch fires with a deleted
   event, the agent invokes the ``relation-broken`` hook. (the service
   role container was removed)
 
 - (w-3) if a unit relation local data node watch fires with a
   modified event, reestablish the watch, and execute the
   relation-changed hook (type changed) if the unit is in variable
   (v-1).
 
 - (w-3) if a unit relation local data node watch fires with a delete
   event, ignore (the agent exists watch must also have fired with a
   delet
