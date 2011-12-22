Agent presence and settings
===========================

Agents are a set of distributed processes within the juju
framework tasked individually with various juju roles. Each agent
process interacts with zookeeper state and its environment to perform
its role.

Common to all agents are the need to make their presence known, such
that it can be monitored for availability, as well the need for storage
so an agent can record its state.

Presence
--------

The presence/aliveness of an agent process within the zookeeper state
hierarchy is denoted by an ephemeral node. This ephemeral presence
node is also used to store transient settings by the agent
process. These transient values will have a scope of the agent process
lifetime. These ephemeral presence nodes are stored under the /agents
container in a hierarchy, according to their agents role. Agents
fufill many different roles within the juju system. Within the
/agents container hierarchy, each agent's ephemeral node is contained
within an <agent-role> container.

For example, unit agents are stored in the following container::

  /agents/unit/
   
And provisioning agents in::

  /agents/provisioning/

The agent presence node within these role containers is further
distinguished by the id it chooses to use within the container. Some
agents are commonly associated to a persistent domain object, such as
a unit or machine, in that case they will utilize the persistent domain
object's id for their node name.

For example, a unit agent for unit 11 (display name: mysql/0), would
have a presence node at::

  /agents/unit/unit-11

For agents not associated to a persistent domain object, the number of
agents is determined by configuration, and they'll utilize an ephemeral
sequence to denote their id. For example the first provisioning agent
process in the system would have a path::

 /agents/provisioning/provisioning-0000000000

and the second

 /agents/provisioning/provisioning-0000000001

Persistence
-----------

All agents are able to store transient settings of the agent process
within their ephemeral presence nodes within zookeeper. If an agent
needs persistent settings, they should be stored on an associated
peristent domain object.


Availability	
------------

One of the key features of the juju framework, is an absence of
single points of failures. To enable availability across agents we'll
run multiple instances of agents as appropriate, monitor the presence
of agents, and restart them as nescessary. Using the role information
and the agent id as encoded in the presence node path, we can dispatch
appropriate error handling and recovery logic, ie. restart a unit
agent, or provisioning agent.

For agents providing cluster wide services, it will be typical to have
multiple agents for each role (ie. provisionig, recovery).

A recovery agent will need to distinguish causal factors regarding the
disappearance of a unit presence node. In addition to error scenarios,
the configuration state may change such that an agent is no longer
nescessary, for example an unused machine being terminated, or a unit no
longer being assigned to a machine. To facilitate identiying the
cause, a recovery agent would subscribe to the topology to distinguish
configuration change vs. a runtime change. For agents not associated
to a persistent domain object, this identification will be based on
examining the configured number of agent for the role, and verifying
that it matches the runtime state.


Startup and recovery
-------------------- 

On startup, an agent will attempt to create its presence node. For
agents associated to persistent domain objects, this process will
either succeed, or result in an error due to an existing agent already
in place, as the ids used are unique to a single instance of the agent
since the id is based on the domain object id. 

For agents not attached to persistent domain objects, they should
verify their configuration parameter for the total number of agents
for the role. 

In the case of a conflict or a satisified configuration the agent
process should terminate with an error message.


Agent state API
---------------


``IAgentAssociated``::

  """An api for persistent domain objects associated to an agent."""
 
  def has_agent():
      """Return boolean whether the agent (presence node) exists."""

  def get_agent_state():
      """Retrieve the agent associated to this domain object."""

  def connect_agent():
      """Create an agent presence node.
      
      This serves to connect the agent process with its agent state,
      and will create the agent presence node if doesn't exist, else
      raise an exception.

      Returns an agent state.
      """


``IAgentState``::

  def get_transient_data():
      """
      Retrieve the transient data for the agent as a byte string.
      """

  def set_transient_state(data):
      """
      Set the transient data for the agent as a byte string.
      """

  def get_domain_object(): 
      """ 
      TBD if Desireable. An agent attached to a persistent domain
      object has all the knowledge to retrieve the associated
      persistent domain object. For a machine agent state, this would
      retrieve the machine state. For a unit agent state this would
      retrieve the unit. Most agent implementations will already have
      access to the domain object, and will likley retrieve or create
      the agent from it.  
      """
