.. _user-tutorial:

User tutorial
=============

Introduction
------------

This tutorial demonstrates basic features of juju from a user perspective.
An juju user would typically be a devops or a sys-admin who is interested in
automated deployment and management of servers and services.

Bootstrapping
-------------

The first step for deploying an juju system is to perform bootstrapping.
Bootstrapping launches a utility instance that is used in all subsequent
operations to launch and orchestrate other instances::

  $ juju bootstrap

Note that while the command should display a message indicating it has finished
successfully, that does not mean the bootstrapping instance is immediately
ready for usage. Bootstrapping an instance can require a couple of minutes. To
check on the status of the juju deployment, we can use the status command::

  $ juju status

If the bootstrapping node has not yet completed bootstrapping, the status
command may either mention the environment is not yet ready, or may display a
connection timeout such as::

  INFO Connecting to environment.
  ERROR Connection refused
  ProviderError: Interaction with machine provider failed:
  ConnectionTimeoutException('could not connect before timeout after 2
  retries',)
  ERROR ProviderError: Interaction with machine
  provider failed: ConnectionTimeoutException('could not connect before timeout
  after 2 retries',)

This is simply an indication the environment needs more time to complete
initialization. It is recommended you retry every minute. Once the environment
has properly initialized, the status command should display::

  machines:
    0: {dns-name: ec2-50-16-61-111.compute-1.amazonaws.com, instance-id: i-2a702745}
    services: {}

Note the following, machine "0" has been started. This is the bootstrapping
node and the first node to be started. The dns-name for the node is printed.
Also the EC2 instance-id is printed. Since no services are yet deployed to the
juju system yet, the list of deployed services is empty

Starting debug-log
------------------

While not a requirement, it is beneficial for the understanding of juju to
start a debug-log session. juju's debug-log provides great insight into the
execution of various hooks as they are triggered by various events. It is
important to understand that debug-log shows events from a distributed
environment (multiple-instances). This means that log lines will alternate
between output from different instances. To start a debug-log session, from a
secondary terminal issue::

  $ juju debug-log
  INFO Connecting to environment.
  INFO Enabling distributed debug log.
  INFO Tailing logs - Ctrl-C to stop.

This will connect to the environment, and start tailing logs.

Deploying service units
-----------------------

Now that we have bootstrapped the juju environment, and started the
debug-log viewer, let's proceed by deploying a mysql service::

  $ juju deploy --repository=/usr/share/doc/juju/examples local:oneiric/mysql
  INFO Connecting to environment.
  INFO Charm deployed as service: 'mysql'
  INFO 'deploy' command finished successfully

Checking the debug-log window, we can see the mysql service unit being
downloaded and started::

  Machine:1: juju.agents.machine DEBUG: Downloading charm
  local:oneiric/mysql-11...
  Machine:1: juju.agents.machine INFO: Started service unit mysql/0

It is important to note the different debug levels. DEBUG is used for very
detailed logging messages, usually you should not care about reading such
messages unless you are trying to debug (hence the name) a specific problem.
INFO debugging level is used for slightly more important informational
messages. In this case, these messages are generated as the mysql charm's
hooks are being executed. Let's check the current status::

  $ juju status
  machines:
    0: {dns-name: ec2-50-16-61-111.compute-1.amazonaws.com, instance-id: i-2a702745}
    1: {dns-name: ec2-50-16-117-185.compute-1.amazonaws.com, instance-id: i-227e294d}
  services:
    mysql:
      charm: local:oneiric/mysql-11
      relations: {}
      units:
        mysql/0:
          machine: 1
          relations: {}
          state: null

We can see a new EC2 instance has now been spun up for mysql. Information for
this instance is displayed as machine number 1 and mysql is now listed under
services. It is apparent the mysql service unit has no relations, since it has
not been connected to wordpress yet. Since this is the first mysql service
unit, it is being referred to as mysql/0, subsequent service units would be
named mysql/1 and so on.

.. note::
        An important distinction to make is the difference between a service
        and a service unit. A service is a high level concept relating to an
        end-user visible service such as mysql. The mysql service would be
        composed of several mysql service units referred to as mysql/0, mysql/1
        and so on.

The mysql service state is listed as null since it's not ready yet.
Downloading, installing, configuring and starting mysql can take some time.
However we don't have to wait for it to configure, let's proceed deploying
wordpress::

  $ juju deploy --repository=/usr/share/doc/juju/examples local:oneiric/wordpress

Let's wait for a minute for all services to complete their configuration cycle and
get properly started, then issue a status command::

  $ juju status
  machines:
    0: {dns-name: ec2-50-16-61-111.compute-1.amazonaws.com, instance-id: i-2a702745}
    1: {dns-name: ec2-50-16-117-185.compute-1.amazonaws.com, instance-id: i-227e294d}
    2: {dns-name: ec2-184-72-156-54.compute-1.amazonaws.com, instance-id: i-9c7e29f3}
  services:
    mysql:
      charm: local:oneiric/mysql-11
      relations: {}
      units:
        mysql/0:
          machine: 1
          relations: {}
          state: started
    wordpress:
      charm: local:oneiric/wordpress-29
      relations: {}
      units:
        wordpress/0:
          machine: 2
          relations: {}
          state: started

mysql/0 as well as wordpress/0 are both now in the started state. Checking the
debug-log would reveal wordpress has been started as well

Adding a relation
-----------------

While mysql and wordpress service units have been started, they are still
isolated from each other. An important concept for juju is connecting
various service units together to create a bigger juju! Adding a relation
between service units causes hooks to trigger, in effect causing all service
units to collaborate and work together to reach the desired end state. Adding a
relation is extremely simple::

  $ juju add-relation wordpress mysql
  INFO Connecting to environment.
  INFO Added mysql relation to all service units.
  INFO 'add_relation' command finished successfully

Checking the juju status we see that the db relation now exists with state
up::

  $ juju status
  machines:
    0: {dns-name: ec2-50-16-61-111.compute-1.amazonaws.com, instance-id: i-2a702745}
    1: {dns-name: ec2-50-16-117-185.compute-1.amazonaws.com, instance-id: i-227e294d}
    2: {dns-name: ec2-184-72-156-54.compute-1.amazonaws.com, instance-id: i-9c7e29f3}
  services:
    mysql:
      charm: local:oneiric/mysql-11
      relations: {db: wordpress}
      units:
        mysql/0:
          machine: 1
          relations:
            db: {state: up}
          state: started
    wordpress:
      charm: local:oneiric/wordpress-29
      relations: {db: mysql}
      units:
        wordpress/0:
          machine: 2
          relations:
            db: {state: up}
          state: started

Exposing the service to the world
---------------------------------

All that remains is to expose the service to the outside world::

    $ juju expose wordpress

You can now point your browser at the public dns-name for instance 2 (running
wordpress) to view the wordpress blog

Tracing hook execution
----------------------

An juju user should never have to trace the execution order of hooks,
however if you are the kind of person who enjoys looking under the hood, this
section is for you. Understanding hook order execution, the parallel nature of
hook execution across instances, and how relation-set in a hook can trigger the
execution of another hook is quite interesting and provides insight into
juju internals

Here are a few important messages from the debug-log of this juju run.  The
date field has been deliberately left in this log, in order to understand the
parallel nature of hook execution.

Things to consider while reading the log include:
 * The time the log message was generated
 * Which service unit is causing the log message (for example mysql/0)
 * The message logging level. In this run DEBUG messages are generated by the
   juju core engine, while WARNING messages are generated by calling
   juju-log from inside charms (which you can read in the examples
   folder)

Let's view select debug-log messages which can help understand the execution
order::

  14:29:43,625 unit:mysql/0: hook.scheduler DEBUG: executing hook for wordpress/0:joined
  14:29:43,626 unit:mysql/0: unit.relation.lifecycle DEBUG: Executing hook db-relation-joined
  14:29:43,660 unit:wordpress/0: hook.scheduler DEBUG: executing hook for mysql/0:joined
  14:29:43,660 unit:wordpress/0: unit.relation.lifecycle DEBUG: Executing hook db-relation-joined
  14:29:43,661 unit:wordpress/0: unit.relation.lifecycle DEBUG: Executing hook db-relation-changed
  14:29:43,789 unit:mysql/0: unit.hook.api WARNING: Creating new database and corresponding security settings
  14:29:43,813 unit:wordpress/0: unit.hook.api WARNING: Retrieved hostname: ec2-184-72-156-54.compute-1.amazonaws.com
  14:29:43,976 unit:mysql/0: unit.relation.lifecycle DEBUG: Executing hook db-relation-changed
  14:29:43,997 unit:wordpress/0: hook.scheduler DEBUG: executing hook for mysql/0:modified
  14:29:43,997 unit:wordpress/0: unit.relation.lifecycle DEBUG: Executing hook db-relation-changed
  14:29:44,143 unit:wordpress/0: unit.hook.api WARNING: Retrieved hostname: ec2-184-72-156-54.compute-1.amazonaws.com
  14:29:44,849 unit:wordpress/0: unit.hook.api WARNING: Creating appropriate upload paths and directories
  14:29:44,992 unit:wordpress/0: unit.hook.api WARNING: Writing wordpress config file /etc/wordpress/config-ec2-184-72-156-54.compute-1.amazonaws.com.php
  14:29:45,130 unit:wordpress/0: unit.hook.api WARNING: Writing apache config file /etc/apache2/sites-available/ec2-184-72-156-54.compute-1.amazonaws.com
  14:29:45,301 unit:wordpress/0: unit.hook.api WARNING: Enabling apache modules: rewrite, vhost_alias
  14:29:45,512 unit:wordpress/0: unit.hook.api WARNING: Enabling apache site: ec2-184-72-156-54.compute-1.amazonaws.com
  14:29:45,688 unit:wordpress/0: unit.hook.api WARNING: Restarting apache2 service


Scaling the juju
--------------------

Assuming your blog got really popular, is having high load and you decided to
scale it up (it's a cloud deployment after all). juju makes this magically
easy. All what is needed is::

  $ juju add-unit wordpress
  INFO Connecting to environment.
  INFO Unit 'wordpress/1' added to service 'wordpress'
  INFO 'add_unit' command finished successfully
  $ juju status
  machines:
    0: {dns-name: ec2-50-16-61-111.compute-1.amazonaws.com, instance-id: i-2a702745}
    1: {dns-name: ec2-50-16-117-185.compute-1.amazonaws.com, instance-id: i-227e294d}
    2: {dns-name: ec2-184-72-156-54.compute-1.amazonaws.com, instance-id: i-9c7e29f3}
    3: {dns-name: ec2-50-16-156-106.compute-1.amazonaws.com, instance-id: i-ba6532d5}
  services:
    mysql:
      charm: local:oneiric/mysql-11
      relations: {db: wordpress}
      units:
        mysql/0:
          machine: 1
          relations:
            db: {state: up}
          state: started
    wordpress:
      charm: local:oneiric/wordpress-29
      relations: {db: mysql}
      units:
        wordpress/0:
          machine: 2
          relations:
            db: {state: up}
          state: started
        wordpress/1:
          machine: 3
          relations:
            db: {state: up}
          state: started


The add-unit command starts a new wordpress instance (wordpress/1), which then
joins the relation with the already existing mysql/0 instance. mysql/0 notices
the database required has already been created and thus decides all needed
configuration has already been done. On the other hand wordpress/1 reads
service settings from mysql/0 and starts configuring itself and joining the
juju. Let's review a short version of debug-log for adding wordpress/1::

  14:36:19,755 unit:mysql/0: hook.scheduler DEBUG: executing hook for wordpress/1:joined
  14:36:19,755 unit:mysql/0: unit.relation.lifecycle DEBUG: Executing hook db-relation-joined
  14:36:19,810 unit:wordpress/1: hook.scheduler DEBUG: executing hook for mysql/0:joined
  14:36:19,811 unit:wordpress/1: unit.relation.lifecycle DEBUG: Executing hook db-relation-joined
  14:36:19,811 unit:wordpress/1: unit.relation.lifecycle DEBUG: Executing hook db-relation-changed
  14:36:19,918 unit:mysql/0: unit.hook.api WARNING: Database already exists, exiting
  14:36:19,938 unit:mysql/0: unit.relation.lifecycle DEBUG: Executing hook db-relation-changed
  14:36:19,990 unit:wordpress/1: unit.hook.api WARNING: Retrieved hostname: ec2-50-16-156-106.compute-1.amazonaws.com
  14:36:20,757 unit:wordpress/1: unit.hook.api WARNING: Creating appropriate upload paths and directories
  14:36:20,916 unit:wordpress/1: unit.hook.api WARNING: Writing wordpress config file /etc/wordpress/config-ec2-50-16-156-106.compute-1.amazonaws.com.php
  14:36:21,088 unit:wordpress/1: unit.hook.api WARNING: Writing apache config file /etc/apache2/sites-available/ec2-50-16-156-106.compute-1.amazonaws.com
  14:36:21,236 unit:wordpress/1: unit.hook.api WARNING: Enabling apache modules: rewrite, vhost_alias
  14:36:21,476 unit:wordpress/1: unit.hook.api WARNING: Enabling apache site: ec2-50-16-156-106.compute-1.amazonaws.com
  14:36:21,682 unit:wordpress/1: unit.hook.api WARNING: Restarting apache2 service

Destroying the environment
--------------------------

Once you are done with an juju deployment, you need to terminate
all running instances in order to stop paying for them. The
destroy-environment command will terminate all running instances in an
environment::

  $ juju destroy-environment

juju will ask for user confirmation before proceeding as this
command will destroy service data in the environment as well.
