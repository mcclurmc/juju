.. _write-charm:

Writing a charm
===============

This tutorial demonstrates the basic workflow for writing, running and
debugging an juju charm. Charms are a way to package and share your
service deployment and orchestration knowledge and share them with the world.

Creating the charm
--------------------

In this example we are going to write a charm to deploy the drupal CMS
system. For the sake of simplicity, we are going to use the mysql charm that
comes bundled with juju in the examples directory. Assuming the current
directory is the juju trunk, let's create the directory hierarchy::

  $ cd examples/oneiric
  mkdir -p drupal/hooks
  vim drupal/metadata.yaml

Note: if you don't have the juju source tree available, the `examples` repository
is installed into `/usr/share/doc/juju`; you can copy the repository to your
current directory, and work from there.

Edit the metadata.yaml file to resemble::

  name: drupal
  revision: 1
  summary: "Drupal CMS"
  description: |
      Installs the drupal CMS system, relates to the mysql charm provided in
      examples directory. Can be scaled to multiple web servers
  requires:
    db:
      interface: mysql

The metadata.yaml file provides metadata around the charm. The file declares
a charm with the name drupal. Since this is the first time to edit this
charm, its revision number is one.  A short and long description of the
charm are provided. The final field is `requires`, this mentions the
interface type required by this charm. Since this drupal charm uses the
services of a mysql database, we need to require it in the metadata. Since this
charm does not provide a service to any other charm, there is no `provides`
field. You might be wondering where did the interface name "mysql" come from,
you can locate the interface information from the mysql charm's
metadata.yaml.  Here it is for convenience::

  name: mysql
  revision: 11
  summary: "MySQL relational database provider"
  description: |
      Installs and configures the MySQL package (mysqldb), then runs it.
  
      Upon a consuming service establishing a relation, creates a new
      database for that service, if the database does not yet
      exist. Publishes the following relation settings for consuming
      services:
  
        database: database name
        user: user name to access database
        password: password to access the database
        host: local hostname
  provides:
    db:
      interface: mysql

That very last line mentions that the interface that mysql provides to us is
"mysql". Also the description mentions that four parameters are sent to the
connecting charm (database, user, password, host) in order to enable it to
connect to the database. We will make use of those variables once we start
writing hooks. Such interface information is either provided in a bundled
README file, or in the description. Of course you can also read the charm
code to discover such information as well

Have a plan
-----------

When attempting to write a charm, it is beneficial to have a mental plan of
what it takes to deploy the software. In our case, you should deploy drupal
manually, understand where its configuration information is written, how the
first node is deployed, and how further nodes are configured. With respect to
this charm, this is the plan

  * Install hook installs all needed components (apache, php, drush)
  * Once the database connection information is ready, call drush on first node
    to perform the initial setup (creates DB tables, completes setup)
  * For scaling onto other nodes, the DB tables have already been set-up. Thus
    we only need to append the database connection information into drupal's
    settings.php file. We will use a template file for that

.. note::
  The hooks in a charm are executable files that can be written using any
  scripting or programming language. In our case, we'll use bash

For production charms it is always recommended that you install software
components from the Ubuntu archive (using apt-get) in order to get security
updates. However in this example I am installing drush (Drupal shell) using
apt-get, then using that to download and install the latest version of drupal.
If you were deploying your own code, you could just as easily install a
revision control tool (bzr, git, hg...etc) and use that to checkout a code
branch to deploy from. This demonstrates the flexibility offered by juju
which doesn't really force you into one way of doing things.

Write hooks
-----------

Let's change into the hooks directory::

  $ cd drupal/hooks
  vim install

Since you should have already installed drupal, you have an idea what it takes
to get it installed. My install script looks like::

  #!/bin/bash
  
  set -eux # -x for verbose logging to juju debug-log
  juju-log "Installing drush,apache2,php via apt-get"
  apt-get -y install drush apache2 php5-gd libapache2-mod-php5 php5-cgi mysql-client-core-5.1
  a2enmod php5
  /etc/init.d/apache2 restart
  juju-log "Using drush to download latest Drupal"
  # Typo on next line, it should be www not ww
  cd /var/ww && drush dl drupal --drupal-project-rename=juju

I have introduced an artificial typo on the last line "ww not www", this is to
simulate any error which you are bound to face sooner or later. Let's create
other hooks::

  $ vim start

The start hook is empty, however it needs to be a valid executable, thus we'll
add the first bash shebang line, here it is::

  #!/bin/bash

Here's the "stop" script::

  #!/bin/bash
  juju-log "Stopping apache"
  /etc/init.d/apache2 stop

The final script, which does most of the work is "db-relation-changed". This
script gets the database connection information set by the mysql charm then
sets up drupal for the first time, and opens port 80 for web access. Let's
start with a simple version that only installs drupal on the first node. Here
it is::

  #!/bin/bash
  set -eux # -x for verbose logging to juju debug-log
  hooksdir=$PWD
  user=`relation-get user`
  password=`relation-get password`
  host=`relation-get host`
  database=`relation-get database`
  # All values are set together, so checking on a single value is enough
  # If $user is not set, DB is still setting itself up, we exit awaiting next run
  [ -z "$user" ] && exit 0
  juju-log "Setting up Drupal for the first time"
  cd /var/www/juju && drush site-install -y standard \
  --db-url=mysql://$user:$password@$host/$database \
  --site-name=juju --clean-url=0
  cd /var/www/juju && chown www-data sites/default/settings.php
  open-port 80/tcp

The script is quite simple, it reads the four variables needed to connect to
mysql, ensures they are not null, then passes them to the drupal installer.
Make sure all the hook scripts have executable permissions, and change
directory above the examples directory::

  $ chmod +x *
  $ cd ../../../..

Checking on the drupal charm file-structure, this is what we have::

  $ find examples/oneiric/drupal
  examples/oneiric/drupal
  examples/oneiric/drupal/metadata.yaml
  examples/oneiric/drupal/hooks
  examples/oneiric/drupal/hooks/db-relation-changed
  examples/oneiric/drupal/hooks/stop
  examples/oneiric/drupal/hooks/install
  examples/oneiric/drupal/hooks/start

Test run
--------

Let us deploy the drupal charm. Remember that the install hook has a problem
and will not exit cleanly. Deploying::

  $ juju bootstrap

Wait a minute for the environment to bootstrap. Keep issuing the status command
till you know the environment is ready::

  $ juju status 
  2011-06-07 14:04:06,816 INFO Connecting to environment.
  machines: 0: {dns-name: ec2-50-19-154-237.compute-1.amazonaws.com, instance-id: i-6fb52301}
  services: {}
  2011-06-07 14:04:11,125 INFO 'status' command finished successfully

It can be beneficial when debugging a new charm to always have the
distributed debug-log running in a separate window::

  $ juju debug-log

Let's deploy the mysql and drupal charms::

  $ juju deploy --repository=examples local:oneiric/mysql
  $ juju deploy --repository=examples local:oneiric/drupal

Once the machines are started (hint: check the debug-log), issue a status
command::

  $ juju status
  machines:
    0: {dns-name: ec2-50-19-154-237.compute-1.amazonaws.com, instance-id: i-6fb52301}
    1: {dns-name: ec2-50-16-9-102.compute-1.amazonaws.com, instance-id: i-19b12777}
    2: {dns-name: ec2-50-17-147-79.compute-1.amazonaws.com, instance-id: i-e7ba2c89}
  services:
    drupal:
      charm: local:oneiric/drupal-1
      relations: {}
      units:
        drupal/1:
          machine: 4
          open-ports: []
          relations: {}
          state: install_error
    mysql:
      charm: local:oneiric/mysql-12
      relations: {}
      units:
        mysql/0:
          machine: 1
          relations: {}
          state: started

Note how mysql is listed as started, while drupal's state is install_error. This is
because the install hook has an error, and did not exit cleanly (exit code 1).

Debugging hooks
---------------

Let's debug the install hook, from a new window::

  $ juju debug-hooks drupal/0

This will connect you to the drupal machine, and present a shell. The way the
debug-hooks functionality works is by starting a new terminal window instead of
executing a hook when it is triggered. This way you get a chance of running the
hook manually, fixing any errors and re-running it again. In order to trigger
re-running the install hook, from another window::

  $ juju resolved --retry drupal/0

Switching to the debug-hooks window, you will notice a new window named
"install" poped up. Note that "install" is the name of the hook that this
debug-hooks session is replacing. We change directory into the hooks directory
and rerun the hook manually::

  $ cd /var/lib/juju/units/drupal-0/charm/hooks/
  $ ./install
  # -- snip --
  + cd /var/ww
  ./install: line 10: cd: /var/ww: No such file or directory

Problem identified. Let's edit the script, changing ww into www. Rerunning it
again should work successfully. This is why it is very good practice to write
hook scripts in an idempotent manner such that rerunning them over and over
always results in the same state. Do not forget to exit the install window by
typing "exit", this signals that the hook has finished executing successfully.
If you have finished debugging, you may want to exit the debug-hooks session
completely by typing "exit" into the very first window Window0

.. note::
  While we have fixed the script, this was done on the remote machine only. You
  need to update the local copy of the charm with your changes, increment the
  resivion number in metadata.yaml and perform a charm upgrade to push the
  changes, like::

  $ juju upgrade-charm --repository=examples/ drupal

Let's continue after having fixed the install error::

  $ juju add-relation mysql drupal

Watching the debug-log window, you can see debugging information to verify the
hooks are working as they should. If you spot any error, you can launch
debug-hooks in another window to start debugging the misbehaving hooks again.
Note that since "add-relation" relates two charms together, you cannot really
retrigger it by simply issuing "resolved --retry" like we did for the install
hook. In order to retrigger the db-relation-changed hook, you need to remove
the relation, and create it again like so::

  $ juju remove-relation mysql drupal
  $ juju add-relation mysql drupal

The service should now be ready for use. The remaining step is to expose it to
public access. While the charm signaled it needs port 80 to be open, for
public accessibility, the port is not open until the administrator explicitly
uses the expose command::

  $ juju expose drupal

Let's see a status with the ports exposed::

  $ juju status
  machines:
    0: {dns-name: ec2-50-19-154-237.compute-1.amazonaws.com, instance-id: i-6fb52301}
    1: {dns-name: ec2-50-16-9-102.compute-1.amazonaws.com, instance-id: i-19b12777}
    2: {dns-name: ec2-50-17-147-79.compute-1.amazonaws.com, instance-id: i-e7ba2c89}
  services:
    drupal:
      exposed: true
      charm: local:oneiric/drupal-1
      relations: {db: mysql}
      units:
        drupal/1:
          machine: 4
          open-ports: [80/tcp]
          relations:
            db: {state: up}
          state: started
    mysql:
      charm: local:oneiric/mysql-12
      relations: {db: drupal}
      units:
        mysql/0:
          machine: 1
          relations:
            db: {state: up}
          state: started


Congratulations, your charm should now be working successfully! The
db-relation-changed hook previously shown is not suitable for scaling drupal to
more than one node, since it always drops the database and recreates a new one.
A more complete hook would need to first check whether or not the DB tables
exist and act accordingly. Here is how such a hook might be written::

  #!/bin/bash
  set -eux # -x for verbose logging to juju debug-log
  hooksdir=$PWD
  user=`relation-get user`
  password=`relation-get password`
  host=`relation-get host`
  database=`relation-get database`
  # All values are set together, so checking on a single value is enough
  # If $user is not set, DB is still setting itself up, we exit awaiting next run
  [ -z "$user" ] && exit 0
  
  if $(mysql -u $user --password=$password -h $host -e 'use drupal; show tables;' | grep -q users); then
      juju-log "Drupal already set-up. Adding DB info to configuration"
      cd /var/www/juju/sites/default
      cp default.settings.php settings.php
      sed -e "s/USER/$user/" \
      -e "s/PASSWORD/$password/" \
      -e "s/HOST/$host/" \
      -e "s/DATABASE/$database/" \
      $hooksdir/drupal-settings.template >> settings.php
  else
      juju-log "Setting up Drupal for the first time"
      cd /var/www/juju && drush site-install -y standard \
      --db-url=mysql://$user:$password@$host/$database \
      --site-name=juju --clean-url=0
  fi
  cd /var/www/juju && chown www-data sites/default/settings.php
  open-port 80/tcp

.. note::
  Any files that you store in the hooks directory are transported as is to the
  deployment machine. You can drop in configuration files or templates that you
  can use from your hook scripts. An example of this technique is the
  drupal-settings.template file that is used in the previous hook. The template
  is rendered using sed, however any other more advanced template engine can be
  used

Here is the template file used::

  $databases = array (
    'default' =>
    array (
      'default' =>
      array (
        'database' => 'DATABASE',
        'username' => 'USER',
        'password' => 'PASSWORD',
        'host' => 'HOST',
        'port' => '',
        'driver' => 'mysql',
        'prefix' => '',
      ),
    ),
  );

Learn more
----------

Read more detailed information about :doc:`charm` and hooks. For more hook
examples, please check the examples directory in the juju source tree, or
check out the various charms already included in `Principia
<https://launchpad.net/principia>`_. 
