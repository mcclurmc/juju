Local provider configuration
----------------------------

The local provider allows for deploying services directly against the local/host machine
using LXC containers with the goal of experimenting with juju and developing formulas.

The local provider has some additional package dependencies. Attempts to use
this provider without these packages installed will terminate with a message
indicating the missing packages.

The following are packages are required.

 - libvirt-bin
 - lxc
 - apt-cacher-ng
 - zookeeper


The local provider can be configured by specifying "provider: local" and a "data-dir":
as an example::

  local:
    type: local
    data-dir: /tmp/local-dev
    admin-secret: b3a5dee4fb8c4fc9a4db04751e5936f4
    default-series: oneiric

Upon running ``juju bootstrap`` a zookeeper instance will be started on the host
along with a machine agent. The bootstrap command will prompt for sudo access
as the machine agent needs to run as root in order to create containers on the
local machine.

The containers created are namespaced in such a way that you can create multiple
environments on a machine. The containers also namespaced by user for multi-user
machines.

Local provider environments do not survive reboots of the host at this time, the
environment will need to be destroyed and recreated after a reboot.


Provider specific options
=========================

  data-dir: 
    Directory for zookeeper state and log files. 
   


 


