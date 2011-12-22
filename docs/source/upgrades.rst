Upgrades
========

A core functionality of any configuration management system is
handling the full lifecycle of service and configuration
upgrades.

Charm upgrades
--------------

A common task when doing charm development is iterating over
charm versions via upgrading charms of a running service
while it's live.

The use case also extends to a user upgrading a deployed
service's charm with a newer version from an upsteam charm
repository.

In some cases a new charm version will also reference newer
software/package versions or new packages.

More details in the `charm upgrades documentation`_

.. _`charm upgrades documentation`: ./charm-upgrades.html


*NOTE* At the moment this is the only upgrade form that juju
provides.

Service upgrades
----------------

There's an interesting set of upgrade use cases which embody lots of
real world usage, which has been left for future work.

One case is where an application is deployed across multiple service
units, and the code needs to be upgraded in lock step across all of
them (either due to software incompatability or data changes in
related services).

Additionally the practices of a rolling uprade, and cloning a service
as an upgrade mechanism are also interesting problems, which are left
for future work.

juju upgrades
-------------

One last upgrade scenario, is upgrading of the juju software
itself.

At the moment juju is deployed from revision control, although it's
being packaged for the future. Currently all of the juju agents
maintain persistent connections to zookeeper, the failure of which may
be grounds for the system to take corrective action. As a simple
notion of performing system wide juju upgrades, the software would
be updated on the existing systems, and then the agents restarted but
instructed to keep their existing zookeeper session ids.
