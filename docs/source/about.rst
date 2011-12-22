About juju
==========

Since long ago, Linux server deployments have been moving towards the
collaboration of multiple physical machines. In some cases, different servers
run each a different set of applications, bringing organization, isolation,
reserved resources, and other desirable characteristics to the composed
assembly. In other situations, servers are set up with very similar
configurations, so that the system becomes more scalable by having load
distributed among the several instances, and so that the overall system becomes
more reliable when the failure of any individual machine does not affect the
assembly as a whole. In this reality, server administrators become invaluable
maestros which orchestrate the placement and connectivity of services within
the assembly of servers.

Given that scenario, it's surprising that most of the efforts towards advancing
the management of software configuration are still bound to individual machines.
Package managers, and software like dbus and gconf are examples of this. Other
efforts do look at the problem of managing multiple machines as a unit, but
interestingly, they are still a mechanism for scaling up the management of
services individually. In other words, they empower the administrator with the
ability to tweak the individual configuration of multiple services at once,
but they do not collaborate towards offering services themselves and other tools
the understanding of the composed juju. This distinction looks subtle in
principle, but it may be a key factor in enabling all the parties (system
administrators, software developers, vendors, and integrators) to collaborate
in deploying, maintaining, and enriching distributed software configurations.

This is the challenge which motivates the research happening through the
juju project at Canonical. juju aims to be a service deployment and
orchestration tool which enables the same kind of collaboration and ease of
use which today is seen around package management to happen on a higher
level, around services.  With juju, different authors are able to create
services independently, and make those services communicate through a simple
configuration protocol.  Then, users can take the product of both authors
and very comfortably deploy those services in an environment, in way
resembling how people are able to install a network of packages with a single
command via APT.
