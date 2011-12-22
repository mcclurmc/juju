Exposing a service
==================

In juju, making a service public -- its ports available for public
use -- requires that it be explicitly *exposed*. Note that this
exposing does not yet involve DNS or other directory information. For
now, it simply makes the service public.

Service exposing works by opening appropriate ports in the firewall of
the cloud provider. Because service exposing is necessarily tied to
the underlying provider, juju manages all aspects of
exposing. Such management ensures that a charm can work with other
cloud providers besides EC2, once support for them is implemented.

juju provides the ``juju expose`` command to expose a service.
For example, you might have deployed a ``my-wordpress`` service, which
is defined by a ``wordpress`` charm. To expose this service, simply
execute the following command::

    juju expose my-wordpress

To stop exposing this service, and make any corresponding firewall
changes immediately, you can run this command::

    juju unexpose my-wordpress

You can see the status of your exposed ports by running the ``juju
status`` command. If ports have been opened by the service and you
have exposed the service, then you will see something like the
following output for the deployed services::

  services:
    wordpress:
      exposed: true
      charm: local:oneiric/wordpress-42
      relations: {db: mysql}
      units:
        wordpress/0:
          machine: 2
          open-ports: [80/tcp]
	  relations:
            db: {state: up}
          state: started
