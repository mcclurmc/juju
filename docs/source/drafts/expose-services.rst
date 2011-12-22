Exposing a service
==================

The following functionality will be implemented at a later date.


``exposed`` and ``unexposed`` hooks
-----------------------------------

Upon a service being exposed, the ``exposed`` hook will be run, if it
is present in the charm.

This may be an appropriate place to run the ``open-port`` command,
however, it is up to the charm author where it should be run, since
it and ``close-port`` are available commands for every hook.

Likewise, when a service is unexposed, the ``unexposed`` hook will be
run, if present. Many charms likely do not need to implement this
hook, however, it could be an opportunity to terminate unnecessary
processes or remove other resources.
