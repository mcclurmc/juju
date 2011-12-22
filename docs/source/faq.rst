Frequently Asked Questions
==========================

Where does the name juju come from?

  It means magic in the same african roots where the word ubuntu comes from.
  Please see http://afgen.com/juju.html for a more detailed explanation.

Why is juju useful?

  juju is a next generation service deployment and orchestration
  framework.  It has been likened to APT for the cloud. With juju,
  different authors are able to create service charms independently, and
  make those services coordinate their communication through a simple
  protocol.  Users can then take the product of different authors and very
  comfortably deploy those services in an environment.  The result is
  multiple machines and components transparently collaborating towards
  providing the requested service.  Read more :doc:`about`

When will it be ready for production?

  As of Ubuntu Natty 11.04, juju is a technology preview. It is not yet
  ready to be used in production. However, adventurous users are encouraged to
  evaluate it, study it, start writing charms for it or start hacking on
  juju internals. The rough roadmap is to have juju packaged for
  Universe by 11.10 release and perhaps in main by 12.04

What language is juju developed in?

  juju itself is developed using Python. However, writing charms for
  juju can be done in any language. All juju cares about is finding a
  set of executable files, which it will trigger appropriately

Does juju start from a pre-configured AMI Image?

  No, juju uses a plain Ubuntu image. All needed components are installed
  at run-time. Then the juju charm is sent to the machine and hooks start
  getting executed in response to events

Is it possible to deploy multiple services per machine?

  Currently each service unit is deployed to a separate machine (ec2 instance)
  that can relate to other services running on different nodes. This was done
  to get juju into a working state faster. juju will definitely support
  multiple services per machine in the future

Is it possible to pass parameters to juju charms?

  Tunables are landing very soon in juju. Once ready you will be able to
  use "juju set service key=value" and respond to that from within the
  juju charm. This will enable dynamic features to be added to charms

Does juju only deploy to the Amazon EC2 cloud?

  Currently yes. However work is underway to enable deploying to LXC containers
  such that you are able to run juju charms on a single local machine
  Also integration work with the `Orchestra <https://launchpad.net/orchestra>`_
  project is underway to enable deployment to hardware machines

What directory are hooks executed in?

  Hooks are executed in the charm directory (the parent directory to the hook
  directory). This is primarily to encourage putting additional resources that
  a hook may use outside of the hooks directory which is the public interface
  of the charm.

How are charms licensed?

  Charms are effectively data inputs to juju, and are therefore
  licensed/copyrighted by the author as an independent work. You are free to
  claim copyright solely for yourself if it's an independent work, and to
  license it as you see fit. If you as the charm author are performing the
  work as a result of a fiduciary agreement, the terms of such agreement come
  into play and so the licensing choice is up to the hiring entity.

How can I contact the juju team?

  User and charm author oriented resources
   * Mailing list: https://lists.ubuntu.com/mailman/listinfo/Ubuntu-cloud
   * IRC #ubuntu-cloud
  juju development
   * Mailing list: https://lists.ubuntu.com/mailman/listinfo/juju
   * IRC #juju (Freenode)

Where can I find out more about juju?

  * Project Site: https://launchpad.net/juju
  * Documentation: https://juju.ubuntu.com/docs/
  * Work Items: https://juju.ubuntu.com/kanban/dublin.html
  * Principia charms project: https://launchpad.net/principia
  * Principia-Tools project: https://launchpad.net/principia-tools
