.. _getting-started:

Getting started
===============

Introduction
------------

This tutorial gets you started with juju. A prerequisite is the
access credentials to a dedicated computing environment such as what
is offered by a virtualized cloud hosting environment.

juju has been designed for environments which can provide a
new machine with an Ubuntu cloud operating system image
on-demand. This includes services such as `Amazon EC2
<http://aws.amazon.com/ec2/>`_ or `RackSpace
<http://www.rackspace.com>`_.

It's also required that the environment provides a permanent storage
facility such as `Amazon S3 <https://s3.amazonaws.com/>`_.

For the moment, though, the only environment supported is EC2.

Running from PPA
----------------

The juju team's Personal Package Archive (PPA) installation is
currently the preferred installation mechanism for juju. It
includes newer upstream versions of binary dependencies like Zookeeper
which are more recent than the latest ubuntu release (natty 11.04) and
contain important bugfixes.

To install juju from the PPA, execute the following in a shell::

  sudo add-apt-repository ppa:juju/pkgs
  sudo apt-get update && sudo apt-get install juju

The juju environment can now be configured per the following.

Configuring your environment
----------------------------

Run the command-line utility with no arguments to create a sample
environment::

  $ juju

This will create the file ``~/.juju/environments.yaml``, which will look
something like this::

  environments:
    sample:
      type: ec2
      control-bucket: juju-faefb490d69a41f0a3616a4808e0766b
      admin-secret: 81a1e7429e6847c4941fda7591246594

Which is a sample environment configured to run with EC2 machines and S3
permanent storage.  To make this environment actually useful, you will need
to tell juju about an AWS access key and secret key.  To do this, you
can either set the ``AWS_ACCESS_KEY_ID`` and ``AWS_SECRET_ACCESS_KEY``
environment variables (as usual for other EC2 tools) or you can add
``access-key`` and ``secret-key`` options to your ``environments.yaml``.
For example::

  environments:
    sample:
      type: ec2
      access-key: YOUR-ACCESS-KEY-GOES-HERE
      secret-key: YOUR-SECRET-KEY-GOES-HERE
      control-bucket: juju-faefb490d69a41f0a3616a4808e0766b
      admin-secret: 81a1e7429e6847c4941fda7591246594

The S3 bucket does not need to exist already.

.. note::
        If you already have an AWS account, you can determine your access key by
        visiting http://aws.amazon.com/account, clicking "Security Credentials" and
        then clicking "Access Credentials".  You'll be taken to a table that lists
        your access keys and has a "show" link for each access key that will reveal
        the associated secret key.
