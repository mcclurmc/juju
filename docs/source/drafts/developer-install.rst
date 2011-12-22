Developer Install
------------------

For folks who want to develop on juju itself, a source install
from trunk or branch is recommended.

To run juju from source, you will need the following dependencies
installed:

 * zookeeper
 * txzookeeper
 * txaws

The juju team recommends install the zookeeper package from the
juju PPA, or a source compilation as of ubuntu natty (11.04) due
to bugs in the packaged version.

On a modern Ubuntu Linux system execute::

 $ sudo apt-get install python-zookeeper python-virtualenv python-yaml

You will also need Python 2.6 or better.

We recommend and demonstrate the use of virtualenv to install juju
and its dependencies in a sandbox, in case you latter install a newer
version via package.

First let's setup a virtualenv::

  $ virtualenv juju
  $ cd juju
  $ source bin/activate

Next we'll fetch and install a few juju dependencies from source::

  $ bzr branch lp:txaws
  $ cd txaws && python setup.py develop && cd..
  $ bzr branch lp:txzookeeper
  $ cd txzookeeper && python setup.py develop && cd..

Lastly, we fetch juju and install it from trunk::

  $ bzr branch lp:juju
  $ cd juju && python setup.py develop

You can now configure your juju environment per the getting-started
documentation.


