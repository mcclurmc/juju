
Charm Namespaces
================

Introduction
------------
  
juju supports deployment of charms from multiple sources.

By default juju searches only the Ubuntu charm namespace to resolve
charms. For example the following command line snippet will install wordpress
from the ubuntu charm namespace.::  

  juju deploy wordpress


In order to support local charm development and completely offline private
repositories, charms can also be deployed directly from a local directory.
For example the following will resolve the wordpress charm to the
$HOME/local_charms directory.::  

  juju deploy --repository=~/local_charms wordpress

With this parameter any charm dependencies from the wordpress charm will be
looked up first in the local directory and then in the ubuntu charm
namespace. So the command line flag '--repository' alters the charm lookup
from the default such that it prepends the local directory to the lookup order.


The lookup order can also be altered to utilize a 3rd party published repository
in preference to the Ubuntu charm repository. For example the following will
perform a charm lookup for wordpress and its dependencies from the published
'openstack' 3rd party repository before looking up dependencies in the Ubuntu
charm repository.:: 

  juju deploy --repository=es:openstack wordpress

The lookup order can also be specified just for a single charm. For example
the following command would deploy the wordpress charm from the openstack
namespace but would resolve dependencies (like apache and mysql) via the ubuntu
namespace.::  

  juju deploy es:openstack/wordpress

The lookup order can also be explicitly specified in the client configuration
to define a custom lookup order without the use of command line options.:: 

  environments.yaml

   repositories:
   
      - http://charms.ubuntu.com/collection/ubuntu
      - http://charms.ubuntu.com/collection/openstack
      - http://charms.ubuntu.com/people/miked
      - /var/lib/charms
      
The repositories in the configuration file are specified as a yaml list, and the
list order defines the lookup order for charms.
 

Deployment
----------

After juju resolves a charm and its dependencies, it bundles them and
deploys them to a machine provider charm cache/repository. This allows the
same charm to be deployed to multiple machines repeatably and with minimal
network transfers.

juju stores the qualified name of the charm when saving it to the machine
provider cache. This allows a charm to be unambigiously identified, ie.
whether it came from the ubuntu namespace or a 3rdparty namespace, or even from
disk.
