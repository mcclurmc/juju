EC2 provider configuration
--------------------------

The EC2 provider accepts a number of configuration options, that can
be specified in the ``environments.yaml`` file under an ec2 provider section.

    access-key:
        The AWS access key to utilize for calls to the AWS APIs.

    secret-key:
        The AWS secret key to utilize for calls to the AWS APIs.

    ec2-uri:
        The EC2 api endpoint URI, by default points to `ec2.amazonaws.com`

    region:
        The EC2 region, by default points to `us-east-1`. If 'ec2-uri' is
        specified, it will take precedence.

    s3-uri:
        The S3 api endpoint URI, by default points to `s3.amazonaws.com`

    control-bucket:
        An S3 bucket unique to the environment, where some runtime metadata and
        charms are stored.

    juju-origin:
        Defines where juju should be obtained for installing in
        machines. Can be set to a "lp:..." branch url, to "ppa" for
        getting packages from the official juju PPA, or to "distro"
        for using packages from the official Ubuntu repositories.

        If this option is not set, juju will attempt to detect the
        correct origin based on its run location and the installed
        juju package.

    default-instance-type:
        The instance type to be used for machines launched within the juju
	environment. Acceptable values are based on EC2 instance type API names
	like t1.micro or m1.xlarge.

    default-image-id:
        The default amazon machine image to utilize for machines in the
        juju environment. If not specified the default image id varies by
        region.

    default-series:
        The default Ubuntu series to use (`oneiric`, for instance). EC2 images
        and charms referenced without an explicit series will both default to
        the value of this setting.

Additional configuration options, not specific to EC2:

    authorized-keys-path:
        The path to a public key to place onto launched machines. If no value
        is provided for either this or ``authorized-keys`` then a search is
        made for some default public keys "id_dsa.pub", "id_rsa.pub",
        "identity.pub". If none of those exist, then a LookupError error is
        raised when launching a machine.

    authorized-keys:
        The full content of a public key to utilize on launched machines.


