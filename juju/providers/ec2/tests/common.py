from yaml import dump

from twisted.internet.defer import fail, succeed

from txaws.s3.client import S3Client
from txaws.s3.exception import S3Error
from txaws.ec2.client import EC2Client
from txaws.ec2.exception import EC2Error
from txaws.ec2.model import Instance, Reservation, SecurityGroup

from juju.lib.mocker import KWARGS, MATCH
from juju.providers.ec2 import MachineProvider
from juju.providers.ec2.machine import EC2ProviderMachine

MATCH_GROUP = MATCH(lambda x: x.startswith("juju-moon"))


class EC2TestMixin(object):

    env_name = "moon"
    service_factory_kwargs = None

    def get_config(self):
        return {"type": "ec2",
                "juju-origin": "distro",
                "admin-secret": "magic-beans",
                "access-key": "0f62e973d5f8",
                "secret-key": "3e5a7c653f59",
                "control-bucket": self.env_name}

    def get_provider(self):
        """Return the ec2 machine provider.

        This should only be invoked after mocker is in replay mode so the
        AWS service class will be appropriately replaced by the mock.
        """
        return MachineProvider(self.env_name, self.get_config())

    def get_instance(self,
                     instance_id, state="running", machine_id=42, **kwargs):
        groups = kwargs.pop("groups",
                            ["juju-%s" % self.env_name,
                             "juju-%s-%s" % (self.env_name, machine_id)])
        reservation = Reservation("x", "y", groups=groups)
        return Instance(instance_id, state, reservation=reservation, **kwargs)

    def assert_machine(self, machine, instance_id, dns_name):
        self.assertTrue(isinstance(machine, EC2ProviderMachine))
        self.assertEquals(machine.instance_id, instance_id)
        self.assertEquals(machine.dns_name, dns_name)

    def get_ec2_error(self, entity_id,
                      format="The instance ID %r does not exist",
                      code=503):
        """Make a representative EC2Error for `entity_id`, eg AWS instance_id.

        This error is paired with `get_wrapped_ec2_text` below. The
        default format represents a fairly common error seen in
        working with EC2. There are others."""
        message = format % entity_id
        return EC2Error(
            "<error><Code>1</Code><Message>%s</Message></error>" % message,
            code)

    def setUp(self):
        # mock out the aws services
        service_factory = self.mocker.replace(
            "txaws.service.AWSServiceRegion")
        self._service = service_factory(KWARGS)

        def store_factory_kwargs(**kwargs):
            self.service_factory_kwargs = kwargs
        self.mocker.call(store_factory_kwargs)
        self.s3 = self.mocker.mock(S3Client)
        self._service.get_s3_client()
        self.mocker.result(self.s3)
        self.ec2 = self.mocker.mock(EC2Client)
        self._service.get_ec2_client()
        self.mocker.result(self.ec2)


class EC2MachineLaunchMixin(object):

    def _mock_launch_utils(self, ami_name="ami-default", **get_ami_kwargs):
        get_public_key = self.mocker.replace(
            "juju.providers.common.utils.get_user_authorized_keys")

        def match_config(arg):
            return isinstance(arg, dict)

        get_public_key(MATCH(match_config))
        self.mocker.result("zebra")

        if not get_ami_kwargs:
            return
        get_ami = self.mocker.replace(
            "juju.providers.ec2.utils.get_current_ami")
        get_ami(KWARGS)

        def check_kwargs(**kwargs):
            self.assertEquals(kwargs, get_ami_kwargs)
            return succeed(ami_name)
        self.mocker.call(check_kwargs)

    def _mock_create_group(self):
        group_name = "juju-%s" % self.env_name
        self.ec2.create_security_group(
            group_name, "juju group for %s" % self.env_name)
        self.mocker.result(succeed(True))

        self.ec2.authorize_security_group(
            group_name, ip_protocol="tcp", from_port="22",
            to_port="22", cidr_ip="0.0.0.0/0")
        self.mocker.result(succeed([self.env_name]))

        self.ec2.describe_security_groups(group_name)
        self.mocker.result(succeed(
            [SecurityGroup(group_name, "", owner_id="123")]))

        self.ec2.authorize_security_group(
            group_name, source_group_name=group_name,
            source_group_owner_id="123")
        self.mocker.result(succeed(True))

    def _mock_create_machine_group(self, machine_id):
        machine_group_name = "juju-%s-%s" % (self.env_name, machine_id)
        self.ec2.create_security_group(
            machine_group_name, "juju group for %s machine %s" % (
                self.env_name, machine_id))
        self.mocker.result(succeed(True))

    def _mock_delete_machine_group(self, machine_id):
        machine_group_name = "juju-%s-%s" % (self.env_name, machine_id)
        self.ec2.delete_security_group(machine_group_name)
        self.mocker.result(succeed(True))

    def _mock_delete_machine_group_was_deleted(self, machine_id):
        machine_group_name = "juju-%s-%s" % (self.env_name, machine_id)
        self.ec2.delete_security_group(machine_group_name)
        self.mocker.result(fail(self.get_ec2_error(
                    machine_group_name,
                    "There are active instances using security group %r")))

    def _mock_get_zookeeper_hosts(self, hosts=None):
        """
        Try to encapsulate a variety of behaviors here..

        if hosts is None, a default host is used.
        if hosts is False, no s3 state is returned
        if hosts are passed as a list of instances, they
           are returned.
        """

        if hosts is None:
            hosts = [self.get_instance(
                "i-es-zoo", private_dns_name="es.example.internal")]

        self.s3.get_object(self.env_name, "provider-state")
        if hosts is False:
            error = S3Error("<error/>", 404)
            error.errors = [{"Code": "NoSuchKey"}]
            self.mocker.result(fail(error))
            return

        state = dump({
            "zookeeper-instances":
            [i.instance_id for i in hosts]})

        self.mocker.result(succeed(state))
        if hosts:
            # connect grabs the first host of a set.
            self.ec2.describe_instances(hosts[0].instance_id)
            self.mocker.result(succeed([hosts[0]]))


class MockInstanceState(object):
    """Mock the result of ec2_describe_instances when called successively.

    Each call of :method:`get_round` returns a list of mock `Instance`
    objects, using the state for that round. Instance IDs not used in
    the round (and passed in from ec2_describe_instances) are
    automatically skipped."""

    def __init__(self, tester, instance_ids, machine_ids, states):
        self.tester = tester
        self.instance_ids = instance_ids
        self.machine_ids = machine_ids
        self.states = states
        self.round = 0

    def get_round(self, *current_instance_ids):
        result = []
        for instance_id, machine_id, state in zip(
                self.instance_ids, self.machine_ids, self.states[self.round]):
            if instance_id not in current_instance_ids:
                # Ignore instance_ids that are no longer being
                # described, because they have since moved into a
                # terminated state
                continue
            result.append(self.tester.get_instance(instance_id,
                                                   machine_id=machine_id,
                                                   state=state))
        self.round += 1
        return succeed(result)


class Observed(object):
    """Minimal wrapper just to ensure :method:`add` returns a `Deferred`."""

    def __init__(self):
        self.items = set()

    def add(self, item):
        self.items.add(item)
        return succeed(True)
