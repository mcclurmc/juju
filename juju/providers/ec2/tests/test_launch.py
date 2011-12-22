import os

import yaml

from twisted.internet.defer import inlineCallbacks, succeed

from txaws.ec2.model import Instance, SecurityGroup

from juju.errors import (
    EnvironmentNotFound, ProviderError, ProviderInteractionError)
from juju.providers.ec2.machine import EC2ProviderMachine

from juju.lib.testing import TestCase
from juju.lib.mocker import MATCH

from .common import EC2TestMixin, EC2MachineLaunchMixin

DATA_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), "data")


class EC2MachineLaunchTest(EC2TestMixin, EC2MachineLaunchMixin, TestCase):

    def _mock_launch(self, instance, expect_ami="ami-default",
                     expect_instance_type="m1.small",
                     cloud_init="launch_cloud_init"):

        def verify_user_data(data):
            expect_path = os.path.join(DATA_DIR, cloud_init)
            with open(expect_path) as f:
                expect_cloud_init = yaml.load(f.read())
            self.assertEquals(yaml.load(data), expect_cloud_init)
            return True

        self.ec2.run_instances(
            image_id=expect_ami,
            instance_type=expect_instance_type,
            max_count=1,
            min_count=1,
            security_groups=["juju-moon", "juju-moon-1"],
            user_data=MATCH(verify_user_data))

        self.mocker.result(succeed([instance]))

    def test_bad_data(self):
        self.mocker.replay()
        d = self.get_provider().start_machine({})
        self.assertFailure(d, ProviderError)

        def verify(error):
            self.assertEquals(
                str(error),
                "Cannot launch a machine without specifying a machine-id")
        d.addCallback(verify)
        return d

    def test_provider_launch(self):
        """
        The provider can be used to launch a machine with a minimal set of
        required packages, repositories, and and security groups.
        """
        self.ec2.describe_security_groups()
        self.mocker.result(succeed([]))
        self._mock_create_group()
        self._mock_create_machine_group("1")
        self._mock_launch_utils(region="us-east-1")
        self._mock_get_zookeeper_hosts()
        self._mock_launch(self.get_instance("i-foobar"))
        self.mocker.replay()

        def verify_result(result):
            (machine,) = result
            self.assert_machine(machine, "i-foobar", "")
        provider = self.get_provider()
        d = provider.start_machine({"machine-id": "1"})
        d.addCallback(verify_result)
        return d

    @inlineCallbacks
    def test_provider_launch_using_branch(self):
        """Can use a juju branch to launch a machine"""
        self.ec2.describe_security_groups()
        self.mocker.result(succeed([]))
        self._mock_create_group()
        self._mock_create_machine_group("1")
        self._mock_launch_utils(region="us-east-1")
        self._mock_get_zookeeper_hosts()
        self._mock_launch(
            self.get_instance("i-foobar"),
            cloud_init="launch_cloud_init_branch")
        self.mocker.replay()

        provider = self.get_provider()
        provider.config["juju-origin"] = "lp:~wizard/juju-juicebar"
        machines = yield provider.start_machine({"machine-id": "1"})
        self.assert_machine(machines[0], "i-foobar", "")

    @inlineCallbacks
    def test_provider_launch_using_ppa(self):
        """Can use the juju ppa to launch a machine"""
        self.ec2.describe_security_groups()
        self.mocker.result(succeed([]))
        self._mock_create_group()
        self._mock_create_machine_group("1")
        self._mock_launch_utils(region="us-east-1")
        self._mock_get_zookeeper_hosts()
        self._mock_launch(
            self.get_instance("i-foobar"),
            cloud_init="launch_cloud_init_ppa")
        self.mocker.replay()

        provider = self.get_provider()
        provider.config["juju-origin"] = "ppa"
        machines = yield provider.start_machine({"machine-id": "1"})
        self.assert_machine(machines[0], "i-foobar", "")

    @inlineCallbacks
    def test_provider_launch_using_explicit_distro(self):
        """Can set juju-origin explicitly to `distro`"""
        self.ec2.describe_security_groups()
        self.mocker.result(succeed([]))
        self._mock_create_group()
        self._mock_create_machine_group("1")
        self._mock_launch_utils(region="us-east-1")
        self._mock_get_zookeeper_hosts()
        self._mock_launch(
            self.get_instance("i-foobar"),
            cloud_init="launch_cloud_init")
        self.mocker.replay()

        provider = self.get_provider()
        provider.config["juju-origin"] = "distro"
        machines = yield provider.start_machine({"machine-id": "1"})
        self.assert_machine(machines[0], "i-foobar", "")

    @inlineCallbacks
    def test_provider_launch_existing_security_group(self):
        """Verify that the launch works if the env security group exists"""
        instance = Instance("i-foobar", "running", dns_name="x1.example.com")
        security_group = SecurityGroup("juju-moon", "some description")

        self.ec2.describe_security_groups()
        self.mocker.result(succeed([security_group]))
        self._mock_create_machine_group("1")
        self._mock_launch_utils(region="us-east-1")
        self._mock_get_zookeeper_hosts()
        self._mock_launch(instance)
        self.mocker.replay()

        provider = self.get_provider()
        provided_machines = yield provider.start_machine({"machine-id": "1"})
        self.assertEqual(len(provided_machines), 1)
        self.assertTrue(isinstance(provided_machines[0], EC2ProviderMachine))
        self.assertEqual(
            provided_machines[0].instance_id, instance.instance_id)

    @inlineCallbacks
    def test_provider_launch_existing_machine_security_group(self):
        """Verify that the launch works if the machine security group exists"""
        instance = Instance("i-foobar", "running", dns_name="x1.example.com")
        machine_group = SecurityGroup(
            "juju-moon-1", "some description")

        self.ec2.describe_security_groups()
        self.mocker.result(succeed([machine_group]))
        self._mock_create_group()
        self._mock_delete_machine_group("1")  # delete existing sg
        self._mock_create_machine_group("1")  # then recreate
        self._mock_launch_utils(region="us-east-1")
        self._mock_get_zookeeper_hosts()
        self._mock_launch(instance)
        self.mocker.replay()

        provider = self.get_provider()
        provided_machines = yield provider.start_machine({"machine-id": "1"})
        self.assertEqual(len(provided_machines), 1)
        self.assertTrue(isinstance(provided_machines[0], EC2ProviderMachine))
        self.assertEqual(
            provided_machines[0].instance_id, instance.instance_id)

    @inlineCallbacks
    def test_provider_launch_existing_machine_security_group_is_active(self):
        """Verify launch fails properly if the machine group is stil active.

        This condition occurs when there is a corresponding machine in
        that security group, generally because it is still shutting
        down."""
        machine_group = SecurityGroup(
            "juju-moon-1", "some description")

        self.ec2.describe_security_groups()
        self.mocker.result(succeed([machine_group]))
        self._mock_create_group()
        self._mock_delete_machine_group_was_deleted("1")  # sg is gone!
        self._mock_launch_utils(region="us-east-1")
        self._mock_get_zookeeper_hosts()
        self.mocker.replay()

        provider = self.get_provider()
        ex = yield self.assertFailure(
            provider.start_machine({"machine-id": "1"}),
            ProviderInteractionError)
        self.assertEqual(
            str(ex),
            "Unexpected EC2Error deleting security group "
            "juju-moon-1: There are active instances using security group "
            "'juju-moon-1'")

    def test_launch_with_no_juju_s3_state(self):
        """
        Attempting to launch without any juju saved state, means
        we can't provide a way for a launched instance to connect
        to the zookeeper shared state. An assertion error is raised
        instead of allowing this.
        """
        self._mock_get_zookeeper_hosts(False)
        self.mocker.replay()

        provider = self.get_provider()
        d = provider.start_machine({"machine-id": "1"})
        self.assertFailure(d, EnvironmentNotFound)
        return d

    def test_launch_with_no_juju_zookeeper_hosts(self):
        """
        Attempting to launch without any juju zookeeper hosts, means
        we can't provide a way for a launched instance to connect
        to the zookeeper shared state. An assertion error is raised
        instead of allowing this.
        """
        self._mock_get_zookeeper_hosts([])
        self.mocker.replay()

        provider = self.get_provider()
        d = provider.start_machine({"machine-id": "1"})
        self.assertFailure(d, EnvironmentNotFound)
        return d

    def test_launch_options_known_instance_type(self):
        self.ec2.describe_security_groups()
        self.mocker.result(succeed([]))
        self._mock_create_group()
        self._mock_create_machine_group("1")
        self._mock_launch_utils(region="us-east-1")
        self._mock_get_zookeeper_hosts()
        self._mock_launch(
            self.get_instance("i-foobar"), expect_instance_type="m1.ginormous")
        self.mocker.replay()

        provider = self.get_provider()
        provider.config["default-instance-type"] = "m1.ginormous"
        return provider.start_machine({"machine-id": "1"})

    def _mock_launch_with_ami_params(self, get_ami_kwargs, expect_ami=None):
        expect_ami = expect_ami or "ami-default"
        self.ec2.describe_security_groups()
        self.mocker.result(succeed([]))
        self._mock_create_group()
        self._mock_create_machine_group("1")
        self._mock_launch_utils(ami_name=expect_ami, **get_ami_kwargs)
        self._mock_get_zookeeper_hosts()
        self._mock_launch(self.get_instance("i-foobar"), expect_ami)

    def test_launch_options_known_ami(self):
        self._mock_launch_with_ami_params({}, expect_ami="ami-different")
        self.mocker.replay()

        provider = self.get_provider()
        provider.config["default-image-id"] = "ami-different"
        return provider.start_machine({"machine-id": "1"})

    def test_launch_options_region(self):
        self._mock_launch_with_ami_params({"region": "somewhere-else-1"})
        self.mocker.replay()

        provider = self.get_provider()
        provider.config["region"] = "somewhere-else-1"
        return provider.start_machine({"machine-id": "1"})

    def test_launch_options_custom_image_options(self):
        self._mock_launch_with_ami_params({
            "region": "us-east-1",
            "ubuntu_release": "choleric",
            "architecture": "quantum86",
            "persistent_storage": "maybe",
            "daily": "perhaps"})
        self.mocker.replay()

        provider = self.get_provider()
        constraints = {
            "ubuntu_release": "choleric",
            "architecture": "quantum86",
            "persistent_storage": "maybe",
            "daily": "perhaps"}
        return provider.start_machine({"machine-id": "1",
                                       "constraints": constraints})
