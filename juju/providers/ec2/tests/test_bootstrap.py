import logging
import os

import yaml

from twisted.internet.defer import succeed

from txaws.ec2.model import SecurityGroup

from juju.lib.mocker import MATCH
from juju.lib.testing import TestCase
from juju.providers.ec2.machine import EC2ProviderMachine

from .common import EC2TestMixin, EC2MachineLaunchMixin


DATA_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), "data")


class EC2BootstrapTest(EC2TestMixin, EC2MachineLaunchMixin, TestCase):

    def _mock_verify(self):
        self.s3.put_object(
            self.env_name, "bootstrap-verify", "storage is writable")
        self.mocker.result(succeed(True))

    def _mock_save(self):
        """Mock saving bootstrap instances to S3."""

        def match_string(data):
            return isinstance(data, str)

        self.s3.put_object(
            self.env_name, "provider-state",
            MATCH(match_string))
        self.mocker.result(succeed(True))

    def _mock_launch(self):
        """Mock launching a bootstrap machine on ec2."""
        def verify_user_data(data):
            expect_path = os.path.join(DATA_DIR, "bootstrap_cloud_init")
            with open(expect_path) as f:
                expect_cloud_init = yaml.load(f.read())
            self.assertEquals(yaml.load(data), expect_cloud_init)
            return True

        self.ec2.run_instances(
            image_id="ami-default",
            instance_type="m1.small",
            max_count=1,
            min_count=1,
            security_groups=["juju-moon", "juju-moon-0"],
            user_data=MATCH(verify_user_data))

    def test_launch_bootstrap(self):
        """The provider bootstrap can launch a bootstrap/zookeeper machine."""

        log = self.capture_logging("juju.common", level=logging.DEBUG)

        self.s3.get_object(self.env_name, "provider-state")
        self.mocker.result(succeed(""))
        self._mock_verify()
        self.ec2.describe_security_groups()
        self.mocker.result(succeed([]))
        self._mock_create_group()
        self._mock_create_machine_group(0)
        self._mock_launch_utils(region="us-east-1")
        self._mock_launch()
        self.mocker.result(succeed([]))
        self._mock_save()
        self.mocker.replay()

        provider = self.get_provider()
        deferred = provider.bootstrap()

        def check_log(result):
            log_text = log.getvalue()
            self.assertIn("Launching juju bootstrap instance", log_text)
            self.assertNotIn("previously bootstrapped", log_text)

        deferred.addCallback(check_log)
        return deferred

    def test_launch_bootstrap_existing_provider_group(self):
        """
        When launching a bootstrap instance the provider will use an existing
        provider instance group.
        """
        self.capture_logging("juju.ec2")
        self.s3.get_object(self.env_name, "provider-state")
        self.mocker.result(succeed(""))
        self._mock_verify()
        self.ec2.describe_security_groups()
        self.mocker.result(succeed([
            SecurityGroup("juju-%s" % self.env_name, "")]))
        self._mock_create_machine_group(0)
        self._mock_launch_utils(region="us-east-1")
        self._mock_launch()
        self.mocker.result(succeed([]))
        self._mock_save()
        self.mocker.replay()

        provider = self.get_provider()
        return provider.bootstrap()

    def test_run_with_loaded_state(self):
        """
        If the provider bootstrap is run when there is already a running
        bootstrap instance, it will just return the existing machine.
        """
        state = yaml.dump({"zookeeper-instances": ["i-foobar"]})
        self.s3.get_object(self.env_name, "provider-state")
        self.mocker.result(succeed(state))
        self.ec2.describe_instances("i-foobar")
        self.mocker.result(succeed([self.get_instance("i-foobar")]))
        self.mocker.replay()

        log = self.capture_logging("juju.common")

        def validate_result(result):
            self.assertTrue(result)
            machine = result.pop()
            self.assertTrue(isinstance(machine, EC2ProviderMachine))
            self.assertEqual(machine.instance_id, "i-foobar")
            self.assertEquals(
                log.getvalue(),
                "juju environment previously bootstrapped.\n")

        provider = self.get_provider()
        d = provider.bootstrap()
        d.addCallback(validate_result)
        return d

    def test_run_with_launch(self):
        """
        The provider bootstrap will launch an instance when run if there
        is no existing instance.
        """
        self.s3.get_object(self.env_name, "provider-state")
        self.mocker.result(succeed(""))
        self._mock_verify()
        self.ec2.describe_security_groups()
        self.mocker.result(succeed([
            SecurityGroup("juju-%s" % self.env_name, "")]))
        self._mock_create_machine_group(0)
        self._mock_launch_utils(region="us-east-1")
        self._mock_launch()
        self.mocker.result(succeed([self.get_instance("i-foobar")]))
        self._mock_save()
        self.mocker.replay()

        def validate_result(result):
            (machine,) = result
            self.assert_machine(machine, "i-foobar", "")
        provider = self.get_provider()
        d = provider.bootstrap()
        d.addCallback(validate_result)
        return d
