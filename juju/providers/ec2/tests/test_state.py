from yaml import dump

from twisted.internet.defer import succeed, fail

from txaws.s3.exception import S3Error

from juju.lib.testing import TestCase
from juju.providers.ec2.tests.common import EC2TestMixin


class EC2StateTest(TestCase, EC2TestMixin):

    def setUp(self):
        EC2TestMixin.setUp(self)
        super(EC2StateTest, self).setUp()

    def test_save(self):
        """
        When passed some juju ec2 machine instances and asked to save,
        the machine, it will serialize the data to an s3 bucket.
        """
        instances = [self.get_instance("i-foobar", dns_name="x1.example.com")]
        state = dump(
            {"zookeeper-instances":
             [[i.instance_id, i.dns_name] for i in instances]})
        self.s3.put_object(
            self.env_name, "provider-state", state),
        self.mocker.result(succeed(state))
        self.mocker.replay()

        provider = self.get_provider()
        d = provider.save_state(
            {"zookeeper-instances":
             [[i.instance_id, i.dns_name] for i in instances]})

        def assert_state(saved_state):
            self.assertEqual(saved_state, state)

        d.addCallback(assert_state)
        return d

    def test_save_non_existant_bucket(self):
        """
        When saving instance information to S3 the EC2 provider will create a
        namespaced bucket specific to the provider instance, if it does not
        already exist.
        """
        instances = [self.get_instance("i-foobar", dns_name="x1.example.com")]
        state = dump(
            {"zookeeper-instances":
             [[i.instance_id, i.dns_name] for i in instances]})
        self.s3.put_object(
            self.env_name, "provider-state", state),
        error = S3Error("<error/>", 404)
        error.errors = [{"Code": "NoSuchBucket"}]
        self.mocker.result(fail(error))
        self.s3.create_bucket(self.env_name)
        self.mocker.result(succeed({}))
        self.s3.put_object(
            self.env_name, "provider-state", state),
        self.mocker.result(succeed(state))
        self.mocker.replay()

        provider = self.get_provider()
        d = provider.save_state(
            {"zookeeper-instances":
             [[i.instance_id, i.dns_name] for i in instances]})

        def assert_state(saved_state):
            self.assertEqual(saved_state, state)

        d.addCallback(assert_state)
        return d

    def test_load(self):
        """
        The provider bootstrap will load and deserialize any saved state from
        s3.
        """
        self.s3.get_object(self.env_name, "provider-state")
        self.mocker.result(succeed(dump({"zookeeper-instances": []})))
        self.mocker.replay()

        provider = self.get_provider()
        d = provider.load_state()

        def assert_load_value(value):
            self.assertEqual(value, {"zookeeper-instances": []})

        d.addCallback(assert_load_value)
        return d

    def test_load_nonexistant_bucket(self):
        """
        When loading saved state from s3, the system returns False if the
        s3 control bucket does not exist.
        """
        self.s3.get_object(self.env_name, "provider-state")
        error = S3Error("<error/>", 404)
        error.errors = [{"Code": "NoSuchBucket"}]
        self.mocker.result(fail(error))
        self.mocker.replay()

        provider = self.get_provider()
        d = provider.load_state()

        def assert_load_value(value):
            self.assertIdentical(value, False)

        d.addCallback(assert_load_value)
        return d

    def test_load_nonexistant(self):
        """
        When loading saved state from S3, the provider bootstrap gracefully
        handles the scenario where there is no saved state.
        """
        self.s3.get_object(self.env_name, "provider-state")
        self.mocker.result(succeed(dump([])))
        self.mocker.replay()

        provider = self.get_provider()
        d = provider.load_state()

        def assert_load_value(value):
            self.assertIdentical(value, False)

        d.addCallback(assert_load_value)
        return d
