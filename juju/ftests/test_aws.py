"""
Validate AWS (EC2/S3) Assumptions via api usage exercise.

Requirements for functional test
 - valid amazon credentials present in the environment as
   AWS_SECRET_ACCESS_KEY, AWS_ACCESS_KEY_ID

"""

import random

from twisted.internet.defer import inlineCallbacks
from twisted.python.failure import Failure

from txaws.service import AWSServiceRegion
from txaws.s3.exception import S3Error

from juju.lib.testing import TestCase


class AWSFunctionalTest(TestCase):

    def setUp(self):
        region = AWSServiceRegion()
        self.ec2 = region.get_ec2_client()
        self.s3 = region.get_s3_client()


class EC2SecurityGroupTest(AWSFunctionalTest):

    def setUp(self):
        super(EC2SecurityGroupTest, self).setUp()
        self.security_group = "juju-test-%s" % (random.random())

    @inlineCallbacks
    def tearDown(self):
        yield self.ec2.delete_security_group(self.security_group)

    @inlineCallbacks
    def test_create_and_get_group(self):
        """Verify input/outputs of creating a security group.

        Specifically we want to see if we can pick up the owner id off the
        group, which we need for group 2 group ec2 authorizations."""
        data = yield self.ec2.create_security_group(
            self.security_group, "test")
        self.assertEqual(data, True)

        info = yield self.ec2.describe_security_groups(self.security_group)
        self.assertEqual(len(info), 1)
        group = info.pop()
        self.assertTrue(group.owner_id)

    @inlineCallbacks
    def test_create_and_authorize_group(self):

        yield self.ec2.create_security_group(self.security_group, "test")
        info = yield self.ec2.describe_security_groups(self.security_group)
        group = info.pop()
        yield self.ec2.authorize_security_group(
            self.security_group,
            source_group_name = self.security_group,
            source_group_owner_id = group.owner_id)

        info = yield self.ec2.describe_security_groups(self.security_group)
        group = info.pop()
        self.assertEqual(group.name, self.security_group)


class S3FilesTest(AWSFunctionalTest):

    def setUp(self):
        super(S3FilesTest, self).setUp()
        self.control_bucket = "juju-test-%s" % (random.random())
        return self.s3.create_bucket(self.control_bucket)

    @inlineCallbacks
    def tearDown(self):
        listing = yield self.s3.get_bucket(self.control_bucket)
        for ob in listing.contents:
            yield self.s3.delete_object(self.control_bucket, ob.key)
        yield self.s3.delete_bucket(self.control_bucket)

    def test_put_object(self):
        """Verify input/outputs of putting an object in the bucket.

        The output is just an empty string on success."""
        d = self.s3.put_object(
            self.control_bucket, "pirates/gold.txt", "blah blah")

        def verify_result(result):
            self.assertEqual(result, "")

        d.addCallback(verify_result)
        return d

    @inlineCallbacks
    def test_get_object(self):
        """Verify input/outputs of getting an object from the bucket."""

        yield self.s3.put_object(
            self.control_bucket, "pirates/ship.txt", "argh argh")
        ob = yield self.s3.get_object(
            self.control_bucket, "pirates/ship.txt")
        self.assertEqual(ob, "argh argh")

    def test_get_object_nonexistant(self):
        """Verify output when an object does not exist."""

        d = self.s3.get_object(self.control_bucket, "pirates/treasure.txt")

        def verify_result(result):
            self.assertTrue(isinstance(result, Failure))
            result.trap(S3Error)

        d.addBoth(verify_result)
        return d
