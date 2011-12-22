"""
Functional test for EC2 Provider.

Requirements for functional test
 - valid amazon credentials present in the environment as
   AWS_SECRET_ACCESS_KEY, AWS_ACCESS_KEY_ID
 - an ssh key (id_dsa, id_rsa, identity) present in ~/.ssh
 - if the key has a password an ssh-agent must be running with
   SSH_AGENT_PID and SSH_AUTH_SOCK set in the environment.

These tests may take several minutes for each test.
"""

from cStringIO import StringIO
import os
import pwd
import sys
import zookeeper

from twisted.internet.defer import inlineCallbacks, Deferred, returnValue

from juju.errors import FileNotFound, EnvironmentNotFound
from juju.providers.ec2 import MachineProvider
from juju.state.sshclient import SSHClient

from juju.lib.testing import TestCase


def wait_for_startup(client, instances, interval=2):
    """Poll EC2 waiting for instance to transition to running state."""
    # XXX should we instead be waiting for /initialized?
    from twisted.internet import reactor

    on_started = Deferred()

    def poll_instance():
        d = client.describe_instances(*[i.instance_id for i in instances])
        d.addCallback(check_status)

    def check_status(instances):
        started = filter(lambda i: i.instance_state == "running", instances)

        if len(started) == len(instances):
            on_started.callback(instances)
        else:
            reactor.callLater(interval, poll_instance)

    reactor.callLater(interval, poll_instance)
    return on_started



def get_juju_branch_url():
    """
    Inspect the current working tree, to determine the juju branch
    to utilize when running functional tests in the cloud. If the current
    directory is a branch, then use its push location. If its a checkout
    then use its bound location.

    Also verify the local tree has no uncommitted changes, and that all
    local commits have been pushed upstream.
    """
    import juju
    from bzrlib import workingtree, branch, errors, transport

    try:
        tree, path = workingtree.WorkingTree.open_containing(
            os.path.abspath(os.path.dirname(juju.__file__)))
    except errors.NotBranchError:
        return "lp:juju"

    if tree.has_changes():
        raise RuntimeError("Local branch has uncommitted changes")

    # only a checkout will have a bound location, typically trunk
    location = tree.branch.get_bound_location()
    if location:
        return location

    # else its a development branch
    location = tree.branch.get_push_location()
    assert location, "Could not determine juju location for ftests"

    # verify the branch is up to date pushed
    local_revno = tree.branch.revno()

    location = location.replace("lp:", "bzr+ssh://bazaar.launchpad.net/")
    t = transport.get_transport(location)
    try:
        remote_branch = branch.Branch.open_from_transport(t)
    except errors.NotBranchError:
        raise RuntimeError("Local branch not pushed")

    remote_revno = remote_branch.revno()
    if not local_revno <= remote_revno:
        raise RuntimeError("Local branch has unpushed changes")

    # the remote bzr invocation prefers lp: addresses
    location = location.replace("bzr+ssh://bazaar.launchpad.net/", "lp:")
    return str(location)


class EC2ProviderFunctionalTest(TestCase):

    def setUp(self):
        super(EC2ProviderFunctionalTest, self).setUp()
        self.username = pwd.getpwuid(os.getuid())[0]
        self.log = self.capture_logging("juju")
        zookeeper.set_debug_level(0)
        juju_branch = ""  # get_juju_branch_url()
        self.provider = MachineProvider("ec2-functional",
            {"control-bucket": "juju-test-%s" % (self.username),
             "admin-secret": "magic-beans",
             "juju-branch": juju_branch})


class EC2MachineTest(EC2ProviderFunctionalTest):

    def _filter_instances(self, instances):
        provider_instances = []
        group_name = "juju-%s" % self.provider.environment_name
        for i in instances:
            if i.instance_state not in ("running", "pending"):
                continue
            if not group_name in i.reservation.groups:
                continue
            provider_instances.append(i)
        return provider_instances

    @inlineCallbacks
    def test_shutdown(self):
        """
        Shutting down the provider, terminates all instances associated to
        the provider instance.
        """

        running = yield self.provider.ec2.describe_instances()
        running_prior = set(self._filter_instances(running))

        result = yield self.provider.shutdown()

        running_set = yield self.provider.ec2.describe_instances()
        running_now = set(self._filter_instances(running_set))

        if result:
            shutdown = running_prior - running_now
            self.assertEqual(len(result), len(shutdown))
            result_ids = [r.instance_id for r in result]
            for i in shutdown:
                self.failUnlessIn(i.instance_id, result_ids)

    @inlineCallbacks
    def test_bootstrap_and_connect(self):
        """
        Launching a bootstrap instance, creates an ec2 instance with
        a zookeeper server running on it. This test may take up to 7m
        """

        machines = yield self.provider.bootstrap()
        instances = yield wait_for_startup(self.provider.ec2, machines)

        test_complete = Deferred()

        def verify_running():
            sys.stderr.write("running; ")

            @inlineCallbacks
            def validate_connected(client):
                self.assertTrue(client.connected)
                sys.stderr.write("connected.")
                exists_deferred, watch_deferred = client.exists_and_watch(
                    "/charms")
                stat = yield exists_deferred
                if stat:
                    test_complete.callback(None)
                    returnValue(True)
                yield watch_deferred
                stat = yield client.exists("/charms")
                self.assertTrue(stat)
                test_complete.callback(None)

            def propogate_failure(failure):
                test_complete.errback(failure)
                return failure

            def close_client(result, client):
                client.close()

            server = "%s:2181" % instances[0].dns_name
            client = SSHClient()
            client_deferred = client.connect(server, timeout=300)
            client_deferred.addCallback(validate_connected)
            client_deferred.addErrback(propogate_failure)
            client_deferred.addBoth(close_client, client)

        yield verify_running()
        yield test_complete

    # set the timeout to something more reasonable for bootstraping
    test_bootstrap_and_connect.timeout = 300

    @inlineCallbacks
    def test_provider_with_nonexistant_zk_instance(self):
        """
        If the zookeeper instances as stored in s3 does not exist, then
        connect should return the appropriate error message.
        """
        self.addCleanup(self.provider.save_state, {})
        yield self.provider.save_state({"zookeeper-instances": [
            "i-a189723", "i-a213213"]})
        d = self.provider.connect()
        yield self.assertFailure(d, EnvironmentNotFound)


class EC2StorageTest(EC2ProviderFunctionalTest):

    def setUp(self):
        super(EC2StorageTest, self).setUp()
        self.s3 = self.provider.s3
        self.storage = self.provider.get_file_storage()
        self.control_bucket = self.provider.config.get("control-bucket")
        return self.s3.create_bucket(self.control_bucket)

    @inlineCallbacks
    def tearDown(self):
        listing = yield self.s3.get_bucket(self.control_bucket)
        for ob in listing.contents:
            yield self.s3.delete_object(self.control_bucket, ob.key)
        yield self.s3.delete_bucket(self.control_bucket)

    @inlineCallbacks
    def test_put_object(self):
        content = "snakes eat rubies"
        yield self.storage.put("files/reptile.txt", StringIO(content))
        s3_content = yield self.s3.get_object(
            self.control_bucket, "files/reptile.txt")
        self.assertEqual(content, s3_content)

    @inlineCallbacks
    def test_get_object(self):
        content = "snakes eat rubies"
        yield self.storage.put("files/reptile.txt", StringIO(content))

        file_obj = yield self.storage.get("files/reptile.txt")
        s3_content = file_obj.read()
        self.assertEqual(content, s3_content)

    def test_get_object_nonexistant(self):
        remote_path = "files/reptile.txt"
        d = self.storage.get(remote_path)
        self.failUnlessFailure(d, FileNotFound)

        def validate_error_message(result):
            self.assertEqual(
                result.path, "s3://%s/%s" % (self.control_bucket, remote_path))

        d.addCallback(validate_error_message)
        return d
