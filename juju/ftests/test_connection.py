"""
Functional tests for secure zookeeper connections using an ssh forwarded port.

Requirements for functional test
 - sshd running on localhost
 - zookeeper running on localhost, listening on port 2181
 - user can log into localhost via key authentication
 - ~/.ssh/id_dsa exists and is configured as an authorized key
"""

import os
import pwd
import zookeeper

from juju.errors import NoConnection
from juju.lib.testing import TestCase
from juju.state.sshclient import SSHClient
from juju.tests.common import get_test_zookeeper_address


class ConnectionTest(TestCase):

    def setUp(self):
        super(ConnectionTest, self).setUp()
        self.username = pwd.getpwuid(os.getuid())[0]
        self.log = self.capture_logging("juju.state.sshforward")
        self.old_user_name = SSHClient.remote_user
        SSHClient.remote_user = self.username
        self.client = SSHClient()
        zookeeper.set_debug_level(0)

    def tearDown(self):
        super(ConnectionTest, self).tearDown()
        zookeeper.set_debug_level(zookeeper.LOG_LEVEL_DEBUG)
        SSHClient.remote_user = self.old_user_name

    def test_connect(self):
        """
        Forwarding a port spawns an ssh process with port forwarding arguments.
        """
        connect_deferred = self.client.connect(
            get_test_zookeeper_address(), timeout=20)

        def validate_connected(client):
            self.assertTrue(client.connected)
            client.close()

        connect_deferred.addCallback(validate_connected)
        return connect_deferred

    def test_invalid_host(self):
        """
        if a connection can not be made before a timeout period, an exception
        is raised.

        with the sshclient layer, tihs test no longer returns a failure..
        and its hard to cleanup the process tunnel..
        """
        SSHClient.remote_user = "rabbit"
        connect_deferred = self.client.connect(
            "foobar.example.com:2181", timeout=10)

        self.failUnlessFailure(connect_deferred, NoConnection)

        def validate_log(result):
            output = self.log.getvalue()
            self.assertTrue(output.strip().startswith(
                "Invalid host for SSH forwarding"))

        connect_deferred.addCallback(validate_log)
        return connect_deferred

    def test_invalid_user(self):
        """
        if a connection can not be made before a timeout period, an exception
        is raised.

        with the sshclient layer, tihs test no longer returns a failure..
        and its hard to cleanup the process tunnel..
        """
        SSHClient.remote_user = "rabbit"
        connect_deferred = self.client.connect(
            get_test_zookeeper_address(), timeout=10)
        self.failUnlessFailure(connect_deferred, NoConnection)

        def validate_log(result):
            output = self.log.getvalue()
            self.assertEqual(output.strip(), "Invalid SSH key")

        connect_deferred.addCallback(validate_log)
        return connect_deferred
