import os
import logging

from twisted.internet.defer import Deferred

from juju.lib.testing import TestCase
from juju.lib.mocker import ARGS, KWARGS
from juju.errors import FileNotFound, NoConnection
from juju.state.sshforward import (
    forward_port, TunnelProtocol, ClientTunnelProtocol, prepare_ssh_sharing)


class ConnectionTest(TestCase):

    def setUp(self):
        super(ConnectionTest, self).setUp()
        self.home = self.makeDir()
        self.change_environment(HOME=self.home)

    def test_invalid_forward_args(self):
        self.assertRaises(
            SyntaxError,
            forward_port,
            "ubuntu", "10000", "localhost", "1000a")

    def test_ssh_spawn(self):
        """
        Forwarding a port spawns an ssh process with port forwarding arguments.
        """
        from twisted.internet import reactor
        mock_reactor = self.mocker.patch(reactor)
        mock_reactor.spawnProcess(ARGS, KWARGS)
        saved = []
        self.mocker.call(lambda *args, **kwargs: saved.append((args, kwargs)))
        self.mocker.result(None)
        self.mocker.replay()
        result = forward_port("ubuntu", 8888, "remote_host", 9999)
        self.assertEquals(result, None)
        self.assertTrue(saved)
        args, kwargs = saved[0]
        self.assertIsInstance(args[0], TunnelProtocol)
        self.assertEquals(args[1], "/usr/bin/ssh")
        self.assertEquals(args[2], [
            "ssh", "-T",
            "-o",
            "ControlPath " + self.home + "/.juju/ssh/master-%r@%h:%p",
            "-o", "ControlMaster no",
            "-o", "PasswordAuthentication no",
            "-Llocalhost:8888:localhost:9999", "ubuntu@remote_host"])
        self.assertEquals(kwargs, {"env": os.environ})

    def test_ssh_spawn_sharing(self):
        """
        When sharing is enabled, ssh will be set up so that it becomes the
        master if there's no other master alive yet.
        """
        from twisted.internet import reactor
        mock_reactor = self.mocker.patch(reactor)
        mock_reactor.spawnProcess(ARGS, KWARGS)
        saved = []
        self.mocker.call(lambda *args, **kwargs: saved.append((args, kwargs)))
        self.mocker.result(None)
        self.mocker.replay()
        result = forward_port("ubuntu", 8888, "remote_host", 9999, share=True)
        self.assertEquals(result, None)
        self.assertTrue(saved)
        args, kwargs = saved[0]
        self.assertIsInstance(args[0], TunnelProtocol)
        self.assertEquals(args[1], "/usr/bin/ssh")
        self.assertEquals(args[2], [
            "ssh", "-T",
            "-o",
            "ControlPath " + self.home + "/.juju/ssh/master-%r@%h:%p",
            "-o", "ControlMaster auto",
            "-o", "PasswordAuthentication no",
            "-Llocalhost:8888:localhost:9999", "ubuntu@remote_host"])
        self.assertEquals(kwargs, {"env": os.environ})

    def test_forward_with_invalid_key(self):
        """
        Using an invalid private key with connect raises an FileNotFound
        exception.
        """
        self.assertRaises(
            FileNotFound,
            forward_port, "ubuntu", 2222, "remote", 2181,
            private_key_path="xyz-123_dsa")

    def test_connect_with_key(self):
        """
        A private key path can optionally be specified as an argument
        to connect in which case its passed to the ssh command line.
        """
        from twisted.internet import reactor
        file_path = self.makeFile("content")
        mock_reactor = self.mocker.patch(reactor)
        mock_reactor.spawnProcess(ARGS, KWARGS)
        saved = []
        self.mocker.call(lambda *args, **kwargs: saved.append((args, kwargs)))
        self.mocker.result("the-process")
        self.mocker.replay()
        result = forward_port("ubuntu", 22181, "remote_host", 2181,
                              private_key_path=file_path)
        self.assertEquals(result, "the-process")
        args, kwargs = saved[0]
        self.assertIsInstance(args[0], TunnelProtocol)
        self.assertEquals(args[1], "/usr/bin/ssh")
        self.assertEquals(args[2], [
            "ssh", "-T",
            "-i", file_path,
            "-o",
            "ControlPath " + self.home + "/.juju/ssh/master-%r@%h:%p",
            "-o", "ControlMaster no",
            "-o", "PasswordAuthentication no",
            "-Llocalhost:22181:localhost:2181", "ubuntu@remote_host"])
        self.assertEquals(kwargs, {"env": os.environ})

    def test_prepare_ssh_sharing_not_master(self):
        args = prepare_ssh_sharing()
        self.assertEquals(args, [
            "-o",
            "ControlPath " + self.home + "/.juju/ssh/master-%r@%h:%p",
            "-o", "ControlMaster no",
        ])
        self.assertFalse(os.path.exists(self.home + "/.juju/ssh"))

    def test_prepare_ssh_sharing_auto_master(self):
        args = prepare_ssh_sharing(auto_master=True)
        self.assertEquals(args, [
            "-o",
            "ControlPath " + self.home + "/.juju/ssh/master-%r@%h:%p",
            "-o", "ControlMaster auto",
        ])
        self.assertTrue(os.path.isdir(self.home + "/.juju/ssh"))

    def test_tunnel_protocol_logs_errors(self):
        """
        When using ssh to setup the tunnel, we log all stderr output, and
        it will get logged at the error level to the connection log handler.
        """
        log = self.capture_logging(level=logging.ERROR)
        protocol = TunnelProtocol()
        protocol.errReceived("test")
        self.assertEqual(log.getvalue(), "SSH tunnel error test\n")

    def test_tunnel_protocol_ignores_warnings(self):
        """
        When using ssh to setup the tunnel, we typically recieve a unknown host
        key warning message on stderr, the tunnel protocol will filter these
        messages.
        """
        log = self.capture_logging(level=logging.ERROR)
        protocol = ClientTunnelProtocol(None, None)

        protocol.errReceived("Warning: Permanently added")
        self.assertEqual(log.getvalue(), "")

    def test_tunnel_protocol_closes_on_error(self):
        """
        When using ssh to setup the tunnel, we typically recieve a unknown host
        key warning message on stderr, the tunnel protocol will filter these
        messages.
        """
        log = self.capture_logging(level=logging.ERROR)
        client = self.mocker.mock()
        tunnel_deferred = Deferred()
        protocol = ClientTunnelProtocol(client, tunnel_deferred)
        client.close()
        self.mocker.replay()

        protocol.errReceived("badness")

        def verify_failure(error):
            self.assertEqual(error.args[0], "SSH forwarding error: badness")
            self.assertEqual(log.getvalue().strip(),
                             "SSH forwarding error: badness")

        self.assertFailure(tunnel_deferred, NoConnection)
        tunnel_deferred.addCallback(verify_failure)
        return tunnel_deferred

    def test_tunnel_protocol_notes_invalid_host(self):
        log = self.capture_logging(level=logging.ERROR)
        client = self.mocker.mock()
        client.close()
        self.mocker.replay()

        tunnel_deferred = Deferred()
        protocol = ClientTunnelProtocol(client, tunnel_deferred)

        message = "ssh: Could not resolve hostname magicbean"
        protocol.errReceived(message)

        expected_message = "Invalid host for SSH forwarding: %s" % message

        def verify_failure(error):
            self.assertEqual(error.args[0], expected_message)
            self.assertEqual(log.getvalue().strip(), expected_message)

        self.assertFailure(tunnel_deferred, NoConnection)
        tunnel_deferred.addCallback(verify_failure)
        return tunnel_deferred

    def test_tunnel_protocol_notes_invalid_key(self):
        log = self.capture_logging(level=logging.ERROR)
        client = self.mocker.mock()
        client.close()
        self.mocker.replay()

        tunnel_deferred = Deferred()
        protocol = ClientTunnelProtocol(client, tunnel_deferred)

        message = "Permission denied"
        protocol.errReceived(message)

        expected_message = "Invalid SSH key"

        def verify_failure(error):
            self.assertEqual(error.args[0], expected_message)
            self.assertEqual(log.getvalue().strip(), expected_message)

        self.failUnlessFailure(tunnel_deferred, NoConnection)
        tunnel_deferred.addCallback(verify_failure)
        return tunnel_deferred

    def test_tunnel_protocol_notes_connection_refused(self):
        client = self.mocker.mock()
        client.close()
        self.mocker.replay()

        tunnel_deferred = Deferred()
        protocol = ClientTunnelProtocol(client, tunnel_deferred)

        message = "blah blah blah Connection refused blah blah"
        protocol.errReceived(message)

        expected_message = "Connection refused"

        def verify_failure(error):
            self.assertEqual(error.args[0], expected_message)

        self.failUnlessFailure(tunnel_deferred, NoConnection)
        tunnel_deferred.addCallback(verify_failure)
        return tunnel_deferred
