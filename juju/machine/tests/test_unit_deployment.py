"""
Service Unit Deployment unit tests
"""
import logging
import os
import sys

from twisted.internet.protocol import ProcessProtocol
from twisted.internet.defer import inlineCallbacks, succeed

import juju
from juju.charm import get_charm_from_path
from juju.charm.tests.test_repository import RepositoryTestBase
from juju.lib.lxc import LXCContainer
from juju.lib.mocker import MATCH, ANY
from juju.lib.twistutils import get_module_directory
from juju.machine.unit import UnitMachineDeployment, UnitContainerDeployment
from juju.machine.errors import UnitDeploymentError
from juju.tests.common import get_test_zookeeper_address


MATCH_PROTOCOL = MATCH(lambda x: isinstance(x, ProcessProtocol))


class UnitMachineDeploymentTest(RepositoryTestBase):

    def setUp(self):
        super(UnitMachineDeploymentTest, self).setUp()
        self.charm = get_charm_from_path(self.sample_dir1)
        self.bundle = self.charm.as_bundle()
        self.juju_directory = self.makeDir()
        self.units_directory = os.path.join(self.juju_directory, "units")
        os.mkdir(self.units_directory)
        self.unit_name = "wordpress/0"

        self.deployment = UnitMachineDeployment(
            self.unit_name,
            self.juju_directory)

        self.assertEqual(
            self.deployment.unit_agent_module, "juju.agents.unit")
        self.deployment.unit_agent_module = "juju.agents.dummy"

    def process_kill(self, pid):
        try:
            os.kill(pid, 9)
        except OSError:
            pass

    def test_unit_name_with_path_manipulation_raises_assertion(self):
        self.assertRaises(
            AssertionError,
            UnitMachineDeployment,
            "../../etc/password/zebra/0",
            self.units_directory)

    def test_unit_directory(self):
        self.assertEqual(
            self.deployment.directory,
            os.path.join(self.units_directory,
                         self.unit_name.replace("/", "-")))

    def test_unit_pid_file(self):
        self.assertEqual(
            self.deployment.pid_file,
            os.path.join(self.units_directory,
                         "%s.pid" % (self.unit_name.replace("/", "-"))))

    def test_service_unit_start(self):
        """
        Starting a service unit will result in a unit workspace being created
        if it does not exist and a running service unit agent.
        """

        d = self.deployment.start(
            "0", get_test_zookeeper_address(), self.bundle)

        @inlineCallbacks
        def validate_result(result):
            # give process time to write its pid
            yield self.sleep(0.1)
            self.addCleanup(
                self.process_kill,
                int(open(self.deployment.pid_file).read()))
            self.assertEqual(result, True)

        d.addCallback(validate_result)
        return d

    def test_deployment_get_environment(self):
        zk_address = get_test_zookeeper_address()
        environ = self.deployment.get_environment(21, zk_address)
        environ.pop("PYTHONPATH")
        self.assertEqual(environ["JUJU_HOME"], self.juju_directory)
        self.assertEqual(environ["JUJU_UNIT_NAME"], self.unit_name)
        self.assertEqual(environ["JUJU_ZOOKEEPER"], zk_address)
        self.assertEqual(environ["JUJU_MACHINE_ID"], "21")

    def test_service_unit_start_with_integer_machine_id(self):
        """
        Starting a service unit will result in a unit workspace being created
        if it does not exist and a running service unit agent.
        """
        d = self.deployment.start(
            21, get_test_zookeeper_address(), self.bundle)

        @inlineCallbacks
        def validate_result(result):
            # give process time to write its pid
            yield self.sleep(0.1)
            self.addCleanup(
                self.process_kill,
                int(open(self.deployment.pid_file).read()))
            self.assertEqual(result, True)

        d.addCallback(validate_result)
        return d

    def test_service_unit_start_with_agent_startup_error(self):
        """
        Starting a service unit will result in a unit workspace being created
        if it does not exist and a running service unit agent.
        """
        self.deployment.unit_agent_module = "magichat.xr1"
        d = self.deployment.start(
            "0", get_test_zookeeper_address(), self.bundle)

        self.failUnlessFailure(d, UnitDeploymentError)

        def validate_result(error):
            self.assertIn("No module named magichat", str(error))

        d.addCallback(validate_result)
        return d

    def test_service_unit_start_agent_arguments(self):
        """
        Starting a service unit will start a service unit agent with arguments
        denoting the current machine id, zookeeper server location, and the
        unit name. Additionally it will configure the log and pid file
        locations.
        """
        machine_id = "0"
        zookeeper_hosts = "menagerie.example.com:2181"

        from twisted.internet import reactor
        mock_reactor = self.mocker.patch(reactor)

        environ = dict(os.environ)
        environ["JUJU_UNIT_NAME"] = self.unit_name
        environ["JUJU_HOME"] = self.juju_directory
        environ["JUJU_MACHINE_ID"] = machine_id
        environ["JUJU_ZOOKEEPER"] = zookeeper_hosts
        environ["PYTHONPATH"] = ":".join(
            filter(None, [
                os.path.dirname(get_module_directory(juju)),
                environ.get("PYTHONPATH")]))

        pid_file = os.path.join(
            self.units_directory,
            "%s.pid" % self.unit_name.replace("/", "-"))

        log_file = os.path.join(
            self.deployment.directory,
            "charm.log")

        args = [sys.executable, "-m", "juju.agents.dummy", "-n",
                "--pidfile", pid_file, "--logfile", log_file]

        mock_reactor.spawnProcess(
            MATCH_PROTOCOL, sys.executable, args, environ)
        self.mocker.replay()
        self.deployment.start(
            machine_id, zookeeper_hosts, self.bundle)

    def xtest_service_unit_start_pre_unpack(self):
        """
        Attempting to start a charm before the charm is unpacked
        results in an exception.
        """
        error = yield self.assertFailure(
            self.deployment.start(
                "0", get_test_zookeeper_address(), self.bundle),
            UnitDeploymentError)
        self.assertEquals(str(error), "Charm must be unpacked first.")

    @inlineCallbacks
    def test_service_unit_destroy(self):
        """
        Forcibly stop a unit, and destroy any directories associated to it
        on the machine, and kills the unit agent process.
        """
        yield self.deployment.start(
            "0", get_test_zookeeper_address(), self.bundle)
        # give the process time to write its pid file
        yield self.sleep(0.1)
        pid = int(open(self.deployment.pid_file).read())
        yield self.deployment.destroy()
        # give the process time to die.
        yield self.sleep(0.1)
        e = self.assertRaises(OSError, os.kill, pid, 0)
        self.assertEqual(e.errno, 3)
        self.assertFalse(os.path.exists(self.deployment.directory))
        self.assertFalse(os.path.exists(self.deployment.pid_file))

    def test_service_unit_destroy_stale_pid(self):
        """
        A stale pid file does not cause any errors.

        We mock away is_running as otherwise it will check for this, but
        there exists a small window when the result may disagree.
        """
        self.makeFile("8917238", path=self.deployment.pid_file)
        mock_deployment = self.mocker.patch(self.deployment)
        mock_deployment.is_running()
        self.mocker.result(succeed(True))
        self.mocker.replay()
        return self.deployment.destroy()

    def test_service_unit_destroy_perm_error(self):
        """
        A stale pid file does not cause any errors.

        We mock away is_running as otherwise it will check for this, but
        there exists a small window when the result may disagree.
        """
        if os.geteuid() == 0:
            return
        self.makeFile("1", path=self.deployment.pid_file)
        mock_deployment = self.mocker.patch(self.deployment)
        mock_deployment.is_running()
        self.mocker.result(succeed(True))
        self.mocker.replay()
        return self.assertFailure(self.deployment.destroy(), OSError)

    @inlineCallbacks
    def test_service_unit_destroy_undeployed(self):
        """
        If the unit is has not been deployed, nothing happens.
        """
        yield self.deployment.destroy()
        self.assertFalse(os.path.exists(self.deployment.directory))

    @inlineCallbacks
    def test_service_unit_destroy_not_running(self):
        """
        If the unit is not running, then destroy will just remove
        its directory.
        """
        self.deployment.unpack_charm(self.bundle)
        self.assertTrue(os.path.exists(self.deployment.directory))
        yield self.deployment.destroy()
        self.assertFalse(os.path.exists(self.deployment.directory))

    def test_unpack_charm(self):
        """
        The deployment unpacks a charm bundle into the unit workspace.
        """
        self.deployment.unpack_charm(self.bundle)
        unit_path = os.path.join(
            self.units_directory, self.unit_name.replace("/", "-"))

        self.assertTrue(os.path.exists(unit_path))
        charm_path = os.path.join(unit_path, "charm")
        self.assertTrue(os.path.exists(charm_path))
        charm = get_charm_from_path(charm_path)
        self.assertEqual(
            charm.get_revision(), self.charm.get_revision())

    def test_unpack_charm_exception_invalid_charm(self):
        """
        If the charm bundle is corrupted or invalid a deployment specific
        error is raised.
        """
        error = self.assertRaises(
            UnitDeploymentError,
            self.deployment.unpack_charm, self.charm)
        self.assertEquals(
            str(error),
            "Invalid charm for deployment: %s" % self.charm.path)

    def test_is_running_no_pid_file(self):
        """
        If there is no pid file the service unit is not running.
        """
        self.assertEqual((yield self.deployment.is_running()), False)

    def test_is_running(self):
        """
        The service deployment will check the pid and validate
        that the pid found is a running process.
        """
        self.makeFile(
            str(os.getpid()), path=self.deployment.pid_file)
        self.assertEqual((yield self.deployment.is_running()), True)

    def test_is_running_against_unknown_error(self):
        """
        If we don't have permission to access the process, the
        original error should get passed along.
        """
        if os.geteuid() == 0:
            return
        self.makeFile("1", path=self.deployment.pid_file)
        self.assertFailure(self.deployment.is_running(), OSError)

    def test_is_running_invalid_pid_file(self):
        """
        If the pid file is corrupted on disk, and does not contain
        a valid integer, then the agent is not running.
        """
        self.makeFile("abcdef", path=self.deployment.pid_file)
        self.assertEqual(
            (yield self.deployment.is_running()), False)

    def test_is_running_invalid_pid(self):
        """
        If the pid file refers to an invalid process then the
        agent is not running.
        """
        self.makeFile("669966", path=self.deployment.pid_file)
        self.assertEqual(
            (yield self.deployment.is_running()), False)


upstart_job_sample = '''\
description "Unit agent for riak/0"
author "Juju Team <juju@lists.canonical.com>"
start on start on filesystem or runlevel [2345]
stop on runlevel [!2345]

respawn

env JUJU_MACHINE_ID="0"
env JUJU_HOME="/var/lib/juju"
env JUJU_ZOOKEEPER="127.0.1.1:2181"
env JUJU_UNIT_NAME="riak/0"'''


class UnitContainerDeploymentTest(RepositoryTestBase):

    unit_name = "riak/0"

    @inlineCallbacks
    def setUp(self):
        yield super(UnitContainerDeploymentTest, self).setUp()
        self.juju_home = self.makeDir()
        # Setup unit namespace
        environ = dict(os.environ)
        environ["JUJU_UNIT_NS"] = "ns1"
        self.change_environment(**environ)

        self.unit_deploy = UnitContainerDeployment(
            self.unit_name, self.juju_home)
        self.charm = get_charm_from_path(self.sample_dir1)
        self.bundle = self.charm.as_bundle()
        self.output = self.capture_logging("unit.deploy", level=logging.DEBUG)

    def get_normalized(self, output):
        # strip python path for comparison
        return "\n".join(filter(None, output.split("\n"))[:-2])

    def test_get_container_name(self):
        self.assertEqual(
            "ns1-riak-0",
            self.unit_deploy.container_name)

    def test_get_upstart_job(self):
        upstart_job = self.unit_deploy.get_upstart_unit_job(
            0, "127.0.1.1:2181")
        job = self.get_normalized(upstart_job)
        self.assertIn('JUJU_ZOOKEEPER="127.0.1.1:2181"', job)
        self.assertIn('JUJU_MACHINE_ID="0"', job)
        self.assertIn('JUJU_UNIT_NAME="riak/0"', job)

    @inlineCallbacks
    def test_destroy(self):
        mock_container = self.mocker.patch(self.unit_deploy.container)
        mock_container.destroy()
        self.mocker.replay()

        yield self.unit_deploy.destroy()

        output = self.output.getvalue()
        self.assertIn("Destroying container...", output)
        self.assertIn("Destroyed container for riak/0", output)

    @inlineCallbacks
    def test_origin_usage(self):
        """The machine agent is started with a origin environment variable
        """
        mock_container = self.mocker.patch(LXCContainer)
        mock_container.is_constructed()
        self.mocker.result(True)
        mock_container.is_constructed()
        self.mocker.result(True)
        self.mocker.replay()

        environ = dict(os.environ)
        environ["JUJU_ORIGIN"] = "lp:~juju/foobar"

        self.change_environment(**environ)
        unit_deploy = UnitContainerDeployment(
            self.unit_name, self.juju_home)
        container = yield unit_deploy._get_master_template(
            "local", "127.0.0.1:1", "abc")
        self.assertEqual(container.origin, "lp:~juju/foobar")
        self.assertEqual(
            container.customize_log,
            os.path.join(self.juju_home, "units", "master-customize.log"))

    @inlineCallbacks
    def test_start(self):
        container = LXCContainer(self.unit_name, None, None, None)
        rootfs = self.makeDir()
        env = dict(os.environ)
        env["JUJU_PUBLIC_KEY"] = "dsa ..."
        self.change_environment(**env)

        mock_deploy = self.mocker.patch(self.unit_deploy)
        # this minimally validates that we are also called with the
        # expect public key
        mock_deploy._get_container(ANY, ANY, ANY, env["JUJU_PUBLIC_KEY"])
        self.mocker.result((container, rootfs))

        mock_container = self.mocker.patch(container)
        mock_container.run()

        self.mocker.replay()

        self.unit_deploy.directory = rootfs
        os.makedirs(os.path.join(rootfs, "etc", "init"))

        yield self.unit_deploy.start("0", "127.0.1.1:2181", self.bundle)

        # Verify the upstart job
        upstart_agent_name = "%s-unit-agent.conf" % (
            self.unit_name.replace("/", "-"))
        content = open(
            os.path.join(rootfs, "etc", "init", upstart_agent_name)).read()
        job = self.get_normalized(content)
        self.assertIn('JUJU_ZOOKEEPER="127.0.1.1:2181"', job)
        self.assertIn('JUJU_MACHINE_ID="0"', job)
        self.assertIn('JUJU_UNIT_NAME="riak/0"', job)

        # Verify the symlink exists
        self.assertTrue(os.path.lexists(os.path.join(
            self.unit_deploy.juju_home, "units",
            self.unit_deploy.unit_path_name, "unit.log")))

        # Verify the charm is on disk.
        self.assertTrue(os.path.exists(os.path.join(
            self.unit_deploy.directory, "var", "lib", "juju", "units",
            self.unit_deploy.unit_path_name, "charm", "metadata.yaml")))

        # Verify the directory structure in the unit.
        self.assertTrue(os.path.exists(os.path.join(
            self.unit_deploy.directory, "var", "lib", "juju", "state")))
        self.assertTrue(os.path.exists(os.path.join(
            self.unit_deploy.directory, "var", "log", "juju")))

        # Verify log output
        output = self.output.getvalue()
        self.assertIn("Charm extracted into container", output)
        self.assertIn("Started container for %s" % self.unit_deploy.unit_name,
                      output)

    @inlineCallbacks
    def test_get_container(self):
        rootfs = self.makeDir()
        container = LXCContainer(self.unit_name, None, None, None)

        mock_deploy = self.mocker.patch(self.unit_deploy)
        mock_deploy._get_master_template(ANY, ANY, ANY)
        self.mocker.result(container)

        mock_container = self.mocker.patch(container)
        mock_container.clone(ANY)
        self.mocker.result(container)

        self.mocker.replay()

        container, rootfs = yield self.unit_deploy._get_container(
            "0", "127.0.0.1:2181", None, "dsa...")

        output = self.output.getvalue()
        self.assertIn("Container created for %s" % self.unit_deploy.unit_name,
                      output)
