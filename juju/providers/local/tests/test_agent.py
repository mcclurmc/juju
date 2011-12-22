import os
import tempfile
import subprocess
from twisted.internet.defer import inlineCallbacks, succeed

from juju.lib.testing import TestCase
from juju.lib.lxc.tests.test_lxc import run_lxc_tests
from juju.tests.common import get_test_zookeeper_address
from juju.providers.local.agent import ManagedMachineAgent


class ManagedAgentTest(TestCase):

    @inlineCallbacks
    def test_managed_agent_args(self):

        captured_args = []

        def intercept_args(args):
            captured_args.extend(args)
            return True

        self.patch(subprocess, "check_call", intercept_args)

        juju_directory = self.makeDir()
        pid_file = self.makeFile()
        log_file = self.makeFile()

        agent = ManagedMachineAgent(
            pid_file, get_test_zookeeper_address(),
            juju_directory=juju_directory,
            log_file=log_file, juju_unit_namespace="ns1",
            juju_origin="lp:juju/trunk")

        mock_agent = self.mocker.patch(agent)
        mock_agent.is_running()
        self.mocker.result(succeed(False))
        self.mocker.replay()

        self.assertEqual(agent.juju_origin, "lp:juju/trunk")
        yield agent.start()

        # Verify machine agent environment
        env_vars = dict(
            [arg.split("=") for arg in captured_args if "=" in arg])
        env_vars.pop("PYTHONPATH")
        self.assertEqual(
            env_vars,
            dict(JUJU_ZOOKEEPER=get_test_zookeeper_address(),
                 JUJU_MACHINE_ID="0",
                 JUJU_HOME=juju_directory,
                 JUJU_ORIGIN="lp:juju/trunk",
                 JUJU_UNIT_NS="ns1"))

    @inlineCallbacks
    def test_managed_agent_root(self):
        juju_directory = self.makeDir()
        pid_file = tempfile.mktemp()
        log_file = tempfile.mktemp()

        # The pid file and log file get written as root
        def cleanup_root_file(cleanup_file):
            subprocess.check_call(
                ["sudo", "rm", "-f", cleanup_file], stderr=subprocess.STDOUT)
        self.addCleanup(cleanup_root_file, pid_file)
        self.addCleanup(cleanup_root_file, log_file)

        agent = ManagedMachineAgent(
            pid_file, machine_id="0", log_file=log_file,
            zookeeper_hosts=get_test_zookeeper_address(),
            juju_directory=juju_directory)

        agent.agent_module = "juju.agents.dummy"
        self.assertFalse((yield agent.is_running()))
        yield agent.start()

        # Give a moment for the process to start and write its config
        yield self.sleep(0.1)
        self.assertTrue((yield agent.is_running()))

        # running start again is fine, detects the process is running
        yield agent.start()
        yield agent.stop()
        self.assertFalse((yield agent.is_running()))

        # running stop again is fine, detects the process is stopped.
        yield agent.stop()

        self.assertFalse(os.path.exists(pid_file))

        # Stop raises runtime error if the process doesn't match up.
        with open(pid_file, "w") as pid_handle:
            pid_handle.write("1")
        self.assertFailure(agent.stop(), RuntimeError)

    # Reuse the lxc flag for tests needing sudo
    test_managed_agent_root.skip = run_lxc_tests()
