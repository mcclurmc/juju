import json
import yaml

from twisted.internet.defer import inlineCallbacks

from juju.control import main
from juju.control.tests.common import ControlToolTest
from juju.lib.tests.test_zklog import LogTestBase


class ControlDebugLogTest(LogTestBase, ControlToolTest):

    @inlineCallbacks
    def setUp(self):
        yield super(ControlDebugLogTest, self).setUp()
        config = {
            "environments": {
                "firstenv": {
                    "type": "dummy", "admin-secret": "homer"}}}
        self.write_config(yaml.dump(config))
        self.config.load()

    @inlineCallbacks
    def test_replay(self):
        """
        Older logs can be replayed without affecting the current
        position pointer.
        """
        self.log = yield self.get_configured_log()
        for i in range(20):
            self.log.info(str(i))

        cli_done = self.setup_cli_reactor()
        self.setup_exit()
        self.mocker.replay()

        stream = self.capture_stream("stdout")
        yield self.client.set("/logs", json.dumps({"next-log-index": 15}))
        main(["debug-log", "--replay", "--limit", "5"])
        yield cli_done
        output = stream.getvalue().split("\n")
        for i in range(5):
            self.assertIn("unit:mysql/0: test-zk-log INFO: %s" % i, output[i])

        # We can run it again and get the same output
        self.mocker.reset()
        cli_done = self.setup_cli_reactor()
        self.setup_exit()
        self.mocker.replay()

        stream = self.capture_stream("stdout")
        yield self.client.set("/logs", json.dumps({"next-log-index": 15}))
        main(["debug-log", "--replay", "--limit", "5"])
        yield cli_done
        output2 = stream.getvalue().split("\n")
        self.assertEqual(output, output2)

        content, stat = yield self.client.get("/logs")
        self.assertEqual(json.loads(content), {"next-log-index": 15})

    @inlineCallbacks
    def test_include_agent(self):
        """Messages can be filtered to include only certain agents."""
        log = yield self.get_configured_log("hook.output", "unit:cassandra/10")
        log2 = yield self.get_configured_log("hook.output", "unit:cassandra/1")
        # example of an agent context name sans ":"
        log3 = yield self.get_configured_log("unit.workflow", "mysql/1")

        for i in range(5):
            log.info(str(i))

        for i in range(5):
            log2.info(str(i))

        for i in range(5):
            log3.info(str(i))

        cli_done = self.setup_cli_reactor()
        self.setup_exit()
        self.mocker.replay()

        stream = self.capture_stream("stdout")
        main(["debug-log", "--include", "cassandra/1", "--limit", "4"])
        yield cli_done

        output = stream.getvalue()
        self.assertNotIn("mysql/1", output)
        self.assertNotIn("cassandra/10", output)
        self.assertIn("cassandra/1", output)

    @inlineCallbacks
    def test_include_log(self):
        """Messages can be filtered to include only certain log channels."""
        log = yield self.get_configured_log("hook.output", "unit:cassandra/1")
        log2 = yield self.get_configured_log("unit.workflow", "unit:mysql/1")
        log3 = yield self.get_configured_log("provisioning", "agent:provision")

        for i in range(5):
            log.info(str(i))

        for i in range(5):
            log2.info(str(i))

        for i in range(5):
            log3.info(str(i))

        cli_done = self.setup_cli_reactor()
        self.setup_exit()
        self.mocker.replay()

        stream = self.capture_stream("stdout")
        main(["debug-log", "--include", "unit.workflow",
              "-i", "agent:provision", "--limit", "8"])
        yield cli_done

        output = stream.getvalue()
        self.assertNotIn("cassandra/1", output)
        self.assertIn("mysql/1", output)
        self.assertIn("provisioning", output)

    @inlineCallbacks
    def test_exclude_agent(self):
        """Messages can be filterd to exclude certain agents."""

        log = yield self.get_configured_log("hook.output", "unit:cassandra/1")
        log2 = yield self.get_configured_log("unit.workflow", "unit:mysql/1")

        for i in range(5):
            log.info(str(i))

        for i in range(5):
            log2.info(str(i))

        cli_done = self.setup_cli_reactor()
        self.setup_exit()
        self.mocker.replay()

        stream = self.capture_stream("stdout")
        main(["debug-log", "--exclude", "cassandra/1", "--limit", "4"])
        yield cli_done

        output = stream.getvalue()
        self.assertNotIn("cassandra/1", output)
        self.assertIn("mysql/1", output)

    @inlineCallbacks
    def test_exclude_log(self):
        """Messages can be filtered to exclude certain log channels."""

        log = yield self.get_configured_log("hook.output", "unit:cassandra/1")
        log2 = yield self.get_configured_log("provisioning", "agent:provision")
        log3 = yield self.get_configured_log("unit.workflow", "unit:mysql/1")

        for i in range(5):
            log.info(str(i))

        for i in range(5):
            log2.info(str(i))

        for i in range(5):
            log3.info(str(i))

        cli_done = self.setup_cli_reactor()
        self.setup_exit()
        self.mocker.replay()

        stream = self.capture_stream("stdout")
        main(["debug-log", "-x", "unit:cass*", "-x", "provisioning",
              "--limit", "4"])
        yield cli_done

        output = stream.getvalue()
        self.assertNotIn("cassandra/1", output)
        self.assertNotIn("provisioning", output)
        self.assertIn("mysql/1", output)

    @inlineCallbacks
    def test_complex_filter(self):
        """Messages can be filtered to include only certain log channels."""
        log = yield self.get_configured_log("hook.output", "unit:cassandra/1")
        log2 = yield self.get_configured_log("hook.output", "unit:cassandra/2")
        log3 = yield self.get_configured_log("hook.output", "unit:cassandra/3")

        for i in range(5):
            log.info(str(i))

        for i in range(5):
            log2.info(str(i))

        for i in range(5):
            log3.info(str(i))

        cli_done = self.setup_cli_reactor()
        self.setup_exit()
        self.mocker.replay()

        stream = self.capture_stream("stdout")
        main(
            ["debug-log", "-i", "cassandra/*", "-x", "cassandra/1", "-n", "8"])
        yield cli_done

        output = stream.getvalue()
        self.assertNotIn("cassandra/1", output)
        self.assertIn("cassandra/2", output)
        self.assertIn("cassandra/3", output)

    @inlineCallbacks
    def test_log_level(self):
        """Messages can be filtered by log level."""
        log = yield self.get_configured_log()
        for i in range(5):
            log.info(str(i))
        for i in range(5):
            log.debug(str(i))
        for i in range(5):
            log.warning(str(i))

        cli_done = self.setup_cli_reactor()
        self.setup_exit()
        self.mocker.replay()

        stream = self.capture_stream("stdout")
        main(["debug-log", "--level", "WARNING", "--limit", "4"])
        yield cli_done

        output = stream.getvalue().split("\n")
        for i in range(4):
            self.assertIn("WARNING", output[i])

    @inlineCallbacks
    def test_log_file(self):
        """Logs can be sent to a file."""
        log = yield self.get_configured_log()
        for i in range(5):
            log.info(str(i))

        cli_done = self.setup_cli_reactor()
        self.setup_exit()
        self.mocker.replay()

        file_path = self.makeFile()
        main(["debug-log", "--output", file_path, "--limit", "4"])
        yield cli_done

        output = open(file_path).read().split("\n")
        for i in range(4):
            self.assertIn("unit:mysql/0: test-zk-log INFO: %s" % i, output[i])

    @inlineCallbacks
    def test_log_object(self):
        """Messages that utilize string interpolation are rendered correctly.
        """

        class Foobar(object):
            def __init__(self, v):
                self._v = v

            def __str__(self):
                return str("Foobar:%s" % self._v)

        log = yield self.get_configured_log("unit.workflow", "unit:mysql/1")

        log.info("found a %s", Foobar(21))
        log.info("jack jumped into a %s", Foobar("cauldron"))

        cli_done = self.setup_cli_reactor()
        self.setup_exit()
        self.mocker.replay()

        stream = self.capture_stream("stdout")
        main(["debug-log", "--limit", "2"])
        yield cli_done

        output = stream.getvalue()

        self.assertIn("Foobar:21", output)
        self.assertIn("Foobar:cauldron", output)
