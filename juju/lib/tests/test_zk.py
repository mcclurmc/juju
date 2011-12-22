import os
import zookeeper

from twisted.internet.defer import inlineCallbacks

from txzookeeper import ZookeeperClient

from juju.lib.testing import TestCase
from juju.lib.zk import Zookeeper, check_zookeeper


sample_package_environment_conf = """\
NAME=zookeeper
ZOOCFGDIR=/etc/$NAME/conf

# TODO this is really ugly
# How to find out, which jars are needed?
# seems, that log4j requires the log4j.properties file to be in the classpath
CLASSPATH="$ZOOCFGDIR:/usr/share/java/jline.jar:/usr/share/java/log4j-1.2.jar:/usr/share/java/xercesImpl.jar:/usr/share/java/xmlParserAPIs.jar:/usr/share/java/zookeeper.jar"

ZOOCFG="$ZOOCFGDIR/zoo.cfg"
ZOO_LOG_DIR=/var/log/$NAME
USER=$NAME
GROUP=$NAME
PIDDIR=/var/run/$NAME
PIDFILE=$PIDDIR/$NAME.pid
SCRIPTNAME=/etc/init.d/$NAME
JAVA=/usr/bin/java
ZOOMAIN="org.apache.zookeeper.server.quorum.QuorumPeerMain"
ZOO_LOG4J_PROP="INFO,ROLLINGFILE"
JMXLOCALONLY=false
JAVA_OPTS=""
"""


class LocalManagedZookeeperTestCase(TestCase):

    def test_get_class_path_from_build(self):
        data_dir = self.makeDir()
        software_dir = self.makeDir()

        os.mkdir(os.path.join(software_dir, "build"))
        lib_dir = os.path.join(software_dir, "build", "lib")
        os.mkdir(lib_dir)

        self.makeFile("", path=os.path.join(
            software_dir, "build", "zookeeper-3.4.0.jar"))

        for p in [
            "jline-0.9.94.jar",
            "netty-3.2.2.Final.jar",
            "log4j-1.2.15.jar",
            "slf4j-log4j12-1.6.1.jar",
            "slf4j-api-1.6.1.jar"]:
            self.makeFile("", path=os.path.join(lib_dir, p))

        instance = Zookeeper(data_dir, 12345, zk_location=software_dir)
        class_path = instance.get_class_path()
        self.assertEqual(class_path.index(data_dir), 0)
        self.assertIn(
            os.path.join(lib_dir, "log4j-1.2.15.jar"), class_path)
        self.assertIn(
            os.path.join(software_dir, "build", "zookeeper-3.4.0.jar"),
            class_path)

    def test_get_class_path_from_package_static(self):
        data_dir = self.makeDir()
        instance = Zookeeper(data_dir, 12345)
        instance.package_class_path_file = sample_package_environment_conf
        class_path = instance.get_class_path()
        self.assertEqual(class_path.index(data_dir), 0)
        self.assertIn("/usr/share/java/jline.jar", class_path)
        self.assertIn("/usr/share/java/log4j-1.2.jar", class_path)
        self.assertIn("/usr/share/java/zookeeper.jar", class_path)

    def test_get_class_path_from_package(self):
        data_dir = self.makeDir()
        instance = Zookeeper(data_dir, 12345)
        class_path = instance.get_class_path()
        self.assertEqual(class_path.index(data_dir), 0)
        self.assertIn("/usr/share/java/jline.jar", class_path)
        self.assertIn("/usr/share/java/log4j-1.2.jar", class_path)
        self.assertIn("/usr/share/java/zookeeper.jar", class_path)

    def test_get_zookeeper_variables(self):
        """All data files are contained in the specified directory.
        """
        data_dir = self.makeDir()
        instance = Zookeeper(data_dir, 12345)
        variables = instance.get_zookeeper_variables()
        variables.pop("class_path")
        for v in variables.values():
            self.assertTrue(v.startswith(data_dir))

    @inlineCallbacks
    def test_managed_zookeeper(self):
        zookeeper.set_debug_level(0)

        # Start zookeeper
        data_dir = self.makeDir()
        instance = Zookeeper(data_dir, 12345)
        yield instance.start()
        self.assertTrue(instance.running)

        # Connect a client
        client = ZookeeperClient("127.0.1.1:12345")
        yield client.connect()
        stat = yield client.exists("/")
        yield client.close()
        self.assertTrue(stat)

        # Stop Instance
        yield instance.stop()
        self.assertFalse(instance.running)

        self.assertFalse(os.path.exists(data_dir))

    if not check_zookeeper():
        notinst = "Zookeeper not installed in the system"
        test_managed_zookeeper.skip = notinst
        test_get_class_path_from_package_static.skip = notinst
        test_get_zookeeper_variables.skip = notinst
        test_get_class_path_from_package.skip = notinst
