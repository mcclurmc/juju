import logging
import os
import tempfile

from juju.lib.zk import Zookeeper
from contextlib import contextmanager


__all__ = ("ManagedZooKeeper",
           "zookeeper_test_context",
           "get_zookeeper_test_address")

log = logging.getLogger("juju.tests.common")


"""Global to manage the ZK test address - only for testing of course!"""
_zookeeper_address = "127.0.0.1:2181"


def get_test_zookeeper_address():
    """Get the current test ZK address, such as '127.0.0.1:2181'"""
    return _zookeeper_address


@contextmanager
def zookeeper_test_context(install_path, port=28181):
    """Manage context to run/stop a ZooKeeper for testing and related vars.

    @param install_path: The path to the install for ZK. Bare word "system"
                         causes special behavior to use system conf for ZK
    @param port: The port to run the managed ZK instance
    """
    global _zookeeper_address

    saved_zookeeper_address = _zookeeper_address
    saved_env = os.environ.get("ZOOKEEPER_ADDRESS")
    test_zookeeper = Zookeeper(
        tempfile.mkdtemp(), port,
        zk_location=install_path, use_deferred=False)

    test_zookeeper.start()
    os.environ["ZOOKEEPER_ADDRESS"] = test_zookeeper.address
    _zookeeper_address = test_zookeeper.address
    try:
        yield test_zookeeper
    finally:
        test_zookeeper.stop()
        _zookeeper_address = saved_zookeeper_address
        if saved_env:
            os.environ["ZOOKEEPER_ADDRESS"] = saved_env
        else:
            del os.environ["ZOOKEEPER_ADDRESS"]
