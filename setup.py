from distutils.core import setup
from glob import glob
import os


def find_packages():
    """
    Compatibility wrapper.

    Taken from storm setup.py.
    """
    try:
        from setuptools import find_packages
        return find_packages()
    except ImportError:
        pass
    packages = []
    for directory, subdirectories, files in os.walk("juju"):
        if '__init__.py' in files:
            packages.append(directory.replace(os.sep, '.'))
    return packages


setup(
    name="juju",
    version="0.5", # Change debian/changelog too, for daily builds.
    description="Cloud automation and orchestration",
    author="Juju Developers",
    author_email="juju@lists.ubuntu.com",
    url="https://launchpad.net/juju",
    license="GPL",
    packages=find_packages(),
    package_data={"juju.lib.lxc": ["data/juju-create", "data/lxc.conf"]},
    scripts=glob("./bin/*"),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "Intended Audience :: Information Technology",
        "Programming Language :: Python",
        "Topic :: Database",
        "Topic :: Internet :: WWW/HTTP",
       ],
    )
