import apt
from juju.lib.testing import TestCase
from juju.providers.local.pkg import check_packages


class PackageInstallTest(TestCase):

    def test_package_reports_missing(self):
        pkg_name = "tryton-modules-sale-opportunity"
        missing_pkg = self.mocker.mock()
        mock_cache = self.mocker.patch(apt.Cache)
        mock_cache[pkg_name]
        self.mocker.result(missing_pkg)
        missing_pkg.is_installed
        self.mocker.result(False)
        self.mocker.replay()
        self.assertEqual(
            check_packages(pkg_name), set([pkg_name]))

    def test_package_reports_installed(self):
        self.assertEqual(
            check_packages("python-apt"), set())

    def test_package_handles_unknown(self):
        self.assertEqual(
            check_packages("global-gook"),
            set(["global-gook"]))
