from juju.lib.testing import TestCase
from juju.lib.loader import get_callable


class LoaderTest(TestCase):

    def test_loader_no_module(self):
        self.failUnlessRaises(ImportError, get_callable,
                              "missing.module")

    def test_loader_bad_specification(self):
        for el in ['', None, 123]:
            e = self.failUnlessRaises(ValueError, get_callable, el)
            self.failUnlessEqual(str(e),
                                 "Invalid import specification: %r" % el)

    def test_loader_non_callable(self):
        # os.sep is a valid import but isn't a callable
        self.failUnlessRaises(ImportError, get_callable, "os.sep")

    def test_loader_valid_specification(self):
        split = get_callable("os.path.split")
        assert callable(split)
        assert split("foo/bar") == ("foo", "bar")

    def test_loader_retrieve_provider(self):
        providers = ["juju.providers.dummy.MachineProvider",
                     "juju.providers.ec2.MachineProvider"]

        for provider_name in providers:
            provider = get_callable(provider_name)
            self.assertEqual(provider.__name__, "MachineProvider")
            self.assertTrue(callable(provider))
