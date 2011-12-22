from juju.errors import (
    JujuError, FileNotFound, FileAlreadyExists,
    NoConnection, InvalidHost, InvalidUser, ProviderError, CloudInitError,
    ProviderInteractionError, CannotTerminateMachine, MachinesNotFound,
    EnvironmentPending, EnvironmentNotFound, IncompatibleVersion,
    InvalidPlacementPolicy)

from juju.lib.testing import TestCase


class ErrorsTest(TestCase):

    def assertIsJujuError(self, error):
        self.assertTrue(isinstance(error, JujuError),
                        "%s is not a subclass of JujuError" %
                        error.__class__.__name__)

    def test_IncompatibleVersion(self):
        error = IncompatibleVersion(123, 42)
        self.assertEqual(
            str(error),
            "Incompatible juju protocol versions (found 123, want 42)")
        self.assertIsJujuError(error)

    def test_FileNotFound(self):
        error = FileNotFound("/path")
        self.assertEquals(str(error), "File was not found: '/path'")
        self.assertIsJujuError(error)

    def test_FileAlreadyExists(self):
        error = FileAlreadyExists("/path")
        self.assertEquals(str(error),
                          "File already exists, won't overwrite: '/path'")
        self.assertIsJujuError(error)

    def test_NoConnection(self):
        error = NoConnection("unable to connect")
        self.assertIsJujuError(error)

    def test_InvalidHost(self):
        error = InvalidHost("Invalid host for SSH forwarding")
        self.assertTrue(isinstance(error, NoConnection))
        self.assertEquals(
            str(error),
            "Invalid host for SSH forwarding")

    def test_InvalidUser(self):
        error = InvalidUser("Invalid SSH key")
        self.assertTrue(isinstance(error, NoConnection))
        self.assertEquals(
            str(error),
            "Invalid SSH key")

    def test_ProviderError(self):
        error = ProviderError("Invalid credentials")
        self.assertIsJujuError(error)

    def test_CloudInitError(self):
        error = CloudInitError("BORKEN")
        self.assertIsJujuError(error)

    def test_ProviderInteractionError(self):
        error = ProviderInteractionError("Bad Stuff")
        self.assertIsJujuError(error)
        self.assertEquals(str(error), "Bad Stuff")

    def test_CannotTerminateMachine(self):
        error = CannotTerminateMachine(0, "environment would be destroyed")
        self.assertIsJujuError(error)
        self.assertEquals(
            str(error),
            "Cannot terminate machine 0: environment would be destroyed")

    def test_MachinesNotFoundSingular(self):
        error = MachinesNotFound(("i-sublimed",))
        self.assertEquals(error.instance_ids, ["i-sublimed"])
        self.assertEquals(str(error),
                          "Cannot find machine: i-sublimed")

    def test_MachinesNotFoundPlural(self):
        error = MachinesNotFound(("i-disappeared", "i-exploded"))
        self.assertEquals(error.instance_ids, ["i-disappeared", "i-exploded"])
        self.assertEquals(str(error),
                          "Cannot find machines: i-disappeared, i-exploded")

    def testEnvironmentNotFoundWithInfo(self):
        error = EnvironmentNotFound("problem")
        self.assertEquals(str(error),
                          "juju environment not found: problem")

    def testEnvironmentNotFoundNoInfo(self):
        error = EnvironmentNotFound()
        self.assertEquals(str(error),
                          "juju environment not found: no details "
                          "available")

    def testEnvironmentPendingWithInfo(self):
        error = EnvironmentPending("problem")
        self.assertEquals(str(error), "problem")

    def testInvalidPlacementPolicy(self):
        error = InvalidPlacementPolicy("x", "foobar",  ["a", "b", "c"])
        self.assertEquals(
            str(error),
            ("Unsupported placement policy: 'x' for provider: 'foobar', "
            "supported policies a, b, c"))
