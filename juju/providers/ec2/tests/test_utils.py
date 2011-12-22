import inspect
import os

from twisted.internet.defer import fail, succeed
from twisted.web.error import Error

from juju.lib.testing import TestCase
from juju.providers import ec2
from juju.providers.ec2.utils import get_current_ami, get_image_id


IMAGE_URI_TEMPLATE = "\
http://uec-images.ubuntu.com/query/%s/server/released.current.txt"

IMAGE_DATA_DIR = os.path.join(
    os.path.dirname(inspect.getabsfile(ec2)), "tests", "data")


class GetCurrentAmiTest(TestCase):

    def test_bad_url(self):
        """
        If the requested page doesn't exist at all, a LookupError is raised
        """
        page = self.mocker.replace("twisted.web.client.getPage")
        page(IMAGE_URI_TEMPLATE % "nutty")
        self.mocker.result(fail(Error("404")))
        self.mocker.replay()
        d = get_current_ami(ubuntu_release="nutty")
        self.failUnlessFailure(d, LookupError)
        return d

    def test_umatched_ami(self):
        """
        If an ami is not found that matches the specifications, then
        a LookupError is raised.
        """
        page = self.mocker.replace("twisted.web.client.getPage")
        page(IMAGE_URI_TEMPLATE % "lucid")
        self.mocker.result(succeed(""))
        self.mocker.replay()
        d = get_current_ami(ubuntu_release="lucid")
        self.failUnlessFailure(d, LookupError)
        return d

    def test_current_ami(self):
        """The current server machine image can be retrieved."""
        page = self.mocker.replace("twisted.web.client.getPage")
        page(IMAGE_URI_TEMPLATE % "lucid")
        self.mocker.result(succeed(
            open(os.path.join(IMAGE_DATA_DIR, "lucid.txt")).read()))

        self.mocker.replay()
        d = get_current_ami(ubuntu_release="lucid")

        def verify_result(result):
            self.assertEqual(result, "ami-714ba518")

        d.addCallback(verify_result)
        return d

    def test_current_ami_by_region(self):
        """The current server machine image can be retrieved by region."""
        page = self.mocker.replace("twisted.web.client.getPage")
        page(IMAGE_URI_TEMPLATE % "lucid")
        self.mocker.result(
            succeed(open(
                os.path.join(IMAGE_DATA_DIR, "lucid.txt")).read()))
        self.mocker.replay()
        d = get_current_ami(ubuntu_release="lucid", region="us-west-1")

        def verify_result(result):
            self.assertEqual(result, "ami-cb97c68e")

        d.addCallback(verify_result)
        return d

    def test_current_ami_non_ebs(self):
        """
        The get_current_ami function accepts several filtering parameters
        to guide image selection.
        """
        page = self.mocker.replace("twisted.web.client.getPage")
        page(IMAGE_URI_TEMPLATE % "lucid")
        self.mocker.result(succeed(
            open(os.path.join(IMAGE_DATA_DIR, "lucid.txt")).read()))
        self.mocker.replay()
        d = get_current_ami(ubuntu_release="lucid", persistent_storage=False)

        def verify_result(result):
            self.assertEqual(result, "ami-2d4aa444")

        d.addCallback(verify_result)
        return d


class GetImageIdTest(TestCase):

    def test_default_image_id(self):
        d = get_image_id({"default-image-id": "ami-burble"}, {})
        d.addCallback(self.assertEquals, "ami-burble")
        return d

    def test_no_constraints(self):
        get_current_ami_m = self.mocker.replace(get_current_ami)
        get_current_ami_m(region="us-east-1")
        self.mocker.result(succeed("ami-giggle"))
        self.mocker.replay()

        d = get_image_id({}, {})
        d.addCallback(self.assertEquals, "ami-giggle")
        return d

    def test_default_series(self):
        get_current_ami_m = self.mocker.replace(get_current_ami)
        get_current_ami_m(region="us-east-1", ubuntu_release="puissant")
        self.mocker.result(succeed("ami-pickle"))
        self.mocker.replay()

        d = get_image_id({"default-series": "puissant"}, {})
        d.addCallback(self.assertEquals, "ami-pickle")
        return d

    def test_uses_constraints(self):
        get_current_ami_m = self.mocker.replace(get_current_ami)
        get_current_ami_m(ubuntu_release="serendipitous", architecture="x512",
                          daily=False, persistent_storage=True,
                          region="blah-north-6")
        self.mocker.result(succeed("ami-tinkle"))
        self.mocker.replay()

        constraints = {
            "architecture": "x512",
            "ubuntu_release": "serendipitous",
            "persistent_storage": True,
            "daily": False}
        d = get_image_id(
            {"region": "blah-north-6", "default-series": "overridden"},
            constraints)
        d.addCallback(self.assertEquals, "ami-tinkle")
        return d
