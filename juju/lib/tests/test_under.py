import string

from juju.lib.testing import TestCase
from juju.lib.under import quote


class UnderTest(TestCase):

    def test_unmodified(self):
        s = string.ascii_letters + string.digits + "-."
        q = quote(s)
        self.assertEquals(quote(s), s)
        self.assertTrue(isinstance(q, str))

    def test_quote(self):
        s = "hello_there/how'are~you-today.sir"
        q = quote(s)
        self.assertEquals(q, "hello_5f_there_2f_how_27_are_7e_you-today.sir")
        self.assertTrue(isinstance(q, str))

    def test_coincidentally_unicode(self):
        s = u"hello_there/how'are~you-today.sir"
        q = quote(s)
        self.assertEquals(q, "hello_5f_there_2f_how_27_are_7e_you-today.sir")
        self.assertTrue(isinstance(q, str))

    def test_necessarily_unicode(self):
        s = u"hello\u1234there"
        self.assertRaises(KeyError, quote, s)
