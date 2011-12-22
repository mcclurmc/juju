import re

from juju.lib.testing import TestCase

from juju.lib.schema import (
    SchemaError, SchemaExpectationError, Constant, Bool,
    Int, Float, String, Unicode, UnicodeOrString, List, KeyDict,
    SelectDict, Dict, Tuple, OneOf, Any, Regex)


PATH = ["<pa", "th>"]


class DummySchema(object):
    def coerce(self, value, path):
        return "hello!"


class SchemaErrorsTest(TestCase):

    def test_invalid_schema(self):
        error = SchemaError(["a", ".", "b"], "error message")
        self.assertEquals(error.path, ["a", ".", "b"])
        self.assertEquals(error.message, "error message")
        self.assertEquals(str(error), "a.b: error message")

    def test_invalid_schema_expectation(self):
        error = SchemaExpectationError(["a", ".", "b"], "<e>", "<g>")
        self.assertEquals(error.path, ["a", ".", "b"])
        self.assertEquals(error.expected, "<e>")
        self.assertEquals(error.got, "<g>")
        self.assertEquals(error.message, "expected <e>, got <g>")
        self.assertEquals(str(error), "a.b: expected <e>, got <g>")


class ConstantTest(TestCase):

    def test_constant(self):
        self.assertEquals(Constant("hello").coerce("hello", PATH), "hello")

    def test_constant_arbitrary(self):
        obj = object()
        self.assertEquals(Constant(obj).coerce(obj, PATH), obj)

    def test_constant_bad(self):
        error = self.assertRaises(SchemaError,
                                  Constant("foo").coerce, "bar", PATH)
        self.assertEquals(str(error), "<path>: expected 'foo', got 'bar'")


class AnyTest(TestCase):

    def test_any(self):
        obj = object()
        self.assertEquals(Any().coerce(obj, object()), obj)


class OneOfTest(TestCase):

    def test_one_of(self):
        schema = OneOf(Constant(None), Unicode())
        self.assertEquals(schema.coerce(None, PATH), None)
        self.assertEquals(schema.coerce(u"foo", PATH), u"foo")

    def test_one_of_bad(self):
        schema = OneOf(Constant(None), Unicode())
        error = self.assertRaises(SchemaError, schema.coerce, "<obj>", PATH)
        # When no values are supported, raise the first error.
        self.assertEquals(str(error), "<path>: expected None, got '<obj>'")


class BoolTest(TestCase):

    def test_bool(self):
        self.assertEquals(Bool().coerce(True, PATH), True)
        self.assertEquals(Bool().coerce(False, PATH), False)

    def test_bool_bad(self):
        error = self.assertRaises(SchemaError, Bool().coerce, 1, PATH)
        self.assertEquals(str(error), "<path>: expected bool, got 1")


class IntTest(TestCase):

    def test_int(self):
        self.assertEquals(Int().coerce(3, PATH), 3)

    def test_int_accepts_long(self):
        self.assertEquals(Int().coerce(3L, PATH), 3)

    def test_int_bad_str(self):
        error = self.assertRaises(SchemaError, Int().coerce, "3", PATH)
        self.assertEquals(str(error), "<path>: expected int, got '3'")

    def test_int_bad_float(self):
        error = self.assertRaises(SchemaError, Int().coerce, 3.0, PATH)
        self.assertEquals(str(error), "<path>: expected int, got 3.0")


class FloatTest(TestCase):

    def test_float(self):
        self.assertEquals(Float().coerce(3.3, PATH), 3.3)

    def test_float_accepts_int(self):
        self.assertEquals(Float().coerce(3, PATH), 3.0)

    def test_float_accepts_long(self):
        self.assertEquals(Float().coerce(3L, PATH), 3.0)

    def test_float_bad_str(self):
        error = self.assertRaises(SchemaError, Float().coerce, "3.0", PATH)
        self.assertEquals(str(error), "<path>: expected number, got '3.0'")


class StringTest(TestCase):

    def test_string(self):
        self.assertEquals(String().coerce("foo", PATH), "foo")

    def test_string_bad_unicode(self):
        error = self.assertRaises(SchemaError, String().coerce, u"foo", PATH)
        self.assertEquals(str(error), "<path>: expected string, got u'foo'")

    def test_string_bad_int(self):
        error = self.assertRaises(SchemaError, String().coerce, 1, PATH)
        self.assertEquals(str(error), "<path>: expected string, got 1")


class UnicodeTest(TestCase):

    def test_unicode(self):
        self.assertEquals(Unicode().coerce(u"foo", PATH), u"foo")

    def test_unicode_bad_str(self):
        error = self.assertRaises(SchemaError, Unicode().coerce, "foo", PATH)
        self.assertEquals(str(error), "<path>: expected unicode, got 'foo'")


class UnicodeOrStringTest(TestCase):

    def test_unicode_or_str(self):
        schema = UnicodeOrString("utf-8")
        self.assertEquals(schema.coerce(u"foo", PATH), u"foo")

    def test_unicode_or_str_accepts_str(self):
        self.assertEquals(UnicodeOrString("utf-8").coerce("foo", PATH), u"foo")

    def test_unicode_or_str_bad(self):
        error = self.assertRaises(SchemaError,
                                  UnicodeOrString("utf-8").coerce, 32, PATH)
        self.assertEquals(str(error),
                          "<path>: expected unicode or utf-8 string, got 32")

    def test_unicode_or_str_decodes(self):
        """UnicodeOrString should decode plain strings."""
        a = u"\N{HIRAGANA LETTER A}"
        self.assertEquals(
            UnicodeOrString("utf-8").coerce(a.encode("utf-8"), PATH),
            a)
        letter = u"\N{LATIN SMALL LETTER A WITH GRAVE}"
        self.assertEquals(
            UnicodeOrString("latin-1").coerce(letter.encode("latin-1"), PATH),
            letter)

    def test_unicode_or_str_bad_encoding(self):
        """Decoding errors should be converted to a SchemaError."""
        error = self.assertRaises(
            SchemaError, UnicodeOrString("utf-8").coerce, "\xff", PATH)
        self.assertEquals(str(error),
                          r"<path>: expected unicode or utf-8 string, "
                          r"got '\xff'")


class RegexTest(TestCase):

    def test_regex(self):
        exp = "\w+"
        pat = re.compile(exp)
        self.assertEquals(Regex().coerce(exp, PATH), pat)

    def test_regex_bad_regex(self):
        exp = "([a-"
        error = self.assertRaises(
            SchemaError, Regex().coerce,  exp, PATH)
        self.assertEquals(str(error), "<path>: expected regex, got '([a-'")


class ListTest(TestCase):

    def test_list(self):
        schema = List(Int())
        self.assertEquals(schema.coerce([1], PATH), [1])

    def test_list_bad(self):
        error = self.assertRaises(SchemaError, List(Int()).coerce, 32, PATH)
        self.assertEquals(str(error), "<path>: expected list, got 32")

    def test_list_inner_schema_coerces(self):
        self.assertEquals(List(DummySchema()).coerce([3], PATH), ["hello!"])

    def test_list_bad_inner_schema_at_0(self):
        error = self.assertRaises(SchemaError,
                                  List(Int()).coerce, ["hello"], PATH)
        self.assertEquals(str(error), "<path>[0]: expected int, got 'hello'")

    def test_list_bad_inner_schema_at_1(self):
        error = self.assertRaises(SchemaError,
                                  List(Int()).coerce, [1, "hello"], PATH)
        self.assertEquals(str(error), "<path>[1]: expected int, got 'hello'")

    def test_list_multiple_items(self):
        a = u"\N{HIRAGANA LETTER A}"
        schema = List(UnicodeOrString("utf-8"))
        self.assertEquals(schema.coerce([a, a.encode("utf-8")], PATH), [a, a])


class TupleTest(TestCase):

    def test_tuple(self):
        self.assertEquals(Tuple(Int()).coerce((1,), PATH), (1,))

    def test_tuple_coerces(self):
        self.assertEquals(
            Tuple(Int(), DummySchema()).coerce((23, object()), PATH),
            (23, "hello!"))

    def test_tuple_bad(self):
        error = self.assertRaises(SchemaError, Tuple().coerce, "hi", PATH)
        self.assertEquals(str(error), "<path>: expected tuple, got 'hi'")

    def test_tuple_inner_schema_bad_at_0(self):
        error = self.assertRaises(SchemaError,
                                  Tuple(Int()).coerce, ("hi",), PATH)
        self.assertEquals(str(error), "<path>[0]: expected int, got 'hi'")

    def test_tuple_inner_schema_bad_at_1(self):
        error = self.assertRaises(SchemaError,
                                  Tuple(Int(), Int()).coerce, (1, "hi"), PATH)
        self.assertEquals(str(error), "<path>[1]: expected int, got 'hi'")

    def test_tuple_must_have_all_items(self):
        error = self.assertRaises(SchemaError,
                                  Tuple(Int(), Int()).coerce, (1,), PATH)
        self.assertEquals(str(error),
                          "<path>: expected tuple with 2 elements, got (1,)")

    def test_tuple_must_have_no_more_items(self):
        error = self.assertRaises(SchemaError,
                                  Tuple(Int()).coerce, (1, 2), PATH)
        self.assertEquals(str(error),
                          "<path>: expected tuple with 1 elements, got (1, 2)")


class DictTest(TestCase):

    def test_dict(self):
        self.assertEquals(Dict(Int(), String()).coerce({32: "hello."}, PATH),
                          {32: "hello."})

    def test_dict_coerces(self):
        self.assertEquals(
            Dict(DummySchema(), DummySchema()).coerce({32: object()}, PATH),
            {"hello!": "hello!"})

    def test_dict_bad(self):
        error = self.assertRaises(SchemaError,
                                  Dict(Int(), Int()).coerce, "hi", PATH)
        self.assertEquals(str(error), "<path>: expected dict, got 'hi'")

    def test_dict_bad_key(self):
        error = self.assertRaises(SchemaError,
                                  Dict(Int(), Int()).coerce, {"hi": 32}, PATH)
        self.assertEquals(str(error), "<path>: expected int, got 'hi'")

    def test_dict_bad_value(self):
        error = self.assertRaises(SchemaError,
                                  Dict(Int(), Int()).coerce, {32: "hi"}, PATH)
        self.assertEquals(str(error), "<path>.32: expected int, got 'hi'")

    def test_dict_bad_value_unstringifiable(self):
        """
        If the path can't be stringified, it's repr()ed.
        """
        a = u"\N{HIRAGANA LETTER A}"
        schema = Dict(Unicode(), Int())
        error = self.assertRaises(SchemaError,
                                  schema.coerce, {a: "hi"}, PATH)
        self.assertEquals(str(error),
                          "<path>.u'\\u3042': expected int, got 'hi'")

    def test_dict_path_without_dots(self):
        """
        The first path entry shouldn't have a dot as prefix.
        """
        schema = Dict(Int(), Dict(Int(), Int()))
        error = self.assertRaises(SchemaError,
                                  schema.coerce, {1: {2: "hi"}}, [])
        self.assertEquals(str(error), "1.2: expected int, got 'hi'")


class KeyDictTest(TestCase):

    def test_key_dict(self):
        self.assertEquals(KeyDict({"foo": Int()}).coerce({"foo": 1}, PATH),
                          {"foo": 1})

    def test_key_dict_coerces(self):
        self.assertEquals(
            KeyDict({"foo": DummySchema()}).coerce({"foo": 3}, PATH),
            {"foo": "hello!"})

    def test_key_dict_bad(self):
        error = self.assertRaises(SchemaError, KeyDict({}).coerce, "1", PATH)
        self.assertEquals(str(error), "<path>: expected dict, got '1'")

    def test_key_dict_bad_value(self):
        schema = KeyDict({"foo": Int()})
        error = self.assertRaises(SchemaError,
                                  schema.coerce, {"foo": "hi"}, PATH)
        self.assertEquals(str(error), "<path>.foo: expected int, got 'hi'")

    def test_key_dict_bad_value_unstringifiable(self):
        """
        If the path can't be stringified, it's repr()ed.
        """
        a = u"\N{HIRAGANA LETTER A}"
        error = self.assertRaises(SchemaError,
                                  KeyDict({a: Int()}).coerce, {a: "hi"}, PATH)
        self.assertEquals(str(error),
                          "<path>.u'\\u3042': expected int, got 'hi'")

    def test_key_dict_unknown_key(self):
        """
        Unknown key/value pairs processed by a KeyDict are left untouched.
        This is an attempt at not eating values by mistake due to something
        like different application versions operating on the same data.
        """
        schema = KeyDict({"foo": Int()})
        self.assertEquals(schema.coerce({"foo": 1, "bar": "hi"}, PATH),
                          {"foo": 1, "bar": "hi"})

    def test_key_dict_multiple_items(self):
        schema = KeyDict({"one": Int(), "two": List(Float())})
        input = {"one": 32, "two": [1.5, 2.3]}
        self.assertEquals(schema.coerce(input, PATH),
                          {"one": 32, "two": [1.5, 2.3]})

    def test_key_dict_arbitrary_keys(self):
        """
        KeyDict doesn't actually need to have strings as keys, just any
        object which hashes the same.
        """
        key = object()
        self.assertEquals(KeyDict({key: Int()}).coerce({key: 32}, PATH),
                          {key: 32})

    def test_key_dict_must_have_all_keys(self):
        """
        dicts which are applied to a KeyDict must have all the keys
        specified in the KeyDict.
        """
        schema = KeyDict({"foo": Int()})
        error = self.assertRaises(SchemaError, schema.coerce, {}, PATH)
        self.assertEquals(str(error), "<path>.foo: required value not found")

    def test_key_dict_optional_keys(self):
        """KeyDict allows certain keys to be optional.
        """
        schema = KeyDict({"foo": Int(), "bar": Int()}, optional=["bar"])
        self.assertEquals(schema.coerce({"foo": 32}, PATH), {"foo": 32})

    def test_key_dict_pass_optional_key(self):
        """Regression test. It should be possible to pass an optional key.
        """
        schema = KeyDict({"foo": Int()}, optional=["foo"])
        self.assertEquals(schema.coerce({"foo": 32}, PATH), {"foo": 32})

    def test_key_dict_path_without_dots(self):
        """
        The first path entry shouldn't have a dot as prefix.
        """
        schema = KeyDict({1: KeyDict({2: Int()})})
        error = self.assertRaises(SchemaError,
                                  schema.coerce, {1: {2: "hi"}}, [])
        self.assertEquals(str(error), "1.2: expected int, got 'hi'")


class SelectDictTest(TestCase):

    def test_select_dict(self):
        schema = SelectDict("name", {"foo": KeyDict({"value": Int()}),
                                     "bar": KeyDict({"value": String()})})
        self.assertEquals(schema.coerce({"name": "foo", "value": 1}, PATH),
                          {"name": "foo", "value": 1})
        self.assertEquals(schema.coerce({"name": "bar", "value": "one"}, PATH),
                          {"name": "bar", "value": "one"})

    def test_select_dict_errors(self):
        schema = SelectDict("name", {"foo": KeyDict({"foo_": Int()}),
                                     "bar": KeyDict({"bar_": Int()})})
        error = self.assertRaises(SchemaError,
                schema.coerce, {"name": "foo"}, PATH)
        self.assertEquals(str(error), "<path>.foo_: required value not found")
        error = self.assertRaises(SchemaError,
                schema.coerce, {"name": "bar"}, PATH)
        self.assertEquals(str(error), "<path>.bar_: required value not found")


class BestErrorTest(TestCase):

    def test_best_error(self):
        """
        OneOf attempts to select a relevant error to report to user
        when no branch of its schema can be satisitifed.
        """

        schema = OneOf(Unicode(),
                       KeyDict({"flavor": String()})
                       )

        # an error related to flavor is more specific and useful
        # than one related to the 1st branch
        error = self.assertRaises(SchemaExpectationError,
                                  schema.coerce,
                                  {"flavor": None}, PATH)
        # the correct error is returned
        self.assertEquals(str(error),
                          "<path>.flavor: expected string, got None")

        # here the error related to Unicode is better
        error = self.assertRaises(SchemaExpectationError,
                                  schema.coerce,
                                  "a string",
                                  PATH)
        self.assertEquals(str(error),
                          "<path>: expected unicode, got 'a string'")

        # and the success case still functions
        self.assertEqual(schema.coerce(u"some unicode", PATH),
                         u"some unicode")

