"""
This module supports a user-level view of the topology, instead of the
low-level perspective of service states and relation states.
"""

from collections import namedtuple


class RelationEndpoint(
    # This idiom allows for the efficient and simple construction of
    # value types based on tuples; notably __eq__ and __hash__ are
    # correctly defined for value types. In addition, the storage
    # overhead is that of an ordinary tuple (although not so important
    # for this usage). See urlib/parse.py in Python 3.x for other
    # examples.
    namedtuple(
        "RelationEndpoint",
        "service_name relation_type relation_name relation_role")):

    __slots__ = ()

    def may_relate_to(self, other):
        """Test whether the `other` endpoint may be used in a common relation.

        RelationEndpoints may be related if they share the same relation_type
        (which is called an "interface" in charms) and one is a ``provides``
        and the other is a ``requires``; or if both endpoints have a
        relation_role of ``peers``.

        Raises a `TypeError` is `other` is not a `RelationEndpoint`.
        """

        if not isinstance(other, RelationEndpoint):
            raise TypeError("Not a RelationEndpoint", other)
        return (self.relation_type == other.relation_type and
                ((self.relation_role == "server" and
                  other.relation_role == "client") or
                 (self.relation_role == "client" and
                  other.relation_role == "server") or
                 (self.relation_role == "peer" and
                  other.relation_role == "peer")))
