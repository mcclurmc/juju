from juju.lib.testing import TestCase
from juju.state.endpoint import RelationEndpoint


class RelationEndpointTest(TestCase):

    def test_may_relate_to(self):
        # TODO: Needs a doc string
        mysql_ep = RelationEndpoint("mysqldb", "mysql", "db", "server")
        blog_ep = RelationEndpoint("blog", "mysql", "mysql", "client")
        pg_ep = RelationEndpoint("postgres", "postgres", "db", "server")

        self.assertRaises(TypeError, mysql_ep.may_relate_to, 42)

        # should relate, along with symmetric case
        self.assert_(mysql_ep.may_relate_to(blog_ep))
        self.assert_(blog_ep.may_relate_to(mysql_ep))

        # no common relation_type
        self.assertFalse(blog_ep.may_relate_to(pg_ep))
        self.assertFalse(pg_ep.may_relate_to(blog_ep))

        # antireflexive om relation_role -
        # must be consumer AND provider or vice versa
        self.assertFalse(blog_ep.may_relate_to(
            RelationEndpoint("foo", "mysql", "db", "client")))
        self.assertFalse(mysql_ep.may_relate_to(
            RelationEndpoint("foo", "mysql", "db", "server")))

        # irreflexive for server/client
        self.assertFalse(mysql_ep.may_relate_to(mysql_ep))
        self.assertFalse(blog_ep.may_relate_to(blog_ep))
        self.assertFalse(pg_ep.may_relate_to(pg_ep))

        # but reflexive for peer
        riak_ep = RelationEndpoint("riak", "riak", "riak", "peer")
        self.assert_(riak_ep.may_relate_to(riak_ep))
