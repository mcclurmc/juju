from juju.lib.testing import TestCase
from juju.agents.dummy import DummyAgent


class DummyTestCase(TestCase):

    def test_start_dummy(self):
        """
        Does nothing.
        """
        agent = DummyAgent()
        result = agent.start()
        self.assertEqual(result, None)
