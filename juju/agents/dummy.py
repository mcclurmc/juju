
from .base import BaseAgent


class DummyAgent(BaseAgent):
    """A do nothing juju agent.

    A bit like a dog, it just lies around basking in the sun,
    doing nothing, nonetheless its quite content. :-)
    """

    def start(self):
        """nothing to see here, move along."""

if __name__ == '__main__':
    DummyAgent.run()
