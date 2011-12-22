from yaml import dump

from juju.providers.dummy import MachineProvider
from juju.control import main, open_tunnel

from .common import ControlToolTest


class OpenTunnelTest(ControlToolTest):

    def test_open_tunnel(self):
        """
        'juju-control bootstrap' will invoke the bootstrap method of all
        configured machine providers in all environments.
        """
        config = {
            "environments": {
                "firstenv": {
                    "type": "dummy", "admin-secret": "homer"}}}
        self.write_config(dump(config))
        self.setup_cli_reactor()
        self.setup_exit(0)

        provider = self.mocker.patch(MachineProvider)
        provider.connect(share=True)

        hanging_deferred = self.mocker.replace(open_tunnel.hanging_deferred)

        def callback(deferred):
            deferred.callback(None)
            return deferred

        hanging_deferred()
        self.mocker.passthrough(callback)
        self.mocker.replay()

        self.capture_stream("stderr")
        main(["open-tunnel"])

        lines = filter(None, self.log.getvalue().split("\n"))
        self.assertEqual(
            lines,
            ["Tunnel to the environment is open. Press CTRL-C to close it.",
             "'open_tunnel' command finished successfully"])
