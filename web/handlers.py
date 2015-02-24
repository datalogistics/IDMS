

import ws4py.client.tornadoclient import TornadoWebSocketClient
import policy.policy as policy

class UNISSocketClient(TornadoWebSocketClient):
    def initialize(self, policies):
        self._policy = policies




class ExnodeSocketClient(UNISSocketClient):
    def opened(self):
        pass

    def recieved_message(self, message):
        pass


class ExtentSocketClient(UNISSocketClient):
    def opened(self):
        pass

    def recieved_message(self, message):
        pass
