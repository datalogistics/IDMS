

import ws4py.client.tornadoclient import TornadoWebSocketClient

class ExnodeSocketClient(TornadoWebSocketClient):
    def opened(self):
        pass

    def recieved_message(self, message):
        pass


class ExtentSocketClient(TornadoWebSocketClient):
    def opened(self):
        pass

    def recieved_message(self, message):
        pass
