

import json
from ws4py.client.tornadoclient import TornadoWebSocketClient

import policy.policy as policy



class UNISSocketClient(TornadoWebSocketClient):
    def initialize(self, policies):
        self._policy = policies




class ExnodeSocketClient(UNISSocketClient):
    def opened(self):
        logging.info("ExnodeClient.opened: Successfully opened exnode socket to UNIS")

    def recieved_message(self, message):
        logging.info("ExnodeClient.recieved_message: Recieved - {message}".format(message = message)
        exnode = json.loads(message)
        self._policy.RegisterExnode(exnode)

class ExtentSocketClient(UNISSocketClient):
    def opened(self):
        logging.info("ExtentClient.opened: Successfully opened extent socket to UNIS")

    def recieved_message(self, message):
        logging.info("ExtentClient.recieved_message: Recieved - {message}".format(message = message)
        extent = json.loads(message)
        self._policy.RegisterExtent(extent)
