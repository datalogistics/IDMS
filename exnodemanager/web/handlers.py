

import json
import tornado
import logging
from ws4py.client.tornadoclient import TornadoWebSocketClient

from exnodemanager.policy.policy import Policy
import settings


class UNISSocketClient(TornadoWebSocketClient):
    def initialize(self, policy):
        self._policy = policy
        
        
        

class ExnodeSocketClient(UNISSocketClient):
    def opened(self):
        logging.info("ExnodeClient.opened: Successfully opened exnode socket to UNIS")
        
    def received_message(self, message):
        logging.info("ExnodeClient.received_message: Recieved - {message}".format(message = message))
        try:
            exnode = json.loads(str(message))
            self._policy.RegisterExnode(exnode)
        except Exception as exp:
            logging.error("ExnodeClient.received_message: Could not serialize enxnode - {0}".format(exp))
    
class ExtentSocketClient(UNISSocketClient):
    def opened(self):
        logging.info("ExtentClient.opened: Successfully opened extent socket to UNIS")
        
    def received_message(self, message):
        logging.info("ExtentClient.received_message: Recieved - {message}".format(message = message))
        try:
            extent = json.loads(str(message))
            self._policy.RegisterExtent(extent)
        except Exception as exp:
            logging.error("ExtentClient.received_message: Could not serialize extent - {0}".format(exp))



def RunUnitTests():
    pol = Policy()

    logging.basicConfig(level = logging.DEBUG)

    # Create websockets to listen for future changes to exnodes and extents
    tmpExnodeUrl = "{protocol}://{host}:{port}/subscribe/{resource}".format(protocol = "ws",
                                                                            host     = settings.UNIS_HOST,
                                                                            port     = settings.UNIS_PORT,
                                                                            resource = "exnode")
    tmpExtentUrl = "{protocol}://{host}:{port}/subscribe/{resource}".format(protocol = "ws",
                                                                            host     = settings.UNIS_HOST,
                                                                            port     = settings.UNIS_PORT,
                                                                            resource = "extent")
    
    print tmpExnodeUrl
    print tmpExtentUrl

    exnode_ws = ExnodeSocketClient(tmpExnodeUrl)
    extent_ws = ExtentSocketClient(tmpExtentUrl)
    exnode_ws.initialize(policy = pol)
    extent_ws.initialize(policy = pol)
    exnode_ws.connect()
    extent_ws.connect()

    tornado.ioloop.IOLoop.instance().start()




if __name__ == "__main__":
    RunUnitTests()
