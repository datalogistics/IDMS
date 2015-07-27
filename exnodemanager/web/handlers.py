
import json
import tornado
from ws4py.client.tornadoclient import TornadoWebSocketClient

import exnodemanager.protocol.factory as factory
import exnodemanager.record as record
import settings

class UNISSocketClient(TornadoWebSocketClient):
    def __init__(self, url):
        self._log = record.getLogger()
        super(UNISSocketClient, self).__init__(url)

    def initialize(self, parent):
        self.parent = parent
        
        

class ExnodeSocketClient(UNISSocketClient):
    def opened(self):
        self._log.info("ExnodeClient.opened: Successfully opened exnode socket to UNIS")
        
    def received_message(self, message):
        try:
            exnode = json.loads(str(message))
            self._log.info("ExnodeClient.received_message: Recieved - {eid}".format(eid = exnode["id"]))
            if exnode["mode"] == "file":
                result = self.parent.create_exnode(exnode)
                self.parent.register_exnode(result)
        except Exception as exp:
            self._log.error("ExnodeClient.received_message: Could not serialize enxnode - {0}".format(exp))
    
class ExtentSocketClient(UNISSocketClient):
    def opened(self):
        self._log.info("ExtentClient.opened: Successfully opened extent socket to UNIS")
        
    def received_message(self, message):
        try:
            tmpAlloc = json.loads(str(message))
            self._log.info("ExtentClient.received_message: Recieved - {eid}".format(eid = tmpAlloc["id"]))
            self._log.debug("Allocation: {alloc}".format(alloc = tmpAlloc))
            alloc = factory.buildAllocation(tmpAlloc)
            if alloc:
                self.parent.register_allocation(alloc)
            else:
                self.parent.remove_allocation(tmpAlloc["id"], tmpAlloc["parent"])
        except Exception as exp:
            self._log.error("ExtentClient.received_message: Could not serialize extent - {0}".format(exp))



            
def RunUnitTests():
    import logging
    logger = record.getLogger()
    logger.setLevel(logging.DEBUG)
    class TestHarness(object):
        def create_exnode(self, exnode):
            return None

        def register_exnode(self, exnode):
            pass

        def register_allocation(self, alloc):
            pass


    harness = TestHarness()

    # Create websockets to listen for future changes to exnodes and extents
    tmpExnodeUrl = "{protocol}://{host}:{port}/subscribe/{resource}".format(protocol = "ws",
                                                                            host     = settings.UNIS_HOST,
                                                                            port     = settings.UNIS_PORT,
                                                                            resource = "exnodes")
    tmpExtentUrl = "{protocol}://{host}:{port}/subscribe/{resource}".format(protocol = "ws",
                                                                            host     = settings.UNIS_HOST,
                                                                            port     = settings.UNIS_PORT,
                                                                            resource = "extents")
    
    exnode_ws = ExnodeSocketClient(tmpExnodeUrl)
    extent_ws = ExtentSocketClient(tmpExtentUrl)
    exnode_ws.initialize(parent = harness)
    extent_ws.initialize(parent = harness)
    exnode_ws.connect()
    extent_ws.connect()

    tornado.ioloop.IOLoop.instance().start()




if __name__ == "__main__":
    RunUnitTests()
