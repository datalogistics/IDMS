'''
@Name:   app.py
@Author: Jeremy Musser
@Date:   02/23/15

------------------------------------------


'''
import logging
import argparse
import sys
import tornado
import tornado.web
import tornado.escape
import tornadoredis
import datetime
import time
import uuid
from netlogger import nllog

import concurrent.futures

import settings
import exnodemanager.record as record
import db

import exnodemanager.protocol.factory as factory

from exnodemanager import instruction
from exnodemanager.policy.composite import CompositePolicy
from handlers import PolicyHandler, SubscriptionHandler, StateHandler

class DemoApplication(tornado.web.Application, nllog.DoesLogging):
    def __init__(self, handlers):
        # Initialize basic configurations and create the unis proxy which serves as the primary
        #   data source.  build_policies constructs the requested policies from the settings file.
        nllog.DoesLogging.__init__(self)
        super(DemoApplication, self).__init__(handlers)
        self.log.info("__init__: Creating new Dispatcher")
        self._db          = db.DemoProxy()
        
        self.trc = tornadoredis.Client()
        self.trc.connect()
        
        self.register()
        
                
    def restartDB(self):
        self.register()


    def register(self):
        self._exnodes     = {}
        # Register all currently availible exnodes on UNIS to the policy
        exnodes = self._db.GetExnodes()
        
        with concurrent.futures.ThreadPoolExecutor(max_workers = settings.THREADS) as executor:
            try:
                for result in executor.map(self.create_exnode, exnodes):
                    self.register_exnode(result)
            except Exception as exp:
                self.log.warn("Could not get exnodes from UNIS")

        
                
    def create_exnode(self, exnode):
        tmpExnode = {}
        tmpExnode["id"] = exnode["id"]
        tmpExnode["raw"] = exnode
        tmpExnode["allocations"] = {}
        
        self.log.info("Creating exnode")
        with concurrent.futures.ThreadPoolExecutor(max_workers = settings.THREADS) as executor:
            for alloc, result in zip(exnode["extents"], executor.map(factory.buildAllocation, exnode["extents"])):
                self.log.info("Serialized alloc: {alloc}".format(alloc = result.GetMetadata().Serialize()))
                tmpExnode["allocations"][result.GetMetadata().Id] = result
                
        return tmpExnode
    
    
    def register_exnode(self, exnode):
        self._exnodes[exnode["id"]] = exnode
        self.log.info("app.registerExnode: Registered exnode [{id}]".format(id = exnode["id"]))
        
        
    def register_allocation(self, alloc):
        tmpMeta   = alloc.GetMetadata()
        tmpExnode = self._exnodes[tmpMeta.exnode]
        tmpId     = tmpMeta.Id
        now       = datetime.datetime.utcnow()
        
        if tmpId in tmpExnode["allocations"]:
            if alloc > tmpExnode["allocations"][tmpId]:
                tmpExnode["allocations"][tmpId] = alloc
        else:
            tmpExnode["allocations"][tmpId] = alloc
            
            
    def check_exnodes(self, policy):
        try:
            policy = CompositePolicy(policy, validate = False, depots = [ ("ibp2.crest.iu.edu", "6714"),
                                                                          ("dresci.crest.iu.edu", "6714"),
                                                                          ("geoserv.ersc.wisc.edu", "6714"),
                                                                          ("192.70.161.87", "6714"),
                                                                          ("192.70.161.60", "6714"),
                                                                          ("tvdlnet1.sfasu.edu", "6714") ])
        except ValueError as exp:
            err = { "Error": "Malformed policy" }
            self.trc.publish("out", tornado.escape.json_encode(err))
            self.log.info("app.Check: Malformed policy")
            return
        self.log.info("app.Check: Analyzing network and data...")
        for key, exnode in self._exnodes.items():
            map(self.process_instruction, policy.GetPendingChanges(exnode))
            
            
    def process_instruction(self, command):
        to_publish = {}
        self.log.info("Recieved command: {command}".format(command = command))
        if command["type"] == instruction.MOVE:
            self.log.info("Moving allocation from {src} to {dest}".format(src = command["allocation"].GetMetadata().getAddress(), dest = command["destination"]))
            alloc = command["allocation"].GetMetadata()
            new_alloc = self.create_allocation(command["destination"], alloc)
            if new_alloc:
                self.register_allocation(new_alloc)
            to_publish = {
                "MOVE": {
                    "src": alloc.getAddress(),
                    "dest": command["destination"],
                    "size": alloc.depotSize,
                    "src_id": alloc.Id,
                    "dst_id": new_alloc.GetMetadata().Id
                }
            }
        elif command["type"] == instruction.COPY:
            self.log.info("Copying allocation from {src} to {dest}".format(src = command["allocation"].GetMetadata().getAddress(), dest = command["destination"]))
            alloc = command["allocation"].GetMetadata()
            new_alloc = self.create_allocation(command["destination"], alloc)
            if new_alloc:
                self.register_allocation(new_alloc)
            to_publish = {
                "COPY": {
                    "src": alloc.getAddress(),
                    "dest": command["destination"],
                    "size": alloc.depotSize,
                    "src_id": alloc.Id,
                    "dst_id": new_alloc.GetMetadata().Id
                }
            }
        elif command["type"] == instruction.REFRESH:
            self.log.info("Refreshing allocation {alloc_id}".format(alloc_id = command["allocation"].GetMetadata().Id))
            alloc = command["allocation"].GetMetadata()
            alloc.end = datetime.datetime.utcnow() + datetime.timedelta(seconds = command["duration"])
            alloc.timestamp = int(time.time())
            to_publish = {
                "REFRESH": {
                    "src": alloc.getAddress(),
                    "size": alloc.depotSize,
                    "id": alloc.Id
                }
            }
        elif command["type"] == instruction.RELEASE:
            alloc = command["allocation"].GetMetadata()
            self.log.info("Releasing allocation {alloc_id}".format(alloc_id = alloc.Id))
            to_publish = {
                "RELEASE": {
                    "src": alloc.getAddress(),
                    "size": alloc.depotSize,
                    "id": alloc.Id
                }
            }
            del self._exnodes[alloc.exnode]["allocations"][alloc.Id]
            
        if to_publish:
            self.trc.publish("out", tornado.escape.json_encode(to_publish))
        
            
    def create_allocation(self, destination, source):
        dummyWRM = uuid.uuid4().int
        dummyStr = "ibp://{host}:{port}/0#{key}/{wrm}/{code}"
        dummyRead = dummyStr.format(host = destination["host"], port = destination["port"], key = uuid.uuid4().hex, wrm = dummyWRM, code = "READ")
        dummyWrite = dummyStr.format(host = destination["host"], port = destination["port"], key = uuid.uuid4().hex, wrm = dummyWRM, code = "WRITE")
        dummyManage = dummyStr.format(host = destination["host"], port = destination["port"], key = uuid.uuid4().hex, wrm = dummyWRM, code = "MANAGE")
        tmpAlloc = source.Clone()
        tmpAlloc.Id = uuid.uuid4().hex
        tmpAlloc.timestamp = int(time.time())
        tmpAlloc.SetReadCapability(dummyRead)
        tmpAlloc.SetWriteCapability(dummyWrite)
        tmpAlloc.SetManageCapability(dummyManage)
        tmpAdapter = factory.buildAllocation(tmpAlloc)
        
        return tmpAdapter
        
        
def main():
    logging.setLoggerClass(nllog.BPLogger)
    logger = record.getLogger(nllog.PROJECT_NAMESPACE, logging.DEBUG)
    logger.setLevel(logging.DEBUG)
    # Parse arguments
    description = """{prog} collects and manages file information stored in UNIS.
                     Depending on the policy, {prog} will refresh, move, or duplicate
                     data based on network conditions.""".format(prog = sys.argv[0])
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('-l', '--log_file', type=str)
    parser.add_argument('-u', '--unis_host')
    parser.add_argument('-p', '--unis_port')
    args = parser.parse_args()
    
    if args.unis_host:
        settings.UNIS_HOST = args.unis_host
    if args.unis_port:
        settings.UNIS_PORT = args.unis_port
        
    app = DemoApplication([
        (r"/submit", PolicyHandler),
        (r"/state", StateHandler),
        (r"/subscribe", SubscriptionHandler)
    ])
    logger.info("app.Run: Starting main loop")
    
    app.listen(8989)
    tornado.ioloop.IOLoop.instance().start()
    
    
if __name__ == "__main__":
    main()

