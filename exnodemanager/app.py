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
import datetime

import concurrent.futures

import settings
import instruction
import record
import web.settings as unis_settings
import web.unisproxy as db

import protocol.factory as factory

from web.handlers import ExnodeSocketClient, ExtentSocketClient


class DispatcherApplication(object):
    def __init__(self):
        # Initialize basic configurations and create the unis proxy which serves as the primary
        #   data source.  build_policies constructs the requested policies from the settings file.
        self._log = record.getLogger()
        self._log.info("__init__: Creating new Dispatcher")
        self._exnodes     = {}
        self._db          = db.UnisProxy(unis_settings.UNIS_HOST, unis_settings.UNIS_PORT)
        self.build_policies()
        
        # Register all currently availible exnodes on UNIS to the policy
        exnodes = self._db.GetExnodes()        

        with concurrent.futures.ThreadPoolExecutor(max_workers = settings.THREADS) as executor:
            for result in executor.map(self.create_exnode, exnodes):
                self.register_exnode(result)

        
        # Create websockets to listen for future changes to exnodes and extents
        tmpExnodeUrl = "{protocol}://{host}:{port}/subscribe/{resource}".format(protocol = "ws",
                                                                          host     = unis_settings.UNIS_HOST,
                                                                          port     = unis_settings.UNIS_PORT,
                                                                          resource = "exnodes")
        tmpExtentUrl = "{protocol}://{host}:{port}/subscribe/{resource}".format(protocol = "ws",
                                                                          host     = unis_settings.UNIS_HOST,
                                                                          port     = unis_settings.UNIS_PORT,
                                                                          resource = "extents")
        
        exnode_ws = ExnodeSocketClient(tmpExnodeUrl)
        extent_ws = ExtentSocketClient(tmpExtentUrl)
        exnode_ws.initialize(parent = self)
        extent_ws.initialize(parent = self)
        exnode_ws.connect()
        extent_ws.connect()


        
    def Run(self):
        self._log.info("app.Run: Starting main loop")
        tornado.ioloop.PeriodicCallback(callback=self.check_exnodes, callback_time=settings.ITERATION_TIME * 1000).start()
        tornado.ioloop.IOLoop.instance().start()
    
    

        
    def create_exnode(self, exnode):
        tmpExnode = {}
        tmpExnode["id"] = exnode["id"]
        tmpExnode["raw"] = exnode
        tmpExnode["allocations"] = {}
        
        self._log.debug("Creating exnode")
        with concurrent.futures.ThreadPoolExecutor(max_workers = settings.THREADS) as executor:
            for alloc, result in zip(exnode["extents"], executor.map(factory.buildAllocation, exnode["extents"])):
                if result:
                    self._log.debug("Serialized alloc: {alloc}".format(alloc = result.GetMetadata().Serialize()))
                    tmpExnode["allocations"][result.GetMetadata().Id] = result
                else:
                    self._log.info("app.CreateExnode: Discarding allocation")
                    self.remove_allocation(alloc["id"], exnode["id"])
                    
        return tmpExnode


    
    def register_exnode(self, exnode):
        self._exnodes[exnode["id"]] = exnode
        self._log.info("app.registerExnode: Registered exnode [{id}]".format(id = exnode["id"]))
        
        

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

    def remove_allocation(self, alloc, exnode):
        if exnode in self._exnodes:
            self._exnodes[exnode]["allocations"].pop(alloc, None)
            

            
    def check_exnodes(self):
        self._log.info("app.Check: Analyzing network and data...")
        for key, exnode in self._exnodes.items():
            for policy in self._policies:
                map(self.process_instruction, policy.GetPendingChanges(exnode))
    


                
    def process_instructions(self, command):
        self._log.debug("Recieved command: {command}".format(command = command))
        if command["type"] == instruction.MOVE:
            self._log.info("Moving allocation from {src} to {dest}".format(src = command["allocation"].GetMetadata().getAddress(), dest = command["destination"]))
            new_alloc = command["allocation"].Move(command["destination"], **command)
            if new_alloc:
                self._db.CreateAllocation(new_alloc)
                self.register_allocation(new_alloc)
        if command["type"] == instruction.COPY:
            self._log.info("Copying allocation from {src} to {dest}".format(src = command["allocation"].GetMetadata().getAddress(), dest = command["destination"]))
            new_alloc = command["allocation"].Copy(command["destination"], **command)
            if new_alloc:
                self._db.CreateAllocation(new_alloc)
                self.register_allocation(new_alloc)
        if command["type"] == instruction.REFRESH:
            self._log.info("Refreshing allocation {alloc_id}".format(alloc_id = command["allocation"].GetMetadata().Id))
            command["allocation"].Manage(duration = command["duration"])
            self._db.UpdateAllocation(command["allocation"])
        if command["type"] == instruction.RELEASE:
            tmpAlloc = command["allocation"]
            self._log.info("Releasing allocation {alloc_id}".format(alloc_id = tmpAlloc.GetMetadata().Id))
            command["allocation"].Release()
            self._db.UpdateAllocation(tmpAlloc)
    
    
    def build_policies(self):
        tmpPolicies = []
        
        for policy in settings.policies:
            try:
                self._log.info("Creating policy: {policy}".format(policy = policy["class"]))
                policy_class = self.get_class(policy["class"])
                
                tmpPolicy = policy_class(**policy["args"])
                if "priority" in policy:
                    tmpPolicy.priority = priority
                    
                for _filter in policy["filters"]:
                    self._log.info("  Adding filter: {filt}".format(filt = _filter["class"]))
                    filter_class = self.get_class(_filter["class"])
                    
                    tmpFilter = filter_class(**_filter["args"])
                    tmpPolicy.AddFilter(tmpFilter)
            
                tmpPolicies.append(tmpPolicy)
            except Exception as exp:
                self._log.warn("app.build_policies: Could not parse policies - {exp}".format(exp = exp))
        
        self._policies = tmpPolicies
    
    def get_class(self, classname):
        try:
            self._log.info("  Importing class: {classname}".format(classname = classname))
            module, name = classname.rsplit('.', 1)
            __import__(module)
            tmpClass = getattr(sys.modules[module], name)
            self._log.info("  Added {classname}".format(classname = tmpClass))
        except Exception as exp:
            self._log.warn("app.get_class: Invalid class name - {exp}".format(exp = exp))
            return None

        return tmpClass


class PurgeApplication(DispatcherApplication):
    def __init__(self):
        self._log = record.getLogger()
        self._log.info("__init__: Creating new Dispatcher")
        self._exnodes     = {}
        self._db          = db.UnisProxy(unis_settings.UNIS_HOST, unis_settings.UNIS_PORT)

        # Register all currently availible exnodes on UNIS to the policy
        exnodes = self._db.GetExnodes()
        if not exnodes:
            self._log.error("Could not retrieve exnodes")
            return
        
        with concurrent.futures.ThreadPoolExecutor(max_workers = settings.THREADS) as executor:
            for result in executor.map(self.create_exnode, exnodes):
                self._exnodes[result["id"]] = result
                self._log.info("app.__init__: Registered exnode [{id}]".format(id = result["id"]))



        allocations = self._db.GetAllocations()
        if not allocations:
            self._log.error("Could not retrieve allocations")
            return
        
        with concurrent.futures.ThreadPoolExecutor(max_workers = settings.THREADS) as executor:
            for result in executor.map(factory.buildAllocation, allocations):
                if result:
                    self._exnodes[result.GetMetadata().exnode][result.GetMetadata().Id] = result


                
    def Run(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers = settings.THREADS) as executor:
            for result in executor.map(self.purge_worker, self._exnodes.values()):
                pass

    
    def purge_worker(self, extent):
        with concurrent.futures.ThreadPoolExecutor(max_workers = settings.THREADS) as executor:
            for result in executor.map(self.purge_allocation, extent["allocations"].values()):
                pass


    def purge_allocation(self, alloc):
        self._log.info("Releasing {alloc_id}".format(alloc_id = alloc.GetMetadata().Id))
        alloc.Release()
        self._db.UpdateAllocation(alloc)
        return ""
        

def main():
    logger = record.getLogger()
    # Parse arguments
    description = """{prog} collects and manages file information stored in UNIS.
                     Depending on the policy, {prog} will refresh, move, or duplicate
                     data based on network conditions.""".format(prog = sys.argv[0])
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('-d', '--debug', action='store_true')
    parser.add_argument('-v', '--verbose', action='store_true')
    parser.add_argument('-l', '--log-file', type=str)
    parser.add_argument('-u', '--unis-host')
    parser.add_argument('-p', '--unis-port', type=int)
    parser.add_argument('--purge', action='store_true')
    args = parser.parse_args()
    do_purge = False

    if args.debug:
        logger.setLevel(logging.DEBUG)
    if args.verbose:
        logger.setLevel(logging.INFO)
    if args.unis_host:
        unis_settings.UNIS_HOST = args.unis_host
    if args.unis_port:
        unis_settings.UNIS_PORT = args.unis_port
    if args.purge:
        do_purge = True

# Create and initialize logger
    if do_purge:
        app = PurgeApplication()
    else:
        app = DispatcherApplication()
    app.Run()


if __name__ == "__main__":
    main()

