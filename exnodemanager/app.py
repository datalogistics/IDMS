'''
@Name:   app.py
@Author: Jeremy Musser
@Date:   02/23/15

------------------------------------------


'''
import logging
import argparse
import sys
import urllib2
import json
import tornado
import datetime

from ws4py.client.tornadoclient import TornadoWebSocketClient
import concurrent.futures

import settings
import web.settings as unis_settings
import policy.refreshpolicy as policy
import protocol.ibpprotocol as protocol
import web.unisproxy as db

from web.handlers import ExnodeSocketClient, ExtentSocketClient

class DispatcherApplication(object):
    def __init__(self):
        logging.info("__init__: Creating new Dispatcher")
        self._pending  = []
        self._policy   = policy.RefreshPolicy()
        self._protocol = protocol.IBPProtocol()
        self._db       = db.UnisProxy(unis_settings.UNIS_HOST, unis_settings.UNIS_PORT)

    # Register all currently availible exnodes on UNIS to the policy
        exnodes = self._db.GetExnodes()
        for exnode in exnodes:
            tmpResult = self._policy.RegisterExnode(exnode)
            logging.info("app.__init__: Registered exnode [{id}]".format(id=tmpResult))

    # Register all currently availible extents on UNIS to the policy
    # and register their id in the action priority queue
        extents = self._db.GetExtents()
        for extent in extents:
            tmpResult = self._policy.RegisterExtent(extent)
            if tmpResult:
                logging.info("app.__init__: Registered extent [{id}]".format(id=tmpResult["id"]))
        
        # Create websockets to listen for future changes to exnodes and extents
        tmpExnodeUrl = "{protocol}://{host}:{port}/subscribe/{resource}".format(protocol = "ws",
                                                                          host     = unis_settings.UNIS_HOST,
                                                                          port     = unis_settings.UNIS_PORT,
                                                                          resource = "exnodes")
        tmpExtentUrl = "{protocol}://{host}:{port}/subscribe/{resource}".format(protocol = "ws",
                                                                          host     = unis_settings.UNIS_HOST,
                                                                          port     = unis_settings.UNIS_PORT,
                                                                          resource = "extent")
        
        exnode_ws = ExnodeSocketClient(tmpExnodeUrl)
        extent_ws = ExtentSocketClient(tmpExtentUrl)
        exnode_ws.initialize(policy = self._policy)
        extent_ws.initialize(policy = self._policy)
        exnode_ws.connect()
        extent_ws.connect()

    def Run(self):
        logging.info("app.Run: Starting main loop")
        tornado.ioloop.PeriodicCallback(callback=self._check_extents, callback_time=settings.ITERATION_TIME * 1000).start()
        tornado.ioloop.IOLoop.instance().start()


    def _check_extents(self):
    # Check for pending changes from the policy
        if self._policy.hasPending():
            tmpPending = self._policy.GetPendingExtents()

            for extent in tmpPending:
                instruction = self._policy.ProcessExtent(extent)
                if instruction:
                    self._process_instruction(instruction)



    def _process_instruction(self, instruction):
        try:
            extent = instruction["extent"]
            source = extent["mapping"]["read"].split("/")
            source = dict(zip(["host", "port"], source[2].split(":")))
            res_id = extent["id"]

            for destination in instruction["addresses"]:
                if "duration" in destination:
                    duration = destination["duration"]
                else:
                    duration = None

                # Instruction is a refresh in place
                if source == destination["address"]:
                    if self._protocol.Manage(source, extent, duration=duration):
                        duration = datetime.timedelta(seconds=duration)
                        newExtent = extent
                        newExtent["lifetimes"][0]["end"]   = (datetime.datetime.utcnow() + duration).strftime("%Y-%m-%d %H:%M:%S")
                        if self._db.UpdateExtent(newExtent):
                            self._policy.ClearExtent(res_id)
                    else:
                        logging.warn("app._process_instructions:  Failed to change duration on depot")

                # Instruction is a copy
                else:
                    response = self._protocol.Copy(source      = source,
                                                   destination = destination["address"],
                                                   extent      = extent,
                                                   duration    = duration)
                    if response:
                        newExtent = { "location": "ibp://", 
                                      "size":    extent["size"], 
                                      "offset":  extent["offset"], 
                                      "parent":  extent["parent"],
                                      "mapping": response["caps"] }

                        newExtent["lifetimes"][0]["start"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        newExtent["lifetimes"][0]["end"]   = (datetime.datetime.utcnow() + duration).strftime("%Y-%m-%d %H:%M:%S")
                        if self._db.UpdateExtent(newExtent):
                            self._policy.ClearExtent(res_id)
                    else:
                        logging.warn("app._process_instructions:  Failed to change duration on depot")
                        
                        
        except Exception as exp:
            logging.warn("app._process_instructions: Could not proccess instruction - {message}".format(message = exp))


class PurgeApplication(DispatcherApplication):
    def __init__(self):
        logging.info("__init__: Creating new Dispatcher")
        self._policy   = policy.RefreshPolicy()
        self._protocol = protocol.IBPProtocol()
        self._db       = db.UnisProxy(unis_settings.UNIS_HOST, unis_settings.UNIS_PORT)

    # Register all currently availible exnodes on UNIS to the policy
        exnodes = self._db.GetExnodes()
        for exnode in exnodes:
            tmpResult = self._policy.RegisterExnode(exnode)

    # Register all currently availible extents on UNIS to the policy
    # and register their id in the action priority queue
        extents = self._db.GetExtents()
        for extent in extents:
            tmpResult = self._policy.RegisterExtent(extent, allow_old=True)

    def Run(self):
        extents = self._policy.extentList()
        
        with concurrent.futures.ThreadPoolExecutor(max_workers = settings.THREADS) as executor:
            for result in executor.map(self.purge_worker, extents):
                logging.info(result)


    def purge_worker(self, extent):
        extent = self._policy.extentList()[extent]
        extent = extent["data"]
        address = extent["mapping"]["read"].split("/")
        address = dict(zip(["host", "port"], address[2].split(":")))
        
        result, log = self._protocol.Release(address, extent)
        newExtent = extent
        newExtent["lifetimes"][0]["end"] = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        result, log2 = self._db.UpdateExtent(newExtent)
        
        return log + log2


def main():
# Parse arguments
    description = """{prog} collects and manages file information stored in UNIS.
                     Depending on the policy, {prog} will refresh, move, or duplicate
                     data based on network conditions.""".format(prog = sys.argv[0])
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('-d', '--debug', action='store_true')
    parser.add_argument('-l', '--log-file', type=str)
    parser.add_argument('-u', '--unis-host')
    parser.add_argument('-p', '--unis-port', type=int)
    parser.add_argument('--purge', action='store_true')
    args = parser.parse_args()
    do_purge = False

    if args.debug:
        settings.LOG_LEVEL = logging.DEBUG
    if args.log_file:
        settings.LOG_PATH = args.log_file
    if args.unis_host:
        unis_settings.UNIS_HOST = args.unis_host
    if args.unis_port:
        unis_settings.UNIS_PORT = args.unis_port
    if args.purge:
        do_purge = True

# Create and initialize logger
    if settings.LOG_PATH:
        logging.basicConfig(filename=settings.LOG_PATH, level=settings.LOG_LEVEL)
    else:
        logging.basicConfig(level=settings.LOG_LEVEL)
    
    if do_purge:
        app = PurgeApplication()
    else:
        app = DispatcherApplication()
    app.Run()


if __name__ == "__main__":
    main()

