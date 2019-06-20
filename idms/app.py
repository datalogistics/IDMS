import argparse
import falcon
import json
import time
import logging as plogging
from lace.logging import trace


from idms import engine
from idms.handlers import PolicyHandler, PolicyTracker, SSLCheck, DepotHandler, BuiltinHandler
from idms.lib.db import DBLayer
from idms.lib.middleware import FalconCORS
from idms.lib.service import IDMSService

from asyncio import TimeoutError
from lace import logging
from lace.logging import trace
from unis import Runtime
from unis.exceptions import ConnectionError
from unis.rest import UnisClient

routes = {
    "p": {"handler": BuiltinHandler},
    "r": {"handler": PolicyHandler},
    "a": {"handler": PolicyTracker},
    "a/{exnode}": {"handler": PolicyTracker},
    "d/{ref}": {"handler": DepotHandler}
}

def _get_app(unis, depots, viz):
    conf = {"auth": False, "secret": "a4534asdfsberwregoifgjh948u12"}
    while True:
        try:
            rt = Runtime(unis, defer_update=True, preload=["nodes", "services"])
        except (ConnectionError, TimeoutError) as exp:
            msg = "Failed to start runtime, retrying... - {}".format(exp)
            logging.getLogger('idms').warn(msg)
            time.sleep(5)
            continue
        break

    master = UnisClient.resolve(unis)
    db = DBLayer(rt, depots, viz)
    engine.run(db)
    service = IDMSService(db, master)
    rt.addService(service)
    
    ensure_ssl = SSLCheck(conf)
    app = falcon.API(middleware=[FalconCORS()])
    for k,v in routes.items():
        handler = v["handler"]
        del v["handler"]
        app.add_route("/{}".format(k), handler(conf, dblayer=db, **v))
    
    return app

def main():
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('-u', '--unis', default='http://wdln-base-station:8888', type=str,
                        help='Set the comma diliminated urls to the unis instances of interest')
    parser.add_argument('-H', '--host', type=str, help='Set the host for the server')
    parser.add_argument('-p', '--port', default=8000, type=int, help='Set the port for the server')
    parser.add_argument('-d', '--debug', default="NONE", type=str, help='Set the log level')
    parser.add_argument('-D', '--depots', default='', type=str, help='Provide a file for the depot decriptions')
    parser.add_argument('-v', '--visualize', default='', type=str, help='Set the server for the visualization effects')
    parser.add_argument('-q', '--viz_port', default='42424', type=str, help='Set the port fo the visualization effects')
    args = parser.parse_args()
    
    plogging.basicConfig(format='%(color)s[%(asctime)-15s] [%(levelname)s] %(name)s%(reset)s %(message)s')
    level = {"NONE": logging.NOTSET, "INFO": logging.INFO, "DEBUG": logging.DEBUG, "TRACE": logging.TRACE_ALL}[args.debug]
    log = logging.getLogger("idms")
    logging.getLogger('libdlt').setLevel(level)
    trace.showCallDepth(True)
    log.setLevel(level)
    host, port = args.host if args.host else "0.0.0.0", args.port
    unis = [str(u) for u in args.unis.split(',')]
    depots = None
    if args.depots:
        with open(args.depots) as f:
            depots = json.load(f)
    viz = "{}:{}".format(args.visualize, args.viz_port) if args.visualize else None
    app = _get_app(unis, depots, viz)
    
    from wsgiref.simple_server import make_server
    server = make_server(host, port, app)
    port = "" if port == 80 else port
    print("Getting topology from {}".format(unis))
    print("Listening on {}{}{}".format(host,":" if port else "", port))
    server.serve_forever()
    
if __name__ == "__main__":
    main()
