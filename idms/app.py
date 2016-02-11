import argparse, importlib
import falcon
import json
import time
import logging as plogging
from lace.logging import trace

from idms import engine, settings
from idms.handlers import PolicyHandler, PolicyTracker, SSLCheck, DepotHandler, BuiltinHandler, StaticHandler, FileHandler, DirHandler
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
    "f": {"handler": FileHandler},
    "dir": {"handler": DirHandler},
    "p": {"handler": BuiltinHandler}, # DEFUNCT
    "r": {"handler": PolicyHandler},
    "a": {"handler": PolicyTracker},
    "a/{exnode}": {"handler": PolicyTracker},
    "d/{ref}": {"handler": DepotHandler},
    "manage": {"handler": StaticHandler},
    "s/{ty}/{filename}": {"handler": StaticHandler}
}

def get_app(conf):
    unis = [str(u) for u in conf['general']['dburl'].split(',')]
    depots = None
    if conf['general']['depotfile']:
        with open(conf['general']['depotfile']) as f:
            depots = json.load(f)

    while True:
        try:
            rt = Runtime(unis, defer_update=True, preload=["nodes", "services"])
        except (ConnectionError, TimeoutError) as exp:
            msg = "Failed to start runtime, retrying... - {}".format(exp)
            logging.getLogger('idms').warn(msg)
            time.sleep(5)
            continue
        break

    master = UnisClient.resolve(unis[0])
    db = DBLayer(rt, depots, conf)
    for plugin in conf['general']['plugins']:
        path = plugin.split('.')
        try:
            module = importlib.import_module('.'.join(path[:-1]))
            cls = getattr(module, path[-1])
            db.add_post_process(cls(db, conf))
        except (ImportError, AttributeError):
            import traceback
            traceback.print_exc()
            logging.getLogger('idms').warn("Bad postprocessing module - {}".format(plugin))
    engine.run(db, conf['general']['loopdelay'])
    service = IDMSService(db, UnisClient.resolve(unis[0]))
    rt.addService(service)
    
    ensure_ssl = SSLCheck(conf)
    app = falcon.API(middleware=[FalconCORS()])
    for k,v in routes.items():
        handler = v["handler"]
        del v["handler"]
        app.add_route("/{}".format(k), handler(conf, dblayer=db, **v))
    
    return app

def setup_logging(level):
    plogging.basicConfig(format='%(color)s[%(asctime)-15s] [%(levelname)s] %(name)s%(reset)s %(message)s')
    level = {"NONE": logging.NOTSET, "INFO": logging.INFO, "DEBUG": logging.DEBUG, "TRACE": logging.TRACE_ALL}[level]
    log = logging.getLogger("idms")
    logging.getLogger('libdlt').setLevel(level)
    logging.getLogger('unis').setLevel(level)
    trace.showCallDepth(True)
    log.setLevel(level)
    return log

conf = settings.CONFIG
def main():
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('-u', '--unis', type=str,
                        help='Set the comma diliminated urls to the unis instances of interest')
    parser.add_argument('-H', '--host', type=str, help='Set the host for the server')
    parser.add_argument('-p', '--port', default=8000, type=int, help='Set the port for the server')
    parser.add_argument('-d', '--debug', type=str, help='Set the log level')
    parser.add_argument('-D', '--depots', type=str, help='Provide a file for the depot decriptions')
    parser.add_argument('-v', '--visualize', type=str, help='Set the server for the visualization effects')
    parser.add_argument('-S', '--staging', type=str, help="Set the accessPoint URL for the depot to stage new data")
    parser.add_argument('-q', '--viz_port', default='42424', type=str, help='Set the port fo the visualization effects')
    args = parser.parse_args()
    conf['general'].update(**{
        'dburl': args.unis or conf['general']['dburl'],
        'loglevel': args.debug or conf['general']['loglevel'],
        'depotfile': args.depots or conf['general']['depotfile'],
        'vizurl': "{}:{}".format(args.visualize, args.viz_port) if args.visualize else conf['general']['vizurl']
    })
    conf['upload']['staging'] = args.staging or conf['upload']['staging']
    
    log = setup_logging(conf['general']['loglevel'])
    app = get_app(conf)
    log.info("Fetching topology from {}".format(conf['general']['dburl']))

    from wsgiref.simple_server import make_server
    host, port = args.host if args.host else "0.0.0.0", args.port
    server = make_server(host, port, app)
    port = "" if port == 80 else port
    log.info("Listening on {}{}{}".format(host,":" if port else "", port))
    server.serve_forever()
