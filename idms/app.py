import argparse, importlib, threading
import falcon, os, json, time, socket
import logging as plogging

from idms import settings
from idms.config import MultiConfig
from idms.handlers import PolicyHandler, PolicyTracker, SSLCheck, DepotHandler, BuiltinHandler, StaticHandler, FileHandler, DirHandler, DownloadHandler, HealthHandler
from idms.lib.db import DBLayer
from idms.lib.middleware import FalconCORS
from idms.lib.service import IDMSService

from asyncio import TimeoutError
from lace import logging
from lace.logging import trace
from unis import Runtime
from unis.exceptions import ConnectionError
from unis.rest import UnisClient

def build_conf():
    plogging.basicConfig(format='%(color)s[%(asctime)-15s] [%(levelname)s] %(name)s%(reset)s %(message)s')
    conf = MultiConfig(settings.CONFIG, "Intelligent Data Management Service curates objects and validates data policy",
                       filevar="$IDMS_CONFIG")
    conf.add_loglevel("TRACE", logging.TRACE_ALL)
    return conf

def get_app(conf=None):
    routes = {
        "f": {"handler": FileHandler},
        "dir": {"handler": DirHandler},
        "p": {"handler": BuiltinHandler}, # DEFUNCT
        "r": {"handler": PolicyHandler},
        "a": {"handler": PolicyTracker},
        "a/{exnode}": {"handler": PolicyTracker},
        "h/{exnode}": {"handler": HealthHandler},
        "d/{ref}": {"handler": DepotHandler},
        "manage": {"handler": StaticHandler, "path": "index.html"},
        "health": {"handler": StaticHandler, "path": "health.html"},
        "s/{ty}/{filename}": {"handler": StaticHandler, "path": ""},
        "sf/{rid}": {"handler": DownloadHandler},
    }

    conf = conf or build_conf().from_file(include_logging=True)
    unis = [str(u) for u in conf['unis'].split(',')]
    depots = None
    if conf['depotfile']:
        with open(os.path.expanduser(conf['depotfile'])) as f:
            depots = json.load(f)

    while True:
        try:
            rt = Runtime(unis, cache={'preload':["nodes","services","exnodes","extents"]})
        except (ConnectionError, TimeoutError) as exp:
            msg = "Failed to start runtime, retrying... - {}".format(exp)
            logging.getLogger('idms').warn(msg)
            time.sleep(5)
            continue
        break

    db = DBLayer(rt, depots, conf)
    for plugin in (conf['plugins'] or []):
        path = plugin.split('.')
        try:
            module = importlib.import_module('.'.join(path[:-1]))
            db.add_post_process(getattr(module, path[-1])(db, conf))
        except (ImportError, AttributeError) as e:
            logging.getLogger('idms').warn(f"Bad postprocessing module [{plugin}] - {e}")
    service = IDMSService(db, UnisClient.resolve(unis[0]))
    rt.addService(service)
    
    ensure_ssl = SSLCheck(conf)
    app = falcon.API(middleware=[FalconCORS()])
    for k,v in routes.items():
        handler = v.pop("handler")
        app.add_route("/{}".format(k), handler(conf, dblayer=db, **v))
    
    return app

conf = settings.CONFIG

def watchdog():
    try:
        from sdnotify import SystemdNotifier
        notify = SystemdNotifier().notify
    except: notify = lambda x,y=0,z=0,a=0: True

    notify("READY=1")
    for _ in range(3):
    #while True:
        notify("WATCHDOG=1")
        time.sleep(5)

def main():
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('-u', '--unis', type=str,
                        help='Set the comma diliminated urls to the unis instances of interest')
    parser.add_argument('-H', '--host', type=str, help='Set the host for the server')
    parser.add_argument('-p', '--port', type=int, help='Set the port for the server')
    parser.add_argument('-D', '--depotfile', type=str, help='Provide a file for the depot decriptions')
    parser.add_argument('-v', '--visualize', type=str, help='Set the server for the visualization effects')
    parser.add_argument('-S', '--upload.staging', type=str, metavar="STAGING", help="Set the accessPoint URL for the depot to stage new data")
    parser.add_argument('-q', '--viz_port', default='42424', type=str, help='Set the port fo the visualization effects')
    parser.add_argument('-V', '--version', action='store_true')
    parser.add_argument('-N', '--sdnotify', action='store_true', help="Enable notifications and watchdog for systemd integration")
    conf = build_conf()
    conf = conf.from_parser(parser, include_logging=True)


    if conf['version']:
        print("IDMS - Intelligent Data Management Service")
        print(f"v{settings.MAJOR_VERSION}.{settings.MINOR_VERSION}.{settings.INC_VERSION}")
        exit(0)
    if conf['upload']['staging'] is None:
        conf['upload']['staging'] = f"ibp://{socket.gethostname()}:6714"
    log = logging.getLogger('idms')
    app = get_app(conf)
    log.info("Fetching topology from {}".format(conf['unis']))
    
    from wsgiref.simple_server import make_server
    host, port = conf['host'] if conf['host'] else "0.0.0.0", conf['port']
    server = make_server(host, int(port), app)
    port = "" if port == 80 else port
    log.info("Listening on {}{}{}".format(host,":" if port else "", port))
    if conf['sdnotify']:
        t = threading.Thread(target=watchdog, daemon=True).start()
    server.serve_forever()

if __name__ == "__main__":
    main()
