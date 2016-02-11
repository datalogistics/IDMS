import os, configparser, logging

def expandvar(x):
    v = os.path.expandvars(x)
    return None if v == x else v

TOKEN_TTL = 3600
INITIAL_THREAD_COUNT = 2
MAX_THREAD_COUNT = 5
ENGINE_LOOP_DELAY = 30
CACHE_DIR = expandvar("$IDMS_CACHE_DIR") or os.path.expanduser('~/.idms/cache')
CONFIG_FILE = expandvar("$IDMS_CONFIG") or os.path.expanduser('~/.idms/idms.cfg')
try:
    os.makedirs(CACHE_DIR)
except FileExistsError:
    pass
except OSError as exp:
    raise

BS = 1024*1024*20
MIME = {
    'HTML': 'text/html',
    'JSON': 'application/json',
    'BSON': 'application/bson',
    'PLAIN': 'text/plain',
    'SSE': 'text/event-stream',
    'PSJSON': 'application/perfsonar+json',
    'PSBSON': 'application/perfsonar+bson',
    'PSXML': 'application/perfsonar+xml',
}

STATIC_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "public")

PLUGINS = [
    "idms.plugins.wdln.WDLNPlugin"
]

SERVICE_TYPES = [
    "datalogistics:wdln:ferry",
    "datalogistics:wdln:base",
    "ibp_server"
]


CONFIG = {
    "general": {
        "plugins": PLUGINS,
        "servicetypes": SERVICE_TYPES,
        "loopdelay": ENGINE_LOOP_DELAY,
        "dburl": "http://localhost:30100",
        "loglevel": "NONE",
        "depotfile": '',
        "vizurl": None,
    },
    "auth": {
        "tokenttl": TOKEN_TTL,
        "auth": False,
        "secret": "a4534asdfsberwregoifgjh948u12"
    },
    "threads": {
        "initialsize": INITIAL_THREAD_COUNT,
        "max": MAX_THREAD_COUNT
    },
    "upload": {
        "blocksize": BS,
        "staging": ''
    }
}

_parser = configparser.ConfigParser(allow_no_value=True)
_parser.read(CONFIG_FILE)
_log = logging.getLogger('idms.config')
_tys = { "true": True, "false": False, "none": None, "": None }
for section in _parser.sections():
    if section not in CONFIG: 
        _log.warn(f"Bad configuration - {section}")
    CONFIG[section].update({k:_tys.get(v, v) for k,v in _parser.items(section)})
