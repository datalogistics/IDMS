import os, logging

MAJOR_VERSION=1
MINOR_VERSION=1
INC_VERSION=  2

def expandvar(x):
    v = os.path.expandvars(x)
    return None if v == x else v

TOKEN_TTL = 3600
INITIAL_THREAD_COUNT = 1
MAX_THREAD_COUNT = 1
ENGINE_LOOP_DELAY = 30
CACHE_DIR = expandvar("$IDMS_CACHE_DIR") or os.path.expanduser('~/.idms/cache')
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
    "plugins": PLUGINS,
    "servicetypes": SERVICE_TYPES,
    "loopdelay": ENGINE_LOOP_DELAY,
    "host": None,
    "port": 8000,
    "unis": "http://localhost:30100",
    "loglevel": "NOTSET",
    "depotfile": '',
    "vizurl": None,
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
