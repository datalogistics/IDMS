import os

TOKEN_TTL = 3600
THREADS = 1
CACHE_DIR = os.path.expanduser('~/.idms_cache')
try:
    os.makedirs(SCHEMA_CACHE_DIR)
except FileExistsError:
    pass
except OSError as exp:
    raise

BS = '4m'
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
