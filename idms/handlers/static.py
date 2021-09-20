import os, falcon

from idms.settings import STATIC_PATH
from idms.handlers.base import _BaseHandler

class StaticHandler(_BaseHandler):
    def __init__(self, conf, dblayer, path):
        super().__init__(conf, dblayer)
        self._path = path
    def on_get(self, req, resp, ty=None, filename=None):
        filename = os.path.join(STATIC_PATH, os.path.join(ty, filename) if ty else self._path)
        resp.status = falcon.HTTP_200
        if filename.endswith('css'): resp.content_type = 'text/css'
        elif filename.endswith('js'): resp.content_type = 'text/javascript'
        else: resp.content_type = 'text/html'
        try:
            with open(filename, 'rb') as f:
                resp.body = f.read()
        except OSError as e:
            print(e)
            resp.status = falcon.HTTP_404
