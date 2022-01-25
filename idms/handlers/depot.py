import falcon
import json

from idms.handlers.base import _BaseHandler
from idms.handlers.utils import get_body

class DepotHandler(_BaseHandler):
    @falcon.after(_BaseHandler.encode_response)
    @get_body
    def on_post(self, req, resp, ref, body):
        try:
            self._db.manage_depots(ref, body['update'])
            resp.status = falcon.HTTP_200
            resp.body = json.dumps({'errorcode': None, "msg": ""})
        except Exception as e:
            resp.status = falcon.HTTP_500
            resp.body = json.dumps({'errorcode': 500, "msg": str(e)})
