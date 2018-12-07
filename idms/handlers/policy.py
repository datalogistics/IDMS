import falcon
import json
import requests

from idms.handlers.base import _BaseHandler
from idms.handlers.utils import get_body
from idms.lib.policy import Policy

class PolicyHandler(_BaseHandler):
    #@falcon.before(_BaseHandler.do_auth)
    @falcon.after(_BaseHandler.encode_response)
    @get_body
    def on_post(self, req, resp, body):
        try:
            assert "$match" in body, "No subject in request"
            assert "$action" in body, "No action in request"
        except AssertionError as exp:
            resp.body = json.dumps({"errorcode": 1, "msg": str(exp)})
            resp.status = falcon.HTTP_401
            return
        try:
            policy_id = Policy(body["$match"], body["$action"])
        except Exception as exp:
            resp.body = json.dumps({"errorcode": 2, "msg": "Malformed policy - {}".format(exp)})
            resp.status = falcon.HTTP_401
            return
         
        resp.body = json.dumps({"errorcode": None, "msg": "", "policyid": policy_id})
        resp.status = falcon.HTTP_200
