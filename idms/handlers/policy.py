import falcon, json, typing, inspect

from idms.handlers.base import _BaseHandler
from idms.handlers.utils import get_body
from idms.lib.policy import Policy
from idms.lib.assertions import assertions, AbstractAssertion

class PolicyHandler(_BaseHandler):
    types = {int: 'int', float: 'float', str: 'str', list: 'strlist',
             typing.List[AbstractAssertion]: 'policylist',
             typing.List[int]: 'numlist',
             typing.List[float]: 'numlist', typing.List[str]: 'strlist'}
    @falcon.after(_BaseHandler.encode_response)
    def on_get(self, req, resp):
        results = {}
        for tag, policy in assertions.items():
            args, sig = {}, inspect.signature(policy.initialize)
            for k,v in list(sig.parameters.items())[1:]:
                args[k] = { 'type': self.types.get(v.annotation, 'str'),
                            'default': v.default if v.default is not inspect._empty else '' }
            results[tag] = { 'args': args, 'description': policy.__doc__ or "" }

        resp.text = results
        resp.status = falcon.HTTP_200
    
    #@falcon.before(_BaseHandler.do_auth)
    @falcon.after(_BaseHandler.encode_response)
    @get_body
    def on_post(self, req, resp, body):
        try:
            assert "$match" in body, "No subject in request"
            assert "$action" in body, "No action in request"
        except AssertionError as exp:
            resp.text = json.dumps({"errorcode": 1, "msg": str(exp)})
            resp.status = falcon.HTTP_401
            return
        try:
            policy = Policy(body["$match"], body["$action"])
            policy_id = self._db.register_policy(policy)
        except Exception as exp:
            resp.text = json.dumps({"errorcode": 2, "msg": "Malformed policy - {}".format(exp)})
            resp.status = falcon.HTTP_401
            return
         
        resp.text = json.dumps({"errorcode": None, "msg": "", "policyid": policy_id})
        resp.status = falcon.HTTP_200
