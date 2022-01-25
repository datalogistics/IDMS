import falcon
import json, time
from libdlt.depot import Depot
from libdlt.protocol.ibp.services import ProtocolService as IBPManager

from idms.handlers.base import _BaseHandler
from libdlt.util.files import ExnodeInfo
from lace import logging

_proxy,log = IBPManager(), logging.getLogger('idms.handler.health')
class HealthHandler(_BaseHandler):
    def on_get(self, req, resp, exnode):
        exnode = self._db._rt.exnodes.first_where({'id': exnode})
        if not exnode:
            resp.body = "{}"
            resp.status = falcon.HTTP_404
            return
        results = {"filename": exnode.name, "rawsize": exnode.size, "depots": []}
        for depot in self._db.get_depots():
            r = {
                "depot": depot.name or depot.id,
                "alive": time.time() - getattr(depot,"lastseen",0) < getattr(depot,"ttl",0),
                "allocs": { "alive": [], "dead": [] }
            }
            for a in exnode.extents:
                if not hasattr(a, "depot"):
                    a.depot = Depot(a.location)
                if a.location == depot.accessPoint:
                    try:
                        if not r['alive']: raise Exception()
                        if not _proxy.probe(a): raise Exception()
                        r['allocs']['alive'].append([a.offset, a.size])
                    except Exception as e:
                        log.debug(f"Health check probe failed - {e}")
                        r['allocs']['dead'].append([a.offset, a.offset + a.size])
            results["depots"].append(r)
        resp.body = json.dumps(results)
        resp.status = falcon.HTTP_200
