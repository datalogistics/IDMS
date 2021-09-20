import time

from collections import defaultdict
from shapely.geometry import Polygon, Point

from idms.lib.assertions.abstract import AbstractAssertion
from idms.lib.assertions.exceptions import SatisfactionError
from idms.lib.utils import ExnodeInfo

_VALID, _INVALID = 0, 1

class GeoFense(AbstractAssertion):
    """
    Places matching data within a geographical region
    """
    tag = "$geo"
    def initialize(self, poly:list, ttl:int=180000):
        self._fense = Polygon(poly)
        self._ttl = ttl

    def apply(self, exnode, db):
        valid_depots = set()
        for depot in db.get_depots():
            loc = depot.runningOn.location
            if hasattr(loc, 'latitude') and hasattr(loc, 'longitude'):
                if self._fense.contains(Point(loc.latitude, loc.longitude)) and \
                   depot.status == 'READY' and \
                   depot.ts + (depot.ttl * 1000000) > time.time() * 1000000:
                    valid_depots.add(depot.accessPoint)

        info, dlist = ExnodeInfo(exnode), [d.location for d in valid_depots]
        if not valid_depots: raise SatisfactionError("No depots found within the fense")
        if not info.is_complete(): raise SatisfactionError("Incomplete Exnode, cannot fill replication")
        
        allocs, state, cs = defaultdict(list), _VALID, 0
        for byte in range(0, exnode.size):
            if state == _VALID and not set(info.replicas_at(offset)) & dlist:
                state, cs = _INVALID, byte
            elif state == _INVALID and set(info.replicas_at(offset)) & dlist:
                state = _VALID
                for a in sorted(info.allocs_in(cs, byte), key=lambda x: x.offset):
                    if a.offset + a.size <= cs:
                        allocs[dlist[0]].append(a)
                    cs = a.offset + a.size

        for d,a in allocs.items():
            db.move_allocs(a, d, self._ttl)

        return not bool(allocs)
