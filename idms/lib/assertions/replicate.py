import time

from collections import defaultdict

from idms.lib.assertions.abstract import AbstractAssertion
from idms.lib.assertions.exceptions import SatisfactionError
from idms.lib.utils import ExnodeInfo

_VALID, _INVALID = 0, 1

class Replicate(AbstractAssertion):
    """
    Ensures data is replicated n times
    """
    tag = "$replicate"
    def initialize(self, copies:int, ttl:int=180000):
        self._ttl = ttl
        self._copies = copies
    
    def apply(self, exnode, db):
        self.log.debug("--Checking exnode health")
        replicas, offset, allocs = set(), 0, defaultdict(list)
        info, active_list = ExnodeInfo(exnode, remote_validate=True), set()
        if not info.is_complete(): raise SatisfactionError("Exnode is incomplete, cannot fill replication")
        
        for depot in db.get_depots():
            print(depot.ts, depot.status, depot.ttl, time.time() * 1000000)
            if depot.status in ['READY', 'UPDATE'] and \
               depot.ts + (depot.ttl * 1000000) > time.time() * 1000000:
                active_list.add(depot.accessPoint)

        if len(active_list) < self._copies: raise SatisfactionError("Too few depots to satisfy replication")

        state, cs, last = _VALID, 0, set(info.replicas_at(0))
        for byte in range(0, exnode.size):
            replicas = set(info.replicas_at(offset))
            if state == _VALID and len(replicas) < self._copies:
                last, state, cs = replicas, _INVALID, byte
            elif state == _INVALID and last ^ replicas:
                if len(replicas) >= self._copies:
                    state = _VALID
                last, dlist = replicas, active_list - last
                for a in sorted(info.allocs_in(cs, byte), key=lambda x: x.offset):
                    if a.offset + a.size <= cs:
                        allocs[dlist[0]].append(a)
                    cs = a.offset + a.size

        for d,a in allocs.items():
            db.move_allocs(a, d, self._ttl)

        return True
