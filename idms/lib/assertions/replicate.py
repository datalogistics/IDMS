import time

from idms.lib.assertions.abstract import AbstractAssertion
from idms.lib.assertions.exceptions import SatisfactionError
from idms.lib.utils import ExnodeInfo

class Replicate(AbstractAssertion):
    tag = "$replicate"
    def initialize(self, copies, ttl):
        self._ttl = ttl
        self._copies = copies
    
    def apply(self, exnode, db):
        info, rcount, active_list = ExnodeInfo(exnode), 0, set
        for depot in db.get_depots():
            if depot.status in ['READY', 'UPDATE'] and \
               depot.ts + (depot.ttl * 1000000) > time.time() * 1000000:
                active_list.add(depot.accessPoint)

        candidates = []
        for depot in active_list:
            if info.is_complete(depot.accessPoint): rcount += 1
            else: candidates.append(depot)

        if rcount + len(candidates) < copies:
            raise SatisfactionError("Too few depots available to satisfy replication")

        for _ in range(copies - rcount):
            depot = candidates.pop()
            db.move_files(exnode, depot, self._ttl)

        return True
