import time

from idms.lib.assertions.abstract import AbstractAssertion
from idms.lib.assertions.exceptions import SatisfactionError
from idms.lib.utils import ExnodeInfo

class Exact(AbstractAssertion):
    """
    Fully loads matching data at a destination
    """
    tag = "$exact"
    def initialize(self, dest:str, ttl:int=180000):
        self._dest = dest
        self._ttl = ttl
    
    def apply(self, ex, db):
        if isinstance(self._dest, str):
            try:
                self._dest = next(d for d in db.get_depots() if d.name == self._dest)
            except StopIteration:
                raise SatisfactionError("Currently unknown destination - {}".format(self._dest))
        
        info = ExnodeInfo(ex, remote_validate=True)
        if not info.is_complete():
            raise SatisfactionError("Exnode is incomplete, cannot fill replication")
        if self._dest.status != 'READY' and self._dest.status != 'UPDATE':
            raise SatisfactionError("Destination is not ready [{}]".format(self._dest.status))
        elif self._dest.ts + (self._dest.ttl * 1000000) < time.time() * 1000000:
            raise SatisfactionError("Destination ttl has expired")
        if not info.is_complete(self._dest.accessPoint):
            db.move_allocs(info.fill(self._dest.accessPoint), self._dest, self._ttl)

        return not info.is_complete()
