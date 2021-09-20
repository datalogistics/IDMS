import time

from idms.lib.assertions.abstract import AbstractAssertion
from idms.lib.assertions.exceptions import SatisfactionError, AssertionError as SatisfactionWarning
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
            raise SatisfactionError(f"Destination is not ready [{self._dest.status}]")
        elif getattr(dst, 'ttl', 0) and time.time() - getattr(dst, 'lastseen', 0) > getattr(dst, 'ttl'):
            if not info.is_complete(self._dest.accessPoint):
                raise SatisfactionError(f"Destination ttl has expired [{self._dest.name}]")
            else:
                raise SatisfactionWarning(f"Destination is unreachble [{self._dest.name}]")
        elif not info.is_complete(self._dest.accessPoint):
            db.move_allocs(info.fill(self._dest.accessPoint), self._dest, self._ttl)

        return not info.is_complete(self._dest.accessPoint)
