import time

from idms.lib.assertions.abstract import AbstractAssertion
from idms.lib.assertions.exceptions import SatisfactionError, AssertionError as SatisfactionWarning
from libdlt.util.files import ExnodeInfo

class All(AbstractAssertion):
    """
    Fully loads matching data to all destinations
    """
    tag = "$all"
    def initialize(self, ttl:int=180000):
        self._ttl = ttl

    def apply(self, ex, db):
        failed, warn, changed = [], [], False
        info = ExnodeInfo(ex, remote_validate=True)
        if not info.is_complete():
            raise SatisfactionError("Exnode is incomplete, cannot fill replication")
        for dst in db.get_depots():
            if dst.status != 'READY' and dst.status != 'UPDATE':
                failed.append(f"Destination is not ready [{dst.status}]")
            elif getattr(dst, 'ttl', 0) and time.time() - getattr(dst, 'lastseen', 0) > getattr(dst, 'ttl'):
                if not info.is_complete(dst.accessPoint):
                    failed.append(f"Destination ttl has expired [{dst.name}]")
                else:
                    warn.append(f"Destination is unreachable [{dst.name}]")
            elif not info.is_complete(dst.accessPoint):
                changed = True
                db.move_allocs(info.fill(dst.accessPoint), dst, self._ttl)

        if failed:
            raise SatisfactionError(", ".join(failed))
        elif warn:
            raise SatisfactionWarning(", ".join(warn))
        return changed
