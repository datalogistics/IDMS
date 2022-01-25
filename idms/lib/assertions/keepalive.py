import time

from libdlt.protocol.ibp.services import ProtocolService as IBPManager
from libdlt.util.files import ExnodeInfo

from idms.lib.assertions.abstract import AbstractAssertion
from idms.lib.assertions.exceptions import SatisfactionError

_VALID, _INVALID = 0, 1

class KeepAlive(AbstractAssertion):
    """
    Ensures file does not expire from timeout.
    """
    tag = "$keepalive"
    def initialize(self, period:int=600, ttl:int=180000):
        self._ttl = int(ttl)
        self._period = int(period)

    def apply(self, exnode, db):
        changed = False
        proxy = IBPManager()

        self.log.debug("--Checking exnode health")
        info = ExnodeInfo(exnode, remote_validate=True)
        if not info.is_complete(): raise SatisfactionError("Exnode is incomplete, cannot maintain full copy")
        for alloc in exnode.extents:
            if int(info[alloc]["duration"]) < self._ttl - self._period:
                changed = True
                proxy.manage(alloc, duration=self._ttl, max_size=alloc.size)
                alloc.lifetime.end = str(int((time.time() + self._ttl) * 1000000))
        return changed
