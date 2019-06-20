import time

from collections import namedtuple

from idms.lib.assertions.abstract import AbstractAssertion
from idms.lib.assertions.exceptions import SatisfactionError

_chunk = namedtuple('chunk', ['offset', 'size'])
class Exact(AbstractAssertion):
    tag = "$exact"
    def initialize(self, dest, ttl):
        self._dest = dest
        self._ttl = ttl
    
    def apply(self, ex, db):
        if isinstance(self._dest, str):
            try:
                self._dest = next(d for d in db.get_depots() if d.name == self._dest)
            except StopIteration:
                raise SatisfactionError("Currently unknown destination - {}".format(self._dest))
        
        depot = self._dest.accessPoint
        done, complete = 0, True
        offsets = sorted([_chunk(e.offset, e.size) for e in ex.extents if e.location == depot])

        for offset, size in offsets:
            if offset > done:
                complete = False
            done = max(done, size+offset)
        
        complete &= done == ex.size
        if self._dest.status != 'READY' and self._dest.status != 'UPDATE':
            raise SatisfactionError("Destination is not ready [{}]".format(self._dest.status))
        elif self._dest.ts + (self._dest.ttl * 1000000) < time.time() * 1000000:
            raise SatisfactionError("Destination has not checked in in", self._dest.ttl)
        if not complete:
            db.move_files(ex, self._dest, self._ttl)

        return not complete

