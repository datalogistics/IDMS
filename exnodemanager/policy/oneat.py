
import datetime

from policy import Policy
from maintaincopies import Record

import exnodemanager.protocol.ibp.allocation

class OneAt(Policy):
    def __init__(self, host, port):
        self._host = host
        self._port = port
        super(OneAt, self).__init__()

    def _apply(self, exnode):
        instructions = []
        record = Record()

        for key, alloc in exnode["allocations"].items():
            if alloc.host == self._host and alloc.port == self._port and alloc.end > datetime.datetime.utcnow():
                record.Add(alloc)

        for key, alloc in exnode["allocations"].items():
            if not alloc.Check():
                continue
            if alloc.host == self._host and alloc.port == self._port:
                continue
                
            holes = record.Difference(alloc)            
            for hole in holes:
                tmpAlloc = allocation.Allocation()
                tmpAlloc.offset = hole["start"]
                tmpAlloc.realSize = hole["end"] - hole["start"]
                tmpAlloc = factory.buildAllocation(tmpAlloc.Serialize())
                record.Add(tmpAlloc)
                
                instructions.append({ "type": instruction.COPY, 
                                      "allocation": alloc, 
                                      "destination": {"host": self._host, "port": self._port}, 
                                      "offset": tmpAlloc.offset, 
                                      "size":   tmpAlloc.realSize })
                
        return instructions
