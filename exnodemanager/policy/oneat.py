
import datetime

from policy import Policy
from exnodemanager import record
import exnodemanager.instruction as instruction

class OneAt(Policy):
    def __init__(self, host, port, validate = True, **kwargs):
        self._log = record.getLogger()
        self._host = host
        self._port = port
        self._validate = validate
        super(OneAt, self).__init__()
        
    def _apply(self, exnode):
        instructions = []
        offset = 0
        index = 0
        sorted_allocs = sorted(exnode["allocations"], key = lambda alloc: exnode["allocations"][alloc].GetMetadata().offset)
        print sorted_allocs
        print("{size}:{offset}".format(size = exnode["raw"]["size"], offset = offset))
        while offset <= exnode["raw"]["size"] and index < len(sorted_allocs):
            tmpMeta = exnode["allocations"][sorted_allocs[index]].GetMetadata()
            if offset < tmpMeta.offset:
                if index == 0:
                    self._log.warn("Exnode offset begins above 0, could not recover file")
                    return []

                prevAlloc = exnode["allocations"][sorted_allocs[index - 1]]
                tmpFill = prevAlloc.GetMetadata()
                if tmpFill.offset + tmpFill.realSize != tmpMeta.offset:
                    self._log.warn("Unfilled gap in exnode, could not recover file [{offset}-{offset2}]".format(offset = tmpFill.offset, offset2 = tmpMeta.offset))
                    return []
                
                instructions.append({ "type": instruction.COPY,
                                      "allocation": prevAlloc,
                                      "destination": { "host": self._host, "port": self._port } })
                
                offset = tmpMeta.offset
                
            if tmpMeta.host == self._host and tmpMeta.port == self._port:
                offset = tmpMeta.offset + tmpMeta.realSize
                
            index += 1

        if offset < exnode["raw"]["size"]:
            tmpAlloc = exnode["allocations"][sorted_allocs[index - 1]]
            tmpMeta = tmpAlloc.GetMetadata()
            if tmpMeta.offset + tmpMeta.realSize != exnode["raw"]["size"]:
                self._log.warn("Unfilled gap in exnode, could not recover file [2]")
                return []

            instructions.append({ "type": instruction.COPY,
                                  "allocation": tmpAlloc,
                                  "destination": { "host": self._host, "port": self._port } })
            
        return instructions
    
