
from collections import deque

import exnodemanager.instruction as instruction
import exnodemanager.protocol.ibp as ibp
import exnodemanager.record as record
import exnodemanager.web.unisproxy as db
import exnodemanager.web.settings as unis_settings

from policy import Policy

class MaintainCopies(Policy):
    def __init__(self, copies, copyduration = 60, validate = True, **kwargs):
        super(MaintainCopies, self).__init__()
        self._log = record.getLogger()
        self._copies = copies
        self._validate = validate
        if not "depots" in kwargs:
            conn = db.UnisProxy(unis_settings.UNIS_HOST, unis_settings.UNIS_PORT)
            self._depots = deque(conn.GetDepots())
        else:
            self._depots = deque(kwargs["depots"])
        self._duration = copyduration
        
    def _apply(self, exnode):
        instructions = []
        seen = []
        tmpExnodes = exnode["allocations"]
        
        for check_id, check_alloc in tmpExnodes.items():
            count = 0
            found = []
            checkMeta = check_alloc.GetMetadata()
            if (checkMeta.realSize, checkMeta.offset) in seen:
                continue
            else:
                seen.append((checkMeta.realSize, checkMeta.offset))
            
            for inner_id, alloc in tmpExnodes.items():
                meta      = alloc.GetMetadata()
                
                if checkMeta.offset == meta.offset and checkMeta.realSize == meta.realSize:
                    count += 1
                    found.append(alloc)
            

            if len(found) > self._copies:
                ordered_found = sorted(found, key = lambda alloc: alloc.GetMetadata().timestamp, reverse = True)
                for i in range(self._copies, len(found)):
                    instructions.append({ "type": instruction.RELEASE, "allocation": ordered_found[i] })
            else:
                for i in range(len(found), self._copies):
                    searching = True
                    while searching:
                        tmpDepot = self._depots.popleft()
                        self._depots.append(tmpDepot)
                        duplicate = False
                        for alloc in found:
                            if alloc.GetMetadata().host == tmpDepot[0]:
                                duplication = True
                                break
                        if not duplicate:
                            searching = False
                      
                    instructions.append({ "type": instruction.COPY, "allocation": check_alloc, "destination": {"host": tmpDepot[0], "port": tmpDepot[1]} })
                
        return instructions


class Record(object):
    def __init__(self):
        self._head = Chunk(0, 0)

    def Add(self, alloc):
        tmpData  = alloc.GetMetadata()
        tmpChunk = Chunk(tmpData.offset, tmpData.offset + tmpData.realSize)
        tmpHead  = self._head

        while tmpHead._next and tmpHead._next.end < tmpChunk.start:
            tmpHead = tmpHead._next

        # Edge case where first chunk overlaps new alloc
        if tmpHead.end > tmpChunk.start:
            return False

        # Case where new chunk overlaps next chunk
        if tmpHead._next and tmpHead._next.start < tmpChunk.end:
            return False

        tmpChunk._next = tmpHead._next
        tmpChunk._prev = tmpHead
        
        tmpHead._next = tmpChunk
        if tmpChunk._next:
            tmpChunk._next._prev = tmpChunk
            
        if tmpHead.end == tmpChunk.start:
            tmpHead.Append(tmpChunk)
            tmpChunk = tmpHead
        
        if tmpChunk._next:
            tmpChunk.Append(tmpChunk._next)

        return True

    def FindHoles(self, size):
        tmpHead = self._head
        tmpHoles = []

        while tmpHead._next and tmpHead.end < size:
            tmpHoles.append({ "start": tmpHead.end, "end": tmpHead._next.start })
            tmpHead = tmpHead._next

        if tmpHead.end < size:
            tmpHoles.append({ "start": tmpHead.end, "end": size })
        
        return tmpHoles

    def Difference(self, alloc):
        results = []

        for hole in self.FindHoles():
            if alloc.offset < hole["end"] or alloc.offset + alloc.realSize > hole["start"]:
                tmpStart = max(hole["start"], alloc.offset)
                tmpEnd   = min(hole["end"], alloc.offset + alloc.realSize)
                results.append({"start": tmpStart, "end": tmpEnd})

        return results


class Chunk(object):
    def __init__(self, start, end):
        self.start = start
        self.end   = end
        self._prev  = None
        self._next  = None

    def Append(self, chunk):
        self.end = chunk.end
        self._next = chunk._next
        if chunk._next:
            chunk._next._prev = self
