
from collections import deque

import exnodemanager.instruction as instruction
import exnodemanager.protocol.ibp as ibp
import exnodemanager.record as record

from policy import Policy


class MaintainCopies(Policy):
    def __init__(self, copies):
        self._log = record.getLogger()
        self._copies = copies
        self._depotList = set()
        super(MaintainCopies, self).__init__()
    
    def _apply(self, exnode):
        instructions = []
        tmpCopies = [ Record() for x in range(self._copies) ]
        tmpHoles  = []
        tmpExtraAllocs = []
        
        self._log.info("!Maintain {copies} copies of {exnode}".format(copies = self._copies, exnode = exnode["raw"]["id"]))
        for key, alloc in exnode["allocations"].items():
            if not alloc.Check():
                instructions.append({ "type": instruction.RELEASE, "allocation": alloc })
                continue

            tmpAlloc = alloc.GetMetadata()
            self._depotList.add((tmpAlloc.host, tmpAlloc.port))
            
            success = False
            for i in range(self._copies):
                if tmpCopies[i].Add(alloc):
                    success = True
                    break
                
            if not success:
                tmpExtraAllocs.append(alloc)

        
        size = exnode["raw"]["size"]
        depots = deque(self._depotList)

        for record in tmpCopies:
            tmpHoles += record.FindHoles(size)

        for hole in tmpHoles:
            for key, alloc in exnode["allocations"].items():
                tmpAlloc = alloc.GetMetadata()
                if tmpAlloc.offset <= hole["start"] and tmpAlloc.offset + tmpAlloc.realSize >= hole["end"]:
                    depot = depots.popleft()
                    depots.append(depot)

                    while not ibp.services.GetStatus({"host": depot[0], "port": depot[1]}):
                        depot = depots.popleft()
                        depots.append(depot)
                    
                    instructions.append({ "type": instruction.COPY, "allocation": alloc, "destination": {"host": depot[0], "port": depot[1]} })
                    break

        for extra in tmpExtraAllocs:
            instructions.append({ "type": instruction.RELEASE, "allocation": alloc })
        
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
