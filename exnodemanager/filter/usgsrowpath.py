
import exnodemanager.record as record

class RowPath(object):
    def __init__(self, startPath, endPath, startRow, endRow):
        self._log = record.getLogger()
        self._startPath = startPath
        self._endPath   = endPath
        self._startRow  = startRow
        self._endRow    = endRow

    def Apply(self, exnode):
        exnode = exnode["raw"]
        try:
            filename = exnode["name"]
            path = int(filename[3:6])
            row  = int(filename[6:9])
            
            if self._startPath < path and path < self._endPath and self._startRow < row and path < self._endRow:
                return True
            else:
                return False
        except Exception as exp:
            self._log.warn("Unable to parse exnode - {exp}".format(exp = exp))
            return False
