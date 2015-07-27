


class Policy(object):
    def __init__(self):
        self._filters = []

    def GetPendingChanges(self, exnode):
        for _filter in self._filters:
            if not _filter.Apply(exnode):
                return []
        
        return self._apply(exnode)
        
        
    def AddFilter(self, new_filter):
        self._filters.append(new_filter)



    def _apply(self, exnode):
        return []
