import logging

from collections import defaultdict

log = logging.getLogger('idms.utils')
class ExnodeInfo(object):
    def __init__(self, ex):
        class _view(object):
            def __init__(self): self._size, self._chunks = ex.size, [[0,0]]
            def fill(self, o, s):
                self._chunks = list(sorted(self._chunks + [[o,o+s]]))
                new_chunks = [[0,0]]
                for chunk in self._chunks:
                    if chunk[0] <= new_chunks[-1][1]: new_chunks[-1][1] = chunk[1]
                    else: new_chunks.append(chunk)
                self._chunks = new_chunks
            @property
            def is_complete(self):
                return self._chunks[0][-1] == self._size
            @property
            def missing(self):
                return [[self._chunks[i][1], self._chunks[i][1] - self._chunks[i+1][0]] for i in range(len(self._chunks) - 1)]

        self._views = defaultdict(_view)
        for e in ex.extents:
            try: self._views[e.location].fill(e.offset, e.size)
            except Exception as exp: log.warn("Bad extent - {}".format(e.id))

    @property
    def views(self):
        return self._views.keys()
    
    def is_complete(self, view=None):
        if view: return view in self._views and self._views[view].is_complete
        else: return any([v.is_complete for v in self._views.values()])
