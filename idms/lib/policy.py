import re, enum, logging, copy, uuid

from collections import namedtuple
from unis.models import Exnode, Service
from threading import Lock

from idms.lib import assertions
from idms.lib.assertions.exceptions import SatisfactionError, AssertionError as SatisfactionWarning

class Status(enum.Enum):
    INACTIVE = 0
    ACTIVE = 1
    BAD = 2
    INPROG = 3

log = logging.getLogger('idms.policy')
class Policy(object):
    def __init__(self, subject, verb):
        self.error = []
        self.desc = subject
        self.verb = assertions.factory(verb)
        self._watch, self._lock = set(), Lock()
        self._broken, self.status = [], Status.INACTIVE
        self.uid = uuid.uuid4()

    def apply(self, db):
        changed = False
        self.error = []
        with self._lock: watch = copy.copy(self._watch)
        for exnode in watch:
            log.debug(["{}\n".format(e.name) for e in db._enroute])
            if exnode in db._enroute: continue
            try:
                changed = self.verb.apply(exnode, db)
                try: self._broken.remove(exnode)
                except: continue
            except SatisfactionError as e:
                log.warn(str(e))
                self.error.append(str(e))
                self.status = Status.BAD
                self._broken.append(exnode)
            except SatisfactionWarning as e:
                self.status = Status.INACTIVE
                self.error.append(str(e))
                return
        if watch and not self._broken:
            self.error = []
            self.status = Status.INPROG if changed else Status.ACTIVE
        
    def watch(self, exnode):
        with self._lock: self._watch.add(exnode)
        self.status = self.status or Status.ACTIVE
    def match(self, exnode):
        # Logical ops
        def _and(n,v):
            return all(_comp(k, n) for k in v)
        def _or(n,v):
            return any(_comp(k, n) for k in v)
        def _not(n,v):
            return not _comp(k, n)
        
        # Comparitors
        def _regex(n,v):
            test = getattr(exnode, n, None)
            v = v if v[-1] == "$" else (v + "$")
            return bool(re.match(v, test))
        def _in(n,v):
            test = getattr(exnode, n, None)
            return test is not None and test in v
        def _gt(n,v):
            test = getattr(exnode, n, None)
            return test is not None and test > v
        def _gte(n,v):
            test = getattr(exnode, n, None)
            return test is not None and test >= v
        def _lt(n,v):
            test = getattr(exnode, n, None)
            return test is not None and test < v
        def _lte(n,v):
            test = getattr(exnode, n, None)
            return test is not None and test <= v
        def _eq(n,v):
            test = getattr(exnode, n, None)
            return test is not None and test == v
        def _comp(v, ctx):
            lmap = {"$or": _or, "$and": _and, "$not": _not}
            cmap = {"$regex": _regex, "$in": _in, "$gt": _gt, "$lt": _lt, "$gte": _gte, "$lte": _lte, "$eq": _eq}
            result = True
            for n,v in v.items():
                fn = lmap.get(n, cmap.get(n, None))
                if fn:
                    result &= fn(ctx,v)
                else:
                    if isinstance(v, dict):
                        result &= _comp(v, n)
                    else:
                        test = getattr(exnode, n, None)
                        result &= test is not None and test == v
            return result
        
        return _comp(self.desc, None)
    def to_JSON(self):
        return {"description": self.desc,
                "policy": self.verb.to_JSON(),
                "status": self.status.name,
                "err": ", ".join(self.error),
                "id": str(self.uid)}
