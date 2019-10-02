from idms.lib.assertions.abstract import AbstractAssertion
from idms.lib.assertions.exceptions import SatisfactionError
from idms.lib.utils import ExnodeInfo

class MaxFlow(AbstractAssertion):
    tag = "$maxflow"
    def initialize(self, dest:str, ttl:int=180000):
        self._dest = dest
        self._ttl = ttl
    
    def apply(self, ex, db):
        if isinstance(self._dest, str):
            try:
                self._dest = next(db._rt.nodes.where({'name': self._dest}))
            except StopIteration:
                raise SatisfactionError("Currently unknown destination - {}".format(self._dest))
        
        info = ExnodeInfo(ex, remote_validate=True)

        raise NotImplementedError()
        
        return not info.is_complete()
