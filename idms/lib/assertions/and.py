import typing

from idms.lib import assertions
from idms.lib.assertions.abstract import AbstractAssertion
from idms.lib.assertions.exceptions import AssertionError as SatisfactionWarning

class Conjunction(AbstractAssertion):
    """
    Enforces several policies on matching data
    """
    tag = "$and"
    def initialize(self, policies:typing.List[AbstractAssertion]):
        self._ls = [assertions.factory(p) for p in policies]

    def apply(self, exnode, runtime):
        warn = []
        complete = True
        for policy in self._ls:
            try: complete &= policy.apply(exnode, runtime)
            except SatisfactionWarning as e:
                warn.append(str(e))

        if warn:
            raise SatisfactionWarning(", ".join(warn))
        return complete
