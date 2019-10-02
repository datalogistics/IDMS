import typing

from idms.lib import assertions
from idms.lib.assertions.abstract import AbstractAssertion

class Conjunction(AbstractAssertion):
    """
    Enforces several policies on matching data
    """
    tag = "$and"
    def initialize(self, policies:typing.List[AbstractAssertion]):
        self._ls = [assertions.factory(p) for p in policies]

    def apply(self, exnode, runtime):
        change = False
        for policy in self._ls:
            change |= policy.apply(exnode, runtime)
        return change
