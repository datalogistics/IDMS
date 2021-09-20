import typing

from idms.lib import assertions
from idms.lib.assertions.abstract import AbstractAssertion
from idms.lib.assertions.exceptions import SatisfactionError, AssertionError as SatisfactionWarning

class Disjunction(AbstractAssertion):
    """
    Ensures one of several policies holds (in order of appearance)
    """
    tag = "$or"
    def initialize(self, policies:typing.List[AbstractAssertion]):
        self._ls = [assertions.factory(p) for p in policies]

    def apply(self, exnode, runtime):
        warn = []
        for policy in self._ls:
            try:
                return policy.apply(exnode, runtime)
            except SatisfactionError as exp:
                msg = "{} failed - {}".format(policy.tag, exp)
                self.log.warn(msg)
                continue
            except SatisfactionWarning as exp:
                warn.append(str(exp))
        if warn:
            raise SatisfactionWarning(", ".join(warn))
        raise SatisfactionError("Disjunction failed, no satisfiable policies")
