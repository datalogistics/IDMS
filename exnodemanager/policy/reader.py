
import json

from composite import CompositePolicy
from policy import Policy

import exnodemanager.record as record


class PolicyReader(Policy):
    def __init__(self, config, **kwargs):
        self._file = config
        self._kwargs = kwargs
        
    def _apply(self, exnode):
        instructions = []
        
        with open(self._file) as policies:
            try:
                tmpPolicies = json.loads(policies)
                composite = CompositePolicy(tmpPolicies, **self._kwargs)
                instructions = composite.GetPendingChanges(exnode)
            except Exception as exp:
                return []

        return instructions
