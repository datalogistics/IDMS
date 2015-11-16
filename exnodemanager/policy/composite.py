
import sys

from policy import Policy
import exnodemanager.record as record

class CompositePolicy(Policy):
    def __init__(self, policies, **kwargs):
        super(CompositePolicy, self).__init__()
        self._log = record.getLogger()
        self._policies = []
        tmpPolicies = []
        self._log.info("--Building Composite Policy")
        
        for policy in policies:
            try:
                self._log.info("  Creating policy: {policy}".format(policy = policy["class"]))
                policy_class = self.get_class(policy["class"])

                policy["args"].update(kwargs)
                tmpPolicy = policy_class(**policy["args"])

                if "priority" in policy:
                    tmpPolicy.priority = priority
                for _filter in policy["filters"]:
                    self._log.info("    Adding filter: {filt}".format(filt = _filter["class"]))
                    filter_class = self.get_class(_filter["class"])
                    
                    tmpFilter = filter_class(**_filter["args"])
                    tmpPolicy.AddFilter(tmpFilter)
            
                tmpPolicies.append(tmpPolicy)
            except Exception as exp:
                self._log.warn("app.build_policies: Could not parse policies - {exp}".format(exp = exp))
        
        self._policies = sorted(tmpPolicies, key = lambda policy: policy.priority, reverse = True)
        self._log.info("--Composite Complete")


    def get_class(self, classname):
        try:
            self._log.info("  Importing class: {classname}".format(classname = classname))
            module, name = classname.rsplit('.', 1)
            __import__(module)
            tmpClass = getattr(sys.modules[module], name)
            self._log.info("  Added {classname}".format(classname = tmpClass))
        except Exception as exp:
            self._log.warn("app.get_class: Invalid class name - {exp}".format(exp = exp))
            return None

        return tmpClass
    
    
    def _apply(self, exnode):
        instructions = []

        for policy in self._policies:
            instructions.extend(policy.GetPendingChanges(exnode))

        return instructions
