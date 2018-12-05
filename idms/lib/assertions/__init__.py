import importlib

from idms.lib.assertions.exceptions import AssertionError

_builtin = [
    "idms.lib.assertions.and.Conjunction",
    "idms.lib.assertions.or.Disjunction",
    "idms.lib.assertions.exact.Exact",
    "idms.lib.assertions.geofense.GeoFense",
    "idms.lib.assertions.replicate.Replicate"
]

assertions = {}
def register(assertion):
    if isinstance(assertion, str):
        path = assertion.split(".")
        module = importlib.import_module(".".join(path[:-1]))
        assertion = getattr(module, path[-1])
    assertions[assertion.tag] = assertion

def factory(desc):
    if "$or" in desc:
        return assertions["$or"]({"policies":desc["$or"]})
    if "$and" in desc:
        return assertions["$and"]({"policies":desc["$and"]})
    if "$type" not in desc or "$args" not in desc or desc["$type"] not in assertions:
        raise AssertionError("Bad assertion type - {}".format(desc[0]))
    return assertions[desc["$type"]](desc["$args"])

for path in _builtin:
    register(path)
