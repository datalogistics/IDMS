from unis.exceptions import UnisReferenceError
from unis.services import RuntimeService
from unis.services.event import new_update_event
from unis.models import Service

class IDMSService(RuntimeService):
    def __init__(self, dblayer, master):
        self._db = dblayer
        self._master = master
        self._known = {}

    @new_update_event(["services", "exnodes"])
    def new(self, resource):
        try:
            if resource.getSource() != self._master: return
        except UnisReferenceError: return
        
        if isinstance(resource, Service):
            if resource.id not in self._known or resource.ts - self._known[resource.id] > getattr(resource, 'ttl', 0):
                self._db.update_policies(resource)
            self._known[resource.id] = getattr(resource, 'ttl', 0)
        else:
            self._db.update_policies(resource)
