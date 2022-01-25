from libdlt.util.files import ExnodeInfo
from idms.lib.assertions.exceptions import SatisfactionError
from idms.plugins import Plugin
from idms.lib.thread import ThreadManager
from unis.rest import UnisClient
from unis.services import RuntimeService
from unis.services.event import update_event

from lace import logging

class WDLNService(RuntimeService):
    def __init__(self, db, stage):
        self.db = db
        self.stage = db.get_depots(stage)
        self.log = logging.getLogger('idms.wdln')

    @update_event("services")
    def check_uploads(self, service):
        if self.stage and hasattr(service, "uploaded_exnodes"):
            for ex in service.uploaded_exnodes:
                self.log.info("Importing file - {}".format(ex.name))
                if not len(ex.extents): continue

                ThreadManager.wait_for(None, self.db.move_allocs(ex.extents, self.stage[0], skip_pp=True))
                local = ex.clone()
                local.extents = []
                self.rt.insert(local, commit=True)
                local._rt_live = False
                for alloc in ex.extents:
                    alloc = alloc.clone()
                    try: del alloc.getObject().__dict__['function']
                    except KeyError: pass
                    alloc.parent = local
                    local.extents.append(alloc)
                    self.rt.insert(alloc, commit=True)

                ex.extendSchema('replica_of', local)
                ex.extendSchema('service', service)

                local._rt_live = True
            service.uploaded_exnodes = []
            self.db._rt._update(service)

class WDLNPlugin(Plugin):
    def __init__(self, db, conf):
        super().__init__(db, conf)
        self.rt.addService(WDLNService(db, conf['upload']['staging']))

    def postprocess(self, new_alloc, old_alloc, dst, ttl):
        self.rt.addSources([{'url': dst.unis_url, 'enabled': True}])
        try: UnisClient.get_uuid(dst.unis_url)
        except: raise SatisfactionError("Failed to connect to remote")

        remote = self.rt.exnodes.first_where({'replica_of': new_alloc.parent, 'service': dst})
        if not remote:
            remote = new_alloc.parent.clone()
            remote.extents = []
            remote.extendSchema('replica_of', new_alloc.parent)
            remote.extendSchema('service', dst)
            self.rt.insert(remote, commit=True, publish_to=dst.unis_url)
            remote._rt_live = False

        replica = new_alloc.clone()
        try: del replica.getObject().__dict__['function']
        except KeyError: pass
        replica.parent = remote
        remote.extents.append(replica)
        self.rt.insert(replica, commit=True, publish_to=dst.unis_url)

        if not hasattr(dst, 'new_exnodes'): dst.extendSchema('new_exnodes', [])
        remote._rt_live = True
        self.rt._update(remote)
        if remote not in dst.new_exnodes and ExnodeInfo(remote).is_complete():
            dst.new_exnodes.append(remote)
            dst.status = "UPDATE"
        self.rt._update(dst)
