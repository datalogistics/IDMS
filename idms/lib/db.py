import copy, os, itertools, logging

from collections import defaultdict
from lace.logging import trace
from libdlt.sessions import Session
from libdlt.schedule import BaseUploadSchedule
from threading import Lock
from unis.models import Exnode

from idms import settings
from idms.lib.thread import ThreadManager

class ForceUpload(BaseUploadSchedule):
    def __init__(self, sources):
        self._used = defaultdict(list)
        self._alt_ls = itertools.cycle(sources)
        self._len = len(sources)
    def get(self, ctx):
        misses = 0
        for depot in self._alt_ls:
            misses += 1
            if misses > self._len:
                raise IndexError
            elif depot not in self._used[ctx['offset']]:
                self._used[ctx['offset']].append(depot)
                return depot

log = logging.getLogger('idms.db')
@trace("idms.db")
class DBLayer(object):
    def __init__(self, runtime, depots, viz):
        self._rt, self._depots, self._viz = runtime, (depots or {}), viz 
        self._workers = ThreadManager()
        self._local_files, self._active = [], []
        self._enroute = set()
        self._lock = Lock()
        
    def move_files(self, exnode, dst=None, ttl=None):
        def _job(exnode, dst, ttl):
            with self._lock: self._enroute.add((exnode, dst))
            depots = self.get_depot_list()
            with Session(self._rt, depots=depots, viz_url=self._viz) as sess:
                if exnode.selfRef not in self._local_files:
                    sess.download(exnode.selfRef, os.path.join(settings.CACHE_DIR, exnode.id))
                    self._local_files.append(exnode.selfRef)
            if dst:
                if not hasattr(dst, 'new_exnodes'): dst.new_exnodes = []
                dst = next(self._rt_services.where({'accessPoint': dst})) if isinstance(dst, str) else dst
                
                with Session(dst.unis_url, depots=depots, bs=settings.BS, viz_url=self._viz) as sess:
                    result = sess.upload(os.path.join(settings.CACHE_DIR, exnode.id), exnode.name,
                                         copies=1, schedule=ForceUpload([dst.accessPoint]),duration=ttl)
                    dst.new_exnodes.append(result.exnode)
                    for alloc in result.exnode.extents:
                        new_alloc = self._rt.insert(alloc.clone(), commit=True)
                        new_alloc.parent = exnode
                        del new_alloc.getObject().__dict__['function']
                        exnode.extents.append(new_alloc)
                dst.status = "UPDATE"
                self._rt.flush()
            with self._lock: self._enroute.remove((exnode, dst))

        with self._lock:
            if (exnode, dst) not in self._enroute: self._workers.add_job(_job, exnode, dst, ttl)

    def register_policy(self, policy):
        if any([p.to_JSON() == policy.to_JSON() for p in self._active]): return
        self._active.append(policy)
        [policy.watch(ex) for ex in self._rt.exnodes.where(lambda x: policy.match(x))]
        policy.apply(self)

    def get_active_policies(self, exnode=None):
        for p in self._active:
            if (not exnode) or p.match(exnode): yield p

    def update_policies(self, resource):
        if isinstance(resource, Exnode):
            log.info("Modified file detected, evaluating policies...")
            matches = [p for p in self._active if p.match(resource)]
            [p.watch(resource) for p in matches]
            [p.apply(self) for p in matches]
        else:
            log.info("Topology changed, evaluating policies...")
            [p.apply(self) for p in self._active]

    def get_policies(self):
        ferries = self._rt.services.where({"serviceType": "datalogistics:wdln:ferry"})
        return [{"ferry_name": ferry.name, "data_lifetime": 1080000} for ferry in ferries]

    def get_depots(self):
        return  list(self._rt.services.where({"serviceType": "datalogistics:wdln:ferry"})) + \
            list(self._rt.services.where({"serviceType": "datalogistics:wdln:base"}))
    
    def get_depot_list(self):
        return {**copy.deepcopy(self._depots),
                **{f.accessPoint: {"enabled": True} for f in self.get_depots()},
                **{f.accessPoint: {"enabled": True} for f in self._rt.services.where({"serviceType": "ibp_server"})}}
    
    def manage_depots(self):
        service = self._rt.services.first_where({'id': ref})
        [setattr(service, k, v) for k,v in services.items()]
        self._rt.flush()
