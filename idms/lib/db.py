import copy, os, itertools, logging, uuid, time

from collections import defaultdict
from lace.logging import trace
from libdlt.protocol.ibp.exceptions import IBPError
from libdlt.protocol.ibp.services import ProtocolService as IBPManager
from libdlt.depot import Depot
from libdlt.sessions import Session
from libdlt.schedule import BaseUploadSchedule
from socketIO_client import SocketIO
from threading import RLock
from uritools import urisplit
from unis import Runtime
from unis.models import Exnode
from unis.rest import UnisClient

from idms.lib.assertions.exceptions import SatisfactionError
from idms.lib.thread import ThreadManager

log = logging.getLogger('idms.db')
@trace("idms.db")
class DBLayer(object):
    def __init__(self, runtime, depots, conf):
        self._servicetypes = conf['servicetypes']
        self._rt, self._depots, self._viz = runtime, (depots or {}), conf['vizurl']
        self._workers = ThreadManager(conf['threads']['initialsize'], conf['threads']['max'])
        self._proxy = IBPManager()
        self._local_files, self._active = [], []
        self._lock, self._flock, self._enroute = RLock(), RLock(), set()
        self._plugins = []

    def _viz_register(self, name, size):
        if self._viz:
            try:
                uid, o = uuid.uuid4().hex, urisplit(self._viz)
                sock = SocketIO(o.host, o.port)
                msg = {"sessionId": uid, "filename": name, "size": size, "connections": 1,
                       "timestamp": time.time()*1e3}
                sock.emit('peri_download_register', msg)
                return uid, sock, name, size
            except Exception as e:
                log.warn(e)
        return None

    def _viz_progress(self, sock, depot, size, offset):
        if self._viz:
            try:
                d = Depot(depot)
                msg = {"sessionId": sock[0],
                       "host":  d.host, "length": size, "offset": offset,
                       "timestamp": time.time()*1e3}
                sock[1].emit('peri_download_pushdata', msg)
            except Exception as e: pass

    def add_post_process(self, cb):
        self._plugins.append(cb)

    def move_allocs(self, allocs, dst=None, ttl=None, skip_pp=False):
        log.info("Moving [{}] {}->{}".format(allocs[0].parent.id, ",".join([str(a.offset) for a in allocs]), dst.name))
        def _job(allocs, dst, ttl):
            socks = {}
            try:
                for a in allocs:
                    p = a.parent
                    if p not in socks:
                        p._rt_live = False
                        socks[p] = self._viz_register(p.name, p.size)
                
                dst = self._rt.services.where({'accessPoint': dst}) if isinstance(dst, str) else dst
                
                log.debug("--Removing excess allocations")
                for chunk in allocs:
                    try:
                        log.debug("--Transferring allocation [{}-{}]".format(chunk.offset, chunk.offset + chunk.size))
                        ex = chunk.parent
                        alloc = self._proxy.allocate(Depot(dst.accessPoint), 0, chunk.size, timeout=2)
                        alloc.offset = chunk.offset
                        self._proxy.send(chunk, alloc)
                        self._viz_progress(socks[ex], alloc.location, alloc.size, alloc.offset)
                        try: del alloc.getObject().__dict__['function']
                        except KeyError: pass
                        alloc.parent = ex

                        if not skip_pp: [p.postprocess(alloc, chunk, dst, ttl) for p in self._plugins]
                        
                        ex.extents.append(alloc)
                        self._rt.insert(alloc, commit=True)
                        self._rt._update(ex)
                        with self._flock:
                            self._rt.flush()
                    except (IBPError, SatisfactionError): pass
                with self._flock:
                    self._rt.flush()
            finally:
                [setattr(e, 'rt_live', True) for e in socks.keys()]
                with self._lock:
                    for a in allocs:
                        log.debug("Removing lock on - {}".format(a.parent.id))
                        self._enroute.remove(a.parent)

        with self._lock: self._enroute |= set([a.parent for a in allocs])
        return self._workers.add_job(_job, allocs, dst, ttl)

    def register_policy(self, policy):
        if any([p.to_JSON() == policy.to_JSON() for p in self._active]): return
        self._active.append(policy)
        [policy.watch(ex) for ex in self._rt.exnodes.where(lambda x: not hasattr(x, 'replica_of') and policy.match(x))]

        with self._lock:
            policy.apply(self)

    def get_active_policies(self, exnode=None):
        for p in self._active:
            if (not exnode) or p.match(exnode): yield p

    def update_policies(self, resource):
        with self._lock:
            if isinstance(resource, Exnode):
                log.info("Modified file detected, evaluating policies...")
                matches = [p for p in self._active if p.match(resource)]
                [p.watch(resource) for p in matches]
                [p.apply(self) for p in matches]
            else:
                log.info("Topology changed, evaluating policies...")
                [p.apply(self) for p in self._active]

    # DEPRECIATED
    def get_policies(self):
        ferries = self._rt.services.where({"serviceType": "datalogistics:wdln:ferry"})
        return [{"ferry_name": ferry.name, "data_lifetime": 1080000} for ferry in ferries]

    def get_depots(self, ap=None):
        if ap:
            return list(filter(lambda x: x, [self._rt.services.first_where({"accessPoint": ap})]))
        return sum([list(self._rt.services.where({"serviceType": s})) for s in self._servicetypes], [])
    
    def get_depot_list(self):
        return {**copy.deepcopy(self._depots),
                **{f.accessPoint: {"enabled": True} for f in self.get_depots()},
                **{f.accessPoint: {"enabled": True} for f in self._rt.services.where({"serviceType": "ibp_server"})}}
    
    def manage_depots(self):
        service = self._rt.services.first_where({'id': ref})
        [setattr(service, k, v) for k,v in services.items()]
        self._rt.flush()
