import copy, os, itertools, logging, uuid, time

from collections import defaultdict
from lace.logging import trace
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

from idms import settings
from idms.lib.thread import ThreadManager
from idms.lib.assertions.exceptions import SatisfactionError

log = logging.getLogger('idms.db')
@trace("idms.db")
class DBLayer(object):
    def __init__(self, runtime, depots, viz):
        self._rt, self._depots, self._viz = runtime, (depots or {}), viz 
        self._workers, self._proxy = ThreadManager(), IBPManager()
        self._local_files, self._active = [], []
        self._lock, self._flock, self._enroute = RLock(), RLock(), set()

    def _viz_register(self, name, size):
        print(self._viz)
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

    def move_files(self, exnode, dst=None, ttl=None):
        log.info("Moving {}->{}".format(exnode.id, dst.name))
        def _job(exnode, dst, ttl):
            def valid(x):
                if not hasattr(x, 'depot'): x.depot = Depot(x.location)
                try: return self._proxy.probe(x)
                except Exception as e:
                    log.warn("Failed to connect with allocation - " + x.location)
                    return False

            remote = None
            try:
                dst = self._rt.services.where({'accessPoint': dst}) if isinstance(dst, str) else dst
                self._rt.addSources([{'url': dst.unis_url, 'enabled': True}])
                try: UnisClient.get_uuid(dst.unis_url)
                except: raise SatisfactionError("Failed to connect to remote")
                remote = self._rt.exnodes.first_where({'replica_of': exnode, 'service': dst})
                if not remote:
                    remote = exnode.clone()
                    remote.extents = []
                    remote.extendSchema('replica_of', exnode)
                    remote.extendSchema('service', dst)
                    self._rt.insert(remote, commit=True, publish_to=dst.unis_url)

                sock = self._viz_register(exnode.name, exnode.size)
                log.debug("--Creating allocation list")
                chunks = list(sorted([x for x in exnode.extents.where(valid)], key=lambda x: x.offset))
                ready = list(sorted([x for x in remote.extents.where(valid)], key=lambda x: x.offset))
                
                log.debug("--Checking exnode validity")
                i, excess = 0, []
                for alloc in chunks:
                    if alloc.offset > i: raise SatisfactionError("Incomplete file, cannot transfer")
                    i = alloc.offset + alloc.size
                    for other in ready:
                        if other.offset <= alloc.offset and other.size + other.offset >= alloc.size + alloc.offset:
                            excess.append(alloc)
                    ready.append(alloc)
                if i < exnode.size: raise SatisfactionError("Incomplete file, too short")
                
                log.debug("--Removing excess allocations")
                for alloc in excess:
                    try: chunks.remove(alloc)
                    except ValueError: pass
                
                if chunks:
                    remote._rt_live, exnode._rt_live = False, False
                    for chunk in chunks:
                        log.debug("--Transferring allocation [{}-{}]".format(chunk.offset, chunk.offset + chunk.size))
                        alloc = self._proxy.allocate(Depot(dst.accessPoint), 0, chunk.size)
                        alloc.offset = chunk.offset
                        exnode.extents.append(alloc)
                        self._proxy.send(chunk, alloc)
                        self._viz_progress(sock, alloc.location, alloc.size, alloc.offset)
                        replica = alloc.clone()
                        del alloc.getObject().__dict__['function']
                        del replica.getObject().__dict__['function']
                        alloc.parent = exnode
                        replica.parent = remote
                        remote.extents.append(replica)
                        self._rt.insert(alloc, commit=True)
                        self._rt.insert(replica, commit=True, publish_to=dst.unis_url)
                    if not hasattr(dst, 'new_exnodes'): dst.extendSchema('new_exnodes', [])
                    dst.new_exnodes.append(remote)
                    dst.status = "UPDATE"
                    self._rt._update(remote)
                    self._rt._update(exnode)
                    with self._flock:
                        self._rt.flush()
            finally:
                if remote: remote._rt_live = True
                if exnode: exnode._rt_live = True
                with self._lock:
                    log.debug("Removing lock on - {}".format(exnode.name))
                    self._enroute.remove(exnode)
                    
        with self._lock: self._enroute.add(exnode)
        self._workers.add_job(_job, exnode, dst, ttl)

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