from idms.lib.assertions.exceptions import SatisfactionError
from unis.rest import UnisClient

def post(rt, new_allocs, old_allocs, dst, ttl):
    remotes = []
    # get/generate remote
    rt.addSources([{'url': dst.unis_url, 'enabled': True}])
    try: UnisClient.get_uuid(dst.unis_url)
    except: raise SatisfactionError("Failed to connect to remote")
    for alloc in new_allocs:
        remote = rt.exnodes.first_where({'replica_of': alloc.parent, 'service': dst})
        if not remote:
            remote = alloc.parent.clone()
            remotes.append(remote)
            remote.extents = []
            remote.extendSchema('replica_of', alloc.parent)
            remote.extendSchema('service', dst)
            rt.insert(remote, commit=True, publish_to=dst.unis_url)
            
            # duplicate new_allocs into remote
            remote._rt_live = False
            replica = alloc.clone()
            del replica.getObject().__dict__['function']
            replica.parent = remote
            remote.extents.append(replica)
            rt.insert(replica, commit=True, publish_to=dst.unis_url)
    
    # set remote to updated
    if not hasattr(dst, 'new_exnodes'): dst.extendSchema('new_exnodes', [])
    for r in remotes:
        r._rt_live = True
        rt._update(r)
        dst.new_exnodes.append(r)
    dst.status = "UPDATE"
    rt._update(dst)
