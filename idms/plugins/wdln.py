from idms.lib.assertions.exceptions import SatisfactionError

def post(rt, new_allocs, old_allocs, dst, ttl):
    remotes = []
    # get/generate remote
    rt.addSource({'url': dst.unis_url, 'enabled': True})
    try: UnisClient.get_uuid(dst.unis_url)
    except: raise SatisfactionError("Failed to connect to remote")
    for alloc in new_allocs:
        remote = rt.exnodes.first_where({'replica_of': alloc.parent, 'service': dst})
        remotes.append(remote)
        if not remote:
            remote = alloc.parent.clone()
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
    for r in remotes: r._rt_live = True
