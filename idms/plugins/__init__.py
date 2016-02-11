
class Plugin(object):
    def __init__(self, db, conf):
        self.db = db
        self.rt = db._rt
        self.conf = conf

    def postprocess(self, new_allocs, old_allocs, dst, ttl):
        pass
