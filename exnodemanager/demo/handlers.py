import tornado
import tornado.web
import tornado.websocket
import tornado.gen
import tornado.escape
import tornadoredis
from netlogger import nllog

import json
import exnodemanager.record as record

class PolicyHandler(tornado.web.RequestHandler, nllog.DoesLogging):
    def __init__(self, *args, **kwargs):
        nllog.DoesLogging.__init__(self)
        super(PolicyHandler, self).__init__(*args, **kwargs)
    
    @tornado.web.asynchronous
    @tornado.web.removeslash
    def post(self):
        self.set_header("Content-type", "text/plain")
        try:
            policy = json.loads(self.request.body)
            self.application.check_exnodes(policy)
        except ValueError as exp:
            self.log.error("Could not parse JSON - {exp}".format(exp = exp))
            
        self.finish()
        
    def set_default_headers(self):
        self.set_header('Access-Control-Allow-Origin', '*')
        self.set_header('Access-Control-Allow-Headers', 'x-requested-with')


class StateHandler(tornado.web.RequestHandler, nllog.DoesLogging):
    def __init__(self, *args, **kwargs):
        nllog.DoesLogging.__init__(self)
        super(StateHandler, self).__init__(*args, **kwargs)

    def get(self):
        self.write(str(len(self.application._exnodes)))
        self.write('[\n')
        for key in self.application._exnodes:
            exnode = self.application._exnodes[key]
            self.write('  {\n')
            self.write('    id: {eid},\n'.format(eid = exnode["id"]))
            self.write('    allocations: {\n')
            for alloc_key in exnode["allocations"]:
                alloc = exnode["allocations"][alloc_key]
                tmpMeta = alloc.GetMetadata()
                self.write('      {aid}:'.format(aid = tmpMeta.Id))
                self.write('      {\n')
                self.write('        id: {aid}\n'.format(aid = tmpMeta.Id))
                self.write('        ts: {time}\n'.format(time = tmpMeta.timestamp))
                self.write('        host: {host}\n'.format(host = tmpMeta.host))
                self.write('        port: {port}\n'.format(port = tmpMeta.port))
                self.write('        start: {start}\n'.format(start = tmpMeta.start))
                self.write('        end: {end}\n'.format(end = tmpMeta.end))
                self.write('        offset: {offset}\n'.format(offset = tmpMeta.offset))
                self.write('        size: {size}\n'.format(size = tmpMeta.realSize))
                self.write('      }\n')
            self.write('    }\n')
            self.write('  },\n')
            
        self.write(']\n')
        self.finish()

    @tornado.web.asynchronous
    @tornado.web.removeslash
    def post(self):
        self.application.restartDB()

    def set_default_headers(self):
        self.set_header('Access-Control-Allow-Origin', '*')
        self.set_header('Access-Control-Allow-Headers', 'x-requested-with')

class SubscriptionHandler(tornado.websocket.WebSocketHandler, nllog.DoesLogging):
    def __init__(self, *args, **kwargs):
        nllog.DoesLogging.__init__(self)
        super(SubscriptionHandler, self).__init__(*args, **kwargs)
    
    def open(self):
        self.log.info("New subscription connected")
        self.client = tornadoredis.Client()
        self.client.connect()
        self.listen()
        
        
    @tornado.gen.engine
    def listen(self):
        yield tornado.gen.Task(self.client.subscribe, "out")
        
        self.client.listen(self.deliver)

    def deliver(self, msg):
        if msg.kind == 'message':
            self.write_message(str(msg.body))
        if msg.kind == 'disconnect':
            self.write_message('This connection terminated '
                               'due to a Redis server error.')
            self.close()


    def on_close(self):
        self.log.info("Closing remote subscription")
        if self.client and self.client.subscribed:
            self.client.unsubscribe("out")
        self.client.disconnect()
        
    def check_origin(self, origin):
        return True
