import falcon, json, time

from collections import defaultdict
from itertools import cycle
from unis.models import Exnode, Extent

from libdlt.protocol.ibp.services import ProtocolService as IBPManager
from libdlt.depot import Depot
from idms.handlers.base import _BaseHandler
from idms.handlers.utils import get_body
from idms.settings import UP_BLOCKSIZE

_proxy = IBPManager()
CR, LF = ord('\r'), ord('\n')
PREAMBLE, HEADERS, BODY, COMPLETE = list(range(4))
class FileHandler(_BaseHandler):
    def __init__(self, conf, dblayer, staging):
        super().__init__(conf, dblayer)
        self._stage = staging

    def _folder(self, root, s):
        return [{'id': e.id, 'mode': e.mode,
                 'name': e.name, 'content': self._folder(e, s)} for e in s({'parent': root})]
    @falcon.after(_BaseHandler.encode_response)
    def on_get(self, req, resp):
        resp.body = self._folder(None, self._db._rt.exnodes.where)
        resp.status = falcon.HTTP_200
    
    def _dispatch_block(self, block, offset, exnode, depots, dlen):
        if not exnode: return
        alloc = None
        for _ in range(dlen):
            try:
                d = Depot(next(depots).accessPoint)
                alloc = _proxy.allocate(d, offset, len(block))
                break
            except Exception as e:
                import traceback
                traceback.print_exc()
        if not alloc: raise Exception("Failed to connect to all depots")
        
        alloc.parent, alloc.offset = exnode, offset
        del alloc.getObject().__dict__['function']
        exnode.extents.append(alloc)
        _proxy.store(alloc, block, len(block))
        self._db._rt.insert(alloc, commit=True)

    @falcon.after(_BaseHandler.encode_response)
    def on_post(self, req, resp):
        depots = self._db.get_depots(self._stage)
        dlen = len(depots)
        if not depots:
            resp.status = falcon.HTTP_500
            resp.body = {"errorcode": 1, "msg": "No staging depot available"}
        else: depots = cycle(depots)

        try:
            if req.content_length and req.content_type.startswith("multipart/form-data"):
                exnodes, workers = [], []
                i, offset, is_file, line = 0, 0, False, bytearray(UP_BLOCKSIZE)
                bound = bytes(req.content_type[req.content_type.find("boundary=")+9:], 'utf-8')
                bound_offset = 0 - (len(bound) + 4)
                body_offset = bound_offset - 2
                length, read = req.content_length, req.stream.read
                lookingfor, state = CR, PREAMBLE
                ex, payload, headers, name = None, defaultdict(list), {}, ''
                for char in read(length):
                    try:
                        line[i], i = char, i + 1
                    except IndexError:
                        workers.append(self._db._workers.add_job(self._dispatch_block, line, offset, ex, depots, dlen))
                        offset, line, i = len(line) + offset, bytearray(UP_BLOCKSIZE), 1
                        line[0] = char
                    
                    if char == lookingfor:
                        if char == CR: lookingfor = LF
                        else:
                            lookingfor = CR
                            if state == PREAMBLE:
                                blob = memoryview(line)[i+bound_offset:i-2]
                                if blob[-2:] == b'--' and blob[:-2] == bound:
                                    i, state, headers = 0, COMPLETE, {}
                                elif blob[2:] == bound:
                                    i, state, headers = 0, HEADERS, {}

                            elif state == HEADERS:
                                if i == 2:
                                    args = headers.get('Content-Disposition', '').split(';')
                                    for i, arg in enumerate(args):
                                        v = arg.strip().split('=')
                                        k, v = v[0], v[1].strip('\"') if len(v) > 1 else ''
                                        args[i] = (k, v)
                                    args = dict(args)
                                    if 'name' not in args:
                                        raise Exception()
                                    payload[args['name']].append({'headers': headers, 'params': args,
                                                                  'content': None})
                                    state, name, offset = BODY, args['name'], 0
                                    if 'filename' in args:
                                        ex = Exnode({'name': args['filename'],
                                                     'mode': 'file',
                                                     'created': int(time.time() * 1000000),
                                                     'modified': int(time.time() * 1000000),
                                                     'parent': None,
                                                     'owner': 'idms',
                                                     'group': 'idms',
                                                     'permission': '744'})
                                        is_file, ex = True, self._db._rt.insert(ex, commit=True)
                                        exnodes.append(ex)
                                    else: is_file = False
                                else:
                                    ty, v = line[:i].split(b':', 1)
                                    headers[ty.decode('utf-8')] = v.decode('utf-8')
                                i = 0

                            elif state == BODY:
                                blob = memoryview(line)[i+bound_offset:i-2]
                                if blob[-2:] == b'--' and blob[:-2] == bound:
                                    state, block = COMPLETE, memoryview(line)[:i+(body_offset-2)]
                                    if is_file and block:
                                        workers.append(self._db._workers.add_job(self._dispatch_block, block, offset, ex, depots, dlen))
                                    i, line, headers = 0, bytearray(UP_BLOCKSIZE), {}
                                    payload[name][-1]['content'] = block
                                elif blob[2:] == bound:
                                    state, block = HEADERS, memoryview(line)[:i+body_offset]
                                    if is_file and block:
                                        workers.append(self._db._workers.add_job(self._dispatch_block, block, offset, ex, depots, dlen))
                                    i, line, headers = 0, bytearray(UP_BLOCKSIZE), {}
                                    payload[name][-1]['content'] = block
        except Exception as e:
            resp.status = falcon.HTTP_400
            resp.body = {"errorcode": 1, "msg": "Malformed multipart content"}

        self._db._workers.wait_for(workers)
        if 'parent' in payload and payload['parent'][0]['content'].tobytes():
            parent = self._db._rt.exnodes.first_where({'id': payload['parent'][0]['content'].tobytes().decode('utf-8')})
            for ex in exnodes: ex.parent = parent
        ex.size = sum([e.size for e in ex.extents])
        self._db._rt.flush()
        resp.body = self._folder(None, self._db._rt.exnodes.where)
        resp.status = falcon.HTTP_200

    @get_body
    @falcon.after(_BaseHandler.encode_response)
    def on_put(self, req, resp, body):
        target = self._db._rt.exnodes.first_where({'id': body['target']})
        to_move = self._db._rt.exnodes.first_where({'id': body['file']})

        if target and to_move:
            to_move.parent = target
            self._db._rt.flush()
        resp.body = self._folder(None, self._db._rt.exnodes.where)
        resp.status = falcon.HTTP_200

class DirHandler(FileHandler):
    def __init__(self, conf, dblayer): super().__init__(conf, dblayer, None)
    
    @get_body
    @falcon.after(_BaseHandler.encode_response)
    def on_post(self, req, resp, body):
        d = self._db._rt.insert(Exnode({'name': body['name'],
                                                     'mode': 'directory',
                                                     'created': int(time.time() * 1000000),
                                                     'modified': int(time.time() * 1000000),
                                                     'parent': None,
                                                     'owner': 'idms',
                                                     'group': 'idms',
                                                     'permission': '744'}), commit=True)
        self._db._rt.flush()
        resp.body = self._folder(None, self._db._rt.exnodes.where)
        resp.status = falcon.HTTP_200
