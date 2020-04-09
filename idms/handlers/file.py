import falcon, json, time

from collections import defaultdict
from itertools import cycle
from unis.models import Exnode, Extent
from unis.exceptions import CollectionIndexError

from libdlt.protocol.ibp.services import ProtocolService as IBPManager
from libdlt.depot import Depot
from idms.handlers.base import _BaseHandler
from idms.handlers.utils import get_body

_proxy = IBPManager()
CR, LF = ord('\r'), ord('\n')
PREAMBLE, HEADERS, BODY, COMPLETE = list(range(4))
class FileHandler(_BaseHandler):
    def _folder(self, root, s):
        return [{'id': e.id, 'mode': e.mode,
                 'name': e.name, 'content': self._folder(e, s)} for e in s(lambda x: x.parent == root and not hasattr(x, 'replica_of'))]
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

    @falcon.after(_BaseHandler.encode_response)
    def on_post(self, req, resp):
        def _createfile(name):
            return Exnode({'name': name.decode('utf-8').strip('\"').strip('\''),
                           'mode': 'file',
                           'created': int(time.time() * 1000000),
                           'modified': int(time.time() * 1000000),
                           'parent': None,
                           'owner': 'idms',
                           'group': 'idms',
                           'permission': '744'})
        def _readblock(block, size, edge, length, read):
            newblock = bytearray(size)
            newblock[:edge] = block[-edge:]
            return req.stream.readinto(memoryview(newblock)[edge:min(size, edge + (length -read))]), newblock

        state, workers, exnodes, is_file = PREAMBLE, [], [], False
        payload, headers, params, name = defaultdict(list), None, None, None
        if req.content_length and req.content_type.startswith("multipart/form-data"):
            try:
                depots = self._db.get_depots(self._conf['upload']['staging'])
                if not depots:
                    resp.status = falcon.HTTP_500
                    resp.body = {"errorcode": 1, "msg": "No staging depot available"}
                    return
                else: dlen, depots = len(depots), cycle(depots)

                bound = bytes(req.content_type[req.content_type.find("boundary=")+9:], 'utf-8')
                boundlen = len(bound)
                overwrite, writesize = boundlen + 4, self._conf['upload']['blocksize']
                blocksize, filesize = writesize + overwrite, 0
                read, length = 0, req.content_length
                do_read, block = True, bytearray(blocksize)
                s = time.time()
                while True:
                    if do_read:
                        r, block = _readblock(block, blocksize, overwrite, length, read)
                        read += r
                
                    if state == BODY:
                        i = block.find(bound)
                        if i == -1 or i > writesize:
                            data = memoryview(block)[:len(block) - overwrite]
                            if is_file:
                                workers.append(self._db._workers.add_job(self._dispatch_block, data, filesize, exnodes[-1], depots, dlen))
                            else:
                                payload[name][-1]['content'] += data
                            filesize += len(data)
                            do_read = True
                            continue
                        blob = memoryview(block)[:i+boundlen+2]
                        data = blob[:i-4]
                        if blob[-2:] == b'--': state = COMPLETE
                        else: state, blob = HEADERS, blob[:-2]
                        if is_file:
                            workers.append(self._db._workers.add_job(self._dispatch_block, data, filesize, exnodes[-1], depots, dlen))
                        else:
                            payload[name][-1]['content'] += data
                        filesize += len(data)
                        r, block = _readblock(block, blocksize, len(block), length, read)
                        read += r
                        do_read = False
                    elif state == HEADERS:
                        state, j = BODY, block.find(b'\r\n\r\n')
                        if j == -1:
                            raise Exception("Header block does not close properly")
                        bstart = j + 4
                        raw, headers, params = block[:j], defaultdict(list), {}
                        for line in raw.split(b'\r\n'):
                            line = line.split(b':')
                            if len(line) == 2:
                                headers[line[0].decode('utf-8').strip()] = line[1]
                                if line[0] == b'Content-Disposition':
                                    for v in line[1].split(b';'):
                                        v = v.split(b'=')
                                        params[v[0].decode('utf-8').strip()] = v[1] if len(v) == 2 else b''
                        if 'filename' in params:
                            filesize, is_file = 0, True
                            exnodes.append(_createfile(params['filename']))
                        else: is_file = False
                        name = params['name'].decode('utf-8')
                        payload[name].append({'headers': headers, 'params': params, 'content': bytearray()})
                    
                        do_read, block = False, block[bstart:]
                    elif state == PREAMBLE:
                        i = block.find(bound)
                        if i == -1:
                            raise Exception("Preamble block does not close properly")
                        blob = memoryview(block)[:i+boundlen+2]
                        if blob[:-2] == b'--': sate = COMPLETE
                        else:
                            j = block.find(b'\r\n\r\n')
                            if j == -1:
                                raise Exception("Header block does not close properly")
                            bstart = j + 4
                            raw, headers, params = block[i+boundlen+2:j], defaultdict(list), {}
                            for line in raw.split(b'\r\n'):
                                line = line.split(b':')
                                if len(line) == 2:
                                    headers[line[0].decode('utf-8').strip().strip('\"')] = line[1]
                                    if line[0] == b'Content-Disposition':
                                        for v in line[1].split(b';'):
                                            v = v.split(b'=')
                                            params[v[0].decode('utf-8').strip().strip('\"')] = v[1] if len(v) == 2 else b''
                            if 'filename' in params:
                                is_file = True
                                exnodes.append(_createfile(params['filename']))
                            else: is_file = False
                            name = params['name'].decode('utf-8').strip('\"').strip("\'")
                            payload[name].append({'headers': headers, 'params': params, 'content': bytearray()})
                            
                            do_read, block = False, block[bstart:]
                            state = BODY
                    else:
                        break
                    if do_read and read >= length: break
            except Exception as e:
                import traceback
                traceback.print_exc()
                resp.status = falcon.HTTP_400
                resp.body = {"errorcode": 1, "msg": "Malformed multipart content"}
                return
            

            self._db._workers.wait_for(workers)
            parent = payload['parent'][0]['content'].decode('utf-8') if 'parent' in payload else None
            parent = self._db._rt.exnodes.first_where(lambda x: x.id == parent)
            for ex in exnodes:
                ex.parent = parent
                self._db._rt.insert(ex, commit=True)
                for alloc in ex.extents:
                    self._db._rt.insert(alloc, commit=True)
            ex.size = sum([e.size for e in ex.extents])
            self._db._rt.flush()
            resp.body = self._folder(None, self._db._rt.exnodes.where)
            resp.status = falcon.HTTP_200
            print((req.content_length / 1000000000 * 8) / (time.time() - s), "Gb/s")

    @get_body
    @falcon.after(_BaseHandler.encode_response)
    def on_put(self, req, resp, body):
        try:
            target = self._db._rt.exnodes.first_where({'id': body['target']})
        except CollectionIndexError:
            target = None
        try:
            to_move = self._db._rt.exnodes.first_where({'id': body['file']})
        except CollectionIndexError:
            to_move = None

        if target and to_move:
            to_move.parent = target
        elif to_move:
            to_move.parent = None
        self._db._rt.flush()
        resp.body = self._folder(None, self._db._rt.exnodes.where)
        resp.status = falcon.HTTP_200

class DirHandler(FileHandler):
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

class IBPReader(object):
    def __init__(self, exnode):
        self._size = exnode.size
        self._buffer, self._head, self._allocs = b"", 0, exnode.extents
        self._buffer = b""

    def read(self, size=None):
        print("Req:", size, len(self._buffer), self._size)
        def alive(a):
            if not hasattr(a, 'depot'): a.depot = Depot(a.location)
            try: return _proxy.probe(a)
            except: return False

        rbuf = self._buffer
        l = len(rbuf)

        if self._head == self._size: return b""
        if l == 0 or l < min(self._size - self._head, (size or 0)):
            allocs = [a for a in self._allocs if a.offset == self._head and alive(a)]
            if not allocs: return b""
            
            self._buffer += _proxy.load(allocs[0])
            l, rbuf = len(self._buffer), self._buffer

        if size is None: size = l
        data, self._buffer = rbuf[:size], rbuf[size:]
        self._head += size
        print(size, len(data), len(self._buffer))
        return data

class DownloadHandler(_BaseHandler):
    def on_get(self, req, resp, rid):
        exnode = self._db._rt.exnodes.first_where({'id': rid})
        if not exnode: raise falcon.HTTPBadRequest(description="Unknown exnode id")
        resp.downloadable_as = exnode.name

        resp.content_length = exnode.size
        resp.status = falcon.HTTP_200
        resp.stream = IBPReader(exnode)
