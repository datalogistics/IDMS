import falcon, json, time, mimetypes

from threading import Thread
from queue import Queue

from collections import defaultdict
from itertools import cycle
from unis.models import Exnode, Extent
from unis.exceptions import CollectionIndexError
from lace import logging

from libdlt.file import DLTFile
from libdlt.protocol.ibp.services import ProtocolService as IBPManager
from libdlt.protocol.exceptions import AllocationError
from libdlt.depot import Depot
from idms.handlers.base import _BaseHandler
from idms.handlers.utils import get_body
from libdlt.util.files import ExnodeInfo

_proxy = IBPManager()
CR, LF = ord('\r'), ord('\n')
PREAMBLE, HEADERS, BODY, COMPLETE = list(range(4))
log = logging.getLogger("idms.handler.file")
class FileHandler(_BaseHandler):
    def _folder(self, root, s):
        result = []
        for e in s(lambda x: x.parent == root and not hasattr(x, 'replica_of')):
            result.append({'id': e.id, 'mode': e.mode,
                           'name': e.name, 'content': self._folder(e, s)})
        return result

    @falcon.after(_BaseHandler.encode_response)
    def on_get(self, req, resp):
        resp.text = self._folder(None, self._db._rt.exnodes.where)
        resp.status = falcon.HTTP_200

    @falcon.after(_BaseHandler.encode_response)
    def on_post(self, req, resp):
        form = req.get_media()
        for part in form:
            if part.filename:
                ex = Exnode({'name': part.secure_filename, 'mode': 'file',
                           'created': int(time.time() * 1000000),
                           'modified': int(time.time() * 1000000),
                           'parent': None,
                           'owner': 'idms',
                           'group': 'idms',
                           'permission': '744'})
                try:
                    with DLTFile(ex, "w", bs=self._conf['upload']['blocksize'],
                                 dest=self._conf['upload']['staging']) as dest:
                        part.stream.pipe(dest)
                except AllocationError as exp:
                    resp.status = falcon.HTTP_500
                    resp.text = {"errorcode": 1, "msg": esp.msg}
                self._db._rt.insert(ex, commit=True)
                for a in ex.extents:
                    self._db._rt.insert(a, commit=True)
                self._db._rt.flush()
        resp.status = falcon.HTTP_201
        resp.text = self._folder(None, self._db._rt.exnodes.where)

    @get_body
    @falcon.after(_BaseHandler.encode_response)
    def on_put(self, req, resp, body):
        log.info(f"Reordering file {body['file']} ~> {body['target']}")
        try:
            target = self._db._rt.exnodes.first_where(lambda x: x.id == body['target'])
        except CollectionIndexError:
            target = None
        try:
            to_move = self._db._rt.exnodes.first_where(lambda x: x.id == body['file'])
        except CollectionIndexError:
            to_move = None

        log.debug(f"--Files {to_move} ~> {target}")
        if target and to_move and target != to_move:
            to_move.parent = target
        elif to_move:
            to_move.parent = None
        self._db._rt.flush()
        resp.text = self._folder(None, self._db._rt.exnodes.where)
        resp.status = falcon.HTTP_200

    @falcon.after(_BaseHandler.encode_response)
    def on_delete(self, req, resp):
        def valid(ex):
            return ExnodeInfo(ex, True).is_complete(self._conf['upload']['staging'])
        remove = list(self._db._rt.exnodes.where(lambda x: x.mode == "file" and not valid(x)))
        [self._db._rt.delete(ex) for ex in remove]

        resp.text = self._folder(None, self._db._rt.exnodes.where)
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
        resp.text = self._folder(None, self._db._rt.exnodes.where)
        resp.status = falcon.HTTP_200

class DownloadHandler(_BaseHandler):
    def on_get(self, req, resp, rid):
        ex = self._db._rt.exnodes.first_where({'id': rid})
        if not ex: raise falcon.HTTPBadRequest(description="Unknown exnode id")

        resp.downloadable_as = ex.name
        resp.content_length = ex.size
        resp.content_type = mimetypes.guess_type(ex.name)[0]

        with DLTFile(ex, "r") as stream:
            resp.stream = stream
