
import netlogger
import uuid
import time

import exnodemanager.protocol.ibp.factory
import datetime

from exnodemanager.protocol.ibp.allocation import Allocation

class DemoProxy(object):
    def GetExnodes(self):
        offset = self.get_offset(100)
        exnodes = [
            {
                "id": "1",
                "size": 10485760,
                "ts": 0,
                "extents":
                [
                    self.create_allocation("1", ("ibp2.crest.iu.edu", "6714"))
                ]
            },
            {
                "id": "2",
                "ts": 0,
                "size": 10485760,
                "extents":
                [
                    self.create_allocation("2", ("geoserv.ersc.wisc.edu", "6714"), duration = 45)
                ]
            },
            {
                "id": "3",
                "size": 10485760,
                "ts": 0,
                "extents":
                [
                    self.create_allocation("3", ("dresci.crest.iu.edu", "6714"))
                ]
            },
            {
                "id": "4",
                "size": 10485760,
                "ts": 0,
                "extents":
                [
                    self.create_allocation("4", ("192.70.161.87", "6714"), duration = 30)
                ]
            },
            {
                "id": "5",
                "size": 1048576000,
                "ts": 0,
                "extents":
                [
                    self.create_allocation("5", ("ibp2.crest.iu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("ibp2.crest.iu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("ibp2.crest.iu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("ibp2.crest.iu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("ibp2.crest.iu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("ibp2.crest.iu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("ibp2.crest.iu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("ibp2.crest.iu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("ibp2.crest.iu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("ibp2.crest.iu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("ibp2.crest.iu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("ibp2.crest.iu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("ibp2.crest.iu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("ibp2.crest.iu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("ibp2.crest.iu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("ibp2.crest.iu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("ibp2.crest.iu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("ibp2.crest.iu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("ibp2.crest.iu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("ibp2.crest.iu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("ibp2.crest.iu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("ibp2.crest.iu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("ibp2.crest.iu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("ibp2.crest.iu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("ibp2.crest.iu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("192.70.161.60", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("192.70.161.60", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("192.70.161.60", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("192.70.161.60", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("192.70.161.60", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("192.70.161.60", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("192.70.161.60", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("192.70.161.60", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("192.70.161.60", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("192.70.161.60", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("192.70.161.60", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("192.70.161.60", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("192.70.161.60", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("192.70.161.60", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("192.70.161.60", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("192.70.161.60", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("192.70.161.60", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("192.70.161.60", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("192.70.161.60", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("192.70.161.60", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("tvdlnet1.sfasu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("tvdlnet1.sfasu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("tvdlnet1.sfasu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("tvdlnet1.sfasu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("tvdlnet1.sfasu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("tvdlnet1.sfasu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("tvdlnet1.sfasu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("tvdlnet1.sfasu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("tvdlnet1.sfasu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("tvdlnet1.sfasu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("geoserv.ersc.wisc.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("geoserv.ersc.wisc.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("geoserv.ersc.wisc.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("geoserv.ersc.wisc.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("geoserv.ersc.wisc.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("geoserv.ersc.wisc.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("geoserv.ersc.wisc.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("geoserv.ersc.wisc.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("geoserv.ersc.wisc.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("geoserv.ersc.wisc.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("geoserv.ersc.wisc.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("geoserv.ersc.wisc.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("geoserv.ersc.wisc.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("geoserv.ersc.wisc.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("geoserv.ersc.wisc.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("geoserv.ersc.wisc.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("geoserv.ersc.wisc.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("geoserv.ersc.wisc.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("geoserv.ersc.wisc.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("geoserv.ersc.wisc.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("dresci.crest.iu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("dresci.crest.iu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("dresci.crest.iu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("dresci.crest.iu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("dresci.crest.iu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("dresci.crest.iu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("dresci.crest.iu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("dresci.crest.iu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("dresci.crest.iu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("dresci.crest.iu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("dresci.crest.iu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("dresci.crest.iu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("dresci.crest.iu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("dresci.crest.iu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("dresci.crest.iu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("dresci.crest.iu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("dresci.crest.iu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("dresci.crest.iu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("dresci.crest.iu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("dresci.crest.iu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("dresci.crest.iu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("dresci.crest.iu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("dresci.crest.iu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("dresci.crest.iu.edu", "6714"), offset = next(offset)),
                    self.create_allocation("5", ("dresci.crest.iu.edu", "6714"), offset = next(offset))
                ]
            }
        ]

        return exnodes
        
    def create_allocation(self, parent, destination, offset = 0, duration = 60):
        uid      = uuid.uuid4().hex
        dummyWRM = uuid.uuid4().int
        dummyStr = "ibp://{host}:{port}/0#{key}/{wrm}/{code}"
        now = datetime.datetime.utcnow()
        tmpAlloc = {
            "parent": parent,
            "lifetimes": [
                {
                    "start": now.strftime("%Y-%m-%d %H:%M:%S"),
                    "end":   (now + datetime.timedelta(minutes = duration)).strftime("%Y-%m-%d %H:%M:%S"),
                }
            ],
            "mapping": {
                "read":   dummyStr.format(host = destination[0], port = destination[1], key = uuid.uuid4().hex, wrm = dummyWRM, code = "READ"),
                "write":  dummyStr.format(host = destination[0], port = destination[1], key = uuid.uuid4().hex, wrm = dummyWRM, code = "WRITE"),
                "manage": dummyStr.format(host = destination[0], port = destination[1], key = uuid.uuid4().hex, wrm = dummyWRM, code = "MANAGE")
            },
            "ts": int(time.time()),
            "alloc_length": 10485760,
            "location": "ibp://{host}:{port}".format(host = destination[0], port = destination[1]),
            "offset": offset,
            "alloc_offset": offset,
            "id": uid,
            "size": 10485760
        }
        
        return tmpAlloc

    def get_offset(self, n):
            index = 0
            num = 0
            while index < n:
                yield num
                num += 10485760
                index += 1
