'''
@Name:   Protocol.py
@Author: Jeremy Musser
@Date:   02/13/15

------------------------------------------

Protocol is an abstract class that describes
the basic interface exposed by protocols for
communicating with backend data storage
depots.

'''


class Protocol(object):
    def __init__(self):
        pass

    def GetStatus(self, address, **kargs):
        pass

    def Copy(self, source, destination, extent, **kwargs):
        pass

    def Move(self, source, destination, extent, **kwargs):
        pass

    def Release(self, address, extent, **kwargs):
        pass

    def Manage(self, address, extent, **kwargs):
        pass
