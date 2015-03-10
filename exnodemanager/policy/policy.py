'''
@Name:   Policy.py
@Author: Jeremy Musser
@Date:   02/23/15

------------------------------------------

Policy is an abstract class that describes
the basic interface exposed by policies that
determine the distribution of resources in
the topology.

Policy implements the default behavior of 
refreshing extents in place within a given
tolerence of their expiration.

'''
import uuid
import datetime
import logging

import random

import settings

class Policy(object):
    # Initializes the policy, this function can be extended to do preprocessing
    # on the topology.
    def __init__(self):
        self._extents = {}
        self._exnodes = {}
        self._event_queue = []



    def exnodeList(self):
        return self._exnodes

    def extentList(self):
        return self._extents


    # RegisterExnode
    # @input:  A serialized dictionary of the exnode (structurally the same 
    #          as the exnode schema)
    # @output: Id referencing the exnode
    # @description:
    #      This function adds an exnode to the currently known exnodes to upkeep.
    def RegisterExnode(self, exnode):
        logging.info("Policy.RegisterExtent: Registering exnode - {0}".format(exnode["id"]))

        # Register extents for the new exnode
        newExnode = {}
        newExnode["data"] = exnode
        
        self._exnodes[exnode["id"]] = newExnode
        return exnode["id"]



    # RegisterExtent
    # @input:  A serilaized dictionary of the extent (structurally the same
    #          as the extent schema)
    # @output: Dictionary containing the id and priority of the extent
    # @description:
    #      This function adds an extent to the currently known extents to upkeep.
    def RegisterExtent(self, extent):
        logging.info("Policy.RegisterExtent: Registering extent - {0}".format(extent["id"]))

        expires = datetime.datetime.strptime(extent["lifetimes"][0]["end"], "%Y-%m-%d %H:%M:%S")
        priority = (expires - datetime.datetime(1970, 1, 1))
        priority = (priority.days * 1440) + priority.seconds
        
        if expires < datetime.datetime.now():
            logging.info("Policy.RegisterExtent: Extent is old, did not register")
            return None
        
        newExtent = {}
        newExtent["data"]  = extent
        newExtent["expires"] = expires
        
        self._extents[extent["id"]] = newExtent
        
        # If the extent already has an event associated with it, remove the old event
        for event in self._event_queue:
            if event["id"] == extent["id"]:
                self._event_queue.remove(event)
                break
                
        # Create an event associated with the new extent
        self._event_queue.append({ "priority": priority, "expires": expires, "id": extent["id"] })
        return { "priority": priority, "id": extent["id"] }



    # ProcessExtent
    # @input:  uuid that points to an extent.
    # @output: Dictionary including the extent to be altered and the addresses
    #          to which the extent should be sent.
    # @description:
    #      This function examines the current internal state, and processes an
    #      extent.
    def ProcessExtent(self, extent_id):
        extent = self._extents[extent_id]["data"]
        expires = self._extents[extent_id]["expires"]
        
        address = extent["mapping"]["write"]
        address = address.split("/")
        address = dict(zip(["host", "port"], address[2].split(":")))
        return { "extent": extent, "addresses": [address] }
        


    # GetPendingExtents
    # @input:  None
    # @output: An Array of extents
    # @description:
    #      Retrieve a list of extents that have pending changes.  These
    #      changes may be due to their lifetimes expiring or new topology 
    #      information causing an extent to invalidate.
    def GetPendingExtents(self):
        tmpPending = []
        tmpLastPendingIndex = 0
        tmpTolerence = datetime.timedelta(**settings.refresh_tolerence)
        
        self._event_queue = sorted(self._event_queue, key = lambda event: event["priority"])
        
        for event in self._event_queue:
            if event["expires"] - datetime.datetime.now() < tmpTolerence:
                tmpPending.append(event["id"])
                logging.debug("diff: {0}".format(event["expires"] - datetime.datetime.now()))
            else:
                break

        return tmpPending



    # hasPending
    # @input:  None
    # @output: Boolean
    # @description:
    #      Returns if there are currently pending extents ready for update.
    def hasPending(self):
        # Check if the first event is within the current tolerence
        self._event_queue = sorted(self._event_queue, key = lambda event: event["priority"])
        tmpExpires = self._event_queue[0]["expires"]
        
        return tmpExpires - datetime.datetime.now() < datetime.timedelta(**settings.refresh_tolerence)



def RunUnitTests():
    logging.basicConfig(level = logging.DEBUG)

    # Create Policy
    pol = Policy()

    assert pol

    # Register exnodes

    for i in range(0, 40):
        pol.RegisterExnode(dict([("id", i), ("created", 1234613412), ("modified", 234512342), ("parent", None), ("mode", "directory")]))
    
    print "\n"
    
    # Register extents (some of which need processing, some of which do not)
    
    for i in range(0, 40):
        tmpExtent = { "id": i, "location": "ibp://", "size": 143, "offest": 0, "parent": random.randrange(40)}
        tmpExtent["mapping"] = { 'read': 'ibp://dresci.incntre.iu.edu:6714/0#7ilECJ4k8tPvJy0hj4mNqY-dusRfjD4p/14420448421248173021/READ', 'write': 'ibp://dresci.incntre.iu.edu:6714/0#tA4iKYc+I9uw0Jfz4DP86hz5viAWPajA/14420448421248173021/WRITE', 'manage': 'ibp://dresci.incntre.iu.edu:6714/0#erQ4tfFofJqOXfBFmpUoLhZ9b1yz5F01/14420448421248173021/MANAGE' }
        
        if i % 2 == 0:
            tmpExtent["lifetimes"] = [{"start": "2015-03-08 20:43:00", "end": "2015-03-08 23:00:00"}]
        else:
            tmpExtent["lifetimes"] = [{"start": "2015-03-08 20:43:00", "end": "2015-03-12 23:00:00"}]

        pol.RegisterExtent(tmpExtent)

    tmpExtent["lifetimes"] = [{"start": "2014-03-08 20:00:00", "end": "2014-03-08 23:00:00"}]
    pol.RegisterExtent(tmpExtent)

    # Get pending
    assert pol.hasPending()

    pending = pol.GetPendingExtents()

    assert pending
    assert len(pending) == 20
    

    # Process pending
    extents = pol.extentList()

    for extent in pending:
        address = extents[extent]["data"]["mapping"]["write"].split("/")[2].split(":")
        dest = dict(zip(["host", "port"], address))

        print dest
        
        result = pol.ProcessExtent(extent)

        assert len(result["addresses"]) == 1
        assert result["addresses"][0] == dest
        assert datetime.datetime.now() - datetime.datetime.strptime(result["extent"]["lifetimes"][0]["end"], "%Y-%m-%d %H:%M:%S") < datetime.timedelta(**settings.refresh_tolerence)


if __name__ == "__main__":
    RunUnitTests()
