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

import policy.defaultSettings as settings

class Policy(object):
    # Initializes the policy, this function can be extended to do preprocessing
    # on the topology.
    def __init__(self, kwargs):
        self._extents = {}
        self._exnodes = {}
        self._event_queue = []


    # RegisterExnode
    # @input:  A serialized dictionary of the exnode (structurally the same 
    #          as the exnode schema)
    # @output: ID of the registered exnode
    # @description:
    #      This function adds an exnode to the currently known exnodes to upkeep.
    def RegisterExnode(self, exnode):
        # Register extents for the new exnode
        newExnode = {}
        newExnode["data"] = exnode
        
        self._exnodes[exnode["id"]] = newExnode
        return { "timestamp": exnode["ts"], "id": exnode["id"] }



    # RegisterExtent
    # @input:  A serilaized dictionary of the extent (structurally the same
    #          as the extent schema)
    # @output: ID that also functions as a key to the extent
    # @description:
    #      This function adds an extent to the currently known extents to upkeep.
    def RegisterExtent(self, extent):
        expires = datetime.datetime.strptime(extent["lifetimes"][0]["end"], "%Y-%m-%d %H:%M:%S")
        priority = (expires - datetime.datetime(1970,1 1)).total_seconds()

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
        address = address.split["/"]
        address = dict(zip(["host", "port"], address[1].split[":"]))
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
        tmpTolerance = datetime.timedelta(**settings.refresh_tolerance)
        
        self._event_queue = sorted(self._event_queue, key = lambda event: event["expires"])
        
        while datetime.datetime.now() - self._event_queue[tmpLastPendingIndex]["expires"] < tmpTolerance:
            tmpPending.append(self._event_queue[tmpLastPendingIndex++]["id"])

        return tmpPending



    # hasPending
    # @input:  None
    # @output: Boolean
    # @description:
    #      Returns if there are currently pending extents ready for update.
    def hasPending(self):
        # Check if the first event is within the current tolerance
        self._event_queue = sorted(self._event_queue, key = lambda event: event["expires"])
        tmpExpires = datetime.datetime.fromtimestamp(self._event_queue[0]["expires"])

        return datetime.datetime.now() - tmpExpires < datetime.timedelta(**settings.refresh_tolerance)
