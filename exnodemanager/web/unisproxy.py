import json
import requests

import exnodemanager.web.settings as settings
import exnodemanager.record as record

class UnisProxy(object):
    def __init__(self, host = settings.UNIS_HOST, port = settings.UNIS_PORT, **kwargs):
        self._log = record.getLogger()
        settings.UNIS_HOST = host
        settings.UNIS_PORT = port



    def CreateAllocation(self, alloc):
    # Attempt to push an updated allocation to UNIS
        try:
            tmpData = alloc.GetMetadata().Serialize()
            url = "{protocol}://{host}:{port}/{collection}/{eid}".format(protocol   = settings.UNIS_PROTOCOL,
                                                                         host       = settings.UNIS_HOST,
                                                                         port       = settings.UNIS_PORT,
                                                                         collection = "extents",
                                                                         eid        = tmpData["id"])
            
            headers  = {'Content-Type': 'application/perfsonar+json'}
            response = requests.post(url, data = json.dumps(tmpData), headers = headers)
            response = response.json()
        except requests.exceptions.RequestException as exp:
            self._log.error("UnisBridge.UpdateExtent: %s" % exp)
            return False #"UnisBridge.UpdateExtent: {exp}\n".format(exp = exp)
        except ValueError as exp:
            self._log.error("UnisBridge.UpdateExtent: {exp}\n, {err}\n".format(exp = exp, err = response.text))
            return False

        return response, ""


    def UpdateAllocation(self, alloc):
        # Attempt to push an updated allocation to UNIS
        try:
            tmpData = alloc.GetMetadata().Serialize()
            url = "{protocol}://{host}:{port}/{collection}/{eid}".format(protocol   = settings.UNIS_PROTOCOL,
                                                                         host       = settings.UNIS_HOST,
                                                                         port       = settings.UNIS_PORT,
                                                                         collection = "extents",
                                                                         eid        = tmpData["id"])
            
            headers  = {'Content-Type': 'application/perfsonar+json'}
            response = requests.put(url, data = json.dumps(tmpData), headers = headers)
            response = response.json()
        except requests.exceptions.RequestException as exp:
            self._log.error("UnisBridge.UpdateExtent: %s" % exp)
            return False  #, "UnisBridge.UpdateExtent: {exp}\n".format(exp = exp)
        except ValueError as exp:
            self._log.error("UnisBridge.UpdateExtent: {exp}\n, {err}\n".format(exp = exp, err = response.text))
            return False

        return response, ""

    def GetExnodes(self):
        # Attempt to get a list of file exnodes from UNIS
        try:
            url = "{protocol}://{host}:{port}/{collection}".format(protocol   = settings.UNIS_PROTOCOL,
                                                                   host       = settings.UNIS_HOST,
                                                                   port       = settings.UNIS_PORT,
                                                                   collection = "exnodes")
            
            response = requests.get(url, params = {"mode": "file"}, headers = {"Accept": "application/perfsonar+json"}).json()
        except requests.exceptions.RequestException as exp:
            self._log.error("UnisBridge.GetExnode: %s" % exp)
            return False
            
        return response

        
    def GetAllocations(self):
        # Attempt to get a list of file extents from UNIS
        try:
            url = "{protocol}://{host}:{port}/{collection}".format(protocol   = settings.UNIS_PROTOCOL,
                                                                   host       = settings.UNIS_HOST,
                                                                   port       = settings.UNIS_PORT,
                                                                   collection = "extents")
            response = requests.get(url, headers = {'Accept': 'application/perfsonar+json'}).json()
        except requests.exceptions.RequestException as exp:
            self._log.error("UnisBridge.GetExtent: %s" % exp)
            return False
            
        return response



def RunUnitTests():
    # Create proxy
    proxy = UnisProxy()
    
    # Get Exnodes
    exnodes = proxy.GetExnodes()
    
    assert exnodes
    print(len(exnodes))
    
    # Get Extents
    allocations = proxy.GetAllocations()
    
    assert allocations
    print(len(allocations))

    # Change Extent
#    newExtent = allocations[0]
    
#    print "\n"
#    print newExtent

#    newExtent["properties"] = {}
#    newExtent["properties"]["metadata"] = {}
#    newExtent["properties"]["metadata"]["warmer"] = "Unit Testing data"

#    proxy.UpdateExtent(newExtent)

if __name__ == "__main__":
    RunUnitTests()
