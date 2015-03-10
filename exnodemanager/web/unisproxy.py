import json
import urllib2
import logging

import settings as settings

class UnisProxy(object):
    def __init__(self, host = settings.UNIS_HOST, port = settings.UNIS_PORT, **kwargs):
        settings.UNIS_HOST = host
        settings.UNIS_PORT = port

    def UpdateExtent(self, extent):
    # Attempt to push a new extent to UNIS
        extent.pop("id", None)
        try:
            url = "{protocol}://{host}:{port}/{collection}".format(protocol   = settings.UNIS_PROTOCOL,
                                                                   host       = settings.UNIS_HOST,
                                                                   port       = settings.UNIS_PORT,
                                                                   collection = "extents")
            request = urllib2.Request(url, data=json.dumps(extent))
            request.add_header("Content-Type", "application/perfsonar+json")            
            response = urllib2.urlopen(request, timeout=20).read()
        except urllib2.URLError as exp:
            logging.error("UnisBridge.UpdateExtent: %s" % exp)
            return False

        return json.loads(response)

    def GetExnodes(self):
    # Attempt to get a list of file exnodes from UNIS
        try:
            url = "{protocol}://{host}:{port}/{collection}?{options}".format(protocol   = settings.UNIS_PROTOCOL,
                                                                             host       = settings.UNIS_HOST,
                                                                             port       = settings.UNIS_PORT,
                                                                             collection = "exnodes",
                                                                             options    = "mode=file")
                                                                             #options    = "mode=file&fields=id,parent,size,name,properties")
            request = urllib2.Request(url)
            request.add_header("Accept", "application/perfsonar+json")
            
            response = urllib2.urlopen(request, timeout=20).read()
        except urllib2.URLError as exp:
            logging.error("UnisBridge.GetExnode: %s" % exp)
            return False
            
        return json.loads(response)

        
    def GetExtents(self):
    # Attempt to get a list of file extents from UNIS
        try:
            url = "{protocol}://{host}:{port}/{collection}?{options}".format(protocol   = settings.UNIS_PROTOCOL,
                                                                             host       = settings.UNIS_HOST,
                                                                             port       = settings.UNIS_PORT,
                                                                             collection = "extents",
                                                                             options    = "")
            request = urllib2.Request(url)
            request.add_header("Accept", "application/perfsonar+json")
            
            response = urllib2.urlopen(request, timeout=20).read()
        except urllib2.URLError as exp:
            logging.error("UnisBridge.GetExtent: %s" % exp)
            return False
            
        return json.loads(response)



def RunUnitTests():
    logging.basicConfig(level = logging.DEBUG)
    
    # Create proxy
    proxy = UnisProxy()
    
    # Get Exnodes
    exnodes = proxy.GetExnodes()
    
    assert exnodes
    print len(exnodes)
    
    # Get Extents
    extents = proxy.GetExtents()
    
    assert extents
    print len(extents)

    # Change Extent
    newExtent = extents[0]
    
    print "\n"
    print newExtent

    newExtent["properties"] = {}
    newExtent["properties"]["metadata"] = {}
    newExtent["properties"]["metadata"]["warmer"] = "Unit Testing data"

    proxy.UpdateExtent(newExtent)

if __name__ == "__main__":
    RunUnitTests()