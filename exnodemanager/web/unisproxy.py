import json
import requests
import logging

import settings as settings

class UnisProxy(object):
    def __init__(self, host = settings.UNIS_HOST, port = settings.UNIS_PORT, **kwargs):
        settings.UNIS_HOST = host
        settings.UNIS_PORT = port

    def UpdateExtent(self, extent):
    # Attempt to push a new extent to UNIS
        try:
            url = "{protocol}://{host}:{port}/{collection}/{eid}".format(protocol   = settings.UNIS_PROTOCOL,
                                                                         host       = settings.UNIS_HOST,
                                                                         port       = settings.UNIS_PORT,
                                                                         collection = "extents",
                                                                         eid        = extent["id"])

            headers  = {'Content-Type': 'application/perfsonar+json'}
            response = requests.put(url, data = json.dumps(extent), headers = headers)
            response = response.json()
#            request = urllib2.Request(url, data=json.dumps(extent))
#            request.add_header("Content-Type", "application/perfsonar+json")
#            response = urllib2.urlopen(request, timeout=20).read()
        except requests.exceptions.RequestException as exp:
#            logging.error("UnisBridge.UpdateExtent: %s" % exp)
            return False, "UnisBridge.UpdateExtent: {exp}\n".format(exp = exp)
        except ValueError as exp:
            return False, "UnisBridge.UpdateExtent: {exp}\n, {err}\n".format(exp = exp, err = response.text)

        return response, ""

    def GetExnodes(self):
    # Attempt to get a list of file exnodes from UNIS
        try:
            url = "{protocol}://{host}:{port}/{collection}".format(protocol   = settings.UNIS_PROTOCOL,
                                                                   host       = settings.UNIS_HOST,
                                                                   port       = settings.UNIS_PORT,
                                                                   collection = "exnodes")
            #options    = "mode=file")
            #options    = "mode=file&fields=id,parent,size,name,properties")
            
            response = requests.get(url, params = {"mode": "file"}, headers = {"Accept": "application/perfsonar+json"}).json()
#            request = urllib2.Request(url)
#            request.add_header("Accept", "application/perfsonar+json")
            
#            response = urllib2.urlopen(request, timeout=20).read()
        except requests.exceptions.RequestException as exp:
            logging.error("UnisBridge.GetExnode: %s" % exp)
            return False
            
        return response

        
    def GetExtents(self):
    # Attempt to get a list of file extents from UNIS
        try:
            url = "{protocol}://{host}:{port}/{collection}".format(protocol   = settings.UNIS_PROTOCOL,
                                                                   host       = settings.UNIS_HOST,
                                                                   port       = settings.UNIS_PORT,
                                                                   collection = "extents")
            response = requests.get(url, headers = {'Accept': 'application/perfsonar+json'}).json()
#            request = urllib2.Request(url)
#            request.add_header("Accept", "application/perfsonar+json")
            
#            response = urllib2.urlopen(request, timeout=20).read()
        except requests.exceptions.RequestException as exp:
            logging.error("UnisBridge.GetExtent: %s" % exp)
            return False
            
        return response



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
