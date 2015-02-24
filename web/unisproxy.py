import web.settings as settings

class UnisBridge(object):
    def __init__(self, host, port, **kwargs):
        self._host = host
        self._port = port
    settings = dict(settings.items() + kwargs)

    def UpdateExtent(self, extent):
    # Attempt to push a new extent to UNIS
        try:
            url = "{protocol}://{host}:{port}/{collection}?{options}".format(protocol   = settings.UNIS_PROTOCOL,
                                                                             host       = settings.UNIS_HOST,
                                                                             port       = settings.UNIS_PORT,
                                                                             collection = "exnodes",
                                                                             options    = "mode=file")
            request = urllib2.Request(url, data=json.dumps(extent))
            response = urllib2.urlopen(request, timeout=5).read()
        except urllib2.URLError as exp:
            logging.error("UnisBridge.UpdateExtent: %s" % exp)

        return json.loads(response)

    def GetExnodes(self):
    # Attempt to get a list of file exnodes from UNIS
        try:
            url = "{protocol}://{host}:{port}/{collection}?{options}".format(protocol   = settings.UNIS_PROTOCOL,
                                                                             host       = settings.UNIS_HOST,
                                                                             port       = settings.UNIS_PORT,
                                                                             collection = "exnodes",
                                                                             options    = "mode=file")
            request = urllib2.Request(url)
            request.add_header("Accept": "application/perfsonar+json")
            
            response = urllib2.urlopen(request, timeout=5).read()
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
            request.add_header("Accept": "application/perfsonar+json")
            
            response = urllib2.urlopen(request, timeout=5).read()
        except urllib2.URLError as exp:
            logging.error("UnisBridge.GetExtent: %s" % exp)
            return False
            
        return json.loads(response)

