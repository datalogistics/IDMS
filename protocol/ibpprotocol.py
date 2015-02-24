'''
@Name:   IBPProtocol.py
@Author: Jeremy Musser
@Date:   02/13/15

------------------------------------------

IBPProtocol includes the specific
implementation details of how to read/
write and manage IBP Depots and exposes
that functionality to other modules.

'''

import settings
import protocol

class IBPProtocol(Protocol):

    def GetStatus(self, address, kwargs = {}):
    # Query the status of a Depot.
    
    # Generate request with the following form
    # PROTOCOL_VERSION[0] IBP_ST_INQ[2] pwd timeout
        pwd = settings.DEFAULT_PASSWORD
        timeout = settings.DEFAULT_TIMEOUT
        
        if "password" in kwargs:
            pwd = kwargs["password"]
        if "timeout" in kwargs:
            timeout = kwargs["timeout"]
        
        try:
            tmpCommand = "{0} {1} {2} {3}\n".format(settings.PROTOCOL_VERSION, settings.IBP_ST_INQ, pwd, timeout)
            result = self._dispatch_command(address["host"], address["port"], tmpCommand)
            result = result.split(" ")
        except:
            return None

        return dict(zip(["total", "used", "volatile", "used-volatile", "max-duration"], result))
        
    def Copy(self, source, destination, chunk, kwargs = {}):
        return self.Move(source, destination, chunk, kwargs)
    
    def Move(self, source, destination, chunk, kwargs = {}):
    # Move a chunk from one {source} Depot to one {destination} depot

    # Generate caps from data
        src_caps = self._get_caps(chunk["mapping"])
        
    # Generate destination Allocation and Capabilities using the form below
    # PROTOCOL_VERSION[0] IBP_ALLOCATE[1] reliability cap_type duration size timeout
        reliability = settings.IBP_HARD
        cap_type    = settings.IBP_BYTEARRAY
        duration    = settings.DEFAULT_DURATION
        timeout     = settings.DEFAULT_TIMEOUT
        
        if "reliability" in kwargs:
            reliability = kwargs["reliability"]
        if "type" in kwargs:
            cap_type = kwargs["type"]
        if "duration" in kwargs:
            duration = kwargs["duration"]
        if "timeout" in kwargs:
            timeout = kwargs["timeout"]

        try:
            tmpCommand = "{0} {1} {2} {3} {4} {5} {6} \n".format(settings.PROTOCOL_VERSION, settings.IBP_ALLOCATE, reliability, cap_type, duration, chunk["size"], timeout)
            result = self._dispatch_command(destination["host"], destination["port"], tmpCommand)
            result = result.splitlines()
        except:
            return None
        
        dest_caps = self._get_caps(dict(zip(["read", "write", "manage"], result)))
        
    # Generate move request with the following form
    # PROTOCOL_VERSION[0] IBP_SEND[5] src_read_key dest_write_key src_WRMKey offset size timeout timeout timeout
        try:
            tmpCommand = "{0} {1} {2} {3} {4} {5} {6} {7} {8} {9} \n".format( \
                                                                           settings.PROTOCOL_VERSION, \
                                                                           settings.IBP_SEND, \
                                                                           src_caps["read"]["key"], \
                                                                           dest_caps["write"]["key"], \
                                                                           src_caps["read"]["wrm_key"], \
                                                                           0, \
                                                                           timeout, \
                                                                           timeout, \
                                                                           timeout, \
                                                                       )
            result = self._dispatch_command(source["host"], source["port"], tmpCommand)
        except:
            return None
        
        if result.startswith("-"):
            return int(result)
        else:
            result = result.split(" ")
            return result[1]
    
    def Manage(self, address, chunk, kwargs = {}):
        cap_type    = 0
        reliability = settings.IBP_HARD
        timeout     = settings.DEFAULT_TIMEOUT
        max_size    = settings.DEFAULT_MAXSIZE
        duration    = settings.DEFAULT_DURATION

    # Generate manage request with the following form
    # PROTOCOL_VERSION[0] IBP_MANAGE[9] manage_key "MANAGE" IBP_CHANGE[43] cap_type max_size duration reliability timeout
        if "cap_type" in kwargs:
            cap_type = kwargs["cap_type"]
        if "reliability" in kwargs:
            reliability = kwargs["reliability"]
        if "timeout" in kwargs:
            timeout = kwargs["timeout"]
        if "max_size" in kwargs:
            max_size = kwargs["max_size"]
        if "duration" in kwargs:
            duration = kwargs["duration"]
            
        try:
            caps = _get_caps(chunk["mapping"])
            tmpCommand = "{0} {1} {2} {3} {4} {5} {6} {7} {8} {9}\n".format(settings.PROTOCOL_VERSION, \
                                                                            settings.IBP_MANAGE, \
                                                                            caps["manage"]["key"], \
                                                                            caps["manage"]["type"], \
                                                                            settings.IBP_CHANGE, \
                                                                            cap_type, \
                                                                            max_size, \
                                                                            duration, \
                                                                            reliability, \
                                                                            timeout
                                                                            )
            result = self._dispatch_command(address["host"], address["port"], tmpCommand)
        except:
            return None

        return True
        
    def _dispatch_command(self, host, port, command):
    # Create socket and configure with host and port
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        sock.connect((_host, _port))
        
        try:
            sock.send(_command)
            response = sock.recv(1024)
        except socket.timeout as e:
            return None
        except e:
            return None
        
        return response
        
    def _get_caps(self, chunk):
        result = {}

        for item, key in chunk:
            tmpSplit = item.split("/")
            result[key] = { "key": tmpSplit[2], "wrm_key": tmpSplit[3], "type": tmpSplit[4] }
        
        return result
