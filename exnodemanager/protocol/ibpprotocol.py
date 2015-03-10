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

import logging
import sys
import argparse
import urllib2
import socket

import settings
from protocol import Protocol

class IBPProtocol(Protocol):

    def GetStatus(self, address, **kwargs):
    # Query the status of a Depot.
    
    # Generate request with the following form
    # PROTOCOL_VERSION[0] IBP_ST_INQ[2] pwd timeout
        pwd = settings.DEFAULT_PASSWORD
        timeout = settings.DEFAULT_TIMEOUT
        tmpCommand = ""
        
        if "password" in kwargs:
            pwd = kwargs["password"]
        if "timeout" in kwargs:
            timeout = kwargs["timeout"]
        
        try:
            tmpCommand = "{0} {1} {2} {3} {4}\n".format(settings.PROTOCOL_VERSION, settings.IBP_STATUS, settings.IBP_ST_INQ, pwd, timeout)
            result = self._dispatch_command(address["host"], address["port"], tmpCommand)
            logging.debug("IBPProtocol.GetStatus: Recievd - {result}".format(result = result))
            result = result.split(" ")
        except Exception as exp:
            logging.warn("IBPProtocol.GetStatus: Failed to get the status of {host}:{port}".format(**address))
            logging.warn("                       -- Request: {command}".format(command = tmpCommand))
            logging.warn("                       -- Error:   {error}".format(error = exp))
            return None
            
        return dict(zip(["total", "used", "volatile", "used-volatile", "max-duration"], result))
        
    def Allocate(self, address, size, **kwargs):
    # Generate destination Allocation and Capabilities using the form below
    # PROTOCOL_VERSION[0] IBP_ALLOCATE[1] reliability cap_type duration size timeout
        reliability = settings.IBP_HARD
        cap_type    = settings.IBP_BYTEARRAY
        timeout     = settings.DEFAULT_TIMEOUT
        duration    = settings.DEFAULT_DURATION
        tmpCommand  = ""
        
        if "reliability" in kwargs:
            reliability = kwargs["reliability"]
        if "type" in kwargs:
            cap_type = kwargs["type"]
        if "duration" in kwargs:
            duration = kwargs["duration"]
        if "timeout" in kwargs:
            timeout = kwargs["timeout"]

        try:
            print "size: {0}".format(size)
            tmpCommand = "{0} {1} {2} {3} {4} {5} {6} \n".format(settings.PROTOCOL_VERSION, settings.IBP_ALLOCATE, reliability, cap_type, duration, size, timeout)
            result = self._dispatch_command(address["host"], address["port"], tmpCommand)
            result = result.split(" ")[1:]
        except Exception as exp:
            logging.warn("IBPProtocol.Allocate: Failed to allocate resource at {host}:{port}".format(**address))
            logging.warn("                      -- Request: {command}".format(command = tmpCommand))
            logging.warn("                      -- Error:   {error}".format(error = exp))
            return None
        
        result = dict(zip(["read", "write", "manage"], result))
        return result

    def Store(self, address, data, size, **kwargs):
        timeout  = settings.DEFAULT_TIMEOUT
        duration = settings.DEFAULT_DURATION
        tmpCommand  = ""

        if "timeout" in kwargs:
            timeout = kwargs["timeout"]
        if "duration" in kwargs:
            duration = kwargs["duration"]
        
        cap_urls = self.Allocate(address, size, **kwargs)
        dest_caps = self._get_caps(cap_urls)

        try:
            tmpCommand = "{0} {1} {2} {3} {4} {5}\n".format(settings.PROTOCOL_VERSION, settings.IBP_STORE, dest_caps["write"]["key"], dest_caps["write"]["wrm_key"], size, timeout)
            result = self._dispatch_command(address["host"], address["port"], tmpCommand)
            result = result.split(" ")
        except:
            logging.warn("IBPProtocol.Store: Failed to store data at {host}:{port}".format(**address))
            logging.warn("                   -- Request: {command}".format(command = tmpCommand))
            return None

        if result[0].startswith("-"):
            logging.warn("IBPProtocol.Store: Could not store extent - {0}".format(result))
            logging.warn("                   -- Request: {command}".format(command = tmpCommand))
            return None
        else:
            return {"caps": cap_urls, "duration": duration }

    
    def Copy(self, source, destination, extent, **kwargs):
        return self.Move(source, destination, extent, **kwargs)
    
    def Move(self, source, destination, extent, **kwargs):
    # Move an extent from one {source} Depot to one {destination} depot
        timeout  = settings.DEFAULT_TIMEOUT
        duration    = settings.DEFAULT_DURATION
        tmpCommand  = ""

        if "timeout" in kwargs:
            timeout = kwargs["timeout"]
        if "duration" in kwargs:
            duration = kwargs["duration"]

    # Generate caps from data
        src_caps  = self._get_caps(extent["mapping"])
        cap_urls  = self.Allocate(destination, 10000, **kwargs)
        dest_caps = self._get_caps(cap_urls)
        
        tmpExtent = { "mapping": cap_urls, "size": extent["size"] }
        print self.Probe(destination, tmpExtent)
    # Generate move request with the following form
    # PROTOCOL_VERSION[0] IBP_SEND[5] src_read_key dest_write_key src_WRMKey offset size timeout timeout timeout
        try:
            tmpCommand = "{0} {1} {2} {3} {4} {5} {6} {7} {7} {7} \n".format( settings.PROTOCOL_VERSION,
                                                                              settings.IBP_SEND,
                                                                              src_caps["read"]["key"],
                                                                              dest_caps["write"]["key"],
                                                                              src_caps["read"]["wrm_key"],
                                                                              0,
                                                                              extent["size"] + 5,
                                                                              timeout
                                                                       )
            result = self._dispatch_command(source["host"], source["port"], tmpCommand)
        except Exception as exp:
            logging.warn("IBPProtocol.Move: Failed to move resource from {host1}:{port1} to {host2}:{port2} - {e}".format(host1 = source["host"], port1 = source["port"], host2 = destination["host"], port2 = destination["port"], e = exp))
            logging.warn("                  -- Request: {command}".format(command = tmpCommand))
            return None
         
        if result.startswith("-"):
            logging.warn("IBPProtocol.Move: Could not move extent - {0}".format(result))
            logging.warn("                  -- Request: {command}".format(command = tmpCommand))
            return None
        else:
            result = result.split(" ")
            return {"caps": cap_urls, "duration": duration }
   

    def Release(self, address, extent, **kwargs):
        return self.Manage(address, extent, duration = 1)


    def Manage(self, address, extent, **kwargs):
        cap_type    = 0
        reliability = settings.IBP_HARD
        timeout     = settings.DEFAULT_TIMEOUT
        max_size    = settings.DEFAULT_MAXSIZE
        duration    = settings.DEFAULT_DURATION
        tmpCommand  = ""

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
            caps = self._get_caps(extent["mapping"])
            tmpCommand = "{0} {1} {2} {3} {4} {5} {6} {7} {8} {9}\n".format(settings.PROTOCOL_VERSION,
                                                                            settings.IBP_MANAGE,
                                                                            caps["manage"]["key"],
                                                                            caps["manage"]["type"],
                                                                            settings.IBP_CHANGE,
                                                                            cap_type,
                                                                            max_size,
                                                                            duration,
                                                                            reliability,
                                                                            timeout
                                                                            )
            result = self._dispatch_command(address["host"], address["port"], tmpCommand)
        except:
            logging.warn("IBPProtocol.Manage: Failed to manage extent at {host}:{port}".format(**address))
            logging.warn("                    -- Request: {command}".format(command = tmpCommand))
            return None

        if result.startswith("-"):
            logging.warn("IBPProtocol.Manage: Could not manage extent - {0}".format(result))
            logging.warn("                   -- Request: {command}".format(command = tmpCommand))
            return None
        else:
            return True

    def Probe(self, address, extent, **kwargs):
        reliability = settings.IBP_HARD
        cap_type    = settings.IBP_BYTEARRAY
        timeout     = settings.DEFAULT_TIMEOUT
        duration    = settings.DEFAULT_DURATION
        max_size    = settings.DEFAULT_MAXSIZE
        tmpCommand = ""
        
        if "reliability" in kwargs:
            reliability = kwargs["reliability"]
        if "type" in kwargs:
            cap_type = kwargs["type"]
        if "duration" in kwargs:
            duration = kwargs["duration"]
        if "timeout" in kwargs:
            timeout = kwargs["timeout"]
        if "max_size" in kwargs:
            max_size = kwargs["max_size"]

        try:
            caps = self._get_caps(extent["mapping"])
            tmpCommand = "{0} {1} {2} {3} {4} {5} {6} {7} {8} {9}\n".format(settings.PROTOCOL_VERSION,
                                                                            settings.IBP_MANAGE,
                                                                            caps["manage"]["key"],
                                                                            caps["manage"]["type"],
                                                                            settings.IBP_PROBE,
                                                                            cap_type,
                                                                            max_size,
                                                                            duration,
                                                                            reliability,
                                                                            timeout
                                                                            )
            result = self._dispatch_command(address["host"], address["port"], tmpCommand)
            logging.debug(result)
            result = result.split(" ")
        except:
            logging.warn("IBPProtocol.Manage: Failed to manage extent at {host}:{port}".format(**address))
            logging.warn("                    -- Request: {command}".format(command = tmpCommand))
            return None

        if result[0].startswith("-"):
            logging.warn("IBPProtocol.Store: Could not probe extent - {0}".format(result))
            logging.warn("                   -- Request: {command}".format(command = tmpCommand))
            return None
        else:
            return dict(zip(["read_count", "write_count", "size", "max_size", "duration", "reliability", "type"], result[1:]))

        
        
        
    def _dispatch_command(self, host, port, command):
    # Create socket and configure with host and port
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        sock.connect((host, port))
        
        try:
            sock.send(command)
            response = sock.recv(1024)
        except socket.timeout as e:
            logging.warn("Socket Timeout - {0}".format(e))
            logging.warn("--Attempted to execute: {0}".format(command))
            return None
        except e:
            logging.warn("Socket error - {0}".format(3))
            logging.warn("--Attempted to execut: {0}".format(command))
            return None
        
        return response
        
    def _get_caps(self, extent):
        result = {}

        for key, item in extent.iteritems():
            tmpSplit = item.split("/")
            result[key] = { "key": tmpSplit[3], "wrm_key": tmpSplit[4], "type": tmpSplit[5] }
        
        return result


def RunUnitTests():
    proto = IBPProtocol()

    description = """{prog}: run unit tests on ibp protocol services.""".format(prog = sys.argv[0])
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('-h1', '--start-depot-host')
    parser.add_argument('-p1', '--start-depot-port', type=int)
    parser.add_argument('-h2', '--end-depot-host')
    parser.add_argument('-p2', '--end-depot-port', type=int)
    args = parser.parse_args()

    logging.basicConfig(level = logging.DEBUG)

    host1 = "dresci.incntre.iu.edu"
    port1 = 6714
    host2 = "128.120.83.7"
    port2 = 6714

    if args.start_depot_host:
        host1 = args.start_depot_host
    if args.start_depot_port:
        port1 = args.start_depot_port
    if args.end_depot_host:
        host2 = args.end_depot_host
    if args.end_depot_port:
        port2 = args.end_depot_port

    # Create Dummy data
    data = """Lorem ipsum dolor sit amet, error elit, in etiam est nunc tempor ut orci, mollis integer ut quisque ullamcorper rhoncus in.""".encode('utf-8')
    
    size = len(data)
    print "Size-init: {size}".format(size = size)

    # Get status of test depots
    status1 = proto.GetStatus({"host": host1, "port": port1})
    status2 = proto.GetStatus({"host": host2, "port": port2})
    
    assert status1
    assert status2
    print status1
    print status2
    
    # Store Dummy data with a duration of 1 hour
    result = ""
    result = proto.Store(address = {"host": host1, "port": port1}, data = data, size = size, duration = 3600)
    
    assert result
    assert result["duration"] == 3600
    print result

    # Build temporary psudo-extent for managing later operations
    tmpExtent = { "mapping": result["caps"], "size": size }

    # Make duration 300
    result = ""
    result = proto.Manage(address = {"host": host1, "port": port1}, extent = tmpExtent, duration = 300)
    
    assert result

    result = ""
    result = proto.Probe(address = {"host": host1, "port": port1}, extent = tmpExtent)

    assert result
    print result

    # Make duration 50000
    result = ""
    result = proto.Manage(address = {"host": host1, "port": port1}, extent = tmpExtent, duration = 50000)

    assert result

    result = ""
    result = proto.Probe(address = {"host": host1, "port": port1}, extent = tmpExtent)

    assert result
    print result

    result = ""
    result = proto.Release(address = {"host": host1, "port": port1}, extent = tmpExtent)
    
    assert result

    result = ""
    result = proto.Probe(address = {"host": host1, "port": port1}, extent = tmpExtent)

    assert result
    print result


    # TODO: Debug Move command
    # Move Dummy data to second depot
#    result = proto.Move(source      = {"host": host1, "port": port1}, 
#                        destination = {"host": host2, "port": port2}, 
#                        extent      = tmpExtent, 
#                        duration    = 3600)


#    assert result
#    assert result["duration"] == 3600
#    print result

    # Build second temporary psudo-extent for managing second depot
#    tmpExtent2 = { "mapping": result["caps"], "size": size }
    
    # Check duration for data on second depot
#    result = proto.Manage(address = {"host": host1, "port": port1}, extent = tmpExtent)
    
#    assert result
#    print result

#    result = proto.Manage(address = {"host": host2, "port": port2}, extent = tmpExtent2)

if __name__ == "__main__":
    RunUnitTests()
