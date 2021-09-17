#!/usr/bin/python3
#
# packet.py
#
# Definition for all the packet-related constants and classes in EasyDB
#

import struct

# request commands
INSERT = 1
UPDATE = 2
DROP = 3
GET = 4    
SCAN = 5
EXIT = 6

# response codes
OK = 1
NOT_FOUND = 2
BAD_TABLE = 3
BAD_QUERY = 4 
TXN_ABORT = 5
BAD_VALUE = 6
BAD_ROW = 7
BAD_REQUEST = 8
BAD_FOREIGN = 9
SERVER_BUSY = 10
UNIMPLEMENTED = 11

# column types
NULL = 0
INTEGER = 1
FLOAT = 2
STRING = 3
FOREIGN = 4

# operator types
class operator:
    AL = 1  # everything
    EQ = 2  # equal 
    NE = 3  # not equal
    LT = 4  # less than
    GT = 5  # greater than 
    LE = 6  # you do not have to implement the following two
    GE = 7
    
# TODO: refactor so that it can send commands with arguments 
def request(sock, command=1, table_nr=0):
    # sending struct request to server
    # buf = struct.pack("!ii", command, table_nr)
    # sock.send(buf)
    buf = struct.pack("!iiiii4sii4siidiiQ", 1, 1, 4,  3, 4, 'adel'.encode(),  3, 4, 'aswd'.encode(),  2, 8, 62.0,  1, 8, 23)
    print(struct.unpack("!iiiii4sii4siidiiQ", buf))
    sock.send(buf)
    
# TODO: refactor so that it can receive response with arguments
def response(sock):
    # expecting struct response, which is 4 bytes
    buf = sock.recv(4)
    return struct.unpack("!i", buf)[0]

