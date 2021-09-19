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

columnDict = {
    int:INTEGER,
    float: FLOAT,
    str:STRING,
}

def insertReq(tableNumber, columns=None, values=None):
    request = struct.pack("!ii", INSERT, tableNumber)
    count = struct.pack("!i", len(columns))
    row = list()
    for index, column in enumerate(columns):
        typePack = FOREIGN
        sizePack = 8
        valuePack = values[index]
        typeSymbol = 'Q'
        if column[1] in columnDict:
            typePack = columnDict[column[1]]
            if typePack == INTEGER:
                typeSymbol = 'Q'
            elif typePack == FLOAT:
                typeSymbol = 'd'
            elif typePack == STRING:
                size =len(values[index]) 
                padding = size - size%4
                typeSymbol = "%ds"%(size) + "%dx"%(padding) if padding != 0 else ''
                sizePack = size + padding 
                valuePack = values[index].encode()
        row.append(struct.pack("!ii%s"%(typeSymbol), typePack, sizePack, valuePack))
    return b''.join([request, count, b''.join(row)])

switcher = {
    1: insertReq
}
    
# TODO: refactor so that it can send commands with arguments 
def request(sock, command=1, table_nr=0, **kwargs):
    # sending struct request to server
    # buf = struct.pack("!ii", command, table_nr)
    # sock.send(buf)
    req = switcher[command]
    return sock.send(req(table_nr, **kwargs))
    buf = struct.pack("!iiiii4sii4siidiiQ", 1, 1, 4,  3, 4, 'adel'.encode(),  3, 4, 'aswd'.encode(),  2, 8, 62.0,  1, 8, 23)
    print(struct.unpack("!iiiii4sii4siidiiQ", buf))
    sock.send(buf)
    
# TODO: refactor so that it can receive response with arguments
def response(sock):
    # expecting struct response, which is 4 bytes
    buf = sock.recv(4)
    return struct.unpack("!i", buf)[0]

