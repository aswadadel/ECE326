#!/usr/bin/python3
#
# packet.py
#
# Definition for all the packet-related constants and classes in EasyDB
#

from easydb.exception import InvalidReference, ObjectDoesNotExist, PacketError, TransactionAbort
import struct
import math

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

responseCodeErrors = {
    NOT_FOUND: ObjectDoesNotExist,
    BAD_FOREIGN: InvalidReference,
    TXN_ABORT: TransactionAbort,
}
responseCodeErrors.update(dict.fromkeys(\
    [BAD_QUERY, BAD_REQUEST, BAD_ROW, BAD_TABLE, BAD_VALUE], PacketError))

columnDict = {
    int:INTEGER,
    float: FLOAT,
    str:STRING,
}

def insertReq(tableNumber, columns=None, values=None):
    request = struct.pack("!ii", INSERT, tableNumber)
    count = struct.pack("!i", len(columns))
    rows = list()
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
                padding =  (4-size%4)%4 
                typeSymbol = "%ds"%(size) + ("%dx"%(padding) if padding != 0 else '')
                sizePack = size + padding 
                valuePack = values[index].encode()
        # print("!ii%s"%(typeSymbol), typePack, sizePack, valuePack)
        rows.append(struct.pack("!ii%s"%(typeSymbol), typePack, sizePack, valuePack))
    return b''.join([request, count, b''.join(rows)])

switcher = {
    1: insertReq
}
    
# TODO: refactor so that it can send commands with arguments 
def request(sock, command=1, table_nr=0, **kwargs):
    # sending struct request to server
    req = switcher[command]
    return sock.send(req(table_nr, **kwargs))
    
# TODO: refactor so that it can receive response with arguments
def response(sock, command=0):
    # expecting struct response, which is 4 bytes
    responseCode = struct.unpack("!i", sock.recv(4))[0]
    if responseCode != OK:
        raise responseCodeErrors[responseCode]()
    if command == INSERT:
        bufPack = 'QQ'
        bufSize = 16
        buffer = sock.recv(bufSize)
        pk, version = struct.unpack("!%s"%(bufPack), buffer)
        return pk, version
    return responseCode