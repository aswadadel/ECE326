#!/usr/bin/python3
#
# packet.py
#
# Definition for all the packet-related constants and classes in EasyDB
#

from easydb.exception import InvalidReference, ObjectDoesNotExist, PacketError, TransactionAbort
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


# db error codes -> python exceptions
responseCodeErrors = {
    NOT_FOUND: ObjectDoesNotExist,
    BAD_FOREIGN: InvalidReference,
    TXN_ABORT: TransactionAbort,
}
responseCodeErrors.update(dict.fromkeys(
    [BAD_QUERY, BAD_REQUEST, BAD_ROW, BAD_TABLE, BAD_VALUE], PacketError))

# python types -> db types
columnDict = {
    int: INTEGER,
    float: FLOAT,
    str: STRING,
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
                typeSymbol = 'q'
            elif typePack == FLOAT:
                typeSymbol = 'd'
            elif typePack == STRING:
                size = len(values[index])
                padding = (4-size % 4) % 4
                typeSymbol = "%ds" % (size) + ("%dx" %
                                               (padding) if padding != 0 else '')
                sizePack = size + padding
                valuePack = values[index].encode()
        # use this to print the row's details before they get packed
        # print("!ii%s"%(typeSymbol), typePack, sizePack, valuePack)
        rows.append(struct.pack("!ii%s" %
                    (typeSymbol), typePack, sizePack, valuePack))
    result = b''.join([request, count, b''.join(rows)])
    print(result)
    return result


def dropReq(tableNumber, pk=None):
    request = struct.pack("!ii", DROP, tableNumber)
    index = struct.pack("!Q", pk)
    return b''.join([request, index])


def getReq(tableNumber, pk=None):
    request = struct.pack("!ii", GET, tableNumber)
    index = struct.pack("!Q", pk)
    return b''.join([request, index])


def exitReq(tableNumber):
    return struct.pack('!ii', EXIT, tableNumber)


def updateReq(tableNumber, columns=None, values=None, version=0, pk=None):
    request = struct.pack("!ii", UPDATE, tableNumber)
    count = struct.pack("!i", len(columns))
    key = struct.pack("!QQ", pk, version)
    print("packing")
    rows = list()
    for index, column in enumerate(columns):
        typePack = FOREIGN
        sizePack = 8
        valuePack = values[index]
        typeSymbol = 'Q'
        if column[1] in columnDict:
            typePack = columnDict[column[1]]
            if typePack == INTEGER:
                typeSymbol = 'q'
            elif typePack == FLOAT:
                typeSymbol = 'd'
            elif typePack == STRING:
                size = len(values[index])
                padding = (4-size % 4) % 4
                typeSymbol = "%ds" % (size) + ("%dx" %
                                               (padding) if padding != 0 else '')
                sizePack = size + padding
                valuePack = values[index].encode()
        # use this to print the row's details before they get packed
        # print("!ii%s"%(typeSymbol), typePack, sizePack, valuePack)
        rows.append(struct.pack("!ii%s" %
                    (typeSymbol), typePack, sizePack, valuePack))
    # pk is the row to update, version is
    # implement me, pack is done here
    # returns new version code
    print("successfully packed")
    result = b''.join(
        [request, key, count, b''.join(rows)])
    print(result)
    return result


# command -> function
# functions must have the following signature: funcName(tableNumber, keyArg=val)->byteArray
switcher = {
    INSERT: insertReq,
    DROP: dropReq,
    EXIT: exitReq,
    UPDATE: updateReq,
    GET: getReq,
}


def request(sock, command=1, table_nr=0, **kwargs):
    req = switcher[command]
    # IMPORTANT: req functions take a tableNumber and any number of keyward arguments
    return sock.send(req(table_nr, **kwargs))


def response(sock, command=0):
    # get response code
    responseCode = struct.unpack("!i", sock.recv(4))[0]
    print("responseCode = ", responseCode)
    if responseCode != OK:
        raise responseCodeErrors[responseCode]()
    # get and unpack the expected byte sequence based on the command's response structure
    if command == INSERT:
        bufPack = 'QQ'
        bufSize = 16
        buffer = sock.recv(bufSize)
        pk, version = struct.unpack("!%s" % (bufPack), buffer)
        return pk, version
    if command in (DROP, EXIT):
        return
    if command == UPDATE:
        print("reading")
        bufPack = 'Q'
        bufSize = 8
        buffer = sock.recv(bufSize)
        version = struct.unpack("!%s" % (bufPack), buffer)
        print("returning")
        return version
    if command == GET:
        print("getting")
        row = list()
        # get command
        bufPack = 'Qi'
        bufSize = 12
        buffer = sock.recv(bufSize)
        version, count = struct.unpack("!%s" % (bufPack), buffer)
        # print("count is %i", count)
        for column in range(count):
            # get the type
            bufPack = 'ii'
            bufSize = 8
            buffer = sock.recv(bufSize)
            valueType, valueSize = struct.unpack("!%s" % (bufPack), buffer)
            print(valueType)
            if valueType == INTEGER:
                bufPack = 'Q'
                bufSize = 8
            elif valueType == FLOAT:
                bufPack = 'd'
                bufSize = 8
            elif valueType == FOREIGN:
                bufPack = 'Q'
                bufSize = 8
            elif valueType == STRING:
                padding = valueSize % 4
                bufSize = padding + valueSize
                bufPack = '%ds' % (bufSize)
                print("bufSize = ", bufSize)
                print("bufPack = ", bufPack)
            buffer = sock.recv(bufSize)
            bufferValue = struct.unpack("!%s" % (bufPack), buffer)
            if(valueType == STRING):
                bufferValue = bufferValue[0].decode(encoding='ascii')
                bufferValue = bufferValue.replace('\x00', '')
            elif(valueType == INTEGER or valueType == FLOAT):
                # for some reason it buffer value puts them in a tuple
                # but not always in the first position in the tuple so this is needed?
                bufferValue = bufferValue[0]
            elif(valueType == FOREIGN):
                # why does this work?
                bufferValue = bufferValue[0]

            row.append(bufferValue)
            print(bufferValue)
        print(row)
        return row, version
    return responseCode
