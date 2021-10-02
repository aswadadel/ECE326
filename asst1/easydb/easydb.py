#!/usr/bin/python3
#
# easydb.py
#
# Definition for the Database class in EasyDB client
#

import socket
from struct import pack
from typing import Dict
from .packet import DROP, EXIT, INSERT, NULL, OK, SERVER_BUSY, request, response, UPDATE, GET, SCAN, AL
from .exception import IntegrityError, InvalidReference, PacketError
import re


class Database:
    def __repr__(self):
        return "<EasyDB Database object>"

    def __init__(self, tables):
        self._socket = None
        # self._tables = set()
        try:
            iter(tables)
        except:
            raise TypeError()
        # used to check if foreign key columns refer to existing tables
        tableNames = set()
        # used to check for duplicate column names
        columnNames = set()
        for table in tables:
            # check if table name is string and isn't duplicated
            if not isinstance(table[0], str):
                raise TypeError()
            if not re.match(r"[a-zA-Z][a-zA-Z0-9_]*", table[0]) or table[0] in tableNames:
                raise ValueError()
            tableNames.add(table[0])
            columnNames.clear()
            # print(table[0])
            for column in table[1]:
                # check if column name is string
                if not isinstance(column[0], str):
                    raise TypeError()
                if not re.match(r"[a-zA-Z][a-zA-Z0-9_]*", column[0]) or column[0] == 'id':
                    raise ValueError()
                # check if column value is set to str, int, float, or a foreign key, and is not a duplicate
                if column[0] in columnNames \
                        or (column[1] not in (str, float, int) and not isinstance(column[1], str)):
                    raise ValueError()
                # check if the foreign key exists and is not self-referencing
                if isinstance(column[1], str) and (column[1] not in tableNames or column[1] == table[0]):
                    raise IntegrityError()
                columnNames.add(column[0])
        self._tables = tables
        self._tableNames = tableNames

    def connect(self, host, port):
        assert(self._socket is None)
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.connect((host, int(port)))

        code = response(self._socket)
        if code == OK:
            return True
        elif code == SERVER_BUSY:
            self._socket.close()
            self._socket = None
            return False
        else:
            raise PacketError("Unexpected code %d during connect()" % code)

    def close(self):
        if self._socket is None:
            return
        request(self._socket, EXIT, 0)
        self._socket.close()
        self._socket = None

    def __getTableIndex(self, tableName):
        tables = self._tables
        tableIndex = None
        if not isinstance(tableName, str):
            raise PacketError()
        for index, table in enumerate(tables):
            if table[0] == tableName:
                tableIndex = index
                break
        if tableIndex is None:
            raise PacketError()
        return tableIndex

    def __getColumnIndex(self, tableIndex, columnName):
        tables = self._tables
        columnIndex = None
        #print("column name = ", columnName)
        if not isinstance(columnName, str):
            raise PacketError()
        for index, columns in enumerate(tables[tableIndex]):
            #print("columns = ", columns)
            #print("index =", index)
            for columnIndex, columnName in enumerate(columns):
                #print("columnIndex = ", columnIndex)
                #print("columnName = ", columnName)
                if columnIndex == columnName:
                    return columnIndex
        if columnIndex is None:
            raise PacketError()
        print("column Index = ", columnIndex)
        return columnIndex

    def __str__(self):
        tables = self._tables
        # used to prettify printing for primitive types
        typesDict = {str: 'string', int: 'integer', float: 'float'}
        # the string returned by function
        tablesString = ''
        for table in tables:
            tablesString += "%s {\n" % (table[0])
            for column in table[1]:
                columnType = column[1]
                if column[1] in typesDict:
                    columnType = typesDict[column[1]]
                tablesString += "%s: %s;\n" % (column[0],  columnType)
            tablesString += '}\n'
        return tablesString

    def insert(self, table_name, values):
        tables = self._tables
        tableIndex = self.__getTableIndex(table_name)
        if len(values) != len(tables[tableIndex][1]):
            raise PacketError()
        for index, column in enumerate(tables[tableIndex][1]):
            if (isinstance(column[1], str) and not isinstance(values[index], int)) \
                    or (not isinstance(column[1], str) and not isinstance(values[index], column[1])):
                raise PacketError()
        request(self._socket, INSERT, table_nr=tableIndex+1,
                columns=tables[tableIndex][1], values=values)
        return response(self._socket, INSERT)

    def update(self, table_name, pk, values, version=0):
        tables = self._tables
        tableIndex = self.__getTableIndex(table_name)
        if not isinstance(pk, int):
            raise PacketError()
        if not isinstance(version, int):
            raise PacketError()
        if len(values) != len(tables[tableIndex][1]):
            raise PacketError()
        for index, column in enumerate(tables[tableIndex][1]):
            if (isinstance(column[1], str) and not isinstance(values[index], int)) \
                    or (not isinstance(column[1], str) and not isinstance(values[index], column[1])):
                raise PacketError()
        request(self._socket, command=UPDATE, table_nr=tableIndex+1,
                columns=tables[tableIndex][1], values=values, version=version, pk=pk)
        return response(self._socket, command=UPDATE)

    def drop(self, table_name, pk):
        if not isinstance(pk, int):
            raise PacketError()
        tableIndex = self.__getTableIndex(table_name)
        request(self._socket, DROP, tableIndex+1, pk=pk)
        response(self._socket, DROP)

    def get(self, table_name, pk):
        if not isinstance(pk, int):
            raise PacketError()
        tableIndex = self.__getTableIndex(table_name)
        request(self._socket, GET, tableIndex+1, pk=pk)
        return response(self._socket, GET)

    def scan(self, table_name, op, column_name=None, value=None):
        if not isinstance(op, int):
            raise PacketError()
        tableIndex = self.__getTableIndex(table_name)
        newValue = value
        if(op == AL):
            columnIndex = 0
            newValue.type = 0
            newValue.size = 0
        else:
            tableIndex = self.__getTableIndex(table_name)
            columnIndex = self.__getColumnIndex(tableIndex, column_name)
        request(self._socket, command=SCAN, table_nr=tableIndex, op=op,
                columnNumber=columnIndex, value=newValue)
        return response(self._socket, SCAN)
