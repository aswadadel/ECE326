#!/usr/bin/python3
#
# easydb.py
#
# Definition for the Database class in EasyDB client
#

import socket
from .packet import EXIT, OK, SERVER_BUSY, request, response
from .exception import IntegrityError, PacketError
import re

class Database:
    def __repr__(self):
        return "<EasyDB Database object>"

    def __init__(self, tables):    
        self._socket = None
        self._tables = set()
        try:
            iterator = iter(tables)
        except:
            raise TypeError()
        # try:
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
                # check if column value is set to str, int, float, or a string
                if column[0] in columnNames \
                    or (column[1] not in (str,float,int) and not isinstance(column[1], str)):  
                    raise ValueError()
                if isinstance(column[1], str) and (column[1] not in tableNames or column[1] == table[0]):
                    raise IntegrityError()
                columnNames.add(column[0])
        # except TypeError:
        #     print("TypeError")
        # except ValueError:
        #     print("ValueError")
        # except IntegrityError:
        #     print("IntegrityError")
        # except:
        #     print("IndexError")
                
        
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
            raise PacketError("Unexpected code %d during connect()"%code)

    def close(self):
        if self._socket is None:
            return
        request(EXIT, 0)
        self._socket.close()
        self._socket = None

    def __str__(self):
        # TODO: implement me
        return ""

    def insert(self, table_name, values):
        # TODO: implement me
        pass

    def update(self, table_name, pk, values, version=None):
        # TODO: implement me
        pass

    def drop(self, table_name, pk):
        # TODO: implement me
        pass
        
    def get(self, table_name, pk):
        # TODO: implement me
        pass

    def scan(self, table_name, op, column_name=None, value=None):
        # TODO: implement me
        pass
                        
