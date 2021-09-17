#!/usr/bin/python3
#
# easydb.py
#
# Definition for the Database class in EasyDB client
#

import socket
from .packet import EXIT, OK, SERVER_BUSY, request, response
from .exception import PacketError

class Database:
    def __repr__(self):
        return "<EasyDB Database object>"

    def __init__(self, tables):    
        self._socket = None
        # TODO: implement me
        
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
        request(self._socket)

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
                        
