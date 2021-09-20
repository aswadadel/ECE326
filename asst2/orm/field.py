#!/usr/bin/python3
#
# field.py
#
# Definitions for all the field types in ORM layer
#

class Integer:   
    def __init__(self, blank=False, default=None, choices=None):
        pass

class Float: 
    def __init__(self, blank=False, default=None, choices=None):
        pass

class String:
    def __init__(self, blank=False, default=None, choices=None):
        pass

class Foreign:
    def __init__(self, table, blank=False):
        pass

class DateTime:
    implemented = False

    def __init__(self, blank=False, default=None, choices=None):
        pass

class Coordinate:
    implemented = False

    def __init__(self, blank=False, default=None, choices=None):
        pass
