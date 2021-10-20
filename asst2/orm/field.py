#!/usr/bin/python3
#
# field.py
#
# Definitions for all the field types in ORM layer
#


class Integer:
    def __init__(self, blank=False, default=0, choices=None):
        self.blank = blank
        self.choices = choices
        if callable(default):
            self.default = default()
        elif default is not 0:
            if type(default) is not int:
                raise TypeError
        else:
            self.default = default

        if choices is not None:
            if blank == True and default not in choices:
                raise TypeError
            for choice in choices:
                if type(choice) is not int:
                    raise TypeError


class Float:
    def __init__(self, blank=False, default=0.0, choices=None):
        self.blank = blank
        self.choices = choices
        if callable(default):
            self.default = default()
        elif default is not 0.0:
            if type(default) is not float and type(default) is not int:
                raise TypeError
        else:
            self.default = default
        if choices is not None:
            if blank == True and default not in choices:
                raise TypeError
            for choice in choices:
                if type(choice) is not float and type(choice) is not int:
                    raise TypeError


class String:
    def __init__(self, blank=False, default="", choices=None):
        self.blank = blank
        self.choices = choices
        if callable(default):
            self.default = default()
        elif default is not "":
            if type(default) is not str:
                raise TypeError
        else:
            self.default = default
        if choices is not None:
            if blank == True and default not in choices:
                raise TypeError
            for choice in choices:
                if type(choice) is not str:
                    raise TypeError

        return


class Foreign:
    def __init__(self, table=None, blank=False):
        if(blank == True):
            self.table = None
            self.blank = True
        elif(blank == False and table == None):
            raise TypeError
        else:
            self.table = table
            self.blank = False
        return


class DateTime:
    implemented = False

    def __init__(self, blank=False, default=None, choices=None):
        pass


class Coordinate:
    implemented = False

    def __init__(self, blank=False, default=None, choices=None):
        pass
