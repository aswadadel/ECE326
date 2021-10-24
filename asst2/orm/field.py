#!/usr/bin/python3
#
# field.py
#
# Definitions for all the field types in ORM layer
#
from datetime import datetime

import orm

class Undefined:
    pass

class Integer:
    def __init__(self, blank=False, default=None, choices=None):
        if default is not None:
            if type(default) is not int:
                raise TypeError
            self.blank = True
            self.default = int(default)
        elif blank is True:
            self.blank = True
            self.default = int(0)
        else:
            self.blank = False

        if choices is not None:
            for choice in choices:
                if type(choice) is not int:
                    raise TypeError

        self.choices = choices
    def __set_name__(self, owner, name):
        self.name = name
    def __get__(self, obj, type=None):
        return int(obj.__dict__.get(self.name))
    def __set__(self, obj, value):
        finalValue = None
        if type(value) not in [int, Undefined]:
            raise TypeError
        if type(value) is Undefined:
            if self.blank is True:
                finalValue = self.default
            else:
                raise AttributeError
        else:
            if self.choices is None:
                finalValue = value
            else:
                if value not in self.choices:
                    raise ValueError
                else:
                    finalValue = value
        obj.__dict__[self.name] = int(finalValue)

class Float:
    def __init__(self, blank=False, default=None, choices=None):
        if default is not None:
            if type(default) not in [float, int]:
                raise TypeError
            self.blank = True
            self.default = float(default)
        elif blank is True:
            self.blank = True
            self.default = float(0)
        else:
            self.blank = False

        if choices is not None:
            for choice in choices:
                if type(choice) not in [float, int]:
                    raise TypeError

        self.choices = choices
    def __set_name__(self, owner, name):
        self.name = name
    def __get__(self, obj, type=None):
        return float(obj.__dict__.get(self.name))
    def __set__(self, obj, value):
        finalValue = None
        if type(value) not in [float, int, Undefined]:
            raise TypeError
        if type(value) is Undefined:
            if self.blank is True:
                finalValue = self.default
            else:
                raise AttributeError
        else:
            if self.choices is None:
                finalValue = value
            else:
                if value not in self.choices:
                    raise ValueError
                else:
                    finalValue = value
        obj.__dict__[self.name] = float(finalValue)

class String:
    def __init__(self, blank=False, default=None, choices=None):
        if default is not None:
            if type(default) is not str:
                raise TypeError
            self.blank = True
            self.default = default
        elif blank is True:
            self.blank = True
            self.default = ""
        else:
            self.blank = False

        if choices is not None:
            for choice in choices:
                if type(choice) is not str:
                    raise TypeError

        self.choices = choices

    def __set_name__(self, owner, name):
        self.name = name

    def __get__(self, obj, type=None):
        return obj.__dict__.get(self.name)

    def __set__(self, obj, value):
        finalValue = None
        if type(value) not in [str, Undefined]:
            raise TypeError
        if type(value) is Undefined:
            if self.blank is True:
                finalValue = self.default
            else:
                raise AttributeError
        else:
            if self.choices is None:
                finalValue = value
            else:
                if value not in self.choices:
                    raise ValueError
                else:
                    finalValue = value
        obj.__dict__[self.name] = finalValue

class Foreign:
    def __init__(self, table=None, blank=False):
        if table is None:
            raise TypeError('here')
        self.blank = blank
        self.table = table

    def __set_name__(self, owner, name):
        self.name = name

    def __get__(self, obj, type=None):
        return obj.__dict__.get(self.name)

    def __set__(self, obj, value):
        if self.blank is False and value is None:
            raise TypeError('here2')
        elif self.blank is True and value is None:
            obj.__dict__[self.name] = value
        elif not isinstance(value, orm.Table):
            raise TypeError('here3')
        else:
            obj.__dict__[self.name] = value

class DateTime:
    implemented = False

    # def __init__(self, blank=False, default=None, choices=None):
    #     if default is not None:
    #         if type(default) is not datetime and callable(default) is False:
    #             raise TypeError
    #         self.blank = True
    #         self.default = default
    #     elif blank is True:
    #         self.blank = True
    #         self.default = datetime.utcfromtimestamp(0)
    #     else:
    #         self.blank = False

    #     if choices is not None:
    #         for choice in choices:
    #             if type(choice) is not datetime:
    #                 raise TypeError
    #     self.choices = choices
    # def __set_name__(self, owner, name):
    #     self.name = name
    # def __get__(self, obj, type=None):
    #     return obj.__dict__.get(self.name)
    # def __set__(self, obj, value):
    #     finalValue = None
    #     if type(value) not in [datetime, Undefined] and not callable(value):
    #         raise TypeError
    #     if type(value) is Undefined:
    #         if self.blank is True:
    #             finalValue = self.default() if callable(self.default)\
    #                 else self.default
    #         else:
    #             raise AttributeError
    #     else:
    #         if self.choices is None:
    #             finalValue = value
    #         else:
    #             if value not in self.choices:
    #                 raise ValueError
    #             else:
    #                 finalValue = value
    #     obj.__dict__[self.name] = finalValue
    # def __init__(self, blank=False, default=None, choices=None):
    #     pass


class Coordinate:
    implemented = False

    def __init__(self, blank=False, default=None, choices=None):
        pass
