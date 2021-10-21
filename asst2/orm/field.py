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
        if type(default) is not int and callable(default) == False:
            raise TypeError

        if choices is not None:
            if blank == True and default not in choices:
                raise TypeError
            for choice in choices:
                if type(choice) is not int:
                    raise TypeError
        if callable(default):
            self.default = default()
        else:
            self.default = default

    # def __str__(self):
    #     return "default: {} blank: {} choices: {} ".format(self.default, self.blank, self.choices)


class Float:
    def __init__(self, blank=False, default=0.0, choices=None):
        self.blank = blank
        self.choices = choices
        if type(default) is not float and type(default) is not int and callable(default) == False:
            raise TypeError

        if choices is not None:
            if blank == True and default not in choices:
                raise TypeError
            for choice in choices:
                if type(choice) is not float and type(choice) is not int:
                    raise TypeError

        if callable(default):
            self.default = default()
        else:
            self.default = default

    # def __str__(self):
    #     return "default: {} blank: {} choices: {} ".format(self.default, self.blank, self.choices)
    '''
    def __set__(self, instance, value):
        if isinstance(value, float):
            return setattr(instance, self.name, value)
        elif isinstance(value, int):
            return setattr(instance, self.name, float(value))
        elif self.chocies is not None: 
            if value not in self.choices:
                raise ValueError
        else:
            raise TypeError
        return
    def __get__(self, instance, owner):
        return
    '''


class String:
    def __init__(self, blank=False, default="", choices=None):
        self.blank = blank
        self.choices = choices
        #print("string default = ", default)
        #print("is callable = ", callable(default))
        if type(default) is not str and callable(default) == False:
            raise TypeError

        if choices is not None:
            if blank == True and default not in choices:
                raise TypeError
            for choice in choices:
                if type(choice) is not str:
                    raise TypeError

        if callable(default):
            self.default = default()
        else:
            self.default = default

        return

    # def __str__(self):
    #     return "default: {} blank: {} choices: {} ".format(self.default, self.blank, self.choices)


class Foreign:
    def __init__(self, table="", blank=False):
        if(blank == True):
            self.table = table
        elif(blank == False and table == None):
            raise TypeError
        elif (blank == False):
            self.table = table

        self.blank = blank
        return

    # def __str__(self):
    #     return "table: {} blank: {} ".format(self.table, self.blank)


class DateTime:
    implemented = False

    def __init__(self, blank=False, default=None, choices=None):
        pass


class Coordinate:
    implemented = False

    def __init__(self, blank=False, default=None, choices=None):
        pass
