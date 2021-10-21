#!/usr/bin/python3
#
# field.py
#
# Definitions for all the field types in ORM layer
#
initMembers = ["blank", "default", "choices", "table"]


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

    def __setattr__(self, name, value):
        # print("name = ", name)
        if name in initMembers:
            # print("\n value = ", value)
            # print("type value = ", type(value))
            self.__dict__[name] = value
        else:
            if isinstance(value, int):
                self.__dict__[name] = value
            else:
                raise ValueError
    # def __str__(self):
    #     return "default: {} blank: {} choices: {} ".format(self.default, self.blank, self.choices)


class Float:
    def __init__(self, blank=False, default=0.0, choices=None):
        self.blank = blank
        self.choices = choices
        if type(default) not in [float, int] and callable(default) == False:
            raise TypeError

        if choices is not None:
            if blank == True and default not in choices:
                raise TypeError
            for choice in choices:
                if type(choice) not in [float, int]:
                    raise TypeError

        if callable(default):
            self.default = float(default())
        else:
            self.default = float(default)

    # def __str__(self):
    #     return "default: {} blank: {} choices: {} ".format(self.default, self.blank, self.choices)
    def __setattr__(self, name, value):
        # print("name = ", name)
        if name in initMembers:
            # print("\n value = ", value)
            # print("type value = ", type(value))
            self.__dict__[name] = value
        else:
            if isinstance(value, float, int):
                self.__dict__[name] = float(value)
            else:
                raise ValueError


class String:
    def __init__(self, blank=False, default="", choices=None):
        self.blank = blank
        self.choices = choices
        # print("string default = ", default)
        # print("is callable = ", callable(default))
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

    def __setattr__(self, name, value):
        if name in initMembers:
            # print("\n value = ", value)
            # print("type value = ", type(value))
            self.__dict__[name] = value
        else:
            print("name = ", name)
            if isinstance(value, str) or (self.choices is not None and value in self.choices):
                self.__dict__[name] = value
            else:
                raise ValueError

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


class DateTime:
    implemented = False

    def __init__(self, blank=False, default=None, choices=None):
        pass


class Coordinate:
    implemented = False

    def __init__(self, blank=False, default=None, choices=None):
        pass
