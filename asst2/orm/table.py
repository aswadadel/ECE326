#!/usr/bin/python3
#
# table.py
#
# Definition for an ORM database table and its metaclass
#

# metaclass of table
# Implement me or change me. (e.g. use class decorator instead)
from typing import OrderedDict
from orm import field


class MetaTable(type):
    reservedWords = ['pk', 'version', 'save', 'delete']
    tables = OrderedDict()

    def __init__(cls, name, bases, attrs):
        if name in MetaTable.tables:
            raise AttributeError

        # add tables to dict
        MetaTable.tables[name] = cls
        cls.column = []
        cls.field = []

        for attribute in attrs:
            if isinstance(attrs[attribute], (field.Integer, field.Float, field.String, field.Foreign)):
                if attribute in MetaTable.reservedWords or "_" in attribute:
                    raise AttributeError
                else:
                    cls.field.append(attrs[attribute])
                    cls.column.append(attribute)

    def __prepare__(cls, name):
        return OrderedDict()

    # Returns an existing object from the table, if it exists.
    #   db: database object, the database to get the object from
    #   pk: int, primary key (ID)
    def get(cls, db, pk):
        obj, version = db.get(cls.__name__, pk)
        return None

    # Returns a list of objects that matches the query. If no argument is given,
    # returns all objects in the table.
    # db: database object, the database to get the object from
    # kwarg: the query argument for comparing
    def filter(cls, db, **kwarg):
        return list()

    # Returns the number of matches given the query. If no argument is given,
    # return the number of rows in the table.
    # db: database object, the database to get the object from
    # kwarg: the query argument for comparing
    def count(cls, db, **kwarg):
        return -1

# table class
# Implement me.


class Table(object, metaclass=MetaTable):

    def __init__(self, db, **kwargs):
        # object /17
        self.pk = None      # id (primary key)
        self.version = None  # version
        # needed for save
        self.db = db

        # setting values for object
        for column in self.column:
            fieldValue = getattr(type(self), column)
            if column not in kwargs:
                print("\n column = ", column)
                if fieldValue.blank == False:
                    raise AttributeError
                else:
                    setattr(self, column, fieldValue.default)
            else:
                setattr(self, column, kwargs[column])
        return
        # FINISH ME

    # Save the row by calling insert or update commands.
    # atomic: bool, True for atomic update or False for non-atomic update
    def save(self, atomic=True):
        # check if it exists already
        # /13
        return

    # Delete the row from the database.
    def delete(self):
        return
