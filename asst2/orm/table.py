#!/usr/bin/python3
#
# table.py
#
# Definition for an ORM database table and its metaclass
#

# metaclass of table
# Implement me or change me. (e.g. use class decorator instead)
from typing import OrderedDict
from orm.easydb.exception import InvalidReference
from orm import field
from .easydb import operator

tables = OrderedDict()


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
    # Note that you must support cascade get, which requires you to convert foreign key references
    # to actual objects, as described in the previous section (save).
    def get(cls, db, pk):
        columnVal, version = db.get(cls.__name__, pk)
        names = []
        values = []
        entries = {}
        for count, colValue in enumerate(columnVal):
            #print("\n count = ", count)
            #print("fieldValue = ", colValue)

            # look up the foreign
            if type(cls.field[count]) == field.Foreign:
                #print("\n foreign detected field was =", cls.column[count])
                # MIGHT BE A SOURCE OF BUGS WITH CUSTOM
                cascTableName = cls.column[count].capitalize()
                cascVal, cascVersion = db.get(
                    cascTableName, colValue)
                #print("cascVal = ", cascVal)
                cascNames = []
                cascValues = []
                cascEntries = {}
                cascObjectType = getattr(cls, cls.column[count]).table

                for colIndex, colName in enumerate(MetaTable.tables[cascTableName].column):
                    #print("name appended = ", colName)
                    #print("value appended = ", cascVal[colIndex])
                    cascNames.append(colName)
                    cascValues.append(cascVal[colIndex])
                # formatting the lists
                for index in range(len(cascValues)):
                    cascEntries[cascNames[index]] = cascValues[index]
                cascObject = cascObjectType(db, **cascEntries)
                names.append(cls.column[count])
                values.append(cascObject)
                pass
            else:
                # not a foreign
                names.append(cls.column[count])
                values.append(colValue)
                pass
            pass
        for i in range(len(values)):
            entries[names[i]] = values[i]
        newTable = cls(db, **entries)
        return newTable

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
            # fieldValue = getattr(type(self), column)
            if column not in kwargs:
                # print("\n column = ", column)
                # print("fieldValue = ", fieldValue)
                # setattr(self, column, fieldValue.default)
                setattr(self, column, field.Undefined())
            else:
                # print("column in kwargs=", column)
                # print("field Value found = ", kwargs[column])
                # setattr(self, column, kwargs[column])
                setattr(self, column, kwargs[column])
        return
        # FINISH ME

    # Save the row by calling insert or update commands.
    # use scan so you dont get errors
    # atomic: bool, True for atomic update or False for non-atomic update
    def save(self, atomic=True):
        version = 0
        entryData = []
        # kinda hacky but it works lol
        tableName = str(type(self)).split(".")[1].replace("'>", "")
        for column in self.column:
            columnValue = getattr(self, column)
            # print("\n columnValue in save =", columnValue)
            # print("\n self type ", type(self))
            # add DateTime and Coordinates when done in here
            if type(columnValue) not in [int, float, str]:
                # do the lookup
                lookupTable = str(type(columnValue))\
                    .split(".")[1].replace("'>", "")
                columnValue

                if(columnValue.pk == None):
                    columnValue.save()
                    pass
                ids = self.db.scan(lookupTable, operator.EQ, "id",
                                   columnValue.pk)
                if len(ids) == 0:
                    raise InvalidReference
                entryData.append(ids[0])
            else:
                entryData.append(columnValue)
        if self.pk is not None:
            # if pk is None then it is not in the database
            if atomic == True:
                version = self.version
            print("data to insert = ", entryData)
            self.version = self.db.update(
                tableName, self.pk, entryData, version)
        else:
            print("data to insert = ", entryData)
            self.pk, self.version = self.db.insert(
                tableName, entryData)
        return

    # Delete the row from the database.
    def delete(self):
        tableName = str(type(self)).split(".")[1].replace("'>", "")
        self.db.drop(tableName, self.pk)
        self.pk = None
        self.version = None
        return
