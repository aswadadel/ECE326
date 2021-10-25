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
from datetime import datetime

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
            if isinstance(attrs[attribute], \
                ( field.DateTime, field.Coordinate, field.Integer, field.Float, field.String, field.Foreign)):
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
        passedLat = False
        coordTemp = None
        fieldIndex = 0
        for count, colValue in enumerate(columnVal):
            #print("\n count = ", count)
            #print("fieldValue = ", colValue)
            # look up the foreign
            if type(cls.field[fieldIndex]) == field.Coordinate:
                if passedLat:
                    passedLat = False
                    names.append(cls.column[fieldIndex])
                    values.append((coordTemp, colValue))
                else:
                    passedLat = True
                    fieldIndex -=1
                    coordTemp = colValue
            elif type(cls.field[fieldIndex]) == field.DateTime:
                names.append(cls.column[fieldIndex])
                values.append(datetime.fromtimestamp(colValue))
            elif type(cls.field[fieldIndex]) == field.Foreign:
                #print("\n foreign detected field was =", cls.column[count])
                # MIGHT BE A SOURCE OF BUGS WITH CUSTOM
                # cascTableName = cls.column[fieldIndex].capitalize()
                # print()
                cascTableName = cls.field[fieldIndex].table.__name__
                cascVal, cascVersion = db.get(
                    cascTableName, colValue)
                #print("cascVal = ", cascVal)
                cascNames = []
                cascValues = []
                cascEntries = {}
                # cascObjectType = getattr(cls, cls.column[count]).table
                cascObjectType = cls.field[fieldIndex].table

                resultIndex = 0
                for colIndex, colName in enumerate(MetaTable.tables[cascTableName].column):
                    cascNames.append(colName)
                    colType = MetaTable.tables[cascTableName].__dict__[colName]
                    if type(colType) is field.Coordinate:
                        cascValues.append((cascVal[resultIndex],cascVal[resultIndex+1]))
                        resultIndex += 1
                    else:
                        cascValues.append(cascVal[resultIndex])
                    resultIndex += 1
                    
                # formatting the lists
                for index in range(len(cascValues)):
                    cascEntries[cascNames[index]] = cascValues[index]
                cascEntries['pk'] = colValue
                cascEntries['version'] = cascVersion
                cascObject = cascObjectType(db, **cascEntries)
                names.append(cls.column[fieldIndex])
                values.append(cascObject)
                pass
            else:
                # not a foreign
                names.append(cls.column[fieldIndex])
                values.append(colValue)
            fieldIndex += 1
        for i in range(len(values)):
            entries[names[i]] = values[i]
        entries['pk'] = pk
        entries['version'] = version
        newTable = cls(db, **entries)
        return newTable

    # Returns a list of objects that matches the query. If no argument is given,
    # returns all objects in the table.
    # db: database object, the database to get the object from
    # kwarg: the query argument for comparing
    def filter(cls, db, **kwarg):
        colName, op, value = None, None, None     
        if len(kwarg) == 0:
            op = operator.AL
        else:
            key, value = list(kwarg.items())[0]
            if not isinstance(value, (int, float, str, tuple, datetime)):
                value = value.pk
            if '__' in key:
                colName, op = key.split('__')
                if op.upper() not in operator.__dict__:
                    raise AttributeError
                op = operator.__dict__[op.upper()]
            else:
                colName, op = key, operator.EQ
        if colName is not None and colName is 'id' and \
            op not in [operator.EQ, operator.NE]:
            raise AttributeError
        pks, pks2 = None, None
        finalPks = None
        result = list()
        if type(value) is tuple:
            pks = db.scan(cls.__name__, op, column_name=colName+'_lat', value=value[0])
            pks2 = db.scan(cls.__name__, op, column_name=colName+'_lon', value=value[1])
            finalPks = list(set(pks) & set(pks2))
        elif type(value) is datetime:
            finalPks = db.scan(cls.__name__, op, column_name=colName, value=value.timestamp())
        else:
            finalPks = db.scan(cls.__name__, op, column_name=colName, value=value)
        for pk in finalPks:
            row = cls.get(db, pk)
            result.append(row)
        return result

    # Returns the number of matches given the query. If no argument is given,
    # return the number of rows in the table.
    # db: database object, the database to get the object from
    # kwarg: the query argument for comparing
    def count(cls, db, **kwarg):
        colName = None
        if len(kwarg) > 0:
            key, value = list(kwarg.items())[0]
            if '__' in key:
                colName, op = key.split('__')
            else:
                colName = key
        if colName is not None and colName not in cls.column and colName != 'id':
            raise AttributeError()
        result = cls.filter(db, **kwarg)
        return len(result)

# table class
# Implement me.


class Table(object, metaclass=MetaTable):

    def __init__(self, db, **kwargs):
        # object /17
        if 'pk' in kwargs:
            self.pk = kwargs['pk']
        else:
            self.pk = None
        if 'version' in kwargs:
            self.version = kwargs['version']
        else:
            self.version = None
        # self.pk = kwargs['pk'] if 'pk' in kwargs else None
        # self.version = kwargs['version'] if 'version' in kwargs else None
        # self.pk = None      # id (primary key)
        # self.version = None  # version
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
        for index, column in enumerate(self.column):
            columnValue = getattr(self, column)
            # print("\n columnValue in save =", columnValue)
            # print("\n self type ", type(self))
            # add DateTime and Coordinates when done in here
            
            fieldType = type(self.field[index])
            if fieldType is field.Coordinate:
                entryData.extend(list(columnValue))
            # if type(columnValue) not in [int, float, str]:
            elif fieldType is field.DateTime:
                entryData.append(columnValue.timestamp())
            elif fieldType is field.Foreign:
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
            self.version = self.db.update(
                tableName, self.pk, entryData, version)
        else:
            # print("data to insert = ", entryData)
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
