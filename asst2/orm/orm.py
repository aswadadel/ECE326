#!/usr/bin/python3
#
# orm.py
#
# Definition for setup and export function
#

# if this errors out again put the asst2.orm.table back in place of orm.table
# the tester was weird with this one
from orm.table import MetaTable
from .easydb import Database

# Return a database object that is initialized, but not yet connected.
#   database_name: str, database name
#   module: module, the module that contains the schema


def setup(database_name, module):
    # Check if the database name is "easydb".
    if database_name != "easydb":
        raise NotImplementedError("Support for %s has not implemented" % (
            str(database_name)))

    schema = getSchema(module)
    return Database(schema)
# note: the export function is defined in __init__.py


def getSchema(module):
    schema = []

    for tableName in MetaTable.tables:
        columns = []
        savedType = None
        tableItems = MetaTable.tables[tableName].__dict__
        if tableName == "Table":
            continue
        for columnName, columnType in tableItems.items():
            if("__" in columnName or "column" in columnName or "field" in columnName):
                pass
            else:
                # print(columnType)
                if 'Integer' in str(columnType):
                    savedType = int
                elif 'Float' in str(columnType):
                    savedType = float
                elif 'String' in str(columnType):
                    savedType = str
                elif 'Foreign' in str(columnType):
                    if columnType.table is not None:
                        savedType = columnType.table.__name__
                # elif 'Coordinate in str(columnType):
                # elif 'DateTime in str(columnType):
                data = (columnName, savedType)
                # print(data)
                columns.append(data)
        schema.append((tableName, tuple(columns)))
    print(schema)
    return schema


def printTable(name):
    return
