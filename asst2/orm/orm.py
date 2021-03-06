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
    # print(schema)
    db = Database(schema)
    return db
# note: the export function is defined in __init__.py


def getSchema(module):
    schema = []

    for tableName in MetaTable.tables:
        columns = []
        savedType = None
        if tableName == "Table":
            continue
        for columnName, columnType in vars(MetaTable.tables[tableName]).items():
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
                    # print('here 1')
                    if columnType.table is not None:
                        savedType = columnType.table.__name__
                    # print('here 2')
                elif 'Coordinate' in str(columnType):
                    columns.append((columnName+'_lat', float))
                    columns.append((columnName+'_lon', float))
                    continue
                elif 'DateTime' in str(columnType):
                    columns.append((columnName, float))
                    continue
                data = (columnName, savedType)
                if savedType != None:
                    columns.append(data)
        schema.append((tableName, tuple(columns)))
    return schema
