#!/usr/bin/python3
#
# orm.py
#
# Definition for setup and export function
#

from .easydb import Database

# Return a database object that is initialized, but not yet connected.
#   database_name: str, database name
#   module: module, the module that contains the schema
def setup(database_name, module):
    # Check if the database name is "easydb".
    if database_name != "easydb":
        raise NotImplementedError("Support for %s has not implemented"%(
            str(database_name)))

    # IMPLEMENT ME
    return Database([]) 

# note: the export function is defined in __init__.py

