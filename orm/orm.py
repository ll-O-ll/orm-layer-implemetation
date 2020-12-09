#!/usr/bin/python3
#
# orm.py
#
# Definition for setup and export function
#
import orm
from .easydb import Database


# Return a database object that is initialized, but not yet connected.
#   database_name: str, database name
#   module: module, the module that contains the schema
def setup(database_name, module):
    # Check if the database name is "easydb".
    if database_name != "easydb":
        raise NotImplementedError("Support for %s has not been implemented" % (str(database_name)))

    # extract all classes that inherit from orm.table
    tables = ()
    classes = {}  # maps the class name to class reference
    # parse through attributes of schema.py
    for (attr_name, attr_val) in vars(module).items():
        if not attr_name.startswith('__') and attr_name != 'orm' and issubclass(attr_val, orm.Table):
            classes[attr_name] = attr_val

    # build the tuple that should contain the schema
    for class_name, class_ref in classes.items():
        inner_tpl = ()
        for field_name in class_ref.field_names:
            tup = class_ref.__dict__[field_name].get_schema_repr_py()
            inner_tpl += tup

        # The first layer will store the table name, and the second layer will store the schema (inner tuple)
        table = (class_name, inner_tpl)

        tables += (table,)

    database = Database(tables)
    return database

# Return a string which can be read by the underlying database to create the 
# corresponding database tables.
#   database_name: str, database name
#   module: module, the module that contains the schema
def export(database_name, module):
    # Check if the database name is "easydb".
    if database_name != "easydb":
        raise NotImplementedError("Support for %s has not implemented" % (str(database_name)))

    # extract all classes that inherit from orm.table
    schema = ""
    classes = {}  # maps the class name to class reference
    # parse through attributes of schema.py
    for (attr_name, attr_val) in vars(module).items():
        if not attr_name.startswith('__') and attr_name != 'orm' and issubclass(attr_val, module.orm.Table):
            classes[attr_name] = attr_val

    # create a mapping from fields to C types
    mapping = {str: "string", int: "integer", float: "float"}

    for class_name, class_ref in classes.items():
        table = f'{class_name}' + "{ "
        cols_str = ""
        for field_name in class_ref.field_names:
            tup = class_ref.__dict__[field_name].get_schema_repr_py()
            for item in tup:
                cols_str += f'{item[0]} : {mapping.get(item[1], item[1])} ; '
        cols_str += "} "
        schema += table + cols_str

    return schema.strip()
