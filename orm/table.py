#!/usr/bin/python3
#
# table.py
#
# Definition for an ORM database table and its metaclass
#
from .easydb.packet import operator
from .field import Field
from orm import field
import datetime

# metaclass of table
class MetaTable(type):

    # Set containing all Table names that have been used up to now
    table_names = set()

    def __init__(cls, name, bases, attrs):
        # Check that this table name hasn't been used previously
        if name in MetaTable.table_names:
            raise AttributeError(f"Table name {name} already defined previously")

        # We will maintain a list of the fields of the class (populated below)
        cls.field_names = []

        for (attr_name, attr_val) in attrs.items():
            if issubclass(type(attr_val), field.Field):
                if not (attr_name[0].isalpha() and attr_name.isalnum() and (
                        attr_name not in ["pk", "version", "save", "delete"])):
                    raise AttributeError("Invalid column name {}".format(attr_name))

                # Add the field to the list of field names of the class
                cls.field_names.append(attr_name)

        # Table created - add it to the list of names of created tables
        MetaTable.table_names.add(name)

    # ensure that the order of the fields are preserved
    # order of iteration is the same as order of insertion
    @classmethod
    def __prepare__(mcs, name, bases, **kwargs):
        import collections
        return collections.OrderedDict(kwargs)

    # Returns an existing object from the table, if it exists.
    #   db: database object, the database to get the object from
    #   pk: int, primary key (ID)
    def get(cls, db, pk):
        # The object returned by the db.get() method is an (ordered) tuple representing the values of each element
        # of the object's corresponding struct
        (fetched_object_values, fetched_object_version) = db.get(cls.__name__, pk)
        fetched_object_values_iter = iter(fetched_object_values)

        object_vals_dict = {}
        # for (field_name, field_value) in zip(cls.field_names, fetched_object_values):
        for field_name in cls.field_names:
            field_value = cls.__dict__[field_name].get_field_assignable_obj_from_stream(fetched_object_values_iter)
            object_vals_dict[field_name] = field_value

        # Since this order of values must match the order of the fields in the 'cls' Table, we simply construct the
        # Table object with this specified values passed in to the Table's __init__() method
        fetched_object_orm = cls(db, **object_vals_dict)
        fetched_object_orm.pk = pk
        fetched_object_orm.version = fetched_object_version
        return fetched_object_orm

    # Returns a list of objects that matches the query. If no argument is given,
    # returns all objects in the table.
    # db: database object, the database to get the object from
    # kwarg: the query argument for comparing
    def filter(cls, db, **kwarg):
        # assume there is only one query argument:
        is_date = False
        values = ()

        if kwarg == {}: # no arguments given; return all ids in Table
            op = "al"
        else:
            for k, v in kwarg.items():
                if "__" in k: # get the column name and the operator type
                    col_name, op = k.split("__")
                else: # if no operator type given in key value assume the operator is eq
                    col_name = k
                    op = "eq"
                if isinstance(v, Table): # if the foreign object was given instead of the foreign id
                    value = v.pk
                else:
                    value = v
            values = (value,) if type(value) is not tuple else value

            # Raise AttributeError if either the field does not exist or the operator is not supported
            if op not in ["ne", "gt", "lt", "al", "eq"]:
                raise AttributeError("The operator {} is not supported".format(op))
            if op != "al" and col_name not in [*cls.field_names, "id"]:
                raise AttributeError("The field {} does not exist".format(col_name))    
        
            decomposed_col_names = [col_name] if col_name == "id" else [field[0] for field in cls.__dict__[col_name].get_schema_repr_py()]

        # get the decomposed value if value is of 
        # class datetime.datetime ... TODO: implement in a cleaner way
            if isinstance(value, datetime.datetime):
                is_date = True
                yr_mon_day, hr_to_s = str(value).split(" ")
                yr, mon, day = yr_mon_day.split("-")
                hr, mit, sec_mic = hr_to_s.split(":")
                # case when microsecond whether provided 
                if "." in sec_mic:
                    sec, mic = sec_mic.split(".")
                else: 
                    sec = sec_mic
                    mic = 0
                    values = int(yr), int(mon), int(day), int(hr), int(mit), int(sec), int(mic)

        # map user input op type to easydb op type
        mapping = {"ne": operator.NE, "gt": operator.GT, "lt": operator.LT, "eq": operator.EQ, "al": operator.AL}

        if op == "al":
            # no column name and value are given
            result = db.scan(cls.__name__, mapping[op])
        else:
            # use result to store a list containing 1 or more lists of ids
            results = []
            for col, val in zip(decomposed_col_names, values):
                results.append(db.scan(cls.__name__, mapping[op], col, val))
            # get the intersection (accounts for location and the rest)
            result = list(set(results[0]).intersection(*results)) if len(results) > 1 else results[0]
            
            if is_date: # need to account for dates e.g. if years are equal, compare months, if months are equal, compare days... 
                # since results are stored in the order of year, month, day,... then the first result should give the correct ids
                for res in results:
                    if res != []:
                        result = res
                    break

        # result contains all the ids
        # convert list of ids into list of objects 
        objects = []

        if result is not None:
            for id_ in result:
                objects.append(cls.get(db, id_))
    
        return objects
    
    # Returns the number of matches given the query. If no argument is given, 
    # return the number of rows in the table.
    # db: database object, the database to get the object from
    # kwarg: the query argument for comparing
    def count(cls, db, **kwarg):
        objects = cls.filter(db, **kwarg)
        return len(objects)


# table class
# Implement me.
class Table(object, metaclass=MetaTable):

    def __init__(self, db, **kwargs):
        self.pk = None  # id (primary key)
        self.version = None  # version
        self.target_db = db

        # Write each Field value specified in 'kwargs' into the appropriate Field of the table
        for field_name in self.__class__.field_names:
            setattr(self, field_name, kwargs.get(field_name, None))

    # Save the row by calling insert or update commands.
    # atomic: bool, True for atomic update or False for non-atomic update
    def save(self, atomic=True):
        # Construct a list of the object's Field values
        object_field_values = []
        for field_name in self.__class__.field_names:
            # Special case for Foreign types: we may have to save the referenced object to the database before
            # proceeding with saving the current object
            if isinstance(self.__class__.__dict__[field_name], field.Foreign):
                foreign_object = getattr(self, field_name, self.__class__)
                if foreign_object.pk is None:
                    foreign_object.save()

            # Fetch the list representation of the field's values
            field_values = self.__class__.__dict__[field_name].get_decomposed_values(self, self.__class__)
            # Append these values to the end of the field_values list
            # Note that the field_names are in the correct order as what database.insert() is expecting (i.e. the order
            # matches that of the database schema). Hence, the field values will also be in the correct order
            object_field_values.extend(field_values)


        # If the object isn't currently on the database, insert it into the database
        if self.pk is None:
            self.pk, self.version = self.target_db.insert(self.__class__.__name__, object_field_values)
        # Otherwise, just update the database entry with the current object's values
        elif atomic:
            self.version = self.target_db.update(self.__class__.__name__, self.pk, object_field_values, self.version)
        else:
            self.version = self.target_db.update(self.__class__.__name__, self.pk, object_field_values)

    # Delete the row from the database.
    def delete(self):
        # Request the database to drop this entry
        self.target_db.drop(self.__class__.__name__, self.pk)

        # If the above request completed successfully, set this object's pk and version to None to indicate that
        # they are no longer stored in the database
        self.pk = None
        self.version = None
