#!/usr/bin/python3
#
# fields.py
#
# Definitions for all the fields in ORM layer
#
import datetime


class Field:
    def __init__(self, blank=False, default=None, choices=None, base_type=None, *base_type_init_args):
        if base_type is None:
            raise AttributeError("Must specify base_type when constructing Field using Field.__init__()")
        self.base_type = base_type

        # Determine whether this field can be left blank
        # Note: if a default value is specified, the parameter 'blank' is ignored and it is assumed that the field
        # can be left blank
        self.blank_allowed = blank or (default is not None)

        # Determine the default value to use for this type based on what is specified in the 'default' parameter
        if default is None:
            if len(base_type_init_args) == 0:
                self.default = base_type()
            else:
                self.default = base_type(*base_type_init_args)
        elif type(default) == base_type:
            self.default = default
        elif callable(default):
            field_default = default()
            if type(field_default) == base_type:
                self.default = field_default
            else:
                # A callable for the default value is specified but it returns of the wrong type, so raise an exception
                raise TypeError("Wrong type returned by default callable")
        else:
            # A default value is specified but it is of the wrong type, so raise an exception
            raise TypeError("Wrong type for default")

        if choices is not None:
            # Ensure that all the specified choices are of the correct type
            for choice in choices:
                if type(choice) != base_type:
                    raise TypeError("Wrong type for choice {} in choices".format(choice))
        self.choices = choices

        # Corner case to check: is the default value a member of the choices list?
        if self.blank_allowed and (self.choices is not None) and (self.default not in self.choices):
            raise TypeError("Invalid default value - not a valid choice")

    def __set_name__(self, owner, name):
        self.attr_name = "_" + name

    def __get__(self, instance, owner):
        return getattr(instance, self.attr_name)

    def __set__(self, instance, value):
        # Depending on what value is specified and whether the field can be blank, determine what value to use
        # when writing to the instance's attribute (this value is stored in value_to_set)
        if value is None:
            if self.blank_allowed:
                value_to_set = self.default
            else:
                raise AttributeError("Cannot leave field {} blank".format(self.attr_name[1:]))
        elif type(value) == self.base_type:
            value_to_set = value
        else:
            raise TypeError("Setting {}: expecting {}, got {}".format(self.attr_name[1:], self.base_type, type(value)))

        # Ensure that the value_to_set is a valid value (i.e. is one of the specified choices)
        if (self.choices is not None) and (value_to_set not in self.choices):
            raise ValueError("Invalid value {} for field {}".format(value_to_set, self.attr_name[1:]))

        setattr(instance, self.attr_name, value_to_set)

    # Returns a tuple of all schema tuples required to represent this field
    # The returned schema is built using Python types
    def get_schema_repr_py(self):
        return ((self.attr_name[1:], self.base_type),)

    # Returns a tuple of all primitive values required to represent the data stored in this field
    # The ordering of these values would match that of the schema returned by get_schema_repr_py()
    def get_decomposed_values(self, instance, owner):
        return (self.__get__(instance, owner), )

    # Takes an iterable and fetches the items needed from it to construct an object of the Field type
    # In a sense, it does the opposite of get_decomposed_values() since it takes the decomposed values (in an iterable)
    # and returns an object that can be used to initialize the Field
    def get_field_assignable_obj_from_stream(self, iterator):
        return next(iterator)


class Integer(Field):
    def __init__(self, blank=False, default=None, choices=None):
        super().__init__(blank, default, choices, int)


class Float(Field):
    def __init__(self, blank=False, default=None, choices=None):
        super().__init__(blank, default, choices, float)

    def __set__(self, instance, value):
        # Special case: Float fields can implicitly convert int to float
        if type(value) == int:
            value = float(value)

        super().__set__(instance, value)


class String(Field):
    def __init__(self, blank=False, default=None, choices=None):
        super().__init__(blank, default, choices, str)


class Foreign(Field):
    def __init__(self, table, blank=False):
        self.blank_allowed = blank
        self.referenced_table_type = table

    def __set__(self, instance, value):
        # Depending on what value is specified and whether the field can be blank, determine what value to use
        # when writing to the instance's attribute (this value is stored in value_to_set)
        if value is None:
            if not self.blank_allowed and (value is None):
                raise AttributeError("Cannot leave field {} blank".format(self.attr_name[1:]))
        # For foreign types, we allow for accessing the referenced row lazily. In this case, we are okay with simply
        # storing the primary key of the referenced row (as an int).
        elif type(value) == self.referenced_table_type:
            # Since the object was passed in directly, we switch to not implementing lazy loading
            instance.is_reference_lazy_loaded = False
        elif type(value) == int and getattr(instance, "is_reference_lazy_loaded", True):
            setattr(instance, "is_reference_lazy_loaded", True)
        else:
            raise TypeError(
                "Setting {}: expected {} (pk) or {}, got {}".format(self.attr_name[1:], int, self.referenced_table_type,
                                                                    type(value)))
        # Store the object itself (whether it is the pk or the object itself)
        setattr(instance, self.attr_name, value)

    def __get__(self, instance, owner):
        # Get whatever information about the referenced table there is in this field (it could be the pk of the object
        # or the actual object itself)
        reference_to_table = getattr(instance, self.attr_name)

        # If the reference is blank, there is no row to return
        if reference_to_table is None:
            return None

        # If the reference above is the object itself, simply return it
        if not instance.is_reference_lazy_loaded:
            return reference_to_table

        # If the reference above is the pk of the object, fetch the actual object from the database and return it
        return self.referenced_table_type.get(instance.target_db, reference_to_table)

    def get_schema_repr_py(self):
        # The schema for Foreign types includes the name of the referenced table as its underlying type
        return ((self.attr_name[1:], self.referenced_table_type.__name__),)

    # The decomposed value for a Foreign field will return a tuple containing the pk of the object (as opposed to the
    # object itself, which is what __get__() returns).
    def get_decomposed_values(self, instance, owner):
        # Get whatever information about the referenced table there is in this field (it could be the pk of the object
        # or the actual object itself)
        reference_to_table = getattr(instance, self.attr_name)

        # If the reference is blank, there is no row to return
        if reference_to_table is None:
            return (0, )

        # If the reference above is already the pk, just return it
        if instance.is_reference_lazy_loaded:
            return (reference_to_table, )

        # If the reference above is the actual object, extract the pk from this object and return it
        return (reference_to_table.pk, )


class DateTime(Field):
    implemented = True

    def __init__(self, blank=False, default=None, choices=None):
        super().__init__(blank, default, choices, datetime.datetime, 1969, 12, 31, 19, 0, 0, 0)

    def __set__(self, instance, value):
        super().__set__(instance, value)
        setattr(instance, self.year_attr_name, getattr(instance, self.attr_name).year)
        setattr(instance, self.month_attr_name, getattr(instance, self.attr_name).month)
        setattr(instance, self.day_attr_name, getattr(instance, self.attr_name).day)
        setattr(instance, self.hour_attr_name, getattr(instance, self.attr_name).hour)
        setattr(instance, self.minute_attr_name, getattr(instance, self.attr_name).minute)
        setattr(instance, self.second_attr_name, getattr(instance, self.attr_name).second)
        setattr(instance, self.microsecond_attr_name, getattr(instance, self.attr_name).microsecond)

    def __set_name__(self, owner, name):
        super().__set_name__(owner, name)
        self.year_attr_name = self.attr_name + "_year"
        self.month_attr_name = self.attr_name + "_month"
        self.day_attr_name = self.attr_name + "_day"
        self.hour_attr_name = self.attr_name + "_hour"
        self.minute_attr_name = self.attr_name + "_minute"
        self.second_attr_name = self.attr_name + "_second"
        self.microsecond_attr_name = self.attr_name + "_microsecond"

    def get_schema_repr_py(self):
        return (self.year_attr_name[1:], int), \
               (self.month_attr_name[1:], int), \
               (self.day_attr_name[1:], int), \
               (self.hour_attr_name[1:], int), \
               (self.minute_attr_name[1:], int), \
               (self.second_attr_name[1:], int), \
               (self.microsecond_attr_name[1:], int)

    # The decomposed value for a DateTime field returns all of the attributes of the underlying DateTime object,
    # packed into a tuple
    def get_decomposed_values(self, instance, owner):
        return getattr(instance, self.year_attr_name), \
               getattr(instance, self.month_attr_name), \
               getattr(instance, self.day_attr_name), \
               getattr(instance, self.hour_attr_name), \
               getattr(instance, self.minute_attr_name), \
               getattr(instance, self.second_attr_name), \
               getattr(instance, self.microsecond_attr_name)

    # Extract the values from the iterator needed to construct a datetime.datetime object (which, in turn, can be
    # used to construct a DateTime Field object)
    def get_field_assignable_obj_from_stream(self, iterator):
        year = next(iterator)
        month = next(iterator)
        day = next(iterator)
        hour = next(iterator)
        minute = next(iterator)
        second = next(iterator)
        microsecond = next(iterator)

        return datetime.datetime(year, month, day, hour, minute, second, microsecond)


class Coordinate(Field):
    implemented = True

    def __init__(self, blank=False, default=None, choices=None):
        # Determine whether this field can be left blank
        # Note: if a default value is specified, the parameter 'blank' is ignored and it is assumed that the field
        # can be left blank
        self.blank_allowed = blank or (default is not None)

        # Determine the default value to use for this type based on what is specified in the 'default' parameter
        if default is None:
            self.default = (0, 0)
        elif type(default) == tuple and len(default) == 2 and type(default[0]) == float and type(default[1]) == float:
            self.default = default
        elif callable(default):
            coord_default = default()
            if type(coord_default) == tuple and len(coord_default) == 2 and type(coord_default[0]) == float and \
                    type(coord_default[1]) == float:
                self.default = coord_default
            else:
                # A default callable is specified but it returns of the wrong type, so raise an exception
                raise TypeError("Wrong type returned by default() - expected 2-tuple of floats")
        else:
            # A default value is specified but it is of the wrong type, so raise an exception
            raise TypeError("Wrong type for default - expected Tuple of length 2")

        # Parse the choices parameter and ensure that all entries are of the appropriate type
        if choices is not None:
            for coord_choice in choices:
                # Ensure that the specified choice is of the correct type
                if type(coord_choice) != tuple or len(coord_choice) != 2:
                    raise TypeError("Wrong type for choice {} in choices - expected tuple".format(coord_choice))
                if type(coord_choice[0]) != float or type(coord_choice[1]) != float:
                    raise TypeError("Wrong type of values in choice {} - expected float".format(coord_choice))
        self.choices = choices

        # Corner case to check: is the default value a member of the choices list?
        if self.blank_allowed and (self.choices is not None) and (self.default not in self.choices):
            raise TypeError("Invalid default value - not a valid choice")

    def __set_name__(self, owner, name):
        super().__set_name__(owner, name)
        self.latitude_attr_name = self.attr_name + "_latitude"
        self.longitude_attr_name = self.attr_name + "_longitude"

    def __get__(self, instance, owner):
        # Return the two values fetched above as a 2-tuple
        return getattr(instance, self.latitude_attr_name), getattr(instance, self.longitude_attr_name)

    def __set__(self, instance, value):
        if value is None:
            if self.blank_allowed:
                value_to_set = self.default
            else:
                raise AttributeError("Cannot leave field {} blank".format(self.attr_name[1:]))
        elif type(value) == tuple and len(value) == 2 and type(value[0]) == float and type(value[1]) == float:
            value_to_set = value
        else:
            raise TypeError("Setting {}: expecting a 2-tuple of floats, got {}".format(self.attr_name[1:], type(value)))

        # Ensure that value_to_set represents a valid (latitude, longitude) geographical coordinate
        if not self.is_valid_coordinate(value_to_set[0]) or not self.is_valid_coordinate(value_to_set[1]):
            raise ValueError("Latitude value not bounded between -90.0 and 90.0")

        # Ensure that the value_to_set is one of the specified choices)
        if (self.choices is not None) and (value_to_set not in self.choices):
            raise ValueError("Invalid value {} for field {}".format(value_to_set, self.attr_name[1:]))

        setattr(instance, self.latitude_attr_name, value_to_set[0])
        setattr(instance, self.longitude_attr_name, value_to_set[1])

    def get_schema_repr_py(self):
        return (self.latitude_attr_name[1:], float), (self.longitude_attr_name[1:], float)

    # The decomposed value for a Coordinate field consists of the two elements of the Coordinate, stored in a tuple
    def get_decomposed_values(self, instance, owner):
        # Note: __get__() already returns this tuple, so we can just reuse that function
        return self.__get__(instance, owner)

    # Extract the values from the iterator needed to construct a tuple (which, in turn, can be used to construct a
    # Coordinate Field object)
    def get_field_assignable_obj_from_stream(self, iterator):
        latitude = next(iterator)
        longitude = next(iterator)

        return latitude, longitude

    @staticmethod
    def is_valid_coordinate(coord_value):
        return -90.0 <= coord_value <= 90.0
