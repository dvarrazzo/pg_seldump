#!/usr/bin/env python3
"""
Representation of a database to handle by the program.

This file is part of pg_seldump.
"""

from .dbobjects import Table


class Database:
    def __init__(self):
        self._objects = []
        self._by_oid = {}
        self._by_name = {}

    def clear(self):
        del self._objects
        self._by_oid.clear()
        self._by_name.clear()

    def add_object(self, obj):
        self._objects.append(obj)
        if obj.oid is not None:
            if obj.oid in self._by_oid:
                raise ValueError(
                    "the database already contains an object with oid %s" % obj.oid
                )
            self._by_oid[obj.oid] = obj

        key = (obj.schema, obj.name)
        if key in self._by_name:
            raise ValueError("the database already contains an object called %s" % obj)
        self._by_name[key] = obj

        return obj

    def get(self, schema=None, name=None, oid=None, cls=None):
        """
        Return an object from the database.

        The object can be specified by schema/name or by oid.
        It is possible to specify the class of the object espected.

        Return None if the object is not found or if it is not the right class.
        """
        if (schema is None) != (name is None):
            raise TypeError("you should either specify both schema and name or none")

        if schema is not None:
            rv = self._by_name.get((schema, name))
        elif oid:
            rv = self._by_oid.get(oid)
        else:
            raise TypeError("you should specify either schema/name or oid")

        # Return the object only if the right class
        if rv and cls is not None:
            if not isinstance(rv, cls):
                rv = None

        return rv

    def __iter__(self):
        yield from self._objects

    def get_tables_using_sequence(self, oid):
        rv = []
        for obj in self:
            if not isinstance(obj, Table):
                continue
            for col in obj.columns:
                if oid in col.used_sequence_oids:
                    rv.append((obj, col))

        return rv

    def add_sequence_user(self, seq, table, col_name):
        col = table.get_column(col_name)
        assert col, "no column %s in table %s" % (col_name, table)
        col.add_used_sequence(seq)
