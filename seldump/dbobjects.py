"""
Representation of database objects to handle by the program.

This file is part of pg_seldump.
"""


import re

from . import consts


class DbObject:
    """
    An object in a database
    """

    __slots__ = (
        "oid",
        "schema",
        "name",
        "escaped",
        "extension",
        "extcondition",
    )

    _kinds = {}

    kind = None

    def __init__(
        self,
        oid,
        schema,
        name,
        escaped=None,
        extension=None,
        extcondition=None,
    ):
        self.oid = oid
        self.schema = schema
        self.name = name
        self.escaped = escaped or self.escape_idents(schema, name)
        self.extension = extension
        self.extcondition = extcondition

    @classmethod
    def from_kind(cls, kind, oid, schema, name, escaped=None, **kwargs):
        # Values from the database or from yaml
        if kind in consts.PG_KINDS:
            kind = consts.PG_KINDS[kind]
        if kind not in cls._kinds:
            raise ValueError("unknow db object kind: %s" % kind)
        return cls._kinds[kind](oid, schema, name, escaped=escaped, **kwargs)

    def __repr__(self):
        return "<%s %s at 0x%x>" % (
            self.__class__.__name__,
            self.escaped,
            id(self),
        )

    def __str__(self):
        return self.escaped

    @classmethod
    def escape_idents(self, *args):
        """
        Convert tokens into dot-separated string to be safety merged to a query
        """
        rv = []
        for arg in args:
            if re.match(r"^[a-z][a-z0-9_]*$", arg):
                rv.append(arg)
            else:
                arg = arg.replace('"', '""')
                rv.append('"%s"' % arg)

        return ".".join(rv)

    @staticmethod
    def register(kind):
        """
        Decorator to allow a class to be instantiated by `from_kind()`.
        """

        def register_(cls):
            if kind in DbObject._kinds:
                raise ValueError(
                    "the kind %s is already associated to a class" % kind
                )
            DbObject._kinds[kind] = cls
            cls.kind = kind
            return cls

        return register_


@DbObject.register(consts.KIND_TABLE)
class Table(DbObject):
    """A table in a database."""

    __slots__ = DbObject.__slots__ + ("_columns", "_cols_by_name",)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._columns = []
        self._cols_by_name = {}

    def add_column(self, column):
        if column.name in self._cols_by_name:
            raise ValueError(
                "the table %s has already a column called %s"
                % (self, column.name)
            )
        self._columns.append(column)
        self._cols_by_name[column.name] = column

    @property
    def columns(self):
        return self._columns[:]

    def get_column(self, name):
        return self._cols_by_name.get(name)


@DbObject.register(consts.KIND_PART_TABLE)
class PartitionedTable(Table):
    """A partitioned table in a database."""

    # TODO: pretty untested


@DbObject.register(consts.KIND_SEQUENCE)
class Sequence(DbObject):
    """A sequence table in a database."""


@DbObject.register(consts.KIND_MATVIEW)
class MaterializedView(DbObject):
    """A materialized view in a database."""


class Column:
    __slots__ = ("name", "type", "escaped", "used_sequence_oids")

    def __init__(self, name, type, escaped=None):
        self.name = name
        self.type = type
        self.escaped = escaped or DbObject.escape_idents(name)
        self.used_sequence_oids = []

    def __repr__(self):
        return "<%s %s at 0x%x>" % (
            self.__class__.__name__,
            self.escaped,
            id(self),
        )

    def __str__(self):
        return self.escaped

    def add_used_sequence(self, seq):
        if not seq.oid:
            raise ValueError("the sequence %s must have an oid" % seq)
        self.used_sequence_oids.append(seq.oid)
