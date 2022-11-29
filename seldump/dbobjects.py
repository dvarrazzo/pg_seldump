"""
Representation of database objects to handle by the program.

This file is part of pg_seldump.
"""


import re

from psycopg import sql

from . import consts


class DbObject:
    """
    An object in a database
    """

    __slots__ = (
        "oid",
        "schema",
        "name",
        "extension",
        "extcondition",
    )

    _kinds = {}

    kind = None

    def __init__(self, oid, schema, name, extension=None, extcondition=None):
        self.oid = oid
        self.schema = schema
        self.name = name
        self.extension = extension
        self.extcondition = extcondition

    @classmethod
    def from_kind(cls, kind, oid, schema, name, **kwargs):
        # Values from the database or from yaml
        if kind in consts.PG_KINDS:
            kind = consts.PG_KINDS[kind]
        if kind not in cls._kinds:
            raise ValueError("unknown db object kind: %s" % kind)
        return cls._kinds[kind](oid, schema, name, **kwargs)

    def __repr__(self):
        return "<%s %s at 0x%x>" % (self.__class__.__name__, self, id(self))

    def __str__(self):
        return self.escape_idents(self.schema, self.name)

    @property
    def ident(self):
        """The object name as an Identifier"""
        return sql.Identifier(self.schema, self.name)

    @classmethod
    def escape_idents(self, *args):
        """
        Convert tokens into dot-separated string to be safety merged to a query

        It gives a mmore readable but less secure version of ident, because it
        doesn't escape keywords; however it's a string so it can be just
        printed, without requiring a connection as Identifier.to_string() does.
        Use it only to display to humans, not to send to the database.
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
                raise ValueError("the kind %s is already associated to a class" % kind)
            DbObject._kinds[kind] = cls
            cls.kind = kind
            return cls

        return register_


@DbObject.register(consts.KIND_TABLE)
class Table(DbObject):
    """A table in a database."""

    __slots__ = DbObject.__slots__ + (
        "columns",
        "_cols_by_name",
        "fkeys",
        "ref_fkeys",
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.columns = []
        self._cols_by_name = {}
        self.fkeys = []
        self.ref_fkeys = []

    def add_column(self, column):
        if column.name in self._cols_by_name:
            raise ValueError(
                "the table %s has already a column called %s" % (self, column.name)
            )
        self.columns.append(column)
        self._cols_by_name[column.name] = column

    def get_column(self, name):
        return self._cols_by_name.get(name)

    def add_fkey(self, fkey):
        for col in fkey.table_cols:
            # using format because https://github.com/psf/black/issues/1259
            assert (
                col in self._cols_by_name
            ), "column {} in fkey {} is not in the table {}".format(col, fkey, self)
        self.fkeys.append(fkey)

    def get_fkey(self, name):
        for fkey in self.fkeys:
            if fkey.name == name:
                return fkey

    def add_ref_fkey(self, fkey):
        for col in fkey.ftable_cols:
            assert (
                col in self._cols_by_name
            ), "column {} in fkey {} is not in the table {}".format(col, fkey, self)
        self.ref_fkeys.append(fkey)


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
    __slots__ = ("name", "type", "used_sequence_oids")

    def __init__(self, name, type):
        self.name = name
        self.type = type
        self.used_sequence_oids = []

    @property
    def ident(self):
        """The column name as an Identifier"""
        return sql.Identifier(self.name)

    def __repr__(self):
        return "<%s %s at 0x%x>" % (self.__class__.__name__, self, id(self))

    def __str__(self):
        return DbObject.escape_idents(self.name)

    def add_used_sequence(self, seq):
        if not seq.oid:
            raise ValueError("the sequence %s must have an oid" % seq)
        self.used_sequence_oids.append(seq.oid)


class ForeignKey:
    __slots__ = (
        "name",
        "table_oid",
        "table_cols",
        "ftable_oid",
        "ftable_cols",
    )

    def __init__(self, name, table_oid, table_cols, ftable_oid, ftable_cols):
        self.name = name
        self.table_oid = table_oid
        self.table_cols = table_cols
        self.ftable_oid = ftable_oid
        self.ftable_cols = ftable_cols

    @property
    def ident(self):
        """The fkey name as an Identifier"""
        return sql.Identifier(self.name)

    def __repr__(self):
        return "<%s %s at 0x%x>" % (
            self.__class__.__name__,
            self.name,
            id(self),
        )
