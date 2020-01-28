#!/usr/bin/env python3
"""
Database object reading

This file is part of pg_seldump.
"""

import logging
from functools import lru_cache

import psycopg2
from psycopg2.extras import NamedTupleCursor

from .consts import DUMPABLE_KINDS, PG_KINDS
from .exceptions import DumpError

logger = logging.getLogger("seldump.dumping")


class DbReader:
    """
    Wrap a database connection and allow to perform queries on the database
    """

    def __init__(self, dsn):
        self.dsn = dsn

    @property
    @lru_cache(maxsize=1)
    def connection(self):
        """
        Return the database connection wrapped by the object
        """
        logger.debug("connecting to '%s'", self.dsn)
        try:
            cnn = psycopg2.connect(self.dsn, cursor_factory=NamedTupleCursor)
        except Exception as e:
            raise DumpError("error connecting to the database: %s" % e)

        cnn.autocommit = True
        return cnn

    def cursor(self):
        return self.connection.cursor()

    def get_objects_to_dump(self):
        """
        Return the list of dumpable objects in the database
        """
        with self.cursor() as cur:
            # The list of all the objects of a schema to include in the dump.
            #
            # Certain objects don't have a state (e.g. views) or are internal
            # (e.g. toast tables) so there is nothing to include in a data-only
            # dump (DUMPABLE_KINDS).
            #
            # If an object belongs to an extension (i.e. the pg_class ->
            # pg_depend -> pg_extension join finds a record), usually it must
            # not be dumped.
            #
            # However extensions can configure certain objects to be dumped. If
            # so their entry in 'extcondition' will we not null: it can be an
            # empty string to say all the table must be dumped, or a 'where'
            # clause specifying what records to dump.
            #
            # https://www.postgresql.org/docs/11/extend-extensions.html
            cur.execute(
                """
                select * from (
                select
                    n.nspname as schema,
                    r.relname as name,
                    r.relkind as kind,
                    e.extname as extension,
                    -- equivalent of
                    -- extcondition[array_position(extconfig, r.oid)]
                    -- but array_position not available < PG 9.5
                    (
                        select extcondition[row_number]
                        from (
                            select unnest, row_number() over ()
                            from (select unnest(extconfig)) t0
                        ) t1
                        where unnest = r.oid
                    ) as condition,
                    pg_catalog.format('%%I.%%I', n.nspname, r.relname)
                        as escaped
                from pg_class r
                join pg_namespace n on n.oid = r.relnamespace
                    and n.nspname !~ '^pg_'
                    and n.nspname <> 'information_schema'
                left join pg_depend d on d.objid = r.oid and d.deptype = 'e'
                left join pg_extension e on d.refobjid = e.oid
                where r.relkind = any(%(stateless)s)
                order by r.relname
                ) x
                where extension is null
                or condition is not null
                """,
                {"stateless": list(DUMPABLE_KINDS)},
            )

            # Replace the kind from the single letter in pg_catalog to a
            # more descriptive string.
            return [r._replace(kind=PG_KINDS[r.kind]) for r in cur]

    def get_table_attributes(self, escaped_name):
        """
        Reurn the list of fields on a table
        """
        with self.cursor() as cur:
            # Select the list of fields in a table
            # attnum gives their order; attnum < 0 are system columns
            # attisdropped flags a dropped column.
            cur.execute(
                """
                select attname as name, quote_ident(attname) as escaped
                from pg_attribute
                where attrelid = %s::regclass
                and attnum > 0 and not attisdropped
                order by attnum
                """,
                (escaped_name,),
            )
            return cur.fetchall()

    def get_sequence_value(self, escaped_name):
        """
        Return the last value from a sequence
        """
        with self.cursor() as cur:
            cur.execute("select last_value from %s" % escaped_name)
            return cur.fetchone()[0]

    def copy_out(self, expression, outfile, table_name=None):
        """
        Copy the data from an expression into an output file
        """
        with self.cursor() as cur:
            try:
                cur.copy_expert("copy %s to stdout" % expression, outfile)
            except psycopg2.DatabaseError as e:
                whence = "table %s" % table_name if table_name else expression
                raise DumpError("failed to copy from %s: %s" % (whence, e))
