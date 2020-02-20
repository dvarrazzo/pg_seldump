#!/usr/bin/env python3

import logging
from functools import lru_cache

import psycopg2
from psycopg2.extras import NamedTupleCursor

from .exceptions import DumpError
from .consts import DUMPABLE_KINDS, PG_KINDS

logger = logging.getLogger("seldump.database")


class DbReader:
    def __init__(self, dsn):
        self.dsn = dsn

    @property
    @lru_cache(maxsize=1)
    def connection(self):
        logger.debug("connecting to '%s'", self.dsn)
        try:
            cnn = psycopg2.connect(self.dsn, cursor_factory=NamedTupleCursor)
        except Exception as e:
            raise DumpError("error connecting to the database: %s" % e)

        cnn.autocommit = True
        return cnn

    def cursor(self):
        return self.connection.cursor()

    def obj_as_string(self, obj):
        """
        Convert a `psycopg.sql.Composable` object to string
        """
        return obj.as_string(self.connection)

    def get_schemas(self):
        logger.debug("looking for schemas")
        with self.cursor() as cur:
            # The system catalogs are 'information_schema' and the ones with
            # 'pg_' prefix: those don't contain any user-defined object.
            cur.execute(
                """
                select n.nspname as name
                from pg_catalog.pg_namespace n
                where n.nspname !~ '^pg_'
                and n.nspname <> 'information_schema'
                order by n.nspname
                """
            )
            rv = [r.name for r in cur]
            logger.debug("found %d schemas", len(rv))
            return rv

    def get_objects_to_dump(self, schema):
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
                left join pg_depend d on d.objid = r.oid and d.deptype = 'e'
                left join pg_extension e on d.refobjid = e.oid
                where r.relkind = any(%(stateless)s)
                and n.nspname = %(schema)s
                order by r.relname
                ) x
                where extension is null
                or condition is not null
                """,
                {"schema": schema, "stateless": list(DUMPABLE_KINDS)},
            )

            # Replace the kind from the single letter in pg_catalog to a
            # more descriptive string.
            return [r._replace(kind=PG_KINDS[r.kind]) for r in cur]

    def get_columns(self, table_escaped):
        """
        Reture the list of columns in a relation
        """
        with self.cursor() as cur:
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
                (table_escaped,),
            )
            return cur.fetchall()

    def get_sequence_value(self, sequence_escaped):
        """
        Return the last value of a sequence.
        """
        with self.cursor() as cur:
            cur.execute("select last_value from %s" % sequence_escaped)
            val = cur.fetchone()[0]
            return val

    def copy(self, stmt, file):
        """
        Run a copy statement.
        """
        with self.cursor() as cur:
            cur.copy_expert(stmt, file)
