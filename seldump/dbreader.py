#!/usr/bin/env python3

"""
Reading object from a PostgreSQL database.

This file is part of pg_seldump.
"""

import logging
from functools import lru_cache

import psycopg2
from psycopg2.extras import NamedTupleCursor

from .consts import DUMPABLE_KINDS, KIND_TABLE, KIND_PART_TABLE, REVKINDS
from .reader import Reader
from .dbobjects import DbObject, Column
from .exceptions import DumpError

logger = logging.getLogger("seldump.dbreader")


class DbReader(Reader):
    def __init__(self, dsn):
        super().__init__()
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

    def load_schema(self):
        for rec in self._fetch_objects():
            obj = DbObject.from_kind(
                rec.kind,
                oid=rec.oid,
                schema=rec.schema,
                name=rec.name,
                escaped=rec.escaped,
                extension=rec.extension,
                extcondition=rec.extcondition,
            )
            self.db.add_object(obj)

        for rec in self._fetch_columns():
            table = self.db.get(oid=rec.table_oid)
            assert table, "no table with oid %s for column %s found" % (
                rec.table_oid,
                rec.name,
            )
            col = Column(name=rec.name, type=rec.type, escaped=rec.escaped)
            table.add_column(col)

        for rec in self._fetch_sequences_deps():
            table = self.db.get(oid=rec.table_oid)
            assert table, "no table with oid %s for sequence %s found" % (
                rec.table_oid,
                rec.seq_oid,
            )
            seq = self.db.get(oid=rec.seq_oid)
            assert seq, "no sequence %s found" % rec.seq_oid
            self.db.add_sequence_user(seq, table, rec.column)

    def _fetch_objects(self):
        logger.debug("fetching database objects")
        with self.cursor() as cur:
            cur.execute(
                """
select
    r.oid as oid,
    s.nspname as schema,
    r.relname as name,
    r.relkind as kind,
    pg_catalog.format('%%I.%%I', s.nspname, r.relname)
        as escaped,

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
    ) as extcondition
from pg_class r
join pg_namespace s on s.oid = r.relnamespace
left join pg_depend d on d.objid = r.oid and d.deptype = 'e'
left join pg_extension e on d.refobjid = e.oid
where r.relkind = any(%(stateless)s)
and s.nspname != 'information_schema'
and s.nspname !~ '^pg_'
order by s.nspname, r.relname
""",
                {"stateless": list(DUMPABLE_KINDS)},
            )
            return cur.fetchall()

    def _fetch_sequences_deps(self):
        logger.debug("fetching sequences dependencies")
        with self.cursor() as cur:
            cur.execute(
                """
select tbl.oid as table_oid, att.attname as column, seq.oid as seq_oid
from pg_depend dep
join pg_attrdef def
    on dep.classid = 'pg_attrdef'::regclass and dep.objid = def.oid
join pg_attribute att on (def.adrelid, def.adnum) = (att.attrelid, att.attnum)
join pg_class tbl on tbl.oid = att.attrelid
join pg_class seq
    on dep.refclassid = 'pg_class'::regclass
    and seq.oid = dep.refobjid
    and seq.relkind = 'S'
"""
            )
            return cur.fetchall()

    def _fetch_columns(self):
        logger.debug("fetching columns")
        with self.cursor() as cur:
            # attnum gives their order; attnum < 0 are system columns
            # attisdropped flags a dropped column.
            cur.execute(
                """
select
    attrelid as table_oid,
    attname as name,
    atttypid::regtype as type,
    quote_ident(attname) as escaped
from pg_attribute a
join pg_class r on r.oid = a.attrelid
join pg_namespace s on s.oid = r.relnamespace
where r.relkind = any(%(kinds)s)
and a.attnum > 0
and not attisdropped
and s.nspname != 'information_schema'
and s.nspname !~ '^pg_'
order by a.attrelid, a.attnum
                """,
                {"kinds": [REVKINDS[KIND_TABLE], REVKINDS[KIND_PART_TABLE]]},
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
