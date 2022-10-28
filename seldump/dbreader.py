#!/usr/bin/env python3

"""
Reading object from a PostgreSQL database.

This file is part of pg_seldump.
"""

import logging
from functools import lru_cache

import psycopg
from psycopg import sql
from psycopg.rows import namedtuple_row

from .consts import DUMPABLE_KINDS, KIND_TABLE, KIND_PART_TABLE, REVKINDS
from .reader import Reader
from .dbobjects import DbObject, Column, ForeignKey
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
            cnn = psycopg.connect(self.dsn, row_factory=namedtuple_row)
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
            col = Column(name=rec.name, type=rec.type)
            table.add_column(col)

        for rec in self._fetch_fkeys():
            table = self.db.get(oid=rec.table_oid)
            assert table, "no table with oid %s for foreign key %s found" % (
                rec.table_oid,
                rec.name,
            )
            ftable = self.db.get(oid=rec.ftable_oid)
            assert ftable, "no table with oid %s for foreign key %s found" % (
                rec.ftable_oid,
                rec.name,
            )
            fkey = ForeignKey(
                name=rec.name,
                table_oid=rec.table_oid,
                table_cols=rec.table_cols,
                ftable_oid=rec.ftable_oid,
                ftable_cols=rec.ftable_cols,
            )
            table.add_fkey(fkey)
            ftable.add_ref_fkey(fkey)

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
    atttypid::regtype as type
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

    def _fetch_fkeys(self):
        logger.debug("fetching foreign keys")
        with self.cursor() as cur:
            cur.execute(
                """
select
    c.conname as name,
    c.conrelid as table_oid,
    array_agg(ra.attname) as table_cols,
    c.confrelid as ftable_oid,
    array_agg(fa.attname) as ftable_cols
from pg_constraint c
join (
    select oid, generate_series(1, array_length(conkey,1)) as attidx
    from pg_constraint
    where contype = 'f') exp on c.oid = exp.oid
join pg_attribute ra
    on (ra.attrelid, ra.attnum) = (c.conrelid, c.conkey[exp.attidx])
join pg_attribute fa
    on (fa.attrelid, fa.attnum) = (c.confrelid, c.confkey[exp.attidx])
join pg_class r on c.conrelid = r.oid
join pg_namespace rs on rs.oid = r.relnamespace
join pg_class fr on c.confrelid = fr.oid
join pg_namespace fs on fs.oid = fr.relnamespace
where rs.nspname != 'information_schema' and rs.nspname !~ '^pg_'
and   fs.nspname != 'information_schema' and fs.nspname !~ '^pg_'
group by 1, 2, 4
order by name
"""
            )
            return cur.fetchall()

    def get_sequence_value(self, seq):
        """
        Return the last value of a sequence.
        """
        with self.cursor() as cur:
            cur.execute(sql.SQL("select last_value from {}").format(seq.ident))
            val = cur.fetchone()[0]
            return val

    def copy(self, stmt, file):
        """
        Run a copy... to stdout statement.
        """
        with self.cursor() as cur:
            with cur.copy(stmt) as copy:
                for data in copy:
                    file.write(data)
