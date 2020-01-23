#!/usr/bin/env python3
"""
Database objects dumping.

This file is part of pg_seldump.
"""

import re
import math
import logging
from datetime import datetime
from functools import lru_cache

import psycopg2
from psycopg2 import sql
from psycopg2.extras import NamedTupleCursor

from .exceptions import DumpError
from .consts import DUMPABLE_KINDS, PG_KINDS, PROJECT_URL, VERSION

logger = logging.getLogger("seldump.dumping")


class Dumper:
    def __init__(self, dsn, matcher):
        self.dsn = dsn
        self.matcher = matcher
        self.outfile = None

        self._start_time = None
        self._copy_start_pos = None
        self._copy_size = None

    def dump_data(self, outfile, test=False):
        self.outfile = outfile

        # Refresh the materialized views at the end.
        # TODO: actually they should be dumped in dependency order.
        objs = []
        matviews = []

        for n in self.get_schemas_to_dump():
            logger.debug("dumping objects in schema %s", n)
            for obj in self.get_objects_to_dump(schema=n):
                if obj.kind == "materialized view":
                    matviews.append(obj)
                else:
                    objs.append(obj)

        if not test:
            self.begin_dump()

        for obj in objs + matviews:
            rule = self.matcher.get_rule(obj)
            if rule is None:
                logger.debug(
                    "%s %s doesn't match any rule: skipping",
                    obj.kind,
                    obj.escaped,
                )
                continue

            logger.debug(
                "%s %s matches rule at %s", obj.kind, obj.escaped, rule.pos
            )
            if rule.action == "skip":
                logger.debug("skipping %s %s", obj.kind, obj.escaped)
                continue
            elif rule.action == "error":
                raise DumpError(
                    "%s %s matches the error rule at %s"
                    % (obj.kind, obj.escaped, rule.pos)
                )

            try:
                meth = getattr(self, "dump_" + obj.kind.replace(" ", "_"))
            except AttributeError:
                raise DumpError(
                    "don't know how to dump objects of kind %s" % obj.kind
                )
            logger.info("dumping %s %s", obj.kind, obj.escaped)
            if not test:
                meth(obj, rule)

        if not test:
            self.end_dump()

    def dump_table(self, table, config):
        self._begin_table(table)
        self._copy_table(table, config)
        self._end_table(table)

    def _begin_table(self, table):
        self.write("\nalter table %s disable trigger all;\n" % table.escaped)

    def _end_table(self, table):
        self.write("\nalter table %s enable trigger all;\n\n" % table.escaped)

        if self._copy_size is not None:
            if self._copy_size >= 1024:
                pretty = " (%s)" % pretty_size(self._copy_size)
            else:
                pretty = ""

            self.write(
                "-- %s bytes written for table %s%s\n\n"
                % (self._copy_size, table.escaped, pretty)
            )

    def _copy_table(self, table, config):
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
                (table.escaped,),
            )
            no_columns = set(config.no_columns)
            replace = config.replace.copy()

            # If False can use "copy table (attrs) to stdout" to dump data.
            # Otherwise must use a slower "copy (query) to stdout"
            select = False

            attrs_in = []
            attrs_out = []
            for attr in cur:
                if attr.name in no_columns:
                    no_columns.remove(attr.name)
                    continue

                attrs_in.append(attr.escaped)
                if attr.name in replace:
                    attrs_out.append("(%s)" % replace.pop(attr.name))
                    select = True
                else:
                    attrs_out.append(attr.escaped)

            if no_columns:
                raise DumpError(
                    "table %s has no attribute %s mentioned in 'no_columns'"
                    % (table.escaped, ", ".join(sorted(no_columns)))
                )
            if replace:
                raise DumpError(
                    "table %s has no attribute %s mentioned in 'replace'"
                    % (table.escaped, ", ".join(sorted(replace)))
                )

            cond = self._get_table_condition(table, config)
            if cond:
                select = True

            if not select:
                source = "%s (%s)" % (table.escaped, ", ".join(attrs_out))
            else:
                source = "(select %s from only %s%s)" % (
                    ", ".join(attrs_out),
                    table.escaped,
                    cond,
                )

            self.write(
                "\ncopy %s (%s) from stdin;\n"
                % (table.escaped, ", ".join(attrs_in))
            )

            logger.debug("exporting using: %s", source)
            self._begin_copy()
            try:
                cur.copy_expert("copy %s to stdout" % source, self.outfile)
            except psycopg2.DatabaseError as e:
                raise DumpError(
                    "failed to copy from table %s: %s" % (table.escaped, e)
                )

            self._end_copy()
            self.write("\\.\n")

    def _get_table_condition(self, table, config):
        conds = []
        if table.condition:
            conds.append(re.replace(r"(?i)^\s*where\s+", table.condition, ""))
        if config.filter:
            conds.append(config.filter)

        if conds:
            return " where " + " and ".join("(%s)" % c for c in conds)
        else:
            return ""

    def dump_sequence(self, seq, config):
        with self.cursor() as cur:
            cur.execute("select last_value from %s" % seq.escaped)
            val = cur.fetchone()[0]
            self.write(
                sql.SQL("\nselect pg_catalog.setval({}, {}, true);\n\n")
                .format(sql.Literal(seq.escaped), sql.Literal(val))
                .as_string(self.connection)
            )

    def dump_materialized_view(self, matview, config):
        self.write("\nrefresh materialized view %s;\n" % matview.escaped)

    def begin_dump(self):
        self.write(
            "-- PostgreSQL data dump generated by pg_seldump %s\n" % VERSION
        )
        self.write("-- %s\n\n" % PROJECT_URL)

        self._start_time = now = datetime.utcnow()
        self.write("-- Data dump started at %sZ\n\n" % now)

        self.write("set session authorization default;\n")

    def end_dump(self):
        self.write("\n\nanalyze;\n\n")

        now = datetime.utcnow()
        elapsed = pretty_timedelta(now - self._start_time)
        self.write("-- Data dump finished at %sZ (%s)\n\n" % (now, elapsed))

        # No highlight please
        self.write("-- vim: set filetype=:\n")

    def get_schemas_to_dump(self):
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

    def cursor(self):
        return self.connection.cursor()

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

    def write(self, data):
        self.outfile.write(data)

    def _begin_copy(self):
        """
        Mark the start of the copy of a table data.

        Memorize where we are in the file output file, if the file is seekable.
        """
        if self.outfile.seekable():
            self._copy_start_pos = self.outfile.tell()

    def _end_copy(self):
        """
        Mark the end of the copy of a table data.

        If the file is seekable return the amout of bytes copied.
        """
        if self.outfile.seekable() and self._copy_start_pos is not None:
            self._copy_size = self.outfile.tell() - self._copy_start_pos
            self._copy_start_pos = None


def pretty_size(size):
    """
    Display a size in bytes in a human friendly way
    """
    if size <= 0:
        # Not bothering with negative numbers
        return "%sB" % size

    suffixes = ("B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB")
    i = int(math.floor(math.log(size, 1024)))
    p = math.pow(1024, i)
    s = round(size / p, 2)
    return "%s %s" % (s, suffixes[i])


def pretty_timedelta(delta):
    """
    Display a time interval in a human friendly way
    """
    rem, secs = divmod(abs(delta.total_seconds()), 60)
    rem, mins = divmod(rem, 60)
    days, hours = divmod(rem, 24)
    parts = [(days, "d"), (hours, "h"), (mins, "m"), (secs, "s")]
    while parts and parts[0][0] == 0:
        del parts[0]
    sign = "-" if delta.total_seconds() < 0 else ""
    return sign + " ".join("%.0f%s" % p for p in parts)
