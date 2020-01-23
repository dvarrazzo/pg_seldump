#!/usr/bin/env python3
"""
Database objects dumping.

This file is part of pg_seldump.
"""

import re
import sys
import logging
from functools import lru_cache
from collections import namedtuple

import psycopg2
from psycopg2 import sql
from psycopg2.extras import NamedTupleCursor

logger = logging.getLogger("seldump.dumper")


class DumpError(Exception):
    """Controlled exception raised by the script."""


class Dumper:
    def __init__(self, dsn, outfile=None, test=False):
        self.dsn = dsn
        self.outfile = outfile or sys.stdout
        self.test = test
        self.config_objs = []

    # Configuration

    def add_config(self, cfg):
        try:
            objs = cfg["db_objects"]
        except (KeyError, TypeError):
            raise DumpError("the config file should have a db_objects list")

        if not isinstance(objs, list):
            raise DumpError("db_objects should be a list, got %s" % type(objs).__name__)

        for cfg in objs:
            self.validate_config(cfg)
            self.config_objs.append(cfg)

    def validate_config(self, cfg):
        if not isinstance(cfg, dict):
            raise DumpError("expected config dict, got %s" % cfg)

        if "name" in cfg and "names" in cfg:
            raise DumpError("config can't specify both name and names, got %s" % cfg)
        if "schema" in cfg and "schemas" in cfg:
            raise DumpError(
                "config can't specify both schema and schemas, got %s" % cfg
            )
        if "kind" in cfg:
            if self.revkinds.get(cfg["kind"]) not in self.dumpable_kinds:
                kinds = sorted(
                    k for k, v in self.revkinds.items() if v in self.dumpable_kinds
                )
                raise DumpError(
                    "bad kind '%s', accepted values are: %s; got %s"
                    % (cfg["kind"], ", ".join(kinds), cfg)
                )
        if "no_columns" in cfg:
            if not isinstance(cfg["no_columns"], list):
                raise DumpError(
                    "bad no_columns %s: must be a list; got %s"
                    % (cfg["no_columns"], cfg)
                )
        if "replace" in cfg:
            if not isinstance(cfg["replace"], dict):
                raise DumpError("bad replace: must be a dict; got %s" % (cfg,))

        unks = set(cfg) - set(
            """
            filter kind name names no_columns replace schema schemas skip
            """.split()
        )
        if unks:
            logger.warning(
                "unknown config options: %s; got %s", ", ".join(sorted(unks)), cfg,
            )

    ObjectConfig = namedtuple(
        "ObjectConfig", "skip no_columns replace filter filename lineno"
    )

    def get_config(self, obj):
        for cfg in self.config_objs:
            if not self.config_matches(cfg, obj):
                continue

            rv = self.ObjectConfig(
                skip=cfg.get("skip", False),
                no_columns=cfg.get("no_columns", []),
                replace=cfg.get("replace", {}),
                filter=cfg.get("filter"),
                filename=cfg.filename,
                lineno=cfg.lineno,
            )
            return rv

    def config_matches(self, cfg, obj):
        if "name" in cfg:
            if cfg["name"] != obj.name:
                return False
        if "names" in cfg:
            if not re.match(cfg["names"], obj.name, re.VERBOSE):
                return False

        if "schema" in cfg:
            if cfg["schema"] != obj.schema:
                return False
        if "schemas" in cfg:
            if not re.match(cfg["schemas"], obj.schema, re.VERBOSE):
                return False

        if "kind" in cfg:
            if obj.kind != cfg["kind"]:
                return False

        return True

    #
    # Data dump
    #

    def dump_data(self, schemas=None):
        # Refresh the materialized views at the end.
        # TODO: actually they should be dumped in dependency order.
        objs = []
        matviews = []

        for n in self.get_schemas_to_dump():
            if schemas is not None and n not in schemas:
                continue

            logger.debug("dumping objects in schema %s", n)
            for obj in self.get_objects_to_dump(schema=n):
                if obj.kind == "materialized view":
                    matviews.append(obj)
                else:
                    objs.append(obj)

        if not self.test:
            self.begin_dump()

        for obj in objs + matviews:
            cfg = self.get_config(obj)
            if cfg is None:
                logger.debug(
                    "%s %s doesn't match any rule: skipping", obj.kind, obj.escaped
                )
                continue

            logger.debug(
                "%s %s matches rule at %s:%s",
                obj.kind,
                obj.escaped,
                cfg.filename,
                cfg.lineno,
            )
            if cfg.skip:
                logger.debug("skipping %s %s", obj.kind, obj.escaped)
                continue

            try:
                meth = getattr(self, "dump_" + obj.kind.replace(" ", "_"))
            except AttributeError:
                raise DumpError("don't know how to dump objects of kind %s" % obj.kind)
            logger.info("dumping %s %s", obj.kind, obj.escaped)
            if not self.test:
                meth(obj, cfg)

        if not self.test:
            self.end_dump()

    # relkind values: https://www.postgresql.org/docs/11/catalog-pg-class.html
    kinds = {
        "r": "table",
        "i": "index",
        "S": "sequence",
        "t": "toast table`",
        "v": "view",
        "m": "materialized view",
        "c": "composite type",
        "f": "foreign table",
        "p": "partitioned table",
        "I": "partitioned index",
    }
    revkinds = {v: k for k, v in kinds.items()}
    stateless_kinds = set("ivcfI")
    dumpable_kinds = set(kinds) - stateless_kinds - set("t")

    def dump_table(self, table, config):
        self._begin_table(table)
        self._copy_table(table, config)
        self._end_table(table)

    def _begin_table(self, table):
        self.write("\nalter table %s disable trigger all;\n" % table.escaped)

    def _end_table(self, table):
        self.write("\nalter table %s enable trigger all;\n\n" % table.escaped)

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
                "\ncopy %s (%s) from stdin;\n" % (table.escaped, ", ".join(attrs_in))
            )

            logger.debug("exporting using: %s", source)
            try:
                cur.copy_expert("copy %s to stdout" % source, self.outfile)
            except psycopg2.DatabaseError as e:
                raise DumpError("failed to copy from table %s: %s" % (table.escaped, e))

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
        self.write("set session authorization default;\n")

    def end_dump(self):
        self.write("\nanalyze;\n")
        # No highlight please
        self.write("\n-- vim: set filetype=:\n")

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
            # The list of all the objects of a schema to inclue in the dump.
            #
            # Certain objects don't have a state (e.g. views) so there is
            # nothing to include in a data-only dump (self.stateless_kinds).
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
                where r.relkind <> all(%(stateless)s)
                and n.nspname = %(schema)s
                order by r.relname
                ) x
                where extension is null
                or condition is not null
                """,
                {"schema": schema, "stateless": list(self.stateless_kinds)},
            )

            # Replace the kind from the single letter in pg_catalog to a
            # more descriptive string.
            return [r._replace(kind=self.kinds[r.kind]) for r in cur]

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
