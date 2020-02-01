#!/usr/bin/env python3
"""
Database objects dumping.

This file is part of pg_seldump.
"""

import re
import math
import logging
from datetime import datetime

from psycopg2 import sql

from .exceptions import DumpError
from .consts import PROJECT_URL, VERSION

logger = logging.getLogger("seldump.dumping")


class Dumper:
    def __init__(self, reader, matcher):
        self.reader = reader
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

        for obj in self.reader.get_objects_to_dump():
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
        no_columns = set(config.no_columns)
        replace = config.replace.copy()

        # If False can use "copy table (attrs) to stdout" to dump data.
        # Otherwise must use a slower "copy (query) to stdout"
        select = False

        attrs_in = []
        attrs_out = []
        for attr in self.reader.get_table_attributes(table.escaped):
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
        self.reader.copy_out(source, self.outfile, table.escaped)

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
        val = self.reader.get_sequence_value(seq.escaped)
        self.write(
            sql.SQL("\nselect pg_catalog.setval({}, {}, true);\n\n")
            .format(sql.Literal(seq.escaped), sql.Literal(val))
            .as_string(self.reader.connection)
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
