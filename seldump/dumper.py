#!/usr/bin/env python3

"""
Object to perform a database dump.

This file is part of pg_seldump.
"""

import re
import logging
from operator import attrgetter

from psycopg import sql

from . import query
from .config import load_yaml, get_config_errors
from .database import Database
from .dumprule import DumpRule, RuleMatch
from .dbobjects import MaterializedView, Sequence, Table
from .exceptions import ConfigError, DumpError

logger = logging.getLogger("seldump.dumper")


class Dumper:
    """
    The logic of a database dump.
    """

    def __init__(self, reader, writer):
        self.db = Database()
        self.reader = reader
        self.writer = writer
        self.rules = []
        self.matches = {}

    @property
    def reader(self):
        return self._reader

    @reader.setter
    def reader(self, reader):
        self._reader = reader
        reader.db = self.db

    def clear(self):
        self.db.clear()
        del self.rules[:]
        self.matches.clear()

    def add_config(self, cfg):
        """
        Add a new config structure to the dumper

        The structure is what parsed by a json file. It must have a list
        of rules called 'db_objects'.

        You can parse a string too, which will be parsed from json.
        """
        if isinstance(cfg, str):
            # This case is mostly used for testing, so not really caring about
            # returning all the errors.
            cfg = load_yaml(cfg)
            errors = get_config_errors(cfg)
            if errors:
                raise ConfigError(errors[0])

        for cfg in cfg["db_objects"]:
            self.rules.append(DumpRule.from_config(cfg))

    def perform_dump(self):
        """
        Perform the dump of a database.

        Read schema and data from the reader, apply the configured rule,
        use the writer to emit dump data.
        """
        self.plan_dump()
        self.run_dump()

    def plan_dump(self):
        """
        Read config, source db, and calculate the operations to perform.

        This step doesn't need a writer.
        """
        self.find_matches()
        self.generate_statements()
        self.report_errors()

    def run_dump(self):
        """
        Perform a dump running the steps previously planned.
        """
        if self.writer is None:
            raise ValueError("no writer set")
        self.apply_actions()

    def find_matches(self):
        """
        Scan the object in the database and create the matches to dump them.
        """
        self.matches.clear()

        # Associate a match to every object of the database
        for obj in self.db:
            assert obj.oid, "by now, every object should have an oid"
            assert obj.oid not in self.matches, "oid {} is duplicate".format(obj.oid)
            match = self.get_match(obj)
            self.matches[obj.oid] = match

        # Find tables we need data to fulfill fkeys
        for obj in self.db:
            if not isinstance(obj, Table):
                continue
            if self.matches[obj.oid].action not in (
                DumpRule.ACTION_DUMP,
                DumpRule.ACTION_REFERENCED,
            ):
                continue
            self._add_referred_tables(obj)

        # Find unmentioned sequences and check if any table depend on them
        for obj in self.db:
            if not isinstance(obj, Sequence):
                continue
            if self.matches[obj.oid].action != DumpRule.ACTION_UNKNOWN:
                continue
            match = self._get_sequence_dependency_match(obj)
            if match is not None:
                self.matches[obj.oid] = match

    def generate_statements(self):
        gen = StatementsGenerator(self)
        for obj in self.db:
            if not isinstance(obj, Table):
                continue
            match = self.matches[obj.oid]
            gen.make_statements(obj, match)

    def report_errors(self):
        """
        If any error has been found report it and terminate the operations.
        """
        # search and report errors
        has_errors = False
        for obj in self.db:
            match = self.matches[obj.oid]
            if match.errors:
                has_errors = True
                for error in match.errors:
                    logger.error("cannot dump %s: %s", obj, error)

        if has_errors:
            raise DumpError()

    def apply_actions(self):
        """
        Apply all the requested actions.
        """
        # Dump sequences after tables: in case a sequence is pushed forward
        # by a operation it's better to have a hole in the sequence than to
        # have it returning a value already consumed.
        # Refresh the materialized views at the end.
        def key(obj):
            if isinstance(obj, Table):
                return 1
            elif isinstance(obj, Sequence):
                return 2
            # TODO: actually matviews should be dumped in dependency order.
            elif isinstance(obj, MaterializedView):
                return 3
            assert False, "what's the position of %s?" % obj

        objs = list(self.db)
        objs.sort(key=key)

        self.writer.begin_dump()

        for obj in objs:
            match = self.matches[obj.oid]
            meth = getattr(self, "_apply_" + match.action, None)
            if meth is None:
                raise DumpError("cannot dump a match %s", match.action)

            meth(obj, match)

        self.writer.end_dump()

    def _add_referred_tables(self, table, seen=None):
        logger.debug("exploring %s foreign keys", table)
        if seen is None:
            seen = set()

        if table.oid in seen:
            return

        seen.add(table.oid)

        for fkey in table.fkeys:
            logger.debug("found fkey %s", fkey.name)
            fmatch = self.matches[fkey.ftable_oid]
            if fmatch.action not in (
                DumpRule.ACTION_UNKNOWN,
                DumpRule.ACTION_REFERENCED,
                DumpRule.ACTION_DUMP,
            ):
                # skip, dump, error: we don't have to navigate it tho
                continue

            if fmatch.action == DumpRule.ACTION_UNKNOWN:
                fmatch.action = DumpRule.ACTION_REFERENCED
            if fkey not in fmatch.referenced_by:
                fmatch.referenced_by.append(fkey)

            self._add_referred_tables(fmatch.obj, seen)

    def _get_sequence_dependency_match(self, seq):
        for table, column in self.db.get_tables_using_sequence(seq.oid):
            ta = self.matches[table.oid]
            if ta.action not in (
                DumpRule.ACTION_DUMP,
                DumpRule.ACTION_REFERENCED,
            ):
                continue

            if column.name in ta.no_columns:
                logger.debug(
                    "%s %s depends on %s.%s which is not dumped",
                    seq.kind,
                    seq,
                    table,
                    column.name,
                )
                continue

            if column.name in ta.replace:
                logger.debug(
                    "%s %s depends on %s.%s which is replaced",
                    seq.kind,
                    seq,
                    table,
                    column.name,
                )
                continue

            # we found a table wanting this sequence
            logger.debug(
                "%s %s is needed by matched %s %s",
                seq.kind,
                seq,
                table.kind,
                table,
            )
            match = RuleMatch(seq, action=DumpRule.ACTION_REFERENCED)
            return match

    def get_match(self, obj):
        """
        Return the best matching rule for an object, None if none found
        """
        if obj.extension is not None and obj.extcondition is None:
            logger.debug(
                "%s %s in extension %s has no dump condition: skipping",
                obj.kind,
                obj,
                obj.extension,
            )
            return RuleMatch(obj, action=DumpRule.ACTION_SKIP)

        rules = [rule for rule in self.rules if rule.match(obj)]
        if not rules:
            return RuleMatch(obj, action=DumpRule.ACTION_UNKNOWN)

        rules.sort(key=attrgetter("score"), reverse=True)
        if len(rules) > 1 and rules[0].score == rules[1].score:
            raise ConfigError(
                "%s %s matches more than one rule: at %s and %s"
                % (obj.kind, obj, rules[0].pos, rules[1].pos)
            )

        return RuleMatch.from_rule(obj, rules[0])

    #
    # Methods to apply rules after statements have been generated
    # (dynamic dispatch from `apply_actions()`)
    #

    def _apply_unknown(self, obj, match):
        logger.debug("%s %s doesn't match any rule: skipping", obj.kind, obj)

    def _apply_skip(self, obj, match):
        logger.debug("skipping %s %s", obj.kind, obj)

    def _apply_dump(self, obj, match):
        meth = getattr(self.writer, "dump_" + obj.kind.replace(" ", "_"), None)
        if meth is None:
            raise DumpError("don't know how to dump objects of kind %s" % obj.kind)
        meth(obj, match)

    def _apply_ref(self, obj, match):
        self._apply_dump(obj, match)


class StatementsGenerator:
    """
    An object which can generate SQL statements out of the dump state.

    SQL statements are first generates into an intermediate structure (as
    seldump.query objects), then converted into a query (as psycopg.sql
    objects) thanks to the SqlQueryVisitor.
    """

    def __init__(self, dumper):
        self.dumper = dumper
        self.db = self.dumper.db
        self._alias_seq = 0

    def make_statements(self, table, match):
        """
        Set the statements to be used by the dump operation on `match`.
        """
        if match.action not in (
            DumpRule.ACTION_DUMP,
            DumpRule.ACTION_REFERENCED,
        ):
            return

        # Discard quietly a table with no column
        if not table.columns:
            match.action = DumpRule.ACTION_SKIP
            return

        self.find_errors(table, match)
        if match.errors:
            return

        self.set_copy_statement(table, match)
        self.set_import_statement(table, match)

    def find_errors(self, table, match):
        """
        Verify correctness of the operation and set errors on `match`.
        """
        for col in match.no_columns:
            if table.get_column(col) is None:
                match.errors.append(
                    "the table doesn't have the column '%s'"
                    " specified in 'no_columns'" % col
                )
        for col in match.replace:
            if table.get_column(col) is None:
                match.errors.append(
                    "the table doesn't have the column '%s'"
                    " specified in 'replace'" % col
                )

        if len(set(match.no_columns)) == len(table.columns):
            match.errors.append(
                "the table has no column left to dump: you should skip it"
            )

    def set_import_statement(self, table, match):
        """
        Set the statement used to import back data on the `match`.

        This statement will usually be printed on output, and will be a COPY
        FROM STDIN to read data from the rest of the file.
        """
        attrs = [col.ident for col in table.columns if col.name not in match.no_columns]

        match.import_statement = sql.SQL("\ncopy {} ({}) from stdin;\n").format(
            table.ident, sql.SQL(", ").join(attrs)
        )

    def set_copy_statement(self, table, match):
        """
        Set the statement used to extract data from the db on the `match`.

        This statement will be a COPY TO STDOUT that will be executed ad dump
        time and the output will be added to the dump file.

        The function also sets `match.query`, the query generated.
        """
        # If False can use "copy table (attrs) to stdout" to dump data.
        # Otherwise must use a slower "copy (query) to stdout"
        if not (
            match.action != DumpRule.ACTION_DUMP
            or match.replace
            or match.filter
            or table.extcondition
            or table.ref_fkeys
        ):
            self._set_copy_to_simple(table, match)
        else:
            match.query = self.make_query(table, match)
            copy = query.CopyOut(match.query)
            match.copy_statement = query.SqlQueryVisitor().visit(copy)

    def _set_copy_to_simple(self, table, match):
        attrs = self._get_dump_attrs(table, match)
        match.copy_statement = sql.SQL("copy {} ({}) to stdout").format(
            table.ident, sql.SQL(", ").join(attrs)
        )

    def make_query(self, table, match):
        """
        Generate the query to execute the desired `match`.
        """
        self._alias_seq = 0

        select = self._get_select(table, match)
        select.columns = self._get_dump_attrs(table, match)
        return select

    def _get_existence(self, table, fkey, parent, seen):
        """
        Return the Exists predicate to limit the query to a fkey content.
        """
        assert fkey.ftable_oid == table.oid
        ptable = self.db.get(oid=fkey.table_oid)
        pmatch = self.dumper.matches[fkey.table_oid]
        subsel = self._get_select(ptable, pmatch, seen=seen)
        fkj = query.FkeyJoin(fkey=fkey, from_=subsel.from_.alias, to=parent)

        return query.Exists(
            query=query.Select(
                columns=[sql.SQL("1")],
                from_=subsel.from_,
                where=self._maybe_and([fkj, subsel.where]),
            )
        )

    def _get_select(self, table, match, seen=None):
        """
        Return the part of the select common to subqueries and external ones.

        The returned select has the attribute list empty: the caller will have
        to populate it.
        """
        alias = self._get_alias()
        where = [self._get_filters(table, match)]
        seen = (seen or set()) | {table.oid}

        srfkeys = []
        for fkey in match.referenced_by:
            # self-referential fkeys are dealt with later
            if fkey.table_oid == fkey.ftable_oid:
                srfkeys.append(fkey)
                continue
            elif fkey.table_oid in seen:
                logger.warning("not going recursive for now")
                continue
            where.append(self._get_existence(table, fkey, parent=alias, seen=seen))

        q = query.Select(
            columns=[],
            from_=query.FromEntry(table, alias=alias),
            where=self._maybe_or(where),
        )

        # if there are self-referential fkeys q is the base of a recursive cte
        if srfkeys:
            rec_alias = alias + "r"
            rec_cond = self._maybe_or(
                [query.FkeyJoin(fkey, rec_alias, alias) for fkey in srfkeys]
            )

            q.columns = [sql.SQL("{}.*").format(sql.Identifier(alias))]
            q = query.Select(
                columns=[],
                from_=query.FromEntry(
                    query.RecursiveCTE(name=rec_alias, base_query=q, rec_cond=rec_cond),
                    alias=rec_alias,
                ),
            )

        return q

    def _get_filters(self, table, match):
        rv = []
        if table.extcondition:
            rv.append(sql.SQL(re.replace(r"(?i)^\s*where\s+", table.extcondition, "")))
        if match.filter:
            rv.append(sql.SQL(match.filter.strip()))

        return self._maybe_and(rv)

    def _get_dump_attrs(self, table, match):
        rv = []
        for col in table.columns:
            if col.name in match.no_columns:
                continue

            if col.name in match.replace:
                rv.append(
                    sql.SQL("({})").format(sql.SQL(match.replace[col.name].strip()))
                )
            else:
                rv.append(col.ident)

        return rv

    def _maybe_and(self, conds):
        return self._maybe_op(conds, query.And)

    def _maybe_or(self, conds):
        return self._maybe_op(conds, query.Or)

    def _maybe_op(self, conds, op):
        conds = [c for c in conds if c is not None]
        if not conds:
            return None
        elif len(conds) == 1:
            return conds[0]
        else:
            return op(conds)

    def _get_alias(self):
        rv = "t%s" % self._alias_seq
        self._alias_seq += 1
        return rv
