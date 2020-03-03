#!/usr/bin/env python3

"""
Object to perform a database dump.

This file is part of pg_seldump.
"""

import re
import logging
from operator import attrgetter

from psycopg2 import sql

from . import query
from .database import Database
from .dumprule import Action, DumpRule
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
        self.actions = {}

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
        self.actions.clear()

    def add_config(self, cfg):
        """
        Add a new config structure to the dumper

        The structure is what parsed by a json file. It must have a list
        of rules called 'db_objects'.
        """
        try:
            objs = cfg["db_objects"]
        except (KeyError, TypeError):
            raise ConfigError(
                "the config file should have a 'db_objects' list"
            )

        if not isinstance(objs, list):
            raise ConfigError(
                "db_objects should be a list, got %s" % type(objs).__name__
            )

        for cfg in objs:
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
        self.gather_actions()
        self.generate_statements()
        self.report_errors()

    def run_dump(self):
        """
        Perform a dump running the steps previously planned.
        """
        if self.writer is None:
            raise ValueError("no writer set")
        self.apply_actions()

    def gather_actions(self):
        """
        Scan the object in the database and create the actions to dump them.
        """
        self.actions.clear()

        # Associate an action to every object of the database
        for obj in self.db:
            assert obj.oid, "by now, every object should have an oid"
            assert obj.oid not in self.actions, "oid {} is duplicate".format(
                obj.oid
            )
            action = self.get_action(obj)
            self.actions[obj.oid] = action

        # Find unmentioned tables we need data to fulfill fkeys
        for obj in self.db:
            if not isinstance(obj, Table):
                continue
            if self.actions[obj.oid].action != Action.ACTION_DUMP:
                continue
            self._add_referred_tables(obj)

        # Find unmentioned sequences and check if any table depend on them
        for obj in self.db:
            if not isinstance(obj, Sequence):
                continue
            if self.actions[obj.oid].action != Action.ACTION_UNKNOWN:
                continue
            action = self._get_sequence_dependency_action(obj)
            if action is not None:
                self.actions[obj.oid] = action

    def generate_statements(self):
        gen = StatementsGenerator(self)
        for obj in self.db:
            if not isinstance(obj, Table):
                continue
            action = self.actions[obj.oid]
            gen.make_statements(obj, action)

    def report_errors(self):
        """
        If any error has been found report it and terminate the operations.
        """
        # search and report errors
        has_errors = False
        for obj in self.db:
            action = self.actions[obj.oid]
            if action.errors:
                has_errors = True
                for error in action.errors:
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
            action = self.actions[obj.oid]
            meth = getattr(self, "_apply_" + action.action, None)
            if meth is None:
                raise DumpError("cannot dump an action %s", action.action)

            meth(obj, action)

        self.writer.end_dump()

    def _add_referred_tables(self, table, seen=None):
        logger.debug("exploring %s foreign keys", table)
        if seen is None:
            seen = set()

        if table.oid in seen:
            return

        seen.add(table.oid)

        for fkey in table.fkeys:
            if fkey.table_oid == fkey.ftable_oid:
                logger.warning(
                    "not dealing with self-referencing fkey %s now", fkey.name
                )
                continue

            logger.debug("found fkey %s", fkey.name)
            faction = self.actions[fkey.ftable_oid]
            if faction.action not in (
                Action.ACTION_UNKNOWN,
                Action.ACTION_REFERENCED,
            ):
                # skip, dump, error: we don't have to navigate it tho
                continue

            faction.action = Action.ACTION_REFERENCED
            faction.referenced_by.append(fkey)

            self._add_referred_tables(faction.obj, seen)

    def _get_sequence_dependency_action(self, seq):
        for table, column in self.db.get_tables_using_sequence(seq.oid):
            ta = self.actions[table.oid]
            if ta.action not in (Action.ACTION_DUMP, Action.ACTION_REFERENCED):
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
            action = Action(seq, action=Action.ACTION_REFERENCED)
            return action

    def get_action(self, obj):
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
            return Action(obj, action=Action.ACTION_SKIP)

        rules = [rule for rule in self.rules if rule.match(obj)]
        if not rules:
            return Action(obj, action=Action.ACTION_UNKNOWN)

        rules.sort(key=attrgetter("score"), reverse=True)
        if len(rules) > 1 and rules[0].score == rules[1].score:
            raise ConfigError(
                "%s %s matches more than one rule: at %s and %s"
                % (obj.kind, obj, rules[0].pos, rules[1].pos)
            )

        return Action.from_rule(obj, rules[0])

    #
    # Methods to apply rules after statements have been generated
    # (dynamic dispatch from `apply_actions()`)
    #

    def _apply_unknown(self, obj, action):
        logger.debug("%s %s doesn't match any rule: skipping", obj.kind, obj)

    def _apply_skip(self, obj, action):
        logger.debug("skipping %s %s", obj.kind, obj)

    def _apply_dump(self, obj, action):
        meth = getattr(self.writer, "dump_" + obj.kind.replace(" ", "_"), None)
        if meth is None:
            raise DumpError(
                "don't know how to dump objects of kind %s" % obj.kind
            )
        meth(obj, action)

    def _apply_ref(self, obj, action):
        self._apply_dump(obj, action)


class StatementsGenerator:
    def __init__(self, dumper):
        self.dumper = dumper
        self.db = self.dumper.db
        self._alias_seq = 0

    def make_statements(self, table, action):
        if action.action not in (Action.ACTION_DUMP, Action.ACTION_REFERENCED):
            return

        # Discard quietly a table with no column
        if not table.columns:
            action.action = Action.ACTION_SKIP
            return

        self.find_errors(table, action)
        if action.errors:
            return

        if action.action == Action.ACTION_DUMP:
            self.set_copy_to_dump(table, action)
        elif action.action == Action.ACTION_REFERENCED:
            self.set_copy_to_ref(table, action)
        else:
            assert False, "{} {} got action {}".format(
                table.kind, table, action.action
            )

        self.set_copy_from(table, action)

    def find_errors(self, table, action):
        for col in action.no_columns:
            if table.get_column(col) is None:
                action.errors.append(
                    "the table doesn't have the column '%s'"
                    " specified in 'no_columns'" % col
                )
        for col in action.replace:
            if table.get_column(col) is None:
                action.errors.append(
                    "the table doesn't have the column '%s'"
                    " specified in 'replace'" % col
                )

        if len(set(action.no_columns)) == len(table.columns):
            action.errors.append(
                "the table has no column left to dump: you should skip it"
            )

    def set_copy_from(self, table, action):
        attrs = [
            col.ident for col in table.columns if col not in action.no_columns
        ]

        action.import_statement = sql.SQL(
            "\ncopy {} ({}) from stdin;\n"
        ).format(table.ident, sql.SQL(", ").join(attrs))

    def set_copy_to_dump(self, table, action):
        # If False can use "copy table (attrs) to stdout" to dump data.
        # Otherwise must use a slower "copy (query) to stdout"
        select = action.replace or action.filter or table.extcondition

        attrs = self._get_dump_attrs(table, action)

        conds = []
        if table.extcondition:
            conds.append(
                sql.SQL(
                    re.replace(r"(?i)^\s*where\s+", table.extcondition, "")
                )
            )
        if action.filter:
            conds.append(sql.SQL(action.filter.strip()))

        if conds:
            conds = sql.SQL(" and ").join(
                sql.SQL("({})").format(cond) for cond in conds
            )
            conds = sql.SQL(" where {}").format(conds)
        else:
            conds = sql.SQL("")

        if not select:
            source = sql.SQL("{} ({})").format(
                table.ident, sql.SQL(", ").join(attrs)
            )
        else:
            source = sql.SQL("(select {} from only {}{})").format(
                sql.SQL(", ").join(attrs), table.ident, conds,
            )

        action.copy_statement = sql.SQL("copy {} to stdout").format(source)

    def set_copy_to_ref(self, table, action):
        self._alias_seq = 0
        q = action.query = self.make_ref_query(table, action)
        stmt = query.SqlQueryVisitor().visit(q)
        stmt = sql.SQL("copy (\n{}\n) to stdout").format(stmt)
        action.copy_statement = stmt

    def make_ref_query(self, table, action):
        # Start with a table
        alias = self._get_alias()

        where = None
        for fkey in action.referenced_by:
            cond = self._get_existence(
                table, fkey, parent=alias, seen={table.oid}
            )
            if where is None:
                where = cond
            elif isinstance(where, query.Or):
                where.conds.append(cond)
            else:
                where = query.Or([where, cond])

        sel = query.Select(
            columns=self._get_dump_attrs(table, action),
            from_=query.FromEntry(table.ident, alias=alias),
            where=where,
        )

        return sel

    def _get_existence(self, table, fkey, parent, seen):
        assert fkey.ftable_oid == table.oid
        alias = self._get_alias()
        ptable = self.db.get(oid=fkey.table_oid)
        paction = self.dumper.actions[fkey.table_oid]
        fkcond = query.FkeyJoin(fkey=fkey, from_=alias, to=parent)

        if paction.action == Action.ACTION_DUMP:
            where = fkcond
            if paction.filter:
                where = query.And([where, sql.SQL(paction.filter.strip())])
            return query.Exists(
                query=query.Select(
                    columns=[sql.SQL("1")],
                    from_=query.FromEntry(ptable, alias=alias),
                    where=where,
                )
            )
        elif paction.action == Action.ACTION_REFERENCED:
            where = None
            for nfkey in paction.referenced_by:
                if ptable.oid in seen:
                    logger.warning("not going recursive for now")
                    continue
                cond = self._get_existence(
                    ptable, nfkey, parent=alias, seen=seen | {ptable.oid}
                )
                if where is None:
                    where = cond
                elif isinstance(where, query.Or):
                    where.conds.append(cond)
                else:
                    where = query.Or([where, cond])

            if where is not None:
                where = query.And([fkcond, where])
            else:
                where = fkcond

            return query.Exists(
                query=query.Select(
                    columns=[sql.SQL("1")],
                    from_=query.FromEntry(ptable, alias=alias),
                    where=where,
                )
            )

    def _get_dump_attrs(self, table, action):
        rv = []
        for col in table.columns:
            if col.name in action.no_columns:
                continue

            if col.name in action.replace:
                rv.append(
                    sql.SQL("({})").format(
                        sql.SQL(action.replace[col.name].strip())
                    )
                )
            else:
                rv.append(col.ident)

        return rv

    def _get_alias(self):
        rv = "t%s" % self._alias_seq
        self._alias_seq += 1
        return rv
