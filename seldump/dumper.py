#!/usr/bin/env python3

"""
Object to perform a database dump.

This file is part of pg_seldump.
"""

import logging
from operator import attrgetter

from .exceptions import ConfigError, DumpError
from .database import Database
from .dumprule import Action, DumpRule
from .dbobjects import MaterializedView, Sequence, Table

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
        self.gather_actions()
        self.apply_actions()

    def gather_actions(self):
        self.actions.clear()

        # Associate an action to every object of the database
        for obj in self.db:
            assert obj.oid, "by now, every object should have an oid"
            assert obj.oid not in self.actions, "oid {} is duplicate".format(
                obj.oid
            )
            action = self.get_object_action(obj)
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

        # search and report errors
        has_errors = False
        for obj in self.db:
            action = self.actions[obj.oid]
            if action.error:
                logger.error("cannot dump %s: %s", obj, action.error)
                has_errors = True

        if has_errors:
            raise DumpError()

    def apply_actions(self):
        # Refresh the materialized views at the end.
        # TODO: actually they should be dumped in dependency order.
        objs = []
        matviews = []

        for obj in self.db:
            if isinstance(obj, MaterializedView):
                matviews.append(obj)
            else:
                objs.append(obj)

        objs.extend(matviews)

        self.writer.begin_dump()

        for obj in objs:
            action = self.actions[obj.oid]
            assert action.action != Action.ACTION_ERROR

            if action.action == Action.ACTION_UNKNOWN:
                logger.debug(
                    "%s %s doesn't match any rule: skipping", obj.kind, obj,
                )
                continue

            if action.action == action.ACTION_SKIP:
                logger.debug("skipping %s %s", obj.kind, obj)
                continue

            try:
                meth = getattr(
                    self.writer, "dump_" + obj.kind.replace(" ", "_")
                )
            except AttributeError:
                raise DumpError(
                    "don't know how to dump objects of kind %s" % obj.kind
                )
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
                    seq.escaped,
                    table.escaped,
                    column.name,
                )
                continue

            if column.name in ta.replace:
                logger.debug(
                    "%s %s depends on %s.%s which is replaced",
                    seq.kind,
                    seq.escaped,
                    table.escaped,
                    column.name,
                )
                continue

            # we found a table wanting this sequence
            logger.debug(
                "%s %s is needed by matched %s %s",
                seq.kind,
                seq.escaped,
                table.kind,
                table.escaped,
            )
            action = Action(seq, action=Action.ACTION_REFERENCED)
            return action

    def get_object_action(self, obj):
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
                % (obj.kind, obj.escaped, rules[0].pos, rules[1].pos)
            )

        return Action(obj, rules[0])
