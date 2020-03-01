#!/usr/bin/env python3

"""
Object to perform a database dump.

This file is part of pg_seldump.
"""

import logging
from operator import attrgetter

from .exceptions import ConfigError, DumpError
from .database import Database
from .dumprule import DumpRule
from .dbobjects import MaterializedView, Sequence

logger = logging.getLogger("seldump.dumper")


class Dumper:
    """
    The logic of a database dump.
    """

    def __init__(self, reader, writer):
        self.db = Database()
        self.reader = reader
        self.reader.db = self.db
        self.writer = writer
        self.rules = []

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
            self.rules.append(DumpRule(**cfg))

    def perform_dump(self):
        """
        Perform the dump of a database.

        Read schema and data from the reader, apply the configured rule,
        use the writer to emit dump data.
        """
        # Refresh the materialized views at the end.
        # TODO: actually they should be dumped in dependency order.
        objs = []
        matviews = []

        for obj in self.db:
            if isinstance(obj, MaterializedView):
                matviews.append(obj)
            else:
                objs.append(obj)

        self.writer.begin_dump()

        for obj in objs + matviews:
            if obj.extension is not None and obj.extcondition is None:
                logger.debug(
                    "%s %s in extension %s has no dump condition: skipping",
                    obj.kind,
                    obj,
                    obj.extension,
                )
                continue

            rule = self.get_rule(obj)
            if rule is None:
                logger.debug(
                    "%s %s doesn't match any rule: skipping", obj.kind, obj,
                )
                continue

            if rule.action == rule.ACTION_SKIP:
                logger.debug("skipping %s %s", obj.kind, obj)
                continue

            elif rule.action == rule.ACTION_ERROR:
                raise DumpError(
                    "%s %s matches the error rule at %s"
                    % (obj.kind, obj, rule.pos)
                )

            try:
                meth = getattr(
                    self.writer, "dump_" + obj.kind.replace(" ", "_")
                )
            except AttributeError:
                raise DumpError(
                    "don't know how to dump objects of kind %s" % obj.kind
                )
            meth(obj, rule)

        self.writer.end_dump()

    def get_rule(self, obj):
        """
        Return the rule matching the object.
        """
        # First just check for a basic rule matching
        rule = self.get_object_rule(obj)
        if rule is not None:
            logger.debug(
                "%s %s matches rule at %s", obj.kind, obj.escaped, rule.pos
            )
            return rule

        # If not found, maybe it's a sequence used by a table dumped anyway
        # in such case we want to dump it
        if isinstance(obj, Sequence):
            return self._get_sequence_dependency_rule(obj)

    def _get_sequence_dependency_rule(self, seq):
        for table, column in self.db.get_tables_using_sequence(seq.oid):
            rule = self.get_object_rule(table)
            if rule is None:
                continue

            if rule.action == rule.ACTION_ERROR:
                raise DumpError(
                    "%s %s depends on %s %s matching the error rule at %s"
                    % (
                        seq.kind,
                        seq.escaped,
                        table.kind,
                        table.escaped,
                        rule.pos,
                    )
                )
            if rule.action == rule.ACTION_SKIP:
                continue

            if column.name in rule.no_columns:
                logger.debug(
                    "%s %s depends on %s.%s which is not dumped",
                    seq.kind,
                    seq.escaped,
                    table.escaped,
                    column.name,
                )
                continue

            if column.name in rule.replace:
                logger.debug(
                    "%s %s depends on %s.%s which is replaced",
                    seq.kind,
                    seq.escaped,
                    table.escaped,
                    column.name,
                )
                continue

            # we found a table wanting this sequence
            rule = DumpRule()
            rule.action = rule.ACTION_DEP
            logger.debug(
                "%s %s is needed by matched %s %s",
                seq.kind,
                seq.escaped,
                table.kind,
                table.escaped,
            )
            return rule

    def get_object_rule(self, obj):
        """
        Return the best matching rule for an object, None if none found
        """
        rules = [rule for rule in self.rules if rule.match(obj)]
        if not rules:
            return None

        rules.sort(key=attrgetter("score"), reverse=True)
        if len(rules) > 1 and rules[0].score == rules[1].score:
            raise ConfigError(
                "%s %s matches more than one rule: at %s and %s"
                % (obj.kind, obj.escaped, rules[0].pos, rules[1].pos)
            )

        return rules[0]
