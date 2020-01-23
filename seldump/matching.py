#!/usr/bin/env python3

"""
Dump rules and matching.

This file is part of pg_seldump.
"""

import re
import logging
from operator import attrgetter
from functools import lru_cache

from .consts import REVKINDS, DUMPABLE_KINDS
from .exceptions import ConfigError

logger = logging.getLogger("seldump.matching")


class DumpRule:
    """
    Dump configuration of a set of database objects

    Each DumpRule has a few selector attributes, to choose which objects it
    applies, and a set of attributes specifying what action to take.
    """

    ACTIONS = ["dump", "skip", "error"]

    def __init__(self):
        # Matching attributes
        self.names = set()
        self.names_re = None
        self.schemas = set()
        self.schemas_re = None
        self.kinds = set()
        self.adjust_score = 0

        # Actions
        self.action = "dump"
        self.no_columns = []
        self.replace = {}
        self.filter = None

        # Description
        self.filename = None
        self.lineno = None

    @property
    @lru_cache(maxsize=1)
    def score(self):
        """
        The score of the rule: the higher the stronger
        """
        score = self.adjust_score
        if self.names:
            score += 1000
        if self.names_re:
            score += 500
        if self.schemas:
            score += 100
        if self.schemas_re:
            score += 50
        if self.kinds:
            score += 10
        return score

    @property
    def pos(self):
        """
        Return the file name and line no where the rule was parsed.
        """
        return "%s:%s" % (self.filename, self.lineno)

    def match(self, obj):
        """
        Return True if the db object *obj* matches the rule.
        """
        if self.names and obj.name not in self.names:
            return False

        if self.names_re is not None and not self.names_re.match(obj.name):
            return False

        if self.schemas and obj.schema not in self.schemas:
            return False

        if self.schemas_re is not None and not self.schemas_re.match(
            obj.schema
        ):
            return False

        if self.kinds and obj.kind not in self.kinds:
            return False

        return True

    @classmethod
    def from_config(cls, cfg):
        """
        Return a new DumpRule from a YAML content.
        """
        rv = cls()
        rv.filename = cfg.filename
        rv.lineno = cfg.lineno

        if not isinstance(cfg, dict):
            raise ConfigError("expected config dictionary, got %s" % cfg)

        if "name" in cfg and "names" in cfg:
            raise ConfigError(
                "can't specify both 'name' and 'names', at %s" % rv.pos
            )

        if "name" in cfg:
            if not isinstance(cfg["name"], str):
                raise ConfigError("'name' should be a string, at %s" % rv.pos)
            rv.names.add(cfg["name"])

        if "names" in cfg:
            if isinstance(cfg["names"], list) and all(
                isinstance(name, str) for name in cfg["names"]
            ):
                rv.names.update(cfg["names"])
            elif isinstance(cfg["names"], str):
                try:
                    rv.names_re = re.compile(cfg["names"], re.VERBOSE)
                except re.error as e:
                    raise ConfigError(
                        "'names' is not a valid regular expression: %s,"
                        " at %s" % (e, rv.pos)
                    )
            else:
                raise ConfigError(
                    "'names' should be a list of strings or a"
                    " regular expression, at %s" % rv.pos
                )

        if "schema" in cfg and "schemas" in cfg:
            raise ConfigError(
                "can't specify both 'schema' and 'schemas', at %s" % rv.pos
            )

        if "schema" in cfg:
            if not isinstance(cfg["schema"], str):
                raise ConfigError(
                    "'schema' should be a string, at %s" % rv.pos
                )
            rv.schemas.add(cfg["schema"])

        if "schemas" in cfg:
            if isinstance(cfg["schemas"], list) and all(
                isinstance(name, str) for name in cfg["schemas"]
            ):
                rv.schemas.update(cfg["schemas"])
            elif isinstance(cfg["schemas"], str):
                try:
                    rv.schemas_re = re.compile(cfg["schemas"], re.VERBOSE)
                except re.error as e:
                    raise ConfigError(
                        "'schemas' is not a valid regular expression: %s,"
                        " at %s" % (e, rv.pos)
                    )
            else:
                raise ConfigError(
                    "'schemas' should be a list of strings or"
                    " a regular expression, at %s" % rv.pos
                )

        if "kind" in cfg and "kinds" in cfg:
            raise ConfigError(
                "can't specify both 'kind' and 'kinds', at %s" % rv.pos
            )

        if "kind" in cfg:
            rv._check_kind(cfg["kind"])
            rv.kinds.add(cfg["kind"])

        if "kinds" in cfg:
            if not isinstance(cfg["kinds"], list):
                raise ConfigError(
                    "'kinds' must be a list of strings, at %s" % rv.pos
                )
            for k in cfg["kinds"]:
                rv._check_kind(k)
            rv.kinds.update(cfg["kinds"])

        if "action" in cfg:
            if str(cfg["action"]).lower() not in DumpRule.ACTIONS:
                actions = ", ".join(DumpRule.ACTIONS)
                raise ConfigError(
                    "bad 'action': '%s'; accepted values are %s, at %s"
                    % (cfg["action"], actions, rv.pos)
                )
            rv.action = cfg["action"].lower()

        if "skip" in cfg:
            if "action" in cfg:
                raise ConfigError(
                    "can't specify both 'skip' and 'action', at %s" % rv.pos
                )
            rv.action = "skip" if cfg["skip"] else "dump"

        if "no_columns" in cfg:
            if not (
                isinstance(cfg["no_columns"], list)
                and all(isinstance(col, str) for col in cfg["no_columns"])
            ):
                raise ConfigError(
                    "'no_columns' must be a list of strings, at %s" % rv.pos
                )
            rv.no_columns = cfg["no_columns"]

        if "replace" in cfg:
            if not (
                isinstance(cfg["replace"], dict)
                and all(isinstance(col, str) for col in cfg["replace"].keys())
                and all(
                    isinstance(expr, str) for expr in cfg["replace"].values()
                )
            ):
                raise ConfigError(
                    "'replace' must be a dictionary of strings, at %s" % rv.pos
                )
            rv.replace = cfg["replace"]

        if "filter" in cfg:
            if not isinstance(cfg["filter"], str):
                raise ConfigError("'filter' must be a string, at %s" % rv.pos)
            rv.filter = cfg["filter"]

        if "adjust_score" in cfg:
            if not isinstance(cfg["adjust_score"], (int, float)):
                raise ConfigError(
                    "'adjust_score' must be a number, at %s" % rv.pos
                )
            rv.adjust_score = cfg["adjust_score"]

        unks = set(cfg) - set(
            """
            name names schema schemas kind kinds
            action no_columns replace filter skip adjust_score
            """.split()
        )
        if unks:
            unks = ", ".join(sorted(unks))
            logger.warning("unknown config option(s): %s, at %s", unks, rv.pos)

        return rv

    def _check_kind(self, k):
        if not isinstance(k, str) or REVKINDS.get(k) not in DUMPABLE_KINDS:
            kinds = ", ".join(
                sorted(k for k, v in REVKINDS.items() if v in DUMPABLE_KINDS)
            )
            raise ConfigError(
                "bad 'kind': '%s'; accepted values are: %s, at %s"
                % (k, kinds, self.pos)
            )


class RuleMatcher:
    def __init__(self):
        self.rules = []

    def add_config(self, cfg):
        """
        Add a new config structure to the matcher

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
            cfg = DumpRule.from_config(cfg)
            self.rules.append(cfg)

    def get_rule(self, obj):
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
