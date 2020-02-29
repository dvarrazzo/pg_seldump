#!/usr/bin/env python3

"""
Rule to configure dumping of certain db objects.

This file is part of pg_seldump.
"""

import re
import logging
from functools import lru_cache

logger = logging.getLogger("seldump.dumprule")


class DumpRule:
    """
    Dump configuration of a set of database objects

    Each DumpRule has a few selector attributes, to choose which objects it
    applies, and a set of attributes specifying what action to take.
    """

    ACTION_DUMP = "dump"
    ACTION_SKIP = "skip"
    ACTION_ERROR = "error"
    ACTION_DEP = "dep"

    # The actions that can be chosen in the config file
    ACTIONS = [ACTION_DUMP, ACTION_SKIP, ACTION_ERROR]

    def __init__(self):
        # Matching attributes
        self.names = set()
        self.names_re = None
        self.schemas = set()
        self.schemas_re = None
        self.kinds = set()
        self.adjust_score = 0

        # Actions
        self.action = self.ACTION_DUMP
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

        if "name" in cfg:
            rv.names.add(cfg["name"])

        if "names" in cfg:
            if isinstance(cfg["names"], list):
                rv.names.update(cfg["names"])
            else:
                rv.names_re = re.compile(cfg["names"], re.VERBOSE)

        if "schema" in cfg:
            rv.schemas.add(cfg["schema"])

        if "schemas" in cfg:
            if isinstance(cfg["schemas"], list):
                rv.schemas.update(cfg["schemas"])
            else:
                rv.schemas_re = re.compile(cfg["schemas"], re.VERBOSE)

        if "kind" in cfg:
            rv.kinds.add(cfg["kind"])

        if "kinds" in cfg:
            rv.kinds.update(cfg["kinds"])

        if "action" in cfg:
            rv.action = cfg["action"].lower()

        if "no_columns" in cfg:
            rv.no_columns = cfg["no_columns"]

        if "replace" in cfg:
            rv.replace = cfg["replace"]

        if "filter" in cfg:
            rv.filter = cfg["filter"]

        if "adjust_score" in cfg:
            rv.adjust_score = cfg["adjust_score"]

        return rv
