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
    ACTION_REFERENCED = "ref"
    ACTION_UNKNOWN = "unknown"

    def __init__(
        self,
        name=None,
        names=None,
        schema=None,
        schemas=None,
        kind=None,
        kinds=None,
        adjust_score=0,
        action=ACTION_DUMP,
        no_columns=None,
        replace=None,
        filter=None,
    ):
        """
        The constructor arguments are the attributes from a valid rule object
        validated according the `schema/config.yaml` rules
        """
        # Matching attributes
        if name is not None:
            names = [name]

        if names is None:
            self.names = set()
            self.names_re = None
        elif isinstance(names, list):
            self.names = set(names)
            self.names_re = None
        else:
            self.names = set()
            self.names_re = re.compile(names, re.VERBOSE)

        if schema is not None:
            schemas = [schema]

        if schemas is None:
            self.schemas = set()
            self.schemas_re = None
        elif isinstance(schemas, list):
            self.schemas = set(schemas)
            self.schemas_re = None
        else:
            self.schemas = set()
            self.schemas_re = re.compile(schemas, re.VERBOSE)

        if kind is not None:
            kinds = [kind]
        self.kinds = set(kinds or ())

        self.adjust_score = adjust_score

        # Actions
        self.action = action
        self.no_columns = no_columns or []
        self.replace = replace or {}
        self.filter = filter

        # Description
        self.filename = None
        self.lineno = None

    @classmethod
    def from_config(cls, cfg):
        """
        Create a rule from a config object.

        It will also try to store the config position if found.
        """
        rv = cls(**cfg)
        rv.filename = getattr(cfg, "filename", None)
        rv.lineno = getattr(cfg, "lineno", None)
        return rv

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

        if self.schemas_re is not None and not self.schemas_re.match(obj.schema):
            return False

        if self.kinds and obj.kind not in self.kinds:
            return False

        return True


class RuleMatch:
    """
    The match between an Rule iand a database object.

    The object is closely connected to a DumpRule, but it extends on that,
    defining an operation taking into account the state of the database too.
    """

    def __init__(self, obj, action=DumpRule.ACTION_UNKNOWN):
        self.obj = obj
        self.action = action

        self.rule = None
        self.no_columns = []
        self.replace = {}
        self.filter = None

        self.referenced_by = []

        self.query = None
        self.import_statement = None
        self.copy_statement = None
        self.errors = []

    @classmethod
    def from_rule(cls, obj, rule):
        rv = cls(obj, rule.action)
        rv.action = rule.action
        rv.no_columns = rule.no_columns
        rv.replace = rule.replace
        rv.filter = rule.filter
        if rule.action == DumpRule.ACTION_ERROR:
            if rule.filename and rule.lineno:
                msg = "the object matches the error rule at %s:%s" % (
                    rule.filename,
                    rule.lineno,
                )
            else:
                msg = "the object matches an error rule"

            rv.errors.append(msg)

        return rv
