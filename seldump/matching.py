#!/usr/bin/env python3

"""
Dump rules and matching.

This file is part of pg_seldump.
"""

import re
import logging
from collections import namedtuple

from .consts import REVKINDS, DUMPABLE_KINDS
from .exceptions import DumpError

logger = logging.getLogger("seldump.matching")


ObjectConfig = namedtuple(
    "ObjectConfig", "skip no_columns replace filter filename lineno"
)


class RuleMatcher:
    def __init__(self):
        self.config_objs = []

    def add_config(self, cfg):
        try:
            objs = cfg["db_objects"]
        except (KeyError, TypeError):
            raise DumpError("the config file should have a db_objects list")

        if not isinstance(objs, list):
            raise DumpError(
                "db_objects should be a list, got %s" % type(objs).__name__
            )

        for cfg in objs:
            self.validate_config(cfg)
            self.config_objs.append(cfg)

    def validate_config(self, cfg):
        if not isinstance(cfg, dict):
            raise DumpError("expected config dict, got %s" % cfg)

        if "name" in cfg and "names" in cfg:
            raise DumpError(
                "config can't specify both name and names, got %s" % cfg
            )
        if "schema" in cfg and "schemas" in cfg:
            raise DumpError(
                "config can't specify both schema and schemas, got %s" % cfg
            )
        if "kind" in cfg:
            if REVKINDS.get(cfg["kind"]) not in DUMPABLE_KINDS:
                kinds = sorted(
                    k for k, v in REVKINDS.items() if v in DUMPABLE_KINDS
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
                "unknown config options: %s; got %s",
                ", ".join(sorted(unks)),
                cfg,
            )

    def get_config(self, obj):
        for cfg in self.config_objs:
            if not self.config_matches(cfg, obj):
                continue

            rv = ObjectConfig(
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
