#!/usr/bin/env python3
"""
Customized YAML parser.

This file is part of pg_seldump.
"""

import yaml


class DictWithPos(dict):
    """
    A dict with attached filename and line numbers where it was parsed from
    """

    __slots__ = ("filename", "lineno")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.filename = None
        self.lineno = None

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, dict(self))


class RichLoader(yaml.SafeLoader):
    """
    YAML parser storing file name and line number for each parsed dicts.
    """

    def construct_yaml_map(self, node):
        data = DictWithPos()
        data.filename = node.start_mark.name
        data.lineno = node.start_mark.line + 1
        yield data
        value = self.construct_mapping(node)
        data.update(value)


# This really overrides the yaml dict parser
RichLoader.add_constructor(
    "tag:yaml.org,2002:map", RichLoader.construct_yaml_map
)


def load_yaml(filename):
    """Load a yaml file from a file."""
    with open(filename) as f:
        return yaml.load(f, Loader=RichLoader)
