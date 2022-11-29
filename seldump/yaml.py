#!/usr/bin/env python3
"""
Customized YAML parser.

This file is part of pg_seldump.
"""

import yaml

import logging

logger = logging.getLogger("seldump.yaml")


class ListWithPos(list):
    """
    A list with attached filename and line numbers where it was parsed from
    """

    __slots__ = ("filename", "lineno", "itemlines")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.filename = None
        self.lineno = None
        self.itemlines = []

    def __repr__(self):
        return super().__repr__()

    def extend(self, other):
        for item in other:
            if isinstance(item, ScalarWithPos):
                self.itemlines.append(item.lineno)
                self.append(item.obj)
            else:
                self.itemlines.append(None)
                self.append(item)


class DictWithPos(dict):
    """
    A dict with attached filename and line numbers where it was parsed from
    """

    __slots__ = ("filename", "lineno", "itemlines")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.filename = None
        self.lineno = None
        self.itemlines = {}

    def __repr__(self):
        return super().__repr__()

    def update(self, other):
        assert isinstance(other, dict)
        for k, v in other.items():
            if isinstance(k, ScalarWithPos):
                self.itemlines[k.obj] = k.lineno
                k = k.obj

            if isinstance(v, ScalarWithPos):
                # The value may be more precise than the key
                self.itemlines[k] = v.lineno
                v = v.obj

            self[k] = v


class ScalarWithPos:
    __slots__ = ("obj", "lineno")

    def __init__(self, obj, node):
        self.obj = obj
        self.lineno = node.start_mark.line + 1

    def __repr__(self):
        return "%s(%r, %r)" % (self.__class__.__name__, self.obj, self.lineno)

    def __hash__(self):
        return hash(self.obj)

    def __getattr__(self, attr):
        return getattr(self.obj, attr)


class RichLoader(yaml.SafeLoader):
    """
    YAML parser storing file name and line number for each parsed dicts.
    """

    def construct_scalar(self, node):
        rv = super().construct_scalar(node)
        return ScalarWithPos(rv, node)

    def construct_yaml_null(self, node):
        rv = super().construct_yaml_null(node)
        return ScalarWithPos(rv, node)

    def construct_yaml_bool(self, node):
        rv = super().construct_yaml_bool(node)
        return ScalarWithPos(rv, node)

    def construct_yaml_int(self, node):
        rv = super().construct_yaml_int(node)
        return ScalarWithPos(rv, node)

    def construct_yaml_float(self, node):
        rv = super().construct_yaml_float(node)
        return ScalarWithPos(rv, node)

    def construct_yaml_map(self, node):
        data = DictWithPos()
        data.filename = node.start_mark.name
        data.lineno = node.start_mark.line + 1
        yield data
        value = self.construct_mapping(node)
        data.update(value)

    def construct_yaml_seq(self, node):
        data = ListWithPos()
        data.filename = node.start_mark.name
        data.lineno = node.start_mark.line + 1
        yield data
        data.extend(self.construct_sequence(node))


# This really overrides the yaml parser
RichLoader.add_constructor("tag:yaml.org,2002:null", RichLoader.construct_yaml_null)
RichLoader.add_constructor("tag:yaml.org,2002:bool", RichLoader.construct_yaml_bool)
RichLoader.add_constructor("tag:yaml.org,2002:int", RichLoader.construct_yaml_int)
RichLoader.add_constructor("tag:yaml.org,2002:float", RichLoader.construct_yaml_float)
RichLoader.add_constructor("tag:yaml.org,2002:map", RichLoader.construct_yaml_map)
RichLoader.add_constructor("tag:yaml.org,2002:seq", RichLoader.construct_yaml_seq)


def load_yaml(stream):
    """Load a yaml file from a file."""
    return yaml.load(stream, Loader=RichLoader)
