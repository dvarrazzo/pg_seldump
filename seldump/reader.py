#!/usr/bin/env python3

"""
Readers base class

This file is part of pg_seldump.
"""

from abc import ABC


class Reader(ABC):
    """
    The base class of an object to read e db to dump.
    """

    def get_objects_to_dump(self):
        pass

    def get_tables_using_sequence(self, oid):
        pass
