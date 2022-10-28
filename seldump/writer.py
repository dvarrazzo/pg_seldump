#!/usr/bin/env python3
"""
Writers base class

This file is part of pg_seldump.
"""

from abc import ABC, abstractmethod


class Writer(ABC):
    """
    The base class of an object to write a db dump.
    """

    @abstractmethod
    def begin_dump(self):
        pass

    @abstractmethod
    def end_dump(self):
        pass

    @abstractmethod
    def dump_table(self, table, match):
        pass

    @abstractmethod
    def dump_sequence(self, seq, match):
        pass

    @abstractmethod
    def dump_materialized_view(self, matview, match):
        pass
