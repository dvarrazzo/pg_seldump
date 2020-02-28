#!/usr/bin/env python3

"""
Readers base class

This file is part of pg_seldump.
"""

from abc import ABC, abstractmethod


class Reader(ABC):
    """
    The base class of an object to read e db to dump.
    """

    def __init__(self):
        self.db = None

    @abstractmethod
    def load_schema(self):
        pass
