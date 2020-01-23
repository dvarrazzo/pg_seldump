#!/usr/bin/env python3

"""
Program exceptions.

This file is part of pg_seldump.
"""


class SelDumpException(Exception):
    """A controlled exception raised by the script."""


class DumpError(SelDumpException):
    """Error dumping the database."""


class ConfigError(SelDumpException):
    """Error parsing configuration."""
