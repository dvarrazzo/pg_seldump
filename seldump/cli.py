#!/usr/bin/env python3
"""
Create a selective dump of a PostgreSQL database.
"""

# This file is part of pg_seldump.

import logging
from argparse import ArgumentParser, RawDescriptionHelpFormatter

from .consts import VERSION


def parse_cmdline():
    parser = ArgumentParser(
        description=__doc__, formatter_class=RawDescriptionHelpFormatter
    )

    parser.add_argument(
        "--version", action="version", version=f"%(prog)s {VERSION}"
    )

    parser.add_argument(
        "--dsn",
        default="",
        help="database connection string [default: %(default)r]",
    )

    parser.add_argument(
        "config_files",
        nargs="+",
        metavar="config",
        help="yaml file describing the data to dump",
    )

    # TODO: drop
    parser.add_argument(
        "-n",
        "--schema",
        nargs="+",
        help="only includes these schemas in the dump",
    )

    parser.add_argument(
        "--test",
        action="store_true",
        help="test the configuration to verify it works as expected",
    )

    g = parser.add_mutually_exclusive_group()
    g.add_argument(
        "-q",
        "--quiet",
        help="talk less",
        dest="loglevel",
        action="store_const",
        const=logging.WARN,
        default=logging.INFO,
    )
    g.add_argument(
        "-v",
        "--verbose",
        help="talk more",
        dest="loglevel",
        action="store_const",
        const=logging.DEBUG,
        default=logging.INFO,
    )

    opt = parser.parse_args()

    return opt
