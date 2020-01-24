#!/usr/bin/env python3
"""
Create a selective dump of a PostgreSQL database.
"""

# This file is part of pg_seldump.

import sys
import logging
from signal import SIGPIPE
from argparse import ArgumentParser, RawDescriptionHelpFormatter

from .consts import VERSION
from .matching import RuleMatcher
from .dumping import Dumper
from .exceptions import SelDumpException, ConfigError
from .yaml import load_yaml

logger = logging.getLogger("seldump")


def main():
    """Run the program, raise exceptions."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    opt = parse_cmdline()
    logger.setLevel(opt.loglevel)

    matcher = RuleMatcher()
    for fn in opt.config_files:
        try:
            cfg = load_yaml(fn)
        except Exception as e:
            raise ConfigError("error loading config file: %s" % e)
        else:
            matcher.add_config(cfg)

    dumper = Dumper(dsn=opt.dsn, matcher=matcher)

    if opt.outfile != "-":
        try:
            outfile = open(opt.outfile, "w")
        except Exception as e:
            raise ConfigError(
                "couldn't open %s for writing: %s" % (outfile, e)
            )
    else:
        outfile = sys.stdout

    try:
        dumper.dump_data(outfile=outfile, test=opt.test)
    finally:
        if opt.outfile != "-":
            outfile.close()


def script():
    """Run the program and terminate the process."""
    try:
        sys.exit(main())

    except SelDumpException as e:
        logger.error("%s", e)
        sys.exit(1)

    except BrokenPipeError as e:
        logger.error("dump interrupted: %s", e)
        # Not entirely correct: might have been ESHUTDOWN
        sys.exit(SIGPIPE + 128)

    except Exception:
        logger.exception("unexpected error")
        sys.exit(1)

    except KeyboardInterrupt:
        logger.info("user interrupt")
        sys.exit(1)


def parse_cmdline():
    parser = ArgumentParser(
        description=__doc__, formatter_class=RawDescriptionHelpFormatter
    )

    parser.add_argument(
        "--version", action="version", version="%%(prog)s %s" % VERSION
    )

    parser.add_argument(
        "config_files",
        nargs="+",
        metavar="config",
        help="yaml file describing the data to dump",
    )

    parser.add_argument(
        "--dsn",
        default="",
        help="database connection string [default: %(default)r]",
    )

    parser.add_argument(
        "--outfile",
        "-o",
        default="-",
        help="the file where to save the dump [default: stdout]",
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
