#!/usr/bin/env python3
"""
Program constants.

This file is part of pg_seldump.
"""

import sys
import logging

from .cli import parse_cmdline
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
            raise ConfigError(f"couldn't open {outfile} for writing: {e}")
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
        sys.exit(1)

    except Exception:
        logger.exception("unexpected error")
        sys.exit(1)

    except KeyboardInterrupt:
        logger.info("user interrupt")
        sys.exit(1)


if __name__ == "__main__":
    main()
