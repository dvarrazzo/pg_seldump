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
from .exceptions import DumpError
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
            raise DumpError("error loading config file: %s" % e)
        else:
            matcher.add_config(cfg)

    dumper = Dumper(dsn=opt.dsn, matcher=matcher)
    dumper.dump_data(schemas=opt.schema or None, test=opt.test)


def script():
    """Run the program and terminate the process."""
    try:
        sys.exit(main())

    except DumpError as e:
        logger.error("%s", e)
        sys.exit(1)

    except Exception:
        logger.exception("unexpected error")
        sys.exit(1)

    except KeyboardInterrupt:
        logger.info("user interrupt")
        sys.exit(1)


if __name__ == "__main__":
    main()
