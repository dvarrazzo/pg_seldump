#!/usr/bin/env python3
"""
Program constants.

This file is part of pg_seldump.
"""

import sys

from .dumper import Dumper, DumpError
from .cli import parse_cmdline
from .yaml import load_yaml

import logging

logger = logging.getLogger("seldump")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def main():
    """Run the program, raise exceptions."""
    opt = parse_cmdline()
    logger.setLevel(opt.loglevel)

    dumper = Dumper(dsn=opt.dsn, test=opt.test)

    for fn in opt.config_files:
        try:
            cfg = load_yaml(fn)
        except Exception as e:
            raise DumpError("error loading config file: %s" % e)
        else:
            dumper.add_config(cfg)

    dumper.dump_data(schemas=opt.schema or None)


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
