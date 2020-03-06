#!/usr/bin/env python3
"""
Pretend to write a dump

This file is part of pg_seldump.
"""

import logging

from .writer import Writer

logger = logging.getLogger("seldump.dummywriter")


class DummyWriter(Writer):
    def begin_dump(self):
        logger.debug("start of dump")

    def end_dump(self):
        logger.debug("end of dump")

    def dump_table(self, table, match):
        logger.info("would dump %s %s", table.kind, table)

    def dump_sequence(self, seq, match):
        logger.info("would dump %s %s", seq.kind, seq)

    def dump_materialized_view(self, matview, match):
        logger.info("would dump %s %s", matview.kind, matview)

    def close(self):
        pass
