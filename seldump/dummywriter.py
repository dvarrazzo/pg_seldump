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

    def dump_table(self, table, action):
        logger.info("would dump %s %s", table.kind, table.escaped)

    def dump_sequence(self, seq, action):
        logger.info("would dump %s %s", seq.kind, seq.escaped)

    def dump_materialized_view(self, matview, action):
        logger.info("would dump %s %s", matview.kind, matview.escaped)

    def close(self):
        pass
