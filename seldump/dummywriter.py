#!/usr/bin/env python3
"""
Pretend to write a dump

This file is part of pg_seldump.
"""

import logging

logger = logging.getLogger("seldump.dummywriter")


class DummyWriter:
    def begin_dump(self):
        logger.debug("start of dump")

    def end_dump(self):
        logger.debug("end of dump")

    def dump_table(self, table, config):
        logger.info("would dump %s %s", table.kind, table.escaped)

    def dump_sequence(self, seq, config):
        logger.info("would dump %s %s", seq.kind, seq.escaped)

    def dump_materialized_view(self, matview, config):
        logger.info("would dump %s %s", matview.kind, matview.escaped)

    def close(self):
        pass
