"""
Program constants.

This file is part of pg_seldump.
"""

VERSION = "0.1"
PROJECT_URL = "https://github.com/dvarrazzo/pg_seldump"

# kinds with a state to be dumped
# other objects are either internal (indexes, toast tables) or stateless (views)
DUMPABLE_KINDS = set("rSmp")

# We don't care enough about the other objects to give them a symbolic const
TABLE = 'table'
PART_TABLE = 'partitioned table'
MATVIEW = 'materialized view'
SEQUENCE = 'sequence'

# relkind values: https://www.postgresql.org/docs/11/catalog-pg-class.html
PG_KINDS = {
    "r": TABLE,
    "i": "index",
    "S": SEQUENCE,
    "t": "toast table`",
    "v": "view",
    "m": MATVIEW,
    "c": "composite type",
    "f": "foreign table",
    "p": PART_TABLE,
    "I": "partitioned index",
}

# reverse map from kind name to relkind
REVKINDS = {v: k for k, v in PG_KINDS.items()}
