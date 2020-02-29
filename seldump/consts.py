"""
Program constants.

This file is part of pg_seldump.
"""

VERSION = "0.3.dev0"
PROJECT_URL = "https://github.com/dvarrazzo/pg_seldump"


# kinds with a state to be dumped
# other objects are either internal (indexes, toast tables) or stateless (views)
DUMPABLE_KINDS = set("rSmp")

KIND_TABLE = "table"
KIND_MATVIEW = "materialized view"
KIND_SEQUENCE = "sequence"
KIND_PART_TABLE = "partitioned table"

# relkind values: https://www.postgresql.org/docs/11/catalog-pg-class.html
PG_KINDS = {
    "r": KIND_TABLE,
    "i": "index",
    "S": KIND_SEQUENCE,
    "t": "toast table`",
    "v": "view",
    "m": KIND_MATVIEW,
    "c": "composite type",
    "f": "foreign table",
    "p": KIND_PART_TABLE,
    "I": "partitioned index",
}

# reverse map from kind name to relkind
REVKINDS = {v: k for k, v in PG_KINDS.items()}
