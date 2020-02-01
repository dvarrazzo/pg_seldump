"""
Program constants.

This file is part of pg_seldump.
"""

VERSION = "0.1"
PROJECT_URL = "https://github.com/dvarrazzo/pg_seldump"

# relkind values: https://www.postgresql.org/docs/11/catalog-pg-class.html
PG_KINDS = {
    "r": "table",
    "i": "index",
    "S": "sequence",
    "t": "toast table`",
    "v": "view",
    "m": "materialized view",
    "c": "composite type",
    "f": "foreign table",
    "p": "partitioned table",
    "I": "partitioned index",
}

# reverse map from kind name to relkind
REVKINDS = {v: k for k, v in PG_KINDS.items()}

# kinds with a state to be dumped
# other objects are either internal (indexes, toast tables) or stateless (views)
DUMPABLE_KINDS = set("rSmp")
