==============================
PostgreSQL selective data dump
==============================

The utility allows to extract data from a PostgreSQL_ database with greater
flexibility that tools like pg_dump_ allow.

``pg_seldump`` reads one of more dump definitions from YAML files and selects
what tables or other database objects to save. It is possible to extract only
certain columns of the tables, only certain records, or to replace certain
values with a different expression, for instance to anonymize data.

.. _PostgreSQL: https://www.postgresql.org/
.. _pg_dump: https://www.postgresql.org/docs/current/app-pgdump.html


Program usage
=============

::

    usage: pg_seldump [-h] [--version] [-n SCHEMA [SCHEMA ...]]
                      [--test] [-q | -v]
                      config [dsn]

    positional arguments:
      config                yaml file describing the data to dump
      dsn                   database connection string [default: '']

    optional arguments:
      --test                test the configuration to verify it works as expected
      -q, --quiet           talk less
      -v, --verbose         talk more

The config file must be a YAML file containing a ``db_objects`` list of
entries. Each entry may have:

Selectors (all the specified ones must match):

- ``name``: name of the db object to dump
- ``names``: regex to match names of db objects to dump
- ``schema``: schema name of the db object to dump
- ``schemas``: regexp to match schema names of the db object to dump
- ``kind``: kind of object match (table, sequence, a few others)

Data modifiers:

- ``skip``: if true don't dump the matching table (default: false)
- ``no_columns``: list of columns names to omit
- ``filter``: WHERE condition to include only a subset of the records in the dump
- ``replace``: mapping from column names to SQL expressions to replace values
  into the dump with somethings else

The objects in the database are matched to the rules in the config files.
Every match will have a score according to how specific was the selector
matched the object (TODO: this is a lie atm):

- ``name``: 1000
- ``names``: 500
- ``schema``: 100
- ``schemas``: 50
- ``kind``: 10
