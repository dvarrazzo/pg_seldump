PostgreSQL selective data dump
==============================

This tool allows to extract data from a PostgreSQL_ database with greater
flexibility that tools like pg_dump_ allow.

``pg_seldump`` reads one of more dump definitions from YAML files and selects
what tables or other database objects to save. It is possible to extract only
certain columns of the tables, only certain records, or to replace certain
values with a different expression, for instance to anonymize data.

The output of the program is a text file which can be used by psql_ to
restore data into a database with a complete schema but with no data (or at
least no conflicting data), e.g. using::

    $ pg_seldump --dsn="dbname=sourcedb" datadump.yaml > dump.sql
    ...
    $ psql -1X --set ON_ERROR_STOP=1 -f dump.sql "dbname=targetdb"

.. _PostgreSQL: https://www.postgresql.org/
.. _pg_dump: https://www.postgresql.org/docs/current/app-pgdump.html
.. _psql: https://www.postgresql.org/docs/current/app-psql.html


Program usage
-------------

Usage::

    pg_seldump [-h] [--version] [--dsn DSN] [--outfile OUTFILE] [--test]
               [-q | -v]
               config [config ...]

    Create a selective dump of a PostgreSQL database.

    positional arguments:
      config                yaml file describing the data to dump

    optional arguments:
      -h, --help            show this help message and exit
      --version             show program's version number and exit
      --dsn DSN             database connection string [default: '']
      --outfile OUTFILE, -o OUTFILE
                            the file where to save the dump [default: stdout]
      --test                test the configuration to verify it works as expected
      -q, --quiet           talk less
      -v, --verbose         talk more

The ``config`` files must be YAML_ files containing a ``db_objects`` list of
entries. Each entry may have:

.. _YAML: https://yaml.org/

Selectors (all the specified ones must match):

- ``name``: name of the db object to dump
- ``names``: list of names or regex of db objects to dump
- ``schema``: schema name of the db object to dump
- ``schemas``: list of schema names or regexp to match schema names of the
  db object to dump
- ``kind``: kind of object to match (table, sequence, a few others)
- ``kinds``: list of kind of objects to match (table, sequence, a few others)
- ``adjust_score``: adjustment for the match score to break rules ties

Data modifiers:

- ``action``: what to do with the matched object:

  - ``dump``: dump the object in the output (default)
  - ``skip``: don't dump the object
  - ``error``: raise an error in case of match (useful to create strict
    description where all the db objects must be mentioned explicitly)

- ``no_columns``: list of columns names to omit
- ``filter``: WHERE condition to include only a subset of the records in the dump
- ``replace``: mapping from column names to SQL expressions to replace values
  into the dump with somethings else

The objects in the database are matched to the rules in the config files.
Every match will have a score according to how specific was the selector
matched the object.

- ``name`` or ``names`` list: 1000
- ``names`` regexp: 500
- ``schema`` or ``schemas`` list: 100
- ``schemas`` regexp: 50
- ``kind`` or ``kinds``: 10

The rule with the highest score will apply. If two rules have exactly the same
score the program will report an error: you can use ``adjust_score`` to break
the tie.
