from collections import OrderedDict, defaultdict

import pytest
import psycopg2
from psycopg2 import sql

from seldump import consts
from seldump.database import Database
from seldump.dbreader import DbReader
from seldump.dbobjects import Table, Sequence, MaterializedView

from .testreader import TestReader


def pytest_addoption(parser):
    parser.addoption(
        "--test-dsn",
        metavar="DSN",
        help="Connection string to run database tests with the `conn` fixture.",
    )


@pytest.fixture()
def dsn(request):
    """Return the dsn used to connect to the `--test-dsn` database."""
    dsn = request.config.getoption("--test-dsn")
    if dsn is None:
        pytest.skip("skipping test as no --test-dsn")
    return dsn


@pytest.fixture()
def conn(dsn):
    """Return a database connection connected to `--test-dsn`."""
    cnn = psycopg2.connect(dsn)
    yield cnn
    cnn.close()


@pytest.fixture()
def dbreader(dsn):
    """Return a DbReader instance connected to the `--test-dsn` database"""
    reader = DbReader(dsn)
    reader.db = Database()
    return reader


@pytest.fixture()
def db():
    """Return an object to perform test operations on a database."""
    return TestingDatabase()


class TestingDatabase:
    """
    An object to create test databases definitions
    """

    __test__ = False

    def create_sample(self, ntables, fkeys=()):
        """
        Create a python schema of a sample database
        """
        rv = OrderedDict()
        for i in range(1, ntables + 1):
            name = "table%s" % i
            rv[name] = t = {
                "schema": "public",
                "name": name,
                "columns": OrderedDict(),
            }
            t["columns"]["id"] = {
                "name": "id",
                "type": "integer",
                "use_sequence": "table%s_id_seq" % i,
            }
            t["columns"]["data"] = {"name": "data", "type": "text"}

        for fkey in fkeys:
            t1, f1, t2, f2 = fkey
            # TODO: multicol fkeys
            assert isinstance(f1, str)
            assert isinstance(f2, str)
            if f1 not in rv[t1]["columns"]:
                rv[t1]["columns"][f1] = {"name": f1, "type": "integer"}
            if f2 not in rv[t2]["columns"]:
                rv[t2]["columns"][f2] = {"name": f2, "type": "integer"}
            fkey = {
                "name": "%s_%s_%s_fkey" % (f1, t2, f2),
                "cols": [f1],
                "ftable": t2,
                "fcols": [f2],
            }
            rv[t1].setdefault("fkeys", []).append(fkey)

        return list(rv.values())

    def clear_database(self, cnn):
        """
        Delete all the objects in the database.

        Really.
        """
        kinds = (consts.KIND_TABLE, consts.KIND_SEQUENCE, consts.KIND_MATVIEW)
        with cnn.cursor() as cur:
            for kind in kinds:
                cur.execute(
                    """
select s.nspname as schema, c.relname as table
from pg_class c
join pg_namespace s on s.oid = c.relnamespace
where c.relkind = %s
and s.nspname != 'information_schema'
and s.nspname !~ '^pg_'
order by 1, 2
                    """,
                    (consts.REVKINDS[kind],),
                )
                recs = cur.fetchall()
                if not recs:
                    continue

                objs = sql.SQL(", ").join(
                    sql.Identifier(schema, table) for schema, table in recs
                )
                stmt = sql.SQL("drop {} if exists {} cascade").format(
                    sql.SQL(kind), objs
                )
                cur.execute(stmt)

    def write_sample(self, cnn, objs):
        reader = TestReader()
        reader.db = Database()
        reader.load_db(objs)
        self.write_db(cnn, reader.db)

    def write_db(self, cnn, db):
        """
        Write the objects from a Database into a db
        """
        self.clear_database(cnn)
        by_class = defaultdict(list)
        # TODO: the name should be in the object - create a better schema
        for obj in db:
            by_class[type(obj)].append(obj)

        # Create the objects
        for obj in by_class[Sequence]:
            self._create_sequence(cnn, db, obj)
        for obj in by_class[Table]:
            self._create_table(cnn, db, obj)
        for obj in by_class[MaterializedView]:
            self._create_matview(cnn, db, obj)

        # Create implicit sequences and fkeys
        for obj in by_class[Table]:
            for fkey in obj.fkeys:
                self._create_fkey(cnn, db, fkey)

        cnn.commit()

    def _create_table(self, cnn, db, table):
        name = sql.Identifier(table.schema, table.name)
        cols = []
        for col in table.columns:
            bits = [sql.Identifier(col.name), sql.SQL(col.type)]
            if col.used_sequence_oids:
                assert len(col.used_sequence_oids) == 1
                # sequence name in a SQL literal, e.g. '"foo"."bar"'
                seq = db.get(oid=col.used_sequence_oids[0])
                seq = sql.Identifier(seq.schema, seq.name)
                seq = sql.Literal(seq.as_string(cnn))
                bits.append(
                    sql.SQL("default nextval({}::regclass)").format(seq)
                )
            cols.append(sql.SQL(" ").join(bits))

        stmt = sql.SQL("create table {} ({})").format(
            name, sql.SQL(", ").join(cols)
        )
        with cnn.cursor() as cur:
            cur.execute(stmt)

    def _create_sequence(self, cnn, db, seq):
        name = sql.Identifier(seq.schema, seq.name)
        stmt = sql.SQL("create sequence {}").format(name)
        with cnn.cursor() as cur:
            cur.execute(stmt)

    def _create_matview(self, cnn, db, seq):
        raise NotImplementedError

    def _create_fkey(self, cnn, db, fkey):
        t1 = db.get(oid=fkey.table_oid)
        t2 = db.get(oid=fkey.ftable_oid)

        idxstmt = sql.SQL(
            """
            create unique index if not exists {} on {} ({})
            """
        ).format(
            sql.Identifier(
                "%s_%s_key" % (t2.name, "_".join(fkey.ftable_cols))
            ),
            sql.Identifier(t2.schema, t2.name),
            sql.SQL(", ").join(map(sql.Identifier, fkey.ftable_cols)),
        )

        fkeystmt = sql.SQL(
            """
            alter table {} add constraint {}
            foreign key ({}) references {} ({})
            """
        ).format(
            sql.Identifier(t1.schema, t1.name),
            sql.Identifier(fkey.name),
            sql.SQL(", ").join(map(sql.Identifier, fkey.table_cols)),
            sql.Identifier(t2.schema, t2.name),
            sql.SQL(", ").join(map(sql.Identifier, fkey.ftable_cols)),
        )
        with cnn.cursor() as cur:
            cur.execute(idxstmt)
            cur.execute(fkeystmt)
