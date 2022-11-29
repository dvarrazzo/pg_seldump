import io
import os
import shutil
import subprocess as sp
from collections import OrderedDict, defaultdict

import pytest
import psycopg
from psycopg import sql

from seldump import consts
from seldump.database import Database
from seldump.dbreader import DbReader
from seldump.dbobjects import Table, Sequence, MaterializedView

from .testreader import TestReader


def pytest_addoption(parser):
    parser.addoption(
        "--test-dsn",
        metavar="DSN",
        default=os.environ.get("SELDUMP_TEST_DSN") or None,
        help="Connection string to run database tests with the `conn` fixture"
        " [you can also use the SELDUMP_TEST_DSN env var].",
    )


@pytest.fixture()
def dsn(request):
    """Return the dsn used to connect to the `--test-dsn` database."""
    dsn = request.config.getoption("--test-dsn")
    if not dsn:
        pytest.skip("skipping test as no --test-dsn")
    return dsn


@pytest.fixture()
def conn(dsn):
    """Return a database connection connected to `--test-dsn`."""
    cnn = psycopg.connect(dsn)
    yield cnn
    cnn.close()


@pytest.fixture()
def fakeconn(monkeypatch):
    """Return a fake connection useful to pass to Composable.as_string()."""

    def fake_as_bytes(self, context):
        return b".".join(b'"%b"' % n.replace('"', '""').encode() for n in self._obj)

    monkeypatch.setattr(sql.Identifier, "as_bytes", fake_as_bytes)
    yield None


@pytest.fixture()
def db():
    """Return an object to perform test operations on a database."""
    return TestingDatabase()


@pytest.fixture
def psql(dsn):
    """Return an object to interact with the test db via psql."""
    return Psql(dsn)


class TestingDatabase:
    """
    An object to create test databases definitions
    """

    __test__ = False

    def __init__(self):
        self.target = None

    @property
    def connection(self):
        if self.target is None:
            raise TypeError("target not set")
        elif isinstance(self.target, DbReader):
            return self.target.connection
        elif isinstance(self.target, psycopg.Connection):
            return self.target
        else:
            raise TypeError(
                "don't know how to create a connection from %s" % self.target
            )

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

    def write_schema(self, objs):
        """
        Create the objs schema into a real database.
        """
        reader = TestReader()
        reader.db = Database()
        reader.load_db(objs)
        self.write_dbobjects(self.connection, reader.db)

    def truncate(self, objs):
        """
        Clear tables, reset sequences.
        """
        with self.connection.transaction():
            with self.connection.cursor() as cur:
                for obj in objs:
                    if isinstance(obj, Table):
                        cur.execute(
                            sql.SQL("truncate only {} cascade").format(obj.ident)
                        )
                    elif isinstance(obj, Sequence):
                        seq = sql.Literal(obj.ident.as_string(cur))
                        cur.execute(sql.SQL("select setval({}, 1, false)").format(seq))
                    elif isinstance(obj, MaterializedView):
                        # matviews later (but they should be toposorted maybe?)
                        pass

                    else:
                        raise TypeError("can't truncate %s" % obj)

                for obj in objs:
                    if isinstance(obj, MaterializedView):
                        cur.execute(
                            sql.SQL("refresh materialized view {}").format(obj.ident)
                        )

    def write_dbobjects(self, cnn, db):
        """
        Write the objects from a Database instance into a real database.
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

    def fill_data(self, table, columns, *datacols):
        if isinstance(columns, str):
            columns = (columns,)
        if len(columns) != len(datacols):
            raise TypeError(
                "got %s column names but %s data columns"
                % (len(columns), len(datacols))
            )

        stmt = sql.SQL("copy {} ({}) from stdin").format(
            sql.Identifier(table),
            sql.SQL(", ").join(map(sql.Identifier, columns)),
        )
        with self.connection.transaction():
            with self.connection.cursor() as curs:
                with curs.copy(stmt) as copy:
                    for rec in zip(*datacols):
                        copy.write_row(rec)

    def _create_table(self, cnn, db, table):
        cols = []
        for col in table.columns:
            bits = [col.ident, sql.SQL(col.type)]
            if col.used_sequence_oids:
                assert len(col.used_sequence_oids) == 1
                # sequence name in a SQL literal, e.g. '"foo"."bar"'
                seq = db.get(oid=col.used_sequence_oids[0]).ident
                seq = sql.Literal(seq.as_string(cnn))
                bits.append(sql.SQL("default nextval({}::regclass)").format(seq))
            cols.append(sql.SQL(" ").join(bits))

        stmt = sql.SQL("create table {} ({})").format(
            table.ident, sql.SQL(", ").join(cols)
        )
        with cnn.cursor() as cur:
            cur.execute(stmt)

    def _create_sequence(self, cnn, db, seq):
        stmt = sql.SQL("create sequence {}").format(seq.ident)
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
            sql.Identifier("%s_%s_key" % (t2.name, "_".join(fkey.ftable_cols))),
            t2.ident,
            sql.SQL(", ").join(map(sql.Identifier, fkey.ftable_cols)),
        )

        fkeystmt = sql.SQL(
            """
            alter table {} add constraint {}
            foreign key ({}) references {} ({})
            """
        ).format(
            t1.ident,
            fkey.ident,
            sql.SQL(", ").join(map(sql.Identifier, fkey.table_cols)),
            t2.ident,
            sql.SQL(", ").join(map(sql.Identifier, fkey.ftable_cols)),
        )
        with cnn.cursor() as cur:
            cur.execute(idxstmt)
            cur.execute(fkeystmt)


class Psql:
    def __init__(self, dsn):
        self.dsn = dsn

    def load_file(self, data):
        if isinstance(data, io.BytesIO):
            data.seek(0)
            data = data.read()
        cmdline = [shutil.which("psql"), "-X", "-1", self.dsn]
        sp.run(cmdline, input=data, stdout=sp.DEVNULL)
