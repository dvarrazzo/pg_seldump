import io
import pytest

from seldump.dumper import Dumper
from seldump.dbreader import DbReader
from seldump.dumpwriter import DumpWriter

from .testreader import TestReader
from .testwriter import TestWriter


pytest_plugins = ("tests.fix_db",)


@pytest.fixture
def dumper():
    """Return a `seldump.Dumper` configured for testing."""
    reader = TestReader()
    writer = TestWriter()
    dumper = Dumper(reader=reader, writer=writer)
    return dumper


@pytest.fixture
def dbdumper(dsn, db):
    """Return a `seldump.Dumper` configured for db interaction."""
    reader = DbReader(dsn)
    writer = DumpWriter(outfile=io.BytesIO(), reader=reader)
    dumper = Dumper(reader=reader, writer=writer)

    # Set so that if the db fixture is used it can write here already
    db.target = reader

    return dumper
