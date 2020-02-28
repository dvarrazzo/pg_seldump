import pytest

from seldump.dumper import Dumper
from seldump.dbobjects import Table, Sequence

from .testreader import TestReader
from .testwriter import TestWriter


@pytest.fixture
def dumper():
    """Return a `seldump.Dumper` configured for testing."""
    reader = TestReader()
    writer = TestWriter()
    dumper = Dumper(reader=reader, writer=writer)
    return dumper


def test_void(dumper):
    """
    On empty input, result is empty
    """
    dumper.perform_dump()
    assert not dumper.writer.dumped


def test_one_table(dumper):
    """
    You can select a single table to dump
    """
    dumper.reader.load_db(
        {
            "table1": {
                "columns": [
                    {
                        "name": "id",
                        "type": "integer",
                        "use_sequence": "table1_id_seq",
                    },
                    {"name": "data", "type": "text"},
                ],
            },
            "table2": {
                "columns": [
                    {
                        "name": "id",
                        "type": "integer",
                        "use_sequence": "table2_id_seq",
                    },
                    {"name": "data", "type": "text"},
                ],
            },
        },
    )
    dumper.add_config({"db_objects": [{"name": "table1"}]})
    dumper.perform_dump()

    objs = [obj for obj, rule in dumper.writer.dumped]

    obj = dumper.reader.db.get("public", "table1")
    assert isinstance(obj, Table)
    assert obj in objs

    obj = dumper.reader.db.get("public", "table2")
    assert isinstance(obj, Table)
    assert obj not in objs

    obj = dumper.reader.db.get("public", "table1_id_seq")
    assert isinstance(obj, Sequence)
    assert obj in objs

    obj = dumper.reader.db.get("public", "table2_id_seq")
    assert isinstance(obj, Sequence)
    assert obj not in objs
