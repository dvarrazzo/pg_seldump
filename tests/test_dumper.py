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


SAMPLE_DB = {
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
}


def test_one_table(dumper):
    """
    You can select a single table to dump
    """
    dumper.reader.load_db(SAMPLE_DB)
    dumper.add_config({"db_objects": [{"name": "table1"}]})
    dumper.perform_dump()

    objs = [obj for obj, rule in dumper.writer.dumped]

    obj = dumper.db.get("public", "table1")
    assert isinstance(obj, Table)
    assert obj in objs

    obj = dumper.db.get("public", "table2")
    assert isinstance(obj, Table)
    assert obj not in objs

    obj = dumper.db.get("public", "table1_id_seq")
    assert isinstance(obj, Sequence)
    assert obj in objs

    obj = dumper.db.get("public", "table2_id_seq")
    assert isinstance(obj, Sequence)
    assert obj not in objs


@pytest.mark.parametrize(
    "details, dumped",
    [
        ({}, True),
        ({"no_columns": ["data"]}, True),
        ({"no_columns": ["id"]}, False),
        ({"replace": {"data": "NULL"}}, True),
        ({"replace": {"id": "NULL"}}, False),
    ],
)
def test_sequence_skipped(dumper, details, dumped):
    dumper.reader.load_db(SAMPLE_DB)

    tbl = dumper.db.get("public", "table1")
    assert isinstance(tbl, Table)
    seq = dumper.db.get("public", "table1_id_seq")
    assert isinstance(seq, Sequence)

    conf = {"name": "table1"}
    conf.update(details)
    dumper.add_config({"db_objects": [conf]})
    dumper.perform_dump()
    objs = [obj for obj, rule in dumper.writer.dumped]
    assert tbl in objs
    if dumped:
        assert len(objs) == 2
        assert seq in objs
    else:
        assert len(objs) == 1
        assert seq not in objs
