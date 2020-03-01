import pytest

from seldump.dbobjects import Table, Sequence


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
def test_sequence_skipped(dumper, db, details, dumped):
    dumper.reader.load_db(db.create_sample(2))

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


def test_sequence_skip_override(dumper, db):
    dumper.reader.load_db(db.create_sample(2))
    tbl = dumper.db.get("public", "table1")
    seq = dumper.db.get("public", "table1_id_seq")
    dumper.add_config(
        {
            "db_objects": [
                {"name": "table1"},
                {"kind": "sequence", "action": "skip"},
            ]
        }
    )

    dumper.perform_dump()
    objs = [obj for obj, rule in dumper.writer.dumped]
    assert len(objs) == 1
    assert seq not in objs
    assert tbl in objs
