import pytest

from seldump.dbobjects import Table, Sequence
from seldump.exceptions import ConfigError


def test_void(dumper):
    """On empty input, result is empty."""
    dumper.perform_dump()
    assert not dumper.writer.dumped


def test_one_table(dumper, db):
    """You can select a single table to dump."""
    dumper.reader.load_db(db.create_sample(2))
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


@pytest.mark.parametrize("spec", ["^table[13]$", ["table1", "table3"]])
def test_names(dumper, db, spec):
    """Tables can be selected by names regexp or list"""
    dumper.reader.load_db(db.create_sample(3))
    dumper.add_config({"db_objects": [{"names": spec}]})
    dumper.perform_dump()

    tables = [obj for obj, rule in dumper.writer.dumped if isinstance(obj, Table)]
    assert len(tables) == 2
    assert dumper.db.get("public", "table1") in tables
    assert dumper.db.get("public", "table2") not in tables
    assert dumper.db.get("public", "table3") in tables


def test_name_over_names_regexp(dumper, db):
    """A specified name overrides a names regexp"""
    dumper.reader.load_db(db.create_sample(3))
    dumper.add_config(
        {
            "db_objects": [
                {"name": "table2", "action": "skip"},
                {"names": "^table.$"},
            ]
        }
    )
    dumper.perform_dump()

    tables = [obj for obj, rule in dumper.writer.dumped if isinstance(obj, Table)]
    assert len(tables) == 2
    assert dumper.db.get("public", "table1") in tables
    assert dumper.db.get("public", "table2") not in tables
    assert dumper.db.get("public", "table3") in tables


@pytest.mark.parametrize("bias", [-1, 0, 1])
def test_name_same_as_names_list(dumper, db, bias):
    """List of names have the same precedence over a single name"""
    dumper.reader.load_db(db.create_sample(2))
    dumper.add_config(
        {
            "db_objects": [
                {"name": "table2", "action": "skip", "adjust_score": bias},
                {"names": ["table1", "table2"]},
            ]
        }
    )
    if bias == 0:
        with pytest.raises(ConfigError):
            dumper.perform_dump()
        return

    dumper.perform_dump()

    tables = [obj for obj, rule in dumper.writer.dumped if isinstance(obj, Table)]
    assert dumper.db.get("public", "table1") in tables

    if bias < 0:
        assert len(tables) == 2
        assert dumper.db.get("public", "table2") in tables
    else:
        assert len(tables) == 1
        assert dumper.db.get("public", "table2") not in tables
