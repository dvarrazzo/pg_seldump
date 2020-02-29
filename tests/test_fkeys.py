from seldump.dbobjects import Table

from .sample_dbs import create_sample_db


def test_fkey_nav(dumper):
    dumper.reader.load_db(
        create_sample_db(3, fkeys=[("table1", "t2id", "table2", "id")])
    )
    dumper.add_config({"db_objects": [{"name": "table1"}]})
    dumper.perform_dump()
    objs = [
        obj for obj, rule in dumper.writer.dumped if isinstance(obj, Table)
    ]
    assert len(objs) == 2


def test_fkey_nav_rec(dumper):
    dumper.reader.load_db(
        create_sample_db(
            3,
            fkeys=[
                ("table1", "t2id", "table2", "id"),
                ("table2", "t3id", "table3", "id"),
            ],
        )
    )
    dumper.add_config({"db_objects": [{"name": "table1"}]})
    dumper.perform_dump()
    objs = [
        obj for obj, rule in dumper.writer.dumped if isinstance(obj, Table)
    ]
    assert len(objs) == 3


def test_fkey_nav_stops_on_skip(dumper):
    dumper.reader.load_db(
        create_sample_db(
            4,
            fkeys=[
                ("table1", "t2id", "table2", "id"),
                ("table2", "t3id", "table3", "id"),
                ("table3", "t4id", "table4", "id"),
            ],
        )
    )
    dumper.add_config(
        {
            "db_objects": [
                {"name": "table1"},
                {"name": "table3", "action": "skip"},
            ]
        }
    )
    dumper.perform_dump()
    objs = [
        obj for obj, rule in dumper.writer.dumped if isinstance(obj, Table)
    ]
    assert len(objs) == 2
