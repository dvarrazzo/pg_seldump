from seldump.dbobjects import Table

from .sample_dbs import create_sample_db


def test_fkey_nav(dumper):
    """
    Test table1:dump -> table2:ref
    """
    dumper.reader.load_db(
        create_sample_db(3, fkeys=[("table1", "t2id", "table2", "id")])
    )
    dumper.add_config({"db_objects": [{"name": "table1"}]})
    dumper.perform_dump()
    objs = [
        obj for obj, action in dumper.writer.dumped if isinstance(obj, Table)
    ]
    assert len(objs) == 2


def test_fkey_nav_rec(dumper):
    """
    Test table1:dump -> table2:ref -> table3:ref
    """
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
        obj for obj, action in dumper.writer.dumped if isinstance(obj, Table)
    ]
    assert len(objs) == 3


def test_fkey_nav_stops_on_skip(dumper):
    """
    Test table1:dump -> table2:ref -> table3:skip -> table4:unknown
    """
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
        obj for obj, action in dumper.writer.dumped if isinstance(obj, Table)
    ]
    assert len(objs) == 2


def test_two_referrers(dumper):
    """
    Test that table4 gets two referrers.

                    -> table2:ref ->
        table1:dump                  table4:ref
                    -> table3:ref ->
    """
    dumper.reader.load_db(
        create_sample_db(
            4,
            fkeys=[
                ("table1", "t2id", "table2", "id"),
                ("table1", "t3id", "table3", "id"),
                ("table2", "t24id", "table4", "id"),
                ("table3", "t34id", "table4", "id"),
            ],
        )
    )
    dumper.add_config({"db_objects": [{"name": "table1"}]})
    dumper.perform_dump()
    objs = [
        obj for obj, action in dumper.writer.dumped if isinstance(obj, Table)
    ]
    assert len(objs) == 4

    (action,) = [
        action for obj, action in dumper.writer.dumped if obj.name == "table4"
    ]
    assert len(action.referenced_by) == 2
    assert sorted(fkey.name for fkey in action.referenced_by) == [
        "t24id_table4_id_fkey",
        "t34id_table4_id_fkey",
    ]
