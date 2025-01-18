from seldump.dbobjects import Table


def test_fkey_nav(dumper, db):
    """
    Test one navigation step

        table1:dump -> table2:ref

        table3:unknown
    """
    dumper.reader.load_db(
        db.create_sample(3, fkeys=[("table1", "t2id", "table2", "id")])
    )
    dumper.add_config({"db_objects": [{"name": "table1"}]})
    dumper.perform_dump()
    objs = [obj for obj, match in dumper.writer.dumped if isinstance(obj, Table)]
    assert len(objs) == 2


def test_fkeyed_dump(dumper, db):
    """
    Test one navigation step

        table1:dump -> table2:dump
    """
    dumper.reader.load_db(
        db.create_sample(2, fkeys=[("table1", "t2id", "table2", "id")])
    )
    dumper.add_config({"db_objects": [{"names": ["table1", "table2"]}]})
    dumper.perform_dump()

    match, = [
        match
        for obj, match in dumper.writer.dumped
        if isinstance(obj, Table) and obj.name == "table2"
    ]
    assert match.query is None


def test_fkey_nav_rec(dumper, db):
    """
    Test navigation is transitive

        table1:dump -> table2:ref -> table3:ref

        table4:unknown
    """
    dumper.reader.load_db(
        db.create_sample(
            4,
            fkeys=[
                ("table1", "t2id", "table2", "id"),
                ("table2", "t3id", "table3", "id"),
            ],
        )
    )
    dumper.add_config({"db_objects": [{"name": "table1"}]})
    dumper.perform_dump()
    objs = [obj for obj, match in dumper.writer.dumped if isinstance(obj, Table)]
    assert len(objs) == 3


def test_fkey_nav_stops_on_skip(dumper, db):
    """
    Test navigation stops at a skip table

        table1:dump -> table2:ref -> table3:skip -> table4:unknown

        table5:unknown
    """
    dumper.reader.load_db(
        db.create_sample(
            5,
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
    objs = [obj for obj, match in dumper.writer.dumped if isinstance(obj, Table)]
    assert len(objs) == 2


def test_two_referrers(dumper, db):
    """
    Test that table4 gets two referrers.

                    -> table2:ref ->
        table1:dump                  table4:ref
                    -> table3:ref ->

        table5:unknown
    """
    dumper.reader.load_db(
        db.create_sample(
            5,
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
    objs = [obj for obj, match in dumper.writer.dumped if isinstance(obj, Table)]
    assert len(objs) == 4

    (match,) = [match for obj, match in dumper.writer.dumped if obj.name == "table4"]
    assert len(match.referenced_by) == 2
    assert sorted(fkey.name for fkey in match.referenced_by) == [
        "t24id_table4_id_fkey",
        "t34id_table4_id_fkey",
    ]


def test_no_endless_loop(dumper, db):
    """
    Test we don't get stuck in a loop

        table1:dump -> table2:ref ->
                            ^        table3:ref
                       table4:ref <-
        table5:unknown
    """
    dumper.reader.load_db(
        db.create_sample(
            5,
            fkeys=[
                ("table1", "t12id", "table2", "id"),
                ("table2", "t3id", "table3", "id"),
                ("table3", "t4id", "table4", "id"),
                ("table4", "t42id", "table2", "id"),
            ],
        )
    )
    dumper.add_config({"db_objects": [{"name": "table1"}]})
    dumper.perform_dump()
    objs = [obj for obj, match in dumper.writer.dumped if isinstance(obj, Table)]
    assert len(objs) == 4

    (match,) = [match for obj, match in dumper.writer.dumped if obj.name == "table2"]
    assert len(match.referenced_by) == 2
    assert sorted(fkey.name for fkey in match.referenced_by) == [
        "t12id_table2_id_fkey",
        "t42id_table2_id_fkey",
    ]
