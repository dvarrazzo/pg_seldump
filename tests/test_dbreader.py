from seldump.dbobjects import Sequence, Table


def test_read_something(dbdumper, db):
    objs = db.create_sample(2, fkeys=[("table1", "t2id", "table2", "id")])
    db.write_schema(objs)
    dbdumper.reader.load_schema()

    ts = {}
    for name in "table1", "table2":
        ts[name] = table = dbdumper.db.get("public", name, cls=Table)
        assert table
        assert table.oid > 1024
        assert table.name == name
        assert table.get_column("id")
        assert table.get_column("data")

        seq = dbdumper.db.get("public", name + "_id_seq", cls=Sequence)
        assert seq

        ((t, c),) = dbdumper.db.get_tables_using_sequence(seq.oid)
        assert t is table
        assert c is table.get_column("id")

    assert ts["table1"].get_column("t2id")

    (fk,) = ts["table1"].fkeys
    assert fk.table_oid == ts["table1"].oid
    assert fk.table_cols == ["t2id"]
    assert fk.ftable_oid == ts["table2"].oid
    assert fk.ftable_cols == ["id"]
