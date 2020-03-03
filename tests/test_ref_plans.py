#!/usr/bin/python3

from seldump import query


def test_subqueries(dumper, db, fakeconn):
    """
    Test one navigation step

        table1:dump -> table2:ref -> table3:ref

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
    dumper.add_config(
        {"db_objects": [{"name": "table1", "filter": "data='aaa'"}]}
    )
    dumper.plan_dump()

    q = dumper.actions[dumper.db.get("public", "table3").oid].query
    assert isinstance(q, query.Select)
    assert q.from_.source.name == "table3"
    assert isinstance(q.where, query.Exists)

    sq = q.where.query
    assert sq.from_.source.name == "table2"
    assert isinstance(sq.where, query.And)
    assert sq.where.conds[0].fkey.name == "t3id_table3_id_fkey"
    assert isinstance(sq.where.conds[1], query.Exists)

    sq = sq.where.conds[1].query
    assert sq.from_.source.name == "table1"
    assert isinstance(sq.where, query.And)
    assert sq.where.conds[0].fkey.name == "t2id_table2_id_fkey"
    assert sq.where.conds[1].string == "data='aaa'"

    stmt = query.SqlQueryVisitor().visit(q)
    assert stmt.as_string(fakeconn)
