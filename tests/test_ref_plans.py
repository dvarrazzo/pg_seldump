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
    # print(q.as_string())
    stmt = query.SqlQueryVisitor().visit(q)
    # print(stmt.as_string(fakeconn))
    assert stmt.as_string(fakeconn)
