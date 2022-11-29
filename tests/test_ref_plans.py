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
    dumper.add_config({"db_objects": [{"name": "table1", "filter": "data='aaa'"}]})
    dumper.plan_dump()

    q = dumper.matches[dumper.db.get("public", "table3").oid].query
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
    assert sq.where.conds[1]._obj == "data='aaa'"

    stmt = query.SqlQueryVisitor().visit(q)
    assert stmt.as_string(fakeconn)


def test_two_dependencies(db, dumper, fakeconn):
    """
    A fkey table can be referred by more than one table.

        table1:dump ->
                        table3:ref -> table4:ref
        table2:dump ->
    """

    objs = db.create_sample(
        4,
        fkeys=[
            ("table1", "t13id", "table3", "id"),
            ("table2", "t23id", "table3", "id"),
            ("table3", "t4id", "table4", "id"),
        ],
    )
    dumper.reader.load_db(objs)

    dumper.add_config(
        """
db_objects:
- name: table1
  filter: data <= 'b'

- name: table2
  filter: data <= 'f'
"""
    )
    dumper.plan_dump()

    q = dumper.matches[dumper.db.get("public", "table4").oid].query
    assert isinstance(q, query.Select)
    assert q.from_.source.name == "table4"
    assert isinstance(q.where, query.Exists)

    sq = q.where.query
    assert sq.from_.source.name == "table3"
    assert isinstance(sq.where, query.And)
    assert sq.where.conds[0].fkey.name == "t4id_table4_id_fkey"
    assert isinstance(sq.where.conds[1], query.Or)
    assert list(map(type, (sq.where.conds[1].conds))) == [query.Exists] * 2

    sqs = [cond.query for cond in sq.where.conds[1].conds]
    sqs.sort(key=lambda sq: sq.from_.source.name)
    assert [sq.from_.source.name for sq in sqs] == ["table1", "table2"]
    assert [sq.where.conds[0].fkey.name for sq in sqs] == [
        "t13id_table3_id_fkey",
        "t23id_table3_id_fkey",
    ]
    assert [sq.where.conds[1]._obj for sq in sqs] == [
        "data <= 'b'",
        "data <= 'f'",
    ]
