from psycopg import sql

from seldump import query
from seldump.database import Database
from seldump.dbobjects import Table, Column, ForeignKey


def test_query_visit(fakeconn):
    """
    Transform the representation into the structure for the following query:

    select * from tab2 t0
    where exists (
        select 1 from (
            select t2id from tab1
            where {cond1}
        ) t1
        where t1.t2id = t0.id;
    )
    """
    db = Database()
    table1 = db.add_object(Table(oid=1, schema="public", name="table1"))
    table2 = db.add_object(Table(oid=2, schema="public", name="table2"))
    table1.add_column(Column("id", "integer"))
    table1.add_column(Column("t2id", "integer"))
    table2.add_column(Column("id", "integer"))
    table1.add_fkey(ForeignKey("t2id_fkey", 1, ["t2id"], 2, ["id"]))

    q = query.Select(
        from_=query.FromEntry(table2, alias="t0"),
        columns=[sql.SQL("*")],
        where=query.Exists(
            query.Select(
                from_=query.FromEntry(table1, alias="t1"),
                columns=[sql.SQL("1")],
                where=query.FkeyJoin(table1.get_fkey("t2id_fkey"), "t1", "t0"),
            )
        ),
    )

    v = query.SqlQueryVisitor()
    stmt = v.visit(q)
    assert stmt.as_string(fakeconn)
