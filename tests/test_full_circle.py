"""
Tests that create schema and data in a real db, dump the data, wipe the db,
restore the data, and verify that stuff is what expected.
"""


def test_a_simple_thing(db, dbdumper, psql):
    """
    With the tables::

        table1:dump -> table2:ref -> table3:ref
        [data <= 'c']

        table4:ignored

    Only required data in tables 1, 2, 3 is dumped.
    Sequences 1, 2, 3 are dumped, seq/table 4 skipped.
    """
    objs = db.create_sample(
        4,
        fkeys=[
            ("table1", "t2id", "table2", "id"),
            ("table2", "t3id", "table3", "id"),
        ],
    )
    db.write_schema(objs)
    db.fill_data("table3", "data", "ijkl")
    db.fill_data("table2", ("data", "t3id"), "efgh", (1, 1, 2, 3))
    db.fill_data("table1", ("data", "t2id"), "abcd", (1, 1, 2, 3))
    db.fill_data("table4", "data", "ijkl")

    dbdumper.reader.load_schema()
    dbdumper.add_config(
        """
db_objects:
- name: table1
  filter: data <= 'c'
"""
    )
    dbdumper.perform_dump()

    db.truncate(dbdumper.db)
    psql.load_file(dbdumper.writer.outfile)

    with dbdumper.reader.connection as cnn:
        with cnn.cursor() as cur:
            cur.execute("select id, data, t2id from table1 order by id")
            assert cur.fetchall() == [(1, "a", 1), (2, "b", 1), (3, "c", 2)]
            cur.execute("select id, data, t3id from table2 order by id")
            assert cur.fetchall() == [(1, "e", 1), (2, "f", 1)]
            cur.execute("select id, data from table3 order by id")
            assert cur.fetchall() == [(1, "i")]
            cur.execute("select id, data from table4 order by id")
            assert cur.fetchall() == []
            cur.execute("select nextval('table1_id_seq')")
            assert cur.fetchone()[0] == 5
            cur.execute("select nextval('table3_id_seq')")
            assert cur.fetchone()[0] == 5
            cur.execute("select nextval('table4_id_seq')")
            assert cur.fetchone()[0] == 1
