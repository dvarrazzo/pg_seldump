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


def test_columns_edit(db, dbdumper, psql):
    """
    Some columns can be omitted, some can be changed.
    """

    objs = db.create_sample(
        3,
        fkeys=[
            ("table1", "t2id", "table2", "id"),
            ("table2", "t3id", "table3", "id"),
        ],
    )

    for i in range(2):
        objs[i]["columns"]["password"] = {"name": "password", "type": "text"}

    db.write_schema(objs)
    db.fill_data("table2", ("data", "password"), "efgh", ["pass"] * 4)
    db.fill_data(
        "table1",
        ("data", "t2id", "password"),
        "abcd",
        (1, 1, 2, 3),
        ["pass"] * 4,
    )

    dbdumper.reader.load_schema()
    dbdumper.add_config(
        """
db_objects:
- name: table1
  filter: data <= 'c'
  no_columns:
  - password
  replace:
    data: "'x'"

- name: table2
  action: ref
  no_columns:
  - password
  replace:
    data: "'y'"
"""
    )
    dbdumper.perform_dump()

    db.truncate(dbdumper.db)
    psql.load_file(dbdumper.writer.outfile)

    with dbdumper.reader.connection as cnn:
        with cnn.cursor() as cur:
            cur.execute(
                "select id, data, password, t2id from table1 order by id"
            )
            assert cur.fetchall() == [
                (1, "x", None, 1),
                (2, "x", None, 1),
                (3, "x", None, 2),
            ]
            cur.execute("select id, data, password from table2 order by id")
            assert cur.fetchall() == [(1, "y", None), (2, "y", None)]


def test_two_dependencies(db, dbdumper, psql):
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

    db.write_schema(objs)
    db.fill_data("table4", ("data"), "opqrst")
    db.fill_data("table3", ("data", "t4id"), "ijklmn", reversed(range(1, 7)))
    db.fill_data("table1", ("data", "t13id"), "abcd", (1, 2, 3, 4))
    db.fill_data("table2", ("data", "t23id"), "efgh", (2, 3, 4, 5))

    dbdumper.reader.load_schema()
    dbdumper.add_config(
        """
db_objects:
- name: table1
  filter: data <= 'b'

- name: table2
  filter: data <= 'f'
"""
    )
    dbdumper.perform_dump()

    db.truncate(dbdumper.db)
    psql.load_file(dbdumper.writer.outfile)

    with dbdumper.reader.connection as cnn:
        with cnn.cursor() as cur:
            cur.execute("select id, data, t13id from table1 order by id")
            assert cur.fetchall() == [(1, "a", 1), (2, "b", 2)]
            cur.execute("select id, data, t23id from table2 order by id")
            assert cur.fetchall() == [(1, "e", 2), (2, "f", 3)]
            cur.execute("select id, data, t4id from table3 order by id")
            assert cur.fetchall() == [(1, "i", 6), (2, "j", 5), (3, "k", 4)]
            cur.execute("select id, data from table4 order by id")
            assert cur.fetchall() == [(4, "r"), (5, "s"), (6, "t")]


def test_dump_and_ref(db, dbdumper, psql):
    """
    Tables can have some records referred, some dumped.

    table1:sel -> table2:sel ->table3
    [where1]      [where2]
    """
    objs = db.create_sample(
        3,
        fkeys=[
            ("table1", "t2id", "table2", "id"),
            ("table2", "t3id", "table3", "id"),
        ],
    )
    db.write_schema(objs)
    db.fill_data("table3", "data", "ijkl")
    db.fill_data("table2", ("data", "t3id"), "efgh", (1, 2, 3, 4))
    db.fill_data("table1", ("data", "t2id"), "abcd", (1, 2, 3, 4))

    dbdumper.reader.load_schema()
    dbdumper.add_config(
        """
db_objects:
- name: table1
  filter: data = 'a'
- name: table2
  filter: data = 'g'
"""
    )
    dbdumper.perform_dump()

    db.truncate(dbdumper.db)
    psql.load_file(dbdumper.writer.outfile)

    with dbdumper.reader.connection as cnn:
        with cnn.cursor() as cur:
            cur.execute("select id, data, t2id from table1 order by id")
            assert cur.fetchall() == [(1, "a", 1)]
            cur.execute("select id, data, t3id from table2 order by id")
            assert cur.fetchall() == [(1, "e", 1), (3, "g", 3)]
            cur.execute("select id, data from table3 order by id")
            assert cur.fetchall() == [(1, "i"), (3, "k")]
