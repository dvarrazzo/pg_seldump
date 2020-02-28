from collections import defaultdict

from seldump.reader import Reader
from seldump.dbobjects import DbObject, Column, Sequence


class TestReader(Reader):
    __test__ = False

    def __init__(self):
        super().__init__()
        self.last_oid = 1

    def load_schema():
        pass

    def load_db(self, objs):
        used_seqs = defaultdict(list)

        for name, obj in objs.items():
            assert '"' not in name  # TODO: test with funny names

            parts = name.split(".")
            assert len(parts) <= 2
            if len(parts) == 1:
                parts.insert(0, "public")
            schema, name = parts

            kind = obj.get("kind", "table")
            oid = obj.get("oid", None)
            if oid is None:
                oid = self.get_oid()

            dbobj = DbObject.from_kind(kind, oid, schema, name)
            if "columns" in obj:
                for col in obj["columns"]:
                    colname = col["name"]
                    coltype = col["type"]
                    dbcol = Column(name=colname, type=coltype)
                    dbobj.add_column(dbcol)

                    if "use_sequence" in col:
                        used_seqs["public", col["use_sequence"]].append(
                            (dbobj, dbcol)
                        )

            self.db.add_object(dbobj)

        # create sequence if needed, attach them to columns
        for (schema, name), objs in used_seqs.items():
            seq = self.db.get(schema, name, cls=Sequence)
            if seq is None:
                seq = Sequence(self.get_oid(), schema, name)
                self.db.add_object(seq)

            for obj, col in objs:
                self.db.add_sequence_user(seq, obj, col.name)

    def get_oid(self):
        oid = self.last_oid
        self.last_oid += 1
        return oid
