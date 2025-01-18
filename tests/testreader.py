from collections import defaultdict

from seldump.reader import Reader
from seldump.dbobjects import DbObject, Column, Sequence, ForeignKey


class TestReader(Reader):
    __test__ = False

    def __init__(self):
        super().__init__()
        self.last_oid = 1

    def load_schema():
        pass

    def load_db(self, objs):
        used_seqs = defaultdict(list)

        for obj in objs:
            name = obj["name"]
            assert '"' not in name  # TODO: test with funny names
            assert "." not in name  # TODO: test with funny names

            schema = obj.get("schmea", "public")

            kind = obj.get("kind", "table")
            oid = obj.get("oid", None)
            if oid is None:
                oid = self.get_oid()

            dbobj = DbObject.from_kind(kind, oid, schema, name)
            if "columns" in obj:
                for colname, col in obj["columns"].items():
                    coltype = col["type"]
                    generated = col.get("generated", None)
                    dbcol = Column(name=colname, type=coltype, generated=generated)
                    dbobj.add_column(dbcol)

                    if "use_sequence" in col:
                        used_seqs["public", col["use_sequence"]].append((dbobj, dbcol))

            self.db.add_object(dbobj)

        # create sequence if needed, attach them to columns
        for (schema, name), dbobjs in used_seqs.items():
            seq = self.db.get(schema, name, cls=Sequence)
            if seq is None:
                seq = Sequence(self.get_oid(), schema, name)
                self.db.add_object(seq)

            for obj, col in dbobjs:
                self.db.add_sequence_user(seq, obj, col.name)

        # create fkeys
        for obj in objs:
            if not obj.get("fkeys"):
                continue
            # TODO: other schemas
            t1 = self.db.get(obj.get("schema", "public"), obj["name"])
            assert t1
            for fkey in obj["fkeys"]:
                # TODO: other schemas
                t2 = self.db.get("public", fkey["ftable"])
                fkey = ForeignKey(
                    name=fkey["name"],
                    table_oid=t1.oid,
                    table_cols=fkey["cols"],
                    ftable_oid=t2.oid,
                    ftable_cols=fkey["fcols"],
                )
                t1.add_fkey(fkey)
                t2.add_ref_fkey(fkey)

    def get_oid(self):
        oid = self.last_oid
        self.last_oid += 1
        return oid
