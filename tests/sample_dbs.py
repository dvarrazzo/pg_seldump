from collections import OrderedDict


def create_sample_db(ntables, fkeys=()):
    """
    Create a python description of a sample database
    """
    rv = {}
    for i in range(1, ntables + 1):
        rv["table%s" % i] = {
            "columns": OrderedDict(
                [
                    (
                        "id",
                        {
                            "type": "integer",
                            "use_sequence": "table%s_id_seq" % i,
                        },
                    ),
                    ("data", {"type": "text"}),
                ]
            ),
        }

    for fkey in fkeys:
        t1, f1, t2, f2 = fkey
        # TODO: multicol fkeys
        assert isinstance(f1, str)
        assert isinstance(f2, str)
        if f1 not in rv[t1]["columns"]:
            rv[t1]["columns"][f1] = {"type": "integer"}
        if f2 not in rv[t2]["columns"]:
            rv[t2]["columns"][f2] = {"type": "integer"}
        fkey = {
            "name": "%s_%s_%s_fkey" % (f1, t2, f2),
            "cols": [f1],
            "ftable": t2,
            "fcols": [f2],
        }
        rv[t1].setdefault("fkeys", []).append(fkey)

    return rv
