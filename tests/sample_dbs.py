def create_sample_db(ntables):
    """
    Create a python description of a sample database
    """
    rv = {}
    for i in range(1, ntables + 1):
        rv["table%s" % i] = {
            "columns": [
                {
                    "name": "id",
                    "type": "integer",
                    "use_sequence": "table%s_id_seq" % i,
                },
                {"name": "data", "type": "text"},
            ],
        }
    return rv
