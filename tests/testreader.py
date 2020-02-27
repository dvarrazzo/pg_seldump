from seldump.reader import Reader


class TestReader(Reader):
    __test__ = False

    def get_objects_to_dump(self):
        return []

    def get_tables_using_sequence(self, oid):
        return []
