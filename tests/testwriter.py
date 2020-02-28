from seldump.writer import Writer


class TestWriter(Writer):
    __test__ = False

    def __init__(self):
        self.reset()

    def reset(self):
        self.dump_started = False
        self.dump_ended = False
        self.closed = False
        self.dumped = []

    def begin_dump(self):
        assert not self.dump_started
        assert not self.dump_ended
        assert not self.closed
        self.dump_started = True

    def end_dump(self):
        assert self.dump_started
        assert not self.dump_ended
        assert not self.closed
        self.dump_ended = True

    def dump_table(self, table, config):
        assert self.dump_started
        assert not self.dump_ended
        assert not self.closed
        self.dumped.append((table, config))

    def dump_sequence(self, seq, config):
        assert self.dump_started
        assert not self.dump_ended
        assert not self.closed
        self.dumped.append((seq, config))

    def dump_materialized_view(self, matview, config):
        assert self.dump_started
        assert not self.dump_ended
        assert not self.closed
        self.dumped.append((matview, config))

    def close(self):
        assert self.dump_started == self.dump_ended
        self.closed = True
