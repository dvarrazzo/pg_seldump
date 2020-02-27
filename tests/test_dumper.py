import pytest

from seldump.dumper import Dumper
from .testreader import TestReader
from .testwriter import TestWriter


@pytest.fixture
def dumper():

    reader = TestReader()
    writer = TestWriter()

    dumper = Dumper(reader=reader, writer=writer)
    return dumper


def test_void(dumper):
    """
    On empty input, result is empty
    """
    dumper.perform_dump()
    assert not dumper.writer.dumped
