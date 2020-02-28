import pytest

from seldump.dumper import Dumper

from .testreader import TestReader
from .testwriter import TestWriter


@pytest.fixture
def dumper():
    """Return a `seldump.Dumper` configured for testing."""
    reader = TestReader()
    writer = TestWriter()
    dumper = Dumper(reader=reader, writer=writer)
    return dumper
