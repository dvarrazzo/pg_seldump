import pytest

from seldump.yaml import load_yaml
from seldump.config import get_config_errors


def test_empty_objs():
    """A config file with an empty list of objects is ok."""
    conf = load_yaml("db_objects: []")
    errors = get_config_errors(conf)
    assert not errors


@pytest.mark.parametrize("data", ["", "[]", "1", "42", '"hi"'])
def test_bad_start(data):
    """The config file must contain a dict."""
    conf = load_yaml(data)
    errors = get_config_errors(conf)
    assert len(errors) == 1
    assert "object" in errors[0]


@pytest.mark.parametrize("data", ["{}", "db_object: []", "{db_objects: [], foo: null}"])
def test_must_have_one_root(data):
    """The config entry must have precisely what expected."""
    conf = load_yaml(data)
    errors = get_config_errors(conf)
    assert errors


def test_bad_dbobjs():
    """The position of a bad entry in the main list is returned correctly."""
    conf = load_yaml(
        """\
db_objects:
  - null
  - "ouch"
  - true
  - 1
  - 1.1
  - []
"""
    )
    errors = get_config_errors(conf)
    assert len(errors) == 6
    for i, error in enumerate(errors):
        assert "<unicode string>:%s" % (i + 2) in error


def test_bad_items_pos():
    """The position of a bad entry in a dict/list is returned correctly."""
    conf = load_yaml(
        """\
db_objects:
  - name: 42
  - name: aaa
    kind: nope
  - name: bbb
    kinds:
     - table
     - boh
     - sequence
     - mah
"""
    )
    errors = get_config_errors(conf)
    assert len(errors) == 4
    assert "<unicode string>:2" in errors[0]
    assert "<unicode string>:4" in errors[1]
    assert "<unicode string>:8" in errors[2]
    assert "<unicode string>:10" in errors[3]


@pytest.mark.parametrize(
    "attr, value", (("name", "x"), ("schema", "y"), ("kind", "table"))
)
def test_no_singular_plural(attr, value):
    args = {"attr": attr, "value": value}
    conf = load_yaml(
        """\
db_objects:
  - %(attr)s: %(value)s
        """
        % args
    )
    errors = get_config_errors(conf)
    assert not errors

    conf = load_yaml(
        """\
db_objects:
  - %(attr)ss: [%(value)s]
        """
        % args
    )
    errors = get_config_errors(conf)
    assert not errors

    conf = load_yaml(
        """\
db_objects:
  - %(attr)s: %(value)s
    %(attr)ss: [%(value)s]
        """
        % args
    )
    errors = get_config_errors(conf)
    assert len(errors) == 1
    assert "string>:3" in errors[0]
    assert "can't specify both" in errors[0]


@pytest.mark.parametrize("attr", ("names", "schemas"))
def test_bad_regexpr(attr):
    conf = load_yaml(
        """\
db_objects:
  - %s: aaa
        """
        % attr
    )
    errors = get_config_errors(conf)
    assert not errors

    conf = load_yaml(
        """\
db_objects:
  - %s: aaa(
        """
        % attr
    )
    errors = get_config_errors(conf)
    assert len(errors) == 1
    assert "string>:2" in errors[0]
    assert "not a valid regular expression" in errors[0]


@pytest.mark.parametrize(
    "kind", ("table", "sequence", "partitioned table", "materialized view")
)
def test_valid_kind(kind):
    conf = load_yaml(
        """\
db_objects:
  - kind: %(kind)s
  - kinds: [%(kind)s]
        """
        % {"kind": kind}
    )
    errors = get_config_errors(conf)
    assert not errors


def test_bad_kind():
    conf = load_yaml(
        """\
db_objects:
  - kind: wat
  - kinds: [wat]
        """
    )
    errors = get_config_errors(conf)
    assert len(errors) == 2
    assert "string>:2" in errors[0]
    assert "'wat' is not one of" in errors[0]
    assert "string>:3" in errors[1]
    assert "'wat' is not one of" in errors[1]


def test_unexpected_attr():
    conf = load_yaml(
        """\
db_objects:
  - name: billy
    kid: table
        """
    )
    errors = get_config_errors(conf)
    assert len(errors) == 1
    assert "string>:3" in errors[0]
    assert "kid" in errors[0]
    assert "unexpected" in errors[0]
