import re
import logging
import pkg_resources

import yaml
from jsonschema import Draft7Validator

from .yaml import load_yaml

logger = logging.getLogger("seldump.config")

validator = Draft7Validator(
    schema=yaml.load(
        pkg_resources.resource_string("seldump", "schema/config.yaml"),
        Loader=yaml.SafeLoader,
    )
)


def load_config(filename):
    """
    Load and validate a configuration file.

    Return the content as a Python object, validated according to the
    ``config.yaml`` schema, else None (and log about errors).
    """
    try:
        with open(filename) as f:
            conf = load_yaml(f)
    except Exception as e:
        logger.error("loading %s: %s", filename, e)
        return None

    errors = get_config_errors(conf, filename)
    if errors:
        for error in errors:
            logger.error("%s", error)
        return None

    return conf


def get_config_errors(conf, filename="<no name>"):
    """
    Validate a configuration object and return the list of errors found.
    """
    rv = []

    # Give a clearer error message than what jsonschema would give
    # Something like: None is not of type 'object'
    if not isinstance(conf, dict):
        msg = "config must be an object containing 'db_objects'"
        rv.append(located_message(None, filename, msg))
        return rv

    errors = list(validator.iter_errors(conf))
    for error in errors:
        loc = location_from_error(conf, error)
        rv.append(located_message(loc, filename, error.message))

    if isinstance(conf.get("db_objects"), list):
        for obj in conf["db_objects"]:
            if isinstance(obj, dict):
                rv.extend(_get_rule_errors(obj, filename))

    # sort by line number
    def lineno(s):
        m = re.search(r":(\d+)", s)
        return int(m.group(1)) if m is not None else 0

    rv.sort(key=lineno)

    return rv


def _get_rule_errors(obj, filename):
    """
    Return additional errors on a configuration objects.

    I don't see obvious ways to return these errors from jsonschema validation.
    """
    rv = []
    if "name" in obj and "names" in obj:
        loc = location_from_attribs(obj, "name", "names")
        msg = "can't specify both 'name' and 'names'"
        rv.append(located_message(loc, filename, msg))

    if isinstance(obj.get("names"), str):
        try:
            re.compile(obj["names"], re.VERBOSE)
        except re.error as e:
            msg = "names: not a valid regular expression: %s" % e
            loc = location_from_attribs(obj, "names")
            rv.append(located_message(loc, filename, msg))

    if "schema" in obj and "schemas" in obj:
        loc = location_from_attribs(obj, "schema", "schemas")
        msg = "can't specify both 'schema' and 'schemas'"
        rv.append(located_message(loc, filename, msg))

    if isinstance(obj.get("schemas"), str):
        try:
            re.compile(obj["schemas"], re.VERBOSE)
        except re.error as e:
            msg = "schemas: not a valid regular expression: %s" % e
            loc = location_from_attribs(obj, "schemas")
            rv.append(located_message(loc, filename, msg))

    if "kind" in obj and "kinds" in obj:
        loc = location_from_attribs(obj, "kind", "kinds")
        msg = "can't specify both 'kind' and 'kinds'"
        rv.append(located_message(loc, filename, msg))

    return rv


def located_message(loc, filename, message):
    """
    Add location informations to a message string.
    """
    if loc:
        return "at %s: %s" % (loc, message)
    else:
        return "in %s: %s" % (filename, message)


def location_from_attribs(conf, *items):
    """
    Return location information from an attrib on a dict parsed from yaml.
    """
    assert items
    filename = getattr(conf, "filename", None)
    itemlines = getattr(conf, "itemlines", None)
    if not (filename and itemlines):
        return
    try:
        linenos = [itemlines[item] for item in items]
    except (KeyError, IndexError):
        return

    return "%s:%s" % (filename, max(linenos))


def location_from_error(conf, error):
    """
    Return location information from a yaml validation error.
    """
    if error.validator == "additionalProperties":
        rv = _location_from_addprops(conf, error)
        if rv is not None:
            return rv

    # find the closest location for the error
    trail = [conf]
    for item in error.path:
        trail.append(trail[-1][item])

    # Does the last element in the error trail have a position?
    filename = getattr(trail[-1], "filename", None)
    lineno = getattr(trail[-1], "lineno", None)
    if filename and lineno:
        return "%s:%s" % (filename, lineno)

    if len(trail) < 2:
        return

    # Is the last element an item into a container that knows positions?
    filename = getattr(trail[-2], "filename", None)
    itemlines = getattr(trail[-2], "itemlines", None)
    if filename and itemlines:
        try:
            lineno = itemlines[error.path[-1]]
        except (KeyError, IndexError):
            # bah, I give up
            return
        else:
            rv = "%s:%s" % (filename, lineno)
            if isinstance(trail[-2], dict):
                # also add the attribute name
                rv = "%s: %s" % (rv, error.path[-1])
            return rv


def _location_from_addprops(conf, error):
    # Special-case this error otherwise it will report the object position
    # not the attribute one.
    filename = getattr(error.instance, "filename", None)
    itemlines = getattr(error.instance, "itemlines", None)
    if not (filename and itemlines):
        return

    # parse the attr name from the error message, I don't see it elsewhere
    m = re.search(r"'([^']*)' was unexpected", error.message)
    if m is None:
        return

    attr = m.group(1)
    if attr in itemlines:
        return "%s:%s" % (filename, itemlines[attr])
