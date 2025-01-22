#!/usr/bin/env python3
"""
pg_seldump -- setup script
"""

# Copyright (C) 2020 Daniele Varrazzo

# This file is part of pg_seldump


import re
import os
from setuptools import setup

# Grab the version without importing the module
# or we will get import errors on install if prerequisites are still missing
fn = os.path.join(os.path.dirname(__file__), "seldump/consts.py")
with open(fn) as f:
    m = re.search(r"""(?mi)^VERSION\s*=\s*["']+([^'"]+)["']+""", f.read())
if m:
    version = m.group(1)
else:
    raise ValueError("cannot find VERSION in the consts module")

# Read the description from the README
with open("README.rst") as f:
    readme = f.read()

classifiers = """
Development Status :: 4 - Beta
Environment :: Console
Intended Audience :: Developers
Intended Audience :: Information Technology
Intended Audience :: Science/Research
Intended Audience :: System Administrators
License :: OSI Approved :: BSD License
Operating System :: POSIX
Programming Language :: Python :: 3
Topic :: Database
Topic :: Software Development
Topic :: Software Development :: Testing
Topic :: System :: Archiving :: Backup
Topic :: System :: Systems Administration
Topic :: Utilities
"""

requirements = """
psycopg >= 3.1
jsonschema >= 4.17
PyYAML >= 6
"""

setup(
    name="pg_seldump",
    description=readme.splitlines()[0],
    long_description="\n".join(readme.splitlines()[2:]).lstrip(),
    author="Daniele Varrazzo",
    author_email="daniele.varrazzo@gmail.com",
    url="https://github.com/dvarrazzo/pg_seldump",
    license="BSD",
    python_requires=">= 3.9",
    install_requires=requirements,
    extras_require={
        "test": [
            "pytest >= 7.2",
        ],
        "dev": [
            "black",
            "flake8",
        ],
    },
    packages=["seldump"],
    package_data={"seldump": ["schema/*.yaml"]},
    entry_points={"console_scripts": ["pg_seldump = seldump.cli:script"]},
    classifiers=[x for x in classifiers.split("\n") if x],
    zip_safe=False,
    version=version,
)
