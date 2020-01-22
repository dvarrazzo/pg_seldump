#!/usr/bin/env python3
"""
pg_seldump -- setup script
"""

# Copyright (C) 2020 Daniele Varrazzo

# This file is part of pg_seldump


import os
from setuptools import setup, find_packages

# Grab the version without importing the module
# or we will get import errors on install if prerequisites are still missing
fn = os.path.join(os.path.dirname(__file__), "seldump/consts.py")
with open(fn) as f:
    for line in f:
        if line.startswith("VERSION ="):
            version = line.split("'")[1]
            break
    else:
        raise ValueError("cannot find VERSION in the consts module")


classifiers = """
Development Status :: 4 - Beta
Environment :: Console
Intended Audience :: Developers
Intended Audience :: Information Technology
Intended Audience :: Science / Research
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
psycopg2
PyYAML
"""

setup(
    name="pg_seldump",
    description=("Selective dump of PostgreSQL data."),
    author="Daniele Varrazzo",
    author_email="daniele.varrazzo@gmail.com",
    url="https://github.com/dvarrazzo/pg_seldump",
    license="BSD",
    python_requires=">=3.6",
    install_requires=requirements,
    packages=find_packages(),
    entry_points={"console_scripts": ["pg_seldump = seldump.__main__:script",]},
    classifiers=[x for x in classifiers.split("\n") if x],
    zip_safe=False,
    version=version,
)
