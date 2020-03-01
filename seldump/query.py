#!/usr/bin/env python3
"""
Objects to generate dynamic queries.

These objects create a small and specific ORM to help generating queries
dynamically.

You can create queries by nesting `QueryNode` subclasses nodes, and use
`SqlQueryVisitor.visit()` to return the SQL statement as
`psycopg2.sql.Composable` object.

This file is part of pg_seldump.
"""

from psycopg2 import sql

from .nodes import Node, NodeVisitor
from .dbobjects import Table


class QueryNode(Node):
    pass


class Select(QueryNode):
    def __init__(self, fro, columns, where):
        self.fro = fro
        self.columns = columns
        self.where = where


class Union(QueryNode):
    def __init__(self, queries):
        self.queries = queries


class FromEntry(Node):
    def __init__(self, source, alias=None):
        self.source = source
        self.alias = alias


class Predicate(Node):
    pass


class Exists(Predicate):
    def __init__(self, query):
        self.query = query


class FkeyJoin(Predicate):
    def __init__(self, fkey, fro, to):
        self.fkey = fkey
        self.fro = fro
        self.to = to


class SqlQueryVisitor(NodeVisitor):
    def __init__(self, db):
        self.db = db

    def visit_Select(self, select):
        cols = []
        for col in select.columns:
            if isinstance(col, sql.Composable):
                cols.append(col)
            elif isinstance(col, str):
                cols.append(sql.Identifier(col))
            else:
                raise TypeError("bad column: %s" % col)

        parts = []
        parts.append(sql.SQL("select"))
        parts.append(sql.SQL(", ").join(cols))
        parts.append(sql.SQL("\nfrom"))
        parts.append(self.visit(select.fro))
        if select.where:
            parts.append(sql.SQL("\nwhere"))
            parts.append(self.visit(select.where))

        return sql.SQL(" ").join(parts)

    def visit_FromEntry(self, fro):
        if isinstance(fro.source, Table):
            rv = sql.Identifier(fro.source.schema, fro.source.name)
        elif isinstance(fro.source, Node):
            rv = self.visit(fro.source)
        else:
            raise TypeError("can't deal with %s in a 'from'", fro.source)

        if fro.alias:
            rv = sql.SQL("{} as {}").format(rv, sql.Identifier(fro.alias))
        return rv

    def visit_Exists(self, exists):
        return sql.SQL("exists (\n{}\n)").format(self.visit(exists.query))

    def visit_FkeyJoin(self, join):
        if len(join.fkey.table_cols) == 1:
            lhs = sql.Identifier(join.fro, join.fkey.table_cols[0])
            rhs = sql.Identifier(join.to, join.fkey.ftable_cols[0])
        else:
            lhs = sql.SQL(", ").join(
                sql.Identifier(join.fro, col) for col in join.fkey.table_cols
            )
            rhs = sql.SQL(", ").join(
                sql.Identifier(join.to, col) for col in join.fkey.ftable_cols
            )

        return sql.SQL("(({}) = ({}))").format(lhs, rhs)
