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

from .nodes import NodeVisitor
from .dbobjects import Table


class QueryNode:
    def as_string(self):
        return PrintQueryVisitor().as_string(self)


class Query(QueryNode):
    pass


class Select(Query):
    def __init__(self, columns, from_, where):
        self.columns = columns
        self.from_ = from_
        self.where = where


class Union(Query):
    def __init__(self, queries):
        self.queries = queries


class FromEntry(QueryNode):
    def __init__(self, source, alias=None):
        self.source = source
        self.alias = alias


class Predicate(QueryNode):
    pass


class Exists(Predicate):
    def __init__(self, query):
        self.query = query


class Or(Predicate):
    def __init__(self, conds):
        self.conds = conds


class And(Predicate):
    def __init__(self, conds):
        self.conds = conds


class FkeyJoin(Predicate):
    def __init__(self, fkey, from_, to):
        self.fkey = fkey
        self.from_ = from_
        self.to = to


class SqlQueryVisitor(NodeVisitor):
    def __init__(self):
        self._level = 0

    def indent(self):
        self._level += 4

    def dedent(self):
        self._level -= 4
        assert self._level >= 0

    def indented(self, obj):
        if self._level:
            return sql.Composed(
                [sql.SQL("\n"), sql.SQL(" " * self._level), obj]
            )
        else:
            return obj

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
        parts.append(self.indented(sql.SQL("select")))
        parts.append(sql.SQL(", ").join(cols))
        parts.append(self.indented(sql.SQL("from")))
        parts.append(self.visit(select.from_))
        if select.where:
            parts.append(self.indented(sql.SQL("where")))
            parts.append(self.visit(select.where))

        return sql.SQL(" ").join(parts)

    def visit_FromEntry(self, from_):
        if isinstance(from_.source, sql.Identifier):
            rv = sql.Composed([sql.SQL("only "), from_.source])
        elif isinstance(from_.source, Table):
            rv = sql.Composed([sql.SQL("only "), from_.source.ident])
        elif isinstance(from_.source, QueryNode):
            rv = self.visit(from_.source)
        else:
            raise TypeError("can't deal with %s in a 'from'", from_.source)

        if from_.alias:
            rv = sql.SQL("{} as {}").format(rv, sql.Identifier(from_.alias))
        return rv

    def visit_Exists(self, exists):
        rv = []
        rv.append(sql.SQL("exists ("))
        self.indent()
        rv.append(self.visit(exists.query))
        self.dedent()
        rv.append(sql.SQL(")"))
        return sql.Composed(rv)

    def visit_And(self, node, kw="and"):
        rv = [sql.SQL("(")]
        self.indent()
        for i, cond in enumerate(node.conds):
            if i:
                rv.append(self.indented(sql.SQL(kw + " ")))
                rv.append(self.visit(cond))
            else:
                rv.append(self.indented(self.visit(cond)))
        rv.append(sql.SQL(")"))
        self.dedent()
        return sql.Composed(rv)

    def visit_Or(self, node):
        return self.visit_And(node, kw="or")

    def visit_FkeyJoin(self, join):
        if len(join.fkey.table_cols) == 1:
            lhs = sql.Identifier(join.from_, join.fkey.table_cols[0])
            rhs = sql.Identifier(join.to, join.fkey.ftable_cols[0])
        else:
            lhs = sql.SQL(", ").join(
                sql.Identifier(join.from_, col) for col in join.fkey.table_cols
            )
            rhs = sql.SQL(", ").join(
                sql.Identifier(join.to, col) for col in join.fkey.ftable_cols
            )

        return sql.SQL("(({}) = ({}))").format(lhs, rhs)

    def visit_Composable(self, obj):
        return obj


class PrintQueryVisitor(NodeVisitor):
    def __init__(self):
        self.reset()

    def reset(self):
        self._stack = []
        self.output = []
        self._level = 0

    def as_string(self, node):
        self.reset()
        self.visit(node)
        return "\n".join(self.output)

    def visit_QueryNode(self, node):
        line = [node.__class__.__name__]
        if not self.empty():
            line.insert(0, "%s:" % self.top())
        self.emit(*line)
        self.indent()
        for k, v in node.__dict__.items():
            if v is None:
                continue
            self.push(k)
            self.visit(v)
            self.pop()

        self.dedent()

    def visit_list(self, L):
        if not self.empty():
            self.emit("%s:" % self.top())
        self.indent()
        for i, item in enumerate(L):
            self.push(i)
            self.visit(item)
            self.pop()
        self.dedent()

    def visit_object(self, obj):
        line = [obj]
        if not self.empty():
            line.insert(0, "%s:" % self.top())
        self.emit(*line)

    def emit(self, *bits):
        self.output.append(" " * self._level + " ".join(map(str, bits)))

    def empty(self):
        return not self._stack

    def push(self, item):
        self._stack.append(item)

    def pop(self):
        return self._stack.pop()

    def top(self):
        return self._stack[-1] if self._stack else None

    def indent(self):
        self._level += 2

    def dedent(self):
        self._level -= 2
        assert self._level >= 0, self._level
