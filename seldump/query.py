#!/usr/bin/env python3
"""
Objects to generate dynamic queries.

These objects create a small and specific ORM to help generating queries
dynamically.

You can create queries by nesting `QueryNode` subclasses nodes, and use
`SqlQueryVisitor.visit()` to return the SQL statement as
`psycopg.sql.Composable` object.

This file is part of pg_seldump.
"""

from psycopg import sql

from .nodes import NodeVisitor
from .dbobjects import Table


class QueryNode:
    def as_string(self):
        return PrintQueryVisitor().as_string(self)


class Query(QueryNode):
    pass


class CopyOut(QueryNode):
    def __init__(self, source):
        self.source = source


class Select(Query):
    def __init__(self, columns, from_, where=None):
        self.columns = columns
        self.from_ = from_
        self.where = where


class RecursiveCTE(Query):
    def __init__(self, name, base_query, rec_cond):
        self.name = name
        self.base_query = base_query
        self.rec_cond = rec_cond


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
        self._first = True

    def indent(self):
        self._level += 4
        self._first = False

    def dedent(self):
        self._level -= 4
        assert self._level >= 0

    def indented(self, obj):
        if self._level or not self._first:
            return sql.Composed([sql.SQL("\n"), sql.SQL(" " * self._level), obj])
        else:
            return obj

    def visit_CopyOut(self, copy):
        parts = []
        parts.append(self.indented(sql.SQL("copy (")))
        self.indent()
        parts.append(self.visit(copy.source))
        self.dedent()
        parts.append(self.indented(sql.SQL(") to stdout")))
        return sql.SQL(" ").join(parts)

    def visit_Select(self, select):
        parts = []
        if isinstance(select.from_.source, RecursiveCTE):
            parts.append(self.indented(sql.SQL("with")))
            parts.append(self.visit(select.from_))

        parts.append(self.indented(sql.SQL("select")))
        parts.append(self._cols_list(select))
        parts.append(self.indented(sql.SQL("from")))

        if not isinstance(select.from_.source, RecursiveCTE):
            parts.append(self.visit(select.from_))
        else:
            parts.append(sql.Identifier(select.from_.source.name))

        if select.where:
            parts.append(self.indented(sql.SQL("where")))
            parts.append(self.visit(select.where))

        return sql.SQL(" ").join(parts)

    def visit_RecursiveCTE(self, cte):
        parts = []
        parts.append(sql.SQL("recursive"))
        parts.append(sql.Identifier(cte.name))
        parts.append(sql.SQL("as ("))
        self.indent()

        parts.append(self.visit(cte.base_query))

        parts.append(self.indented(sql.SQL("union")))

        parts.append(self.indented(sql.SQL("select")))
        parts.append(self._cols_list(cte.base_query))
        parts.append(self.indented(sql.SQL("from")))
        assert cte.base_query.from_.alias
        parts.append(self.visit(cte.base_query.from_))
        parts.append(self.indented(sql.SQL("join")))
        parts.append(sql.Identifier(cte.name))
        parts.append(sql.SQL("on"))
        parts.append(self.visit(cte.rec_cond))

        self.dedent()
        parts.append(self.indented(sql.SQL(")")))

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

        if from_.alias and not isinstance(from_.source, RecursiveCTE):
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

    def _cols_list(self, select):
        cols = []
        for col in select.columns:
            if isinstance(col, sql.Composable):
                cols.append(col)
            elif isinstance(col, str):
                cols.append(sql.Identifier(col))
            else:
                raise TypeError("bad column: %s" % col)

        return sql.SQL(", ").join(cols)


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
