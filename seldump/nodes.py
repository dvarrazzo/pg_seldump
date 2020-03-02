#!/usr/bin/env python3
"""
Base classes to implement the visitor pattern.

https://en.wikipedia.org/wiki/Visitor_pattern

This file is part of pg_seldump.
"""


class Node:
    """
    A node which can accept a NodeVisitor.

    This impleentation is Liskov-friendly.
    """


class NodeVisitor:
    def visit(self, node):
        for cls in node.__class__.__mro__:
            meth = getattr(self, "visit_" + cls.__name__, None)
            if meth is not None:
                return meth(node)

        else:
            # We expect at list visit_object to be implemented
            assert False, "no visitor method found in %r" % self

    def visit_object(self, node):
        raise NotImplementedError(
            "visitor %s cannot handle node %s"
            % (self.__class__.__name__, node.__class__.__name__)
        )
