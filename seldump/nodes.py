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

    def accept(self, visitor):
        for cls in self.__class__.__mro__:
            meth = getattr(visitor, "visit_" + cls.__name__, None)
            if meth is not None:
                return meth(self)

        else:
            # We expect at list visit_object to be implemented
            assert False, "no visitor method found in %r" % visitor


class NodeVisitor:
    def visit(self, node):
        return node.accept(self)

    def visit_object(self, node):
        raise NotImplementedError(
            "visitor %s cannot handle node %s"
            % (self.__class__.__name__, node.__class__.__name__)
        )
