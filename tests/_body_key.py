"""One canonical key for a function body, shared by the copy detectors and the shared-body pins (PRE-1008 M-3, B3).

The key is the sha256[:16] of an explicit serialisation of the function's AST after removing what a copy can
change without changing behaviour: its name, decorators, docstring, every type annotation (parameters, return,
``x: T = ...`` statements) and its parameter names (renamed ``_a0``, ``_a1``, ... in signature order, uses
included). Neither ``ast.dump`` nor ``ast.unparse`` is stable across Python versions (28 of 63 pins failed on 3.12
with identical code), so :func:`_serialise` writes every field itself; the key is the same on 3.10 through 3.14.
"""

from __future__ import annotations

import ast
import copy
import hashlib
import inspect
import textwrap
from typing import Any


class _Canonical(ast.NodeTransformer):
    def __init__(self, names: dict[str, str]) -> None:
        self.names = names

    def visit_Name(self, node: ast.Name) -> ast.AST:
        if node.id in self.names:
            node.id = self.names[node.id]
        return node

    def visit_AnnAssign(self, node: ast.AnnAssign) -> Any:
        self.generic_visit(node)
        if node.value is None:  # a bare ``x: T`` declares nothing at runtime
            return None
        return ast.copy_location(ast.Assign(targets=[node.target], value=node.value), node)


def body_key(fn: Any) -> str:
    """The canonical key of ``fn`` (a function object or an ``ast.FunctionDef``)."""
    node = fn if isinstance(fn, ast.AST) else ast.parse(textwrap.dedent(inspect.getsource(fn))).body[0]
    assert isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)), type(node)
    node = copy.deepcopy(node)
    if node.body and isinstance(node.body[0], ast.Expr) and isinstance(node.body[0].value, ast.Constant):
        node.body = node.body[1:] or [ast.Pass()]
    a = node.args
    params = [
        *a.posonlyargs,
        *a.args,
        *([a.vararg] if a.vararg else []),
        *a.kwonlyargs,
        *([a.kwarg] if a.kwarg else []),
    ]
    names = {p.arg: f"_a{i}" for i, p in enumerate(params)}
    for p in params:
        p.arg, p.annotation = names[p.arg], None
    node.name, node.decorator_list, node.returns = "_", [], None
    if hasattr(node, "type_params"):  # 3.12+
        node.type_params = []
    node = _Canonical(names).visit(node)
    return hashlib.sha256(_serialise(node).encode()).hexdigest()[:16]


# Fields that exist on some supported versions only, or carry no behaviour.
_SKIP_FIELDS = frozenset({"type_params", "type_comment", "kind"})


def _serialise(node: Any) -> str:
    """Every field of every node, explicitly, so the text is the same on 3.10-3.14.

    ``ast.dump`` omits fields left at their default from 3.13 on, and ``ast.unparse`` parenthesises a
    comprehension's tuple target on 3.10 (``for (k, v) in``) but not on 3.13, so neither is stable.
    """
    if isinstance(node, ast.AST):
        fields = ",".join(f"{f}={_serialise(getattr(node, f, None))}" for f in node._fields if f not in _SKIP_FIELDS)
        return f"{type(node).__name__}({fields})"
    if isinstance(node, list):
        return "[" + ",".join(_serialise(n) for n in node) + "]"
    return repr(node)
