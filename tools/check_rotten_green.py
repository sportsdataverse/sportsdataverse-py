"""Flag "rotten green" tests: tests that stay green whatever the code under test returns.

A test is rotten green when every assertion it makes unconditionally is a *weak* one (an
``assert_*_equal`` call counts as a value assert) --
``isinstance(...)``, ``x is not None``, ``len(x) > 0`` / ``x.height > 0`` / ``not x.is_empty()``
or a bare truthiness check -- so an empty or wrong frame of the right type passes. Asserts
under an ``if`` (``if df.height > 0: assert ...``) do not count: they never run on the empty
frame they were meant to catch. A ``pytest.skip(...)`` or imperative ``pytest.xfail(...)`` call in a
test body is flagged too: a test that bails out when the data comes back empty reports
"skipped" / "xfailed" for exactly the regression it exists to catch, and never runs its assertion
(assert the condition instead, or use a strict ``@pytest.mark.xfail(raises=...)``).

Usage::

    uv run --frozen --all-extras python tools/check_rotten_green.py tests/a.py tests/b.py

Prints one line per finding and a final ``ROTTEN GREEN: <n>``; exits 1 when ``n > 0``.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path
from typing import Iterator, List, Tuple

_SIZE_ATTRS = {"height", "width", "size", "shape"}


def _is_zero_or_one(node: ast.expr) -> bool:
    return isinstance(node, ast.Constant) and node.value in (0, 1) and not isinstance(node.value, bool)


def _is_size(node: ast.expr) -> bool:
    """``len(x)``, ``x.height``, ``x.shape[0]``, ``x.size`` ..."""
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "len":
        return True
    if isinstance(node, ast.Attribute) and node.attr in _SIZE_ATTRS:
        return True
    if isinstance(node, ast.Subscript):
        return _is_size(node.value)
    return False


def _weak(node: ast.expr) -> bool:
    """True when the assert expression cannot tell a right answer from a wrong non-empty one."""
    if isinstance(node, ast.BoolOp) and isinstance(node.op, ast.And):
        return all(_weak(v) for v in node.values)
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in ("isinstance", "bool", "len"):
        return True
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
        inner = node.operand
        return isinstance(inner, ast.Call) and isinstance(inner.func, ast.Attribute) and inner.func.attr == "is_empty"
    if isinstance(node, (ast.Name, ast.Attribute)):
        return True  # bare truthiness: `assert df`, `assert out.columns`
    if isinstance(node, ast.Compare) and len(node.ops) == 1:
        op, right, left = node.ops[0], node.comparators[0], node.left
        if isinstance(op, ast.IsNot) and isinstance(right, ast.Constant) and right.value is None:
            return True
        if _is_size(left) and isinstance(op, (ast.Gt, ast.GtE, ast.NotEq)) and _is_zero_or_one(right):
            return True
    return False


def _is_assert_call(stmt: ast.stmt) -> bool:
    """``assert_frame_equal(...)``, ``pd.testing.assert_series_equal(...)``: a value assert."""
    if not (isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Call)):
        return False
    func = stmt.value.func
    name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", "")
    return name.startswith("assert_")


def _asserts(body: List[ast.stmt], conditional: bool = False) -> Iterator[Tuple[ast.stmt, bool]]:
    for stmt in body:
        if isinstance(stmt, ast.Assert) or _is_assert_call(stmt):
            yield stmt, conditional
        elif isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)):
            continue
        elif isinstance(stmt, ast.If):
            yield from _asserts(stmt.body, True)
            yield from _asserts(stmt.orelse, True)
        elif isinstance(stmt, ast.Try):
            for block in (stmt.body, stmt.orelse, stmt.finalbody):
                yield from _asserts(block, conditional)
            for handler in stmt.handlers:
                yield from _asserts(handler.body, True)
        else:
            for field in ("body", "orelse"):
                inner = getattr(stmt, field, None)
                if isinstance(inner, list):
                    yield from _asserts(inner, conditional)


def _skips(fn: ast.AST) -> List[int]:
    lines = []
    for node in ast.walk(fn):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in ("skip", "xfail")
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "pytest"
        ):
            lines.append(node.lineno)
    return lines


def _tests(tree: ast.Module) -> Iterator[ast.FunctionDef]:
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name.startswith("test"):
            yield node
        elif isinstance(node, ast.ClassDef) and node.name.startswith("Test"):
            for sub in node.body:
                if isinstance(sub, ast.FunctionDef) and sub.name.startswith("test"):
                    yield sub


def check(path: Path) -> List[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    out = []
    for fn in _tests(tree):
        found = list(_asserts(fn.body))
        firm = [a for a, cond in found if not cond]
        if found and not any(not isinstance(a, ast.Assert) or not _weak(a.test) for a in firm):
            out.append(f"{path}:{fn.lineno}: {fn.name}: only weak or conditional asserts")
        for line in _skips(fn):
            out.append(f"{path}:{line}: {fn.name}: pytest.skip() / pytest.xfail() in the test body")
    return out


def main(argv: List[str]) -> int:
    findings = [f for arg in argv for f in check(Path(arg))]
    for f in findings:
        print(f)
    print(f"ROTTEN GREEN: {len(findings)}")
    return 1 if findings else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
