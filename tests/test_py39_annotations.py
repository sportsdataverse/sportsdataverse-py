"""requires-python is >=3.9: no runtime-evaluated PEP 604 (``X | Y``) annotations.

Builtin generics (``list[int]``) are fine on 3.9; ``int | None`` in a signature
raises TypeError at import unless the module has ``from __future__ import annotations``.
Detected with ``ast`` so it works on any interpreter.
"""

import ast
import pathlib

import sportsdataverse

ROOT = pathlib.Path(sportsdataverse.__file__).parent


def _has_future(tree):
    return any(
        isinstance(n, ast.ImportFrom) and n.module == "__future__" and any(a.name == "annotations" for a in n.names)
        for n in tree.body
    )


def _union(node):
    return node is not None and any(isinstance(n, ast.BinOp) and isinstance(n.op, ast.BitOr) for n in ast.walk(node))


def _runtime_unions(tree):
    out = []
    for n in ast.walk(tree):
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)):
            a = n.args
            anns = [x.annotation for x in a.posonlyargs + a.args + a.kwonlyargs + [a.vararg, a.kwarg] if x]
            out += [x.lineno for x in anns + [n.returns] if _union(x)]
        elif isinstance(n, (ast.Module, ast.ClassDef)):  # not function locals: those are never evaluated
            out += [m.annotation.lineno for m in n.body if isinstance(m, ast.AnnAssign) and _union(m.annotation)]
    return out


def test_no_runtime_pep604_without_future_import():
    bad = {}
    for p in sorted(ROOT.rglob("*.py")):
        tree = ast.parse(p.read_text(encoding="utf-8"), feature_version=(3, 9))
        if not _has_future(tree) and (lines := _runtime_unions(tree)):
            bad[str(p.relative_to(ROOT))] = lines
    assert not bad, f"PEP 604 annotations evaluated at import on Python 3.9: {bad}"


def test_detector_flags_unprotected_module():
    tree = ast.parse("def f(x: int | None): ...")
    assert _runtime_unions(tree) and not _has_future(tree)
    assert _has_future(ast.parse("from __future__ import annotations\ndef f(x: int | None): ..."))
