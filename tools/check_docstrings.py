"""Check public docstrings for the napoleon sections CLAUDE.md requires.

Every public function defined in the given modules (files or package directories) must
carry ``Args:`` (when it takes parameters), ``Returns:``, ``Raises:`` (when its body has a
``raise``), ``Example:`` and ``See Also:``; every public class ``Example:`` and ``See Also:``.
No docstring may hold a ``>>>`` doctest prompt.

    uv run python tools/check_docstrings.py sportsdataverse/find.py sportsdataverse/validation

Prints one line per gap, then ``DOCSTRINGS: <n> missing sections, <m> doctest prompts``;
exits 1 when either count is non-zero. Static (AST) only: nothing is imported.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

_SECTIONS = ("Args:", "Returns:", "Raises:", "Example:", "See Also:")


def _params(node: ast.FunctionDef | ast.AsyncFunctionDef) -> list[str]:
    a = node.args
    names = [p.arg for p in (*a.posonlyargs, *a.args, *a.kwonlyargs)]
    names += [p.arg for p in (a.vararg, a.kwarg) if p is not None]
    return [n for n in names if n not in ("self", "cls")]


def _raises(node: ast.AST) -> bool:
    """Whether the function's own body (not a nested def) has a ``raise``."""
    stack = list(ast.iter_child_nodes(node))
    while stack:
        n = stack.pop()
        if isinstance(n, ast.Raise):
            return True
        if not isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)):
            stack.extend(ast.iter_child_nodes(n))
    return False


def _required(node: ast.AST) -> list[str]:
    if isinstance(node, ast.ClassDef):
        return ["Example:", "See Also:"]
    need = ["Args:"] if _params(node) else []  # type: ignore[arg-type]
    need.append("Returns:")
    if _raises(node):
        need.append("Raises:")
    return need + ["Example:", "See Also:"]


def check_file(path: Path) -> tuple[list[str], int, int]:
    """Return ``(gap lines, missing-section count, doctest-prompt count)`` for one module."""
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    lines: list[str] = []
    missing = prompts = 0
    for node in tree.body:
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) or node.name.startswith("_"):
            continue
        doc = ast.get_docstring(node) or ""
        gaps = [s for s in _required(node) if s not in doc]
        if gaps:
            missing += len(gaps)
            lines.append(f"{path}:{node.lineno} {node.name}: missing {', '.join(gaps)}")
        if ">>>" in doc:
            prompts += 1
            lines.append(f"{path}:{node.lineno} {node.name}: doctest prompt")
    return lines, missing, prompts


def main(argv: list[str]) -> int:
    files: list[Path] = []
    for arg in argv:
        p = Path(arg)
        files += sorted(p.rglob("*.py")) if p.is_dir() else [p]
    missing = prompts = 0
    for f in files:
        lines, m, d = check_file(f)
        missing, prompts = missing + m, prompts + d
        for line in lines:
            print(line)
    print(f"DOCSTRINGS: {missing} missing sections, {prompts} doctest prompts")
    return 1 if missing or prompts else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
