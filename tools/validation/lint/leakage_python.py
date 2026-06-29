from __future__ import annotations

import ast
import os
from collections.abc import Iterator
from pathlib import Path

from tools.validation.findings import Finding, Severity
from tools.validation.lint import EXCLUDE_DIRS as _EXCLUDE_DIRS

_LAG_CALLS = frozenset(
    {
        "shift",
        "diff",
        "cum_sum",
        "cum_prod",
        "cum_max",
        "cum_min",
        "cum_count",
        "cumsum",
        "cumprod",
        "cummax",
        "cummin",
        "cumcount",
    }
)
_GROUP_CALLS = frozenset({"over", "group_by", "groupby"})


def _iter_py_files(root: Path) -> Iterator[Path]:
    for p in root.rglob("*.py"):
        if any(part in _EXCLUDE_DIRS for part in p.parts):
            continue
        yield p


def _is_grouped(node: ast.AST, parents: dict[int, ast.AST]) -> bool:
    """True if an enclosing ``.over()``/``.group_by()`` call wraps this lag call."""
    cur: ast.AST | None = node
    while cur is not None:
        parent = parents.get(id(cur))
        if isinstance(parent, ast.Attribute) and parent.attr in _GROUP_CALLS:
            return True
        cur = parent
    return False


def _receiver_is_grouped(node: ast.Call) -> bool:
    """True if the lag call's receiver chain originates from a grouping call.

    Catches pandas ``df.groupby("g")["x"].shift(1)`` and polars
    ``pl.col("y").over("g").shift(1)`` — where the grouping call is a
    *descendant* of the lag call (in ``node.func.value``), not an ancestor.
    """
    cur: ast.AST | None = node.func
    while cur is not None:
        if isinstance(cur, ast.Call) and isinstance(cur.func, ast.Attribute) and cur.func.attr in _GROUP_CALLS:
            return True
        if isinstance(cur, ast.Attribute):
            cur = cur.value
        elif isinstance(cur, ast.Subscript):
            cur = cur.value
        elif isinstance(cur, ast.Call):
            cur = cur.func
        else:
            cur = None
    return False


def _lint_source(src: str, rel: str) -> list[Finding]:
    findings: list[Finding] = []
    try:
        tree = ast.parse(src)
    except SyntaxError:
        return [Finding("leakage_lint", Severity.WARN, "", rel, f"could not parse {rel}", locator={"file": rel})]
    parents: dict[int, ast.AST] = {}
    for parent in ast.walk(tree):
        for child in ast.iter_child_nodes(parent):
            parents[id(child)] = parent
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in _LAG_CALLS
            and not _is_grouped(node, parents)
            and not _receiver_is_grouped(node)
        ):
            findings.append(
                Finding(
                    "leakage_lint",
                    Severity.WARN,
                    "",
                    rel,
                    f"{node.func.attr}() at {rel}:{node.lineno} is not grouped by "
                    ".over()/.group_by() (possible cross-game leak)",
                    locator={"file": rel, "line": node.lineno, "call": node.func.attr},
                    needs_judgment=True,
                )
            )
    return findings


def run(path: str) -> list[Finding]:
    """Lint Python source for ungrouped lag/cumulative ops (possible leakage).

    Walks ``.py`` files under ``path`` (a single file or a directory; directory
    walks skip vendored/build dirs) and flags ``.shift``/``.diff``/``.cum_*``/
    pandas ``cumsum`` calls that are not wrapped by an ``.over()``/``.group_by()``
    grouping, as WARN findings routed to judgment.

    Args:
        path: A file or directory to lint (``${ENV}`` vars are expanded).

    Returns:
        A list of Finding records; one ERROR finding if ``path`` does not exist.
    """
    root = Path(os.path.expandvars(path))
    if not root.exists():
        return [
            Finding(
                "leakage_lint",
                Severity.ERROR,
                "",
                path,
                f"lint path does not exist: {root}",
                locator={"path": str(root)},
            )
        ]
    files = [root] if root.is_file() else list(_iter_py_files(root))
    findings: list[Finding] = []
    for f in files:
        if f.suffix != ".py":
            continue
        try:
            src = f.read_text(encoding="utf-8")
        except OSError:
            continue
        findings.extend(_lint_source(src, str(f)))
    return findings
