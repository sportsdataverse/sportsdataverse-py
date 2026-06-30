from __future__ import annotations

import csv
import io
import os
import re
import shutil
import subprocess
from collections.abc import Iterator
from pathlib import Path

from tools.validation.findings import Finding, Severity
from tools.validation.lint import EXCLUDE_DIRS as _BASE_EXCLUDE_DIRS

_HELPER = Path(__file__).parent / "getparsedata.R"
_RSCRIPT_TIMEOUT = 60  # seconds per file

# dplyr/base window ops that look back/accumulate across rows (leak if ungrouped)
_LAG_CALLS = frozenset({"lag", "lead", "cumsum", "cumprod", "cummax", "cummin", "cummean"})
# calls that establish a grouping scope (ungroup() deliberately excluded — it REMOVES grouping)
_GROUP_CALLS = frozenset({"group_by", "with_groups", "group_split"})
_BY_ARG = ".by"  # dplyr per-operation grouping, a SYMBOL_SUB named arg
_EXCLUDE_DIRS = _BASE_EXCLUDE_DIRS | frozenset({"renv", "packrat"})

_VERSION_RE = re.compile(r"R-(\d+)\.(\d+)\.(\d+)")


def rscript_path() -> str | None:
    """Locate an ``Rscript`` executable, or ``None`` if R is unavailable.

    Resolution order: the ``SDV_RSCRIPT`` env override, then ``Rscript`` on
    ``PATH``, then the highest-versioned ``R-*/bin/Rscript.exe`` under the
    Windows ``Program Files\\R`` tree (version-aware, not lexicographic).

    Returns:
        An absolute path to an ``Rscript`` executable, or ``None``.
    """
    override = os.environ.get("SDV_RSCRIPT")
    if override and Path(override).exists():
        return override
    found = shutil.which("Rscript") or shutil.which("Rscript.exe")
    if found:
        return found
    program_files = os.environ.get("ProgramFiles", "C:\\Program Files")
    candidates = list(Path(program_files, "R").glob("*/bin/Rscript.exe"))
    if not candidates:
        return None

    def _version_key(p: Path) -> tuple[int, int, int]:
        m = _VERSION_RE.search(p.parent.parent.name)
        return (int(m.group(1)), int(m.group(2)), int(m.group(3))) if m else (0, 0, 0)

    return str(max(candidates, key=_version_key))


def _iter_r_files(root: Path) -> Iterator[Path]:
    """Yield ``.R``/``.r`` files under ``root``, skipping vendored/build dirs.

    Args:
        root: Directory to walk.

    Returns:
        An iterator of source paths (case-insensitive ``.r`` suffix, de-duplicated
        so a case-insensitive filesystem does not yield each file twice).
    """
    seen: set[Path] = set()
    for p in root.rglob("*"):
        if p.suffix.lower() != ".r" or not p.is_file():
            continue
        if any(part in _EXCLUDE_DIRS for part in p.parts):
            continue
        resolved = p.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        yield p


def _root(nid: int, parent: dict[int, int], ids: set[int]) -> int:
    """Walk ``parent`` from ``nid`` to its top-level statement node (parent ``0``).

    Args:
        nid: Starting node id.
        parent: ``id -> parent`` map from the parse-data frame.
        ids: Set of all known (positive) node ids; an out-of-set parent (e.g. a
            negative COMMENT parent) terminates the walk.

    Returns:
        The id of the enclosing top-level statement node.
    """
    cur = nid
    seen: set[int] = set()
    while True:
        p = parent.get(cur, 0)
        if p == 0 or p not in ids or cur in seen:
            return cur
        seen.add(cur)
        cur = p


def _analyze_parsedata(rows: list[dict[str, str]], rel: str) -> list[Finding]:
    """Flag ungrouped lag/cumulative window calls in a getParseData frame.

    Groups every call by its top-level statement root, then a lag/cumulative call
    is clean iff a grouping signal (``group_by``/``with_groups``/``group_split``
    or a ``.by=`` arg) shares its root. This is pipe-aware: R's ``|>`` nests
    ``group_by`` inside the downstream ``mutate`` call, so they share a root.

    Args:
        rows: Parsed ``utils::getParseData`` rows (columns include ``id``,
            ``parent``, ``token``, ``text``, ``line1``).
        rel: Display path for the analysed source file.

    Returns:
        One WARN ``needs_judgment`` Finding per ungrouped lag/cumulative call.

    Note:
        Grouping is matched at TOP-LEVEL-STATEMENT granularity. Inside a
        function body or a ``{`` block (the dominant shape for real R package
        code) every pipe chain shares one statement root, so a grouping signal
        in ANY chain marks every lag/cumulative call in that whole statement
        clean — a grouped chain therefore MASKS a genuinely-ungrouped lag/
        cumulative elsewhere in the same function. This is an accepted, deliberate
        false negative (never a false positive); WARN-only / ``needs_judgment``
        routes survivors to Tier-2, and finer per-statement granularity is a
        planned follow-up.
    """
    try:
        ids = {int(r["id"]) for r in rows}
        parent = {int(r["id"]): int(r["parent"]) for r in rows}
    except (ValueError, KeyError) as exc:
        return [
            Finding(
                "leakage_lint",
                Severity.WARN,
                "",
                rel,
                f"could not parse getParseData CSV for {rel}: {exc}",
                locator={"file": rel},
            )
        ]
    group_roots: set[int] = set()
    for r in rows:
        token, text = r.get("token", ""), r.get("text", "")
        if (token == "SYMBOL_FUNCTION_CALL" and text in _GROUP_CALLS) or (token == "SYMBOL_SUB" and text == _BY_ARG):
            group_roots.add(_root(int(r["id"]), parent, ids))
    findings: list[Finding] = []
    for r in rows:
        if r.get("token") != "SYMBOL_FUNCTION_CALL" or r.get("text") not in _LAG_CALLS:
            continue
        if _root(int(r["id"]), parent, ids) in group_roots:
            continue
        try:
            line = int(r["line1"])
        except (KeyError, ValueError):
            continue
        call = r.get("text", "")
        findings.append(
            Finding(
                "leakage_lint",
                Severity.WARN,
                "",
                rel,
                f"{call}() at {rel}:{line} is not grouped by group_by()/.by= (possible cross-game leak)",
                locator={"file": rel, "line": line, "call": call},
                needs_judgment=True,
            )
        )
    return findings


def _parse_data_csv(rscript: str, file: Path) -> tuple[str | None, str]:
    """Invoke the R helper for one file; return ``(csv_text, error_message)``.

    Args:
        rscript: Path to the ``Rscript`` executable.
        file: Source file to parse.

    Returns:
        ``(stdout, "")`` on success, or ``(None, message)`` on timeout, OS error,
        or a non-zero exit (an unparseable ``.R`` file).
    """
    try:
        proc = subprocess.run(
            [rscript, str(_HELPER), str(file)],
            capture_output=True,
            text=True,
            timeout=_RSCRIPT_TIMEOUT,
        )
    except subprocess.TimeoutExpired:
        return None, f"R lint timed out after {_RSCRIPT_TIMEOUT}s for {file}"
    except OSError as exc:
        return None, f"R lint could not invoke Rscript for {file}: {exc}"
    if proc.returncode != 0:
        return None, f"could not parse {file} (Rscript exit {proc.returncode}): {proc.stderr[:500]}"
    return proc.stdout, ""


def run(path: str) -> list[Finding]:
    """Lint R source for ungrouped lag/cumulative window ops (possible leakage).

    Locates ``Rscript`` (env override / PATH / Program Files); if none is found
    returns a single INFO Finding rather than failing. Otherwise dumps each
    ``.R``/``.r`` file's ``utils::getParseData`` parse tree (skipping vendored
    dirs) and flags ``lag``/``lead``/``cum*`` calls whose top-level statement is
    not grouped by ``group_by()``/``group_split()``/``with_groups()``/``.by=`` as
    WARN findings routed to judgment.

    Args:
        path: A file or directory to lint (``${ENV}`` vars are expanded).

    Returns:
        A list of Finding records; one ERROR finding if ``path`` does not exist,
        or one INFO finding if ``Rscript`` is unavailable.
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
    rscript = rscript_path()
    if rscript is None:
        return [
            Finding(
                "leakage_lint",
                Severity.INFO,
                "",
                path,
                "R lint skipped: Rscript not found",
                locator={"path": str(root)},
            )
        ]
    files = [root] if root.is_file() else list(_iter_r_files(root))
    findings: list[Finding] = []
    for f in files:
        if f.suffix.lower() != ".r":
            continue
        csv_text, error = _parse_data_csv(rscript, f)
        if csv_text is None:
            findings.append(Finding("leakage_lint", Severity.WARN, "", str(f), error, locator={"file": str(f)}))
            continue
        rows = list(csv.DictReader(io.StringIO(csv_text)))
        findings.extend(_analyze_parsedata(rows, str(f)))
    return findings
