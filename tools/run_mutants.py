"""Run the mutation registry: apply each registered mutant, run its test, expect red.

Every row of ``tests/mutants/registry.yaml`` names a source ``file``, a ``snippet`` that occurs
exactly once in it (never a line number, so an edit above the site does not orphan the row),
the ``mutant`` text that replaces the snippet, the pytest ``test`` that must go red, and the
``finding`` it guards. ``expected: kill`` rows fail this script when the test stays green;
``expected: {survive_until: <task>}`` rows document a test gap the audit measured and are
reported, never fatal, until that task lands (an early kill is reported as ``killed early``).

The mutation is a plain text replacement applied in place and restored in a ``finally``; no
mutmut, no extra dependency. If the runner is killed hard (SIGKILL, power loss) the ``finally``
cannot run: ``git checkout -- <file>`` restores the site; CI checkouts are disposable.

Exit status is 1 iff a ``kill`` row survived, or ANY row could not be applied: the snippet
drifted or is duplicated, the test file is gone, the mutant does not compile, a ``kill`` row's
test is already red without the mutant, the test selects nothing (pytest rc 5), or pytest
crashed / hung. An error is never an
expected outcome, whatever ``expected`` says: otherwise a refactor would orphan a
``survive_until`` row with CI green.

Every pytest subprocess (the unmutated baseline and the mutated run alike) gets its own fresh
``PYTHONPYCACHEPREFIX`` directory plus ``PYTHONDONTWRITEBYTECODE=1``. Without that, a baseline run
leaves a ``.pyc`` keyed on the source's integer-second mtime and size, and a same-length mutant
written in the same second imports the ORIGINAL bytecode (a false SURVIVED); a ``.pyc`` left by an
earlier run or step is ignored the same way.

Usage::

    uv run --frozen python tools/run_mutants.py            # run every row
    uv run --frozen python tools/run_mutants.py --only ratings-level   # one group / finding / id
    uv run --frozen python tools/run_mutants.py --shard 2/11  # rows 1, 12, 23, ... (0-based) of N CI shards (the matrix width)
    uv run --frozen python tools/run_mutants.py --list     # print the rows, run nothing
    uv run --frozen python tools/run_mutants.py --dry-run  # validate snippets + tests, run nothing
"""

from __future__ import annotations

import argparse
import os
import shlex
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

ROOT = Path(__file__).resolve().parents[1]
REGISTRY = ROOT / "tests" / "mutants" / "registry.yaml"
#: The test command: this interpreter (CI runs the runner under ``uv run --frozen python``), so a
#: timeout kills pytest itself rather than a ``uv`` wrapper that could leave it running.
PYTEST_CMD = [sys.executable, "-m", "pytest"]
PYTEST_ARGS = ["-x", "-q", "-p", "no:cacheprovider"]
#: Seconds one pytest run may take before the row is an error (and the source is restored).
ROW_TIMEOUT = 900
#: pytest exit codes read as "the mutant made the test red": 1 = tests failed, 2 = interrupted
#: (collection errors under a mutant that breaks an import land here; a mutant that does not
#: compile is rejected before pytest runs, and a `kill` row's test must be green beforehand).
KILLED_RCS = (1, 2)
SURVIVED_RCS = (0,)  # 5 (nothing collected) is an error: a test that selects nothing proves nothing


class _StrictLoader(yaml.SafeLoader):
    """SafeLoader that rejects a mapping key given twice (safe_load would keep the last one)."""

    def construct_mapping(self, node, deep=False):  # type: ignore[override]
        keys = [self.construct_object(k, deep=deep) for k, _ in node.value]
        dupes = sorted({k for k in keys if keys.count(k) > 1})
        if dupes:
            raise yaml.constructor.ConstructorError(None, None, f"duplicate key(s) {dupes}", node.start_mark)
        return super().construct_mapping(node, deep=deep)


def load_registry(path: Path) -> List[Dict[str, Any]]:
    rows = yaml.load(path.read_text(encoding="utf-8"), Loader=_StrictLoader)  # noqa: S506 -- SafeLoader subclass
    if not isinstance(rows, list) or not rows:
        raise SystemExit(f"{path}: expected a non-empty list of rows")
    seen = set()
    for i, r in enumerate(rows):
        missing = [k for k in ("id", "file", "snippet", "mutant", "test", "finding", "expected") if not r.get(k)]
        if missing:
            raise SystemExit(f"{path}: row {i} ({r.get('id', '?')}) is missing {missing}")
        if "line" in r:
            raise SystemExit(f"{path}: row {r['id']} is keyed on line:; key rows on snippet: only")
        if r["id"] in seen:
            raise SystemExit(f"{path}: duplicate id {r['id']}")
        seen.add(r["id"])
        exp = r["expected"]
        if exp != "kill" and not (isinstance(exp, dict) and exp.get("survive_until")):
            raise SystemExit(f"{path}: row {r['id']}: expected must be `kill` or `{{survive_until: <task>}}`")
        if r["snippet"] == r["mutant"]:
            raise SystemExit(f"{path}: row {r['id']}: mutant equals snippet")
    return rows


def survive_until(row: Dict[str, Any]) -> Optional[str]:
    exp = row["expected"]
    return str(exp["survive_until"]) if isinstance(exp, dict) else None


def group_of(row: Dict[str, Any]) -> str:
    return str(row.get("group") or row["finding"])


def select(rows: List[Dict[str, Any]], only: Optional[str]) -> List[Dict[str, Any]]:
    if not only:
        return rows
    picked = [r for r in rows if only in (r["id"], r["finding"], group_of(r))]
    if not picked:
        raise SystemExit(f"--only {only}: no row has that id, finding or group")
    return picked


def shard(rows: List[Dict[str, Any]], spec: Optional[str]) -> List[Dict[str, Any]]:
    """Shard ``I/N`` (1-based) of the rows, round-robin by registry order: the N shards partition the rows."""
    if not spec:
        return rows
    try:
        i, n = (int(x) for x in spec.split("/"))
    except ValueError:
        raise SystemExit(f"--shard {spec}: expected I/N, e.g. 2/4") from None
    if not 1 <= i <= n:
        raise SystemExit(f"--shard {spec}: need 1 <= I <= N")
    picked = rows[i - 1 :: n]
    if not picked:  # a matrix wider than the registry must not report a green shard that ran nothing
        raise SystemExit(f"--shard {spec}: selects no rows ({len(rows)} rows across {n} shards)")
    return picked


def mutate(text: str, row: Dict[str, Any]) -> str:
    return text.replace(row["snippet"], row["mutant"], 1)


def check_site(row: Dict[str, Any], root: Path) -> Optional[str]:
    """Why the mutant cannot be applied, or None."""
    src = root / row["file"]
    if not src.is_file():
        return f"file not found: {row['file']}"
    text = src.read_text(encoding="utf-8")
    n = text.count(row["snippet"])
    if n != 1:
        return f"snippet occurs {n} times in {row['file']} (need exactly 1)"
    if src.suffix == ".py":
        try:
            compile(mutate(text, row), row["file"], "exec")
        except SyntaxError as e:
            return f"mutant does not compile: {e.msg} (line {e.lineno})"
    test_file = shlex.split(row["test"])[0].split("::")[0]
    if not (root / test_file).exists():
        return f"test not found: {test_file}"
    return None


def _pytest(row: Dict[str, Any], root: Path) -> Dict[str, Any]:
    cmd = [*PYTEST_CMD, *shlex.split(row["test"]), *PYTEST_ARGS]
    with tempfile.TemporaryDirectory(prefix="run-mutants-pycache-", ignore_cleanup_errors=True) as cache:
        env = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1", "PYTHONPYCACHEPREFIX": cache}
        try:
            # argv is a list built from the committed registry, run with shell=False (no shell parsing).
            # pytest's output is UTF-8; decoding it with the locale codec (cp1252 on Windows) raises in
            # the reader thread and leaves proc.stdout None. `replace` keeps a stray byte from killing the run.
            proc = subprocess.run(  # nosemgrep: python.lang.security.audit.dangerous-subprocess-use-audit
                cmd,
                cwd=root,
                env=env,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=ROW_TIMEOUT,
            )
        except subprocess.TimeoutExpired:
            return {"rc": None, "tail": f"timed out after {ROW_TIMEOUT}s"}
    tail = (proc.stdout + proc.stderr).strip().splitlines()[-1:]
    return {"rc": proc.returncode, "tail": tail[0] if tail else ""}


def run_row(row: Dict[str, Any], root: Path) -> Dict[str, Any]:
    """Apply the mutant, run the test, restore the file. Returns status/rc/seconds/detail."""
    why = check_site(row, root)
    if why:
        return {"status": "error", "detail": why, "seconds": 0.0}
    src = root / row["file"]
    original = src.read_bytes()
    t0 = time.monotonic()
    if row["expected"] == "kill":  # a red test proves nothing about the mutant
        base = _pytest(row, root)
        if base["rc"] != 0:
            return {
                "status": "error",
                "rc": base["rc"],
                "seconds": time.monotonic() - t0,
                "detail": f"test is not green without the mutant: {base['tail']}",
            }
    try:
        src.write_bytes(mutate(original.decode("utf-8"), row).encode("utf-8"))
        run = _pytest(row, root)
    finally:
        src.write_bytes(original)
    rc = run["rc"]
    if rc in KILLED_RCS:
        status, detail = "killed", ""
    elif rc in SURVIVED_RCS:
        status, detail = "survived", run["tail"]
    elif rc == 5:
        status, detail = "error", f"no tests collected (pytest rc 5): {run['tail']}"
    else:
        status, detail = "error", run["tail"]
    return {"status": status, "rc": rc, "seconds": time.monotonic() - t0, "detail": detail}


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--registry", type=Path, default=REGISTRY)
    ap.add_argument("--root", type=Path, default=ROOT, help="source tree the rows' paths are relative to")
    ap.add_argument("--only", help="run the rows with this id, finding or group only")
    ap.add_argument("--shard", help="run shard I/N (1-based, round-robin) of the selected rows")
    ap.add_argument("--list", action="store_true", help="print the rows and exit")
    ap.add_argument("--dry-run", action="store_true", help="validate every snippet and test path; run nothing")
    args = ap.parse_args(argv)

    rows = shard(select(load_registry(args.registry), args.only), args.shard)
    print(f"REGISTRY: {len(rows)} rows ({args.registry}){f' shard {args.shard}' if args.shard else ''}")
    if args.list or args.dry_run:
        bad = 0
        for r in rows:
            exp = "kill" if r["expected"] == "kill" else f"survive_until {survive_until(r)}"
            problem = check_site(r, args.root) if args.dry_run else None
            bad += problem is not None
            print(
                f"  {r['id']:<42} {r['finding']:<8} {exp:<22} {r['file']}  ->  {r['test']}"
                + (f"  !! {problem}" if problem else "")
            )
        if args.dry_run:
            print(f"DRY-RUN: {len(rows) - bad}/{len(rows)} rows applicable")
        return 1 if bad else 0

    results = []
    for r in rows:
        res = run_row(r, args.root)
        until = survive_until(r)
        if res["status"] == "error":
            verdict = "FAIL"  # never expected, whatever `expected` says
        elif until is None:
            verdict = "OK" if res["status"] == "killed" else "FAIL"
        else:
            verdict = "killed early" if res["status"] == "killed" else f"expected (until {until})"
        results.append((r, res, verdict))
        rc = f" rc={res['rc']}" if res.get("rc") is not None else ""
        detail = f"  {res['detail']}" if res.get("detail") else ""
        print(
            f"{res['status'].upper():<9} {r['id']:<42} {r['finding']:<8} {res['seconds']:6.1f}s{rc}  [{verdict}]{detail}"
        )

    groups: Dict[str, List[Any]] = {}
    for r, res, _ in results:
        groups.setdefault(group_of(r), []).append(res["status"] == "killed")
    for g, ks in groups.items():
        print(f"{g}: {sum(ks)}/{len(ks)} mutants killed")

    fatal = [r["id"] for r, res, v in results if v == "FAIL"]
    killed = sum(res["status"] == "killed" for _, res, _ in results)
    early = sum(v == "killed early" for _, _, v in results)
    errors = sum(res["status"] == "error" for _, res, _ in results)
    print(
        f"MUTANTS: {killed} killed ({early} early), {len(results) - killed - errors} survived, {errors} errors, "
        f"{len(fatal)} unexpected -> {'FAIL ' + ', '.join(fatal) if fatal else 'OK'}"
    )
    return 1 if fatal else 0


if __name__ == "__main__":
    sys.exit(main())
