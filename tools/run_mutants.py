"""Run the mutation registry: apply each registered mutant, run its test, expect red.

Every row of ``tests/mutants/registry.yaml`` names a source ``file``, a ``snippet`` that occurs
exactly once in it (never a line number, so an edit above the site does not orphan the row),
the ``mutant`` text that replaces the snippet, the pytest ``test`` that must go red, and the
``finding`` it guards. ``expected: kill`` rows fail this script when the test stays green;
``expected: {survive_until: <task>}`` rows document a test gap the audit measured and are
reported, never fatal, until that task lands (an early kill is reported as ``killed early``).

The mutation is a plain text replacement applied in place and restored in a ``finally``;
no mutmut, no extra dependency. Exit status is 1 iff a ``kill`` row survived, errored or
could not be applied.

Usage::

    uv run --frozen python tools/run_mutants.py            # run every row
    uv run --frozen python tools/run_mutants.py --only ratings-level   # one group / finding / id
    uv run --frozen python tools/run_mutants.py --list     # print the rows, run nothing
    uv run --frozen python tools/run_mutants.py --dry-run  # validate snippets + tests, run nothing
"""

from __future__ import annotations

import argparse
import shlex
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

ROOT = Path(__file__).resolve().parents[1]
REGISTRY = ROOT / "tests" / "mutants" / "registry.yaml"
#: The test command; a test overrides it to run pytest in a scratch tree without uv.
PYTEST_CMD = ["uv", "run", "--frozen", "pytest"]
PYTEST_ARGS = ["-x", "-q", "-p", "no:cacheprovider"]
#: pytest exit codes read as "the mutant made the test red": 1 = tests failed, 2 = interrupted
#: (collection errors under a mutant that breaks an import land here).
KILLED_RCS = (1, 2)
SURVIVED_RCS = (0, 5)  # 5 = nothing collected: the test cannot have gone red


def load_registry(path: Path) -> List[Dict[str, Any]]:
    rows = yaml.safe_load(path.read_text(encoding="utf-8"))
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


def check_site(row: Dict[str, Any], root: Path) -> Optional[str]:
    """Why the mutant cannot be applied, or None."""
    src = root / row["file"]
    if not src.is_file():
        return f"file not found: {row['file']}"
    n = src.read_text(encoding="utf-8").count(row["snippet"])
    if n != 1:
        return f"snippet occurs {n} times in {row['file']} (need exactly 1)"
    test_file = shlex.split(row["test"])[0].split("::")[0]
    if not (root / test_file).exists():
        return f"test not found: {test_file}"
    return None


def run_row(row: Dict[str, Any], root: Path) -> Dict[str, Any]:
    """Apply the mutant, run the test, restore the file. Returns status/rc/seconds."""
    why = check_site(row, root)
    if why:
        return {"status": "error", "detail": why, "seconds": 0.0}
    src = root / row["file"]
    original = src.read_bytes()
    mutated = original.decode("utf-8").replace(row["snippet"], row["mutant"], 1).encode("utf-8")
    cmd = [*PYTEST_CMD, *shlex.split(row["test"]), *PYTEST_ARGS]
    t0 = time.monotonic()
    try:
        src.write_bytes(mutated)
        proc = subprocess.run(cmd, cwd=root, capture_output=True, text=True)
    finally:
        src.write_bytes(original)
    secs = time.monotonic() - t0
    rc = proc.returncode
    if rc in KILLED_RCS:
        status = "killed"
    elif rc in SURVIVED_RCS:
        status = "survived"
    else:
        status = "error"
    tail = (proc.stdout + proc.stderr).strip().splitlines()[-1:] if status != "killed" else []
    return {"status": status, "rc": rc, "seconds": secs, "detail": tail[0] if tail else ""}


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--registry", type=Path, default=REGISTRY)
    ap.add_argument("--root", type=Path, default=ROOT, help="source tree the rows' paths are relative to")
    ap.add_argument("--only", help="run the rows with this id, finding or group only")
    ap.add_argument("--list", action="store_true", help="print the rows and exit")
    ap.add_argument("--dry-run", action="store_true", help="validate every snippet and test path; run nothing")
    args = ap.parse_args(argv)

    rows = select(load_registry(args.registry), args.only)
    print(f"REGISTRY: {len(rows)} rows ({args.registry})")
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
        if until is None:
            verdict = "OK" if res["status"] == "killed" else "FAIL"
        else:
            verdict = {"killed": "killed early", "survived": f"expected (until {until})"}.get(res["status"], "FAIL")
            if res["status"] == "error":
                verdict = f"error (until {until})"
        results.append((r, res, verdict))
        rc = f" rc={res['rc']}" if "rc" in res else ""
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
    print(
        f"MUTANTS: {killed} killed ({early} early), {len(results) - killed} survived/errored, "
        f"{len(fatal)} unexpected -> {'FAIL ' + ', '.join(fatal) if fatal else 'OK'}"
    )
    return 1 if fatal else 0


if __name__ == "__main__":
    sys.exit(main())
