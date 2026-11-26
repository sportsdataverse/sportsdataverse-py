"""Mine the exported function names of the sister SDV R packages into a YAML set.

The sdv-py wrappers deliberately mirror the R sisters' names (hoopR / wehoop /
cfbfastR / baseballr / fastRhockey, plus nflverse's nflreadr / nflfastR). The docs
generator uses this to render a per-league "Python <-> R parity" table that links a
Python function to its same-named R function's pkgdown reference -- but only when
that R function actually exists, so the link can't 404.

Reading the R ``NAMESPACE`` files (``git show <ref>:NAMESPACE``, default ``origin/HEAD`` so a
feature branch checked out locally is never mined) is offline but requires the R repos to be
checked out, which they are NOT on CI. So we MINE the exports into a committed
artifact (``tools/codegen/r_exports.yaml``) that ``generate.py`` reads instead --
keeping the offline ``--check`` deterministic, exactly like
``r_column_descriptions.yaml``.

Run (``git fetch`` each R repo first so ``origin/HEAD`` is current):
    uv run python tools/codegen/build_r_exports.py                   # re-mine every package
    uv run python tools/codegen/build_r_exports.py hoopR             # re-mine only the named package(s)
    uv run python tools/codegen/build_r_exports.py --ref main hoopR  # mine another ref

Naming packages keeps every other package's committed list as it is, so one sister's
refresh does not drag in the others' drift.

Output YAML shape::

    cfbfastR: [espn_cfb_pbp, load_cfb_pbp, ...]
    hoopR: [...]
    ...
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
_DEV = "C:/Users/saiem/Documents/GitHub-Data/sdv-dev"
_NFLVERSE = f"{_DEV}/nflverse-dev"

# package -> local git checkout holding its NAMESPACE
NAMESPACES: dict[str, Path] = {
    "cfbfastR": Path(f"{_DEV}/cfbfastR-dev/cfbfastR"),
    "hoopR": Path(f"{_DEV}/hoopR-dev/hoopR"),
    "wehoop": Path(f"{_DEV}/wehoop-dev/wehoop"),
    "baseballr": Path(f"{_DEV}/baseball-dev/baseballr"),
    "fastRhockey": Path(f"{_DEV}/hockey-dev/fastRhockey"),
    "nflreadr": Path(f"{_NFLVERSE}/nflreadr"),
    "nflfastR": Path(f"{_NFLVERSE}/nflfastR"),
}

OUTPUT = ROOT / "tools" / "codegen" / "r_exports.yaml"

# `export(name)` or `export("name")` -- not exportPattern / S3method / exportClasses.
_EXPORT_RE = re.compile(r'^\s*export\(\s*"?([A-Za-z._][A-Za-z0-9._]*)"?\s*\)')


def mine_namespace(repo: Path, ref: str) -> list[str]:
    """Return the sorted exported function names in *repo*'s NAMESPACE at git *ref*.

    Exits non-zero when the file can't be read, so a missing checkout never writes an empty list."""
    proc = subprocess.run(
        ["git", "-C", str(repo), "show", f"{ref}:NAMESPACE"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if proc.returncode != 0:
        sys.exit(f"cannot read {ref}:NAMESPACE in {repo}: {proc.stderr.strip()}")
    names: set[str] = set()
    for line in proc.stdout.splitlines():
        m = _EXPORT_RE.match(line)
        if m is not None:
            names.add(m.group(1))
    return sorted(names)


def main(only: list[str] | None = None, ref: str = "origin/HEAD") -> None:
    unknown = set(only or []) - set(NAMESPACES)
    if unknown:
        sys.exit(f"unknown package(s) {sorted(unknown)}; choose from {sorted(NAMESPACES)}")
    if only and not OUTPUT.exists():
        # a selective run copies the other packages from the committed file; without it they would be emptied
        sys.exit(f"{OUTPUT} is missing; restore it (git checkout) before re-mining only {sorted(only)}")
    committed = yaml.safe_load(OUTPUT.read_text(encoding="utf-8")) if only else {}
    out: dict[str, list[str]] = {}
    for pkg, repo in NAMESPACES.items():
        if only and pkg not in only:
            out[pkg] = list(committed.get(pkg) or [])
            continue
        names = mine_namespace(repo, ref)
        out[pkg] = names
        print(f"  {pkg}: {len(names)} exports")

    header = (
        "# Exported function names of the sister SDV R packages (from each NAMESPACE).\n"
        "# Drives the per-league Python<->R parity table in generate.py.\n"
        "# Regenerate with:\n"
        "#   uv run python tools/codegen/build_r_exports.py\n"
    )
    yaml_str = yaml.dump(out, allow_unicode=True, default_flow_style=False, sort_keys=True, width=120)
    OUTPUT.write_text(header + yaml_str, encoding="utf-8", newline="\n")
    print(f"\nWrote {OUTPUT}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mine the sister R packages' exports into r_exports.yaml.")
    parser.add_argument("packages", nargs="*", help="re-mine only these packages (default: all)")
    parser.add_argument("--ref", default="origin/HEAD", help="git ref to read each NAMESPACE from")
    args = parser.parse_args()
    main(args.packages or None, args.ref)
