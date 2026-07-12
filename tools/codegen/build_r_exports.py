"""Mine the exported function names of the sister SDV R packages into a YAML set.

The sdv-py wrappers deliberately mirror the R sisters' names (hoopR / wehoop /
cfbfastR / baseballr / fastRhockey, plus nflverse's nflreadr / nflfastR). The docs
generator uses this to render a per-league "Python <-> R parity" table that links a
Python function to its same-named R function's pkgdown reference -- but only when
that R function actually exists, so the link can't 404.

Reading the live R ``NAMESPACE`` files is offline but requires the R repos to be
checked out, which they are NOT on CI. So we MINE the exports into a committed
artifact (``tools/codegen/r_exports.yaml``) that ``generate.py`` reads instead --
keeping the offline ``--check`` deterministic, exactly like
``r_column_descriptions.yaml``.

Run:
    python tools/codegen/build_r_exports.py

Output YAML shape::

    cfbfastR: [espn_cfb_pbp, load_cfb_pbp, ...]
    hoopR: [...]
    ...
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
_DEV = "C:/Users/saiem/Documents/GitHub-Data/sdv-dev"
_NFLVERSE = f"{_DEV}/nflverse-dev"

# package -> NAMESPACE file
NAMESPACES: dict[str, Path] = {
    "cfbfastR": Path(f"{_DEV}/cfbfastR-dev/cfbfastR/NAMESPACE"),
    "hoopR": Path(f"{_DEV}/hoopR-dev/hoopR/NAMESPACE"),
    "wehoop": Path(f"{_DEV}/wehoop-dev/wehoop/NAMESPACE"),
    "baseballr": Path(f"{_DEV}/baseball-dev/baseballr/NAMESPACE"),
    "fastRhockey": Path(f"{_DEV}/hockey-dev/fastRhockey/NAMESPACE"),
    "nflreadr": Path(f"{_NFLVERSE}/nflreadr/NAMESPACE"),
    "nflfastR": Path(f"{_NFLVERSE}/nflfastR/NAMESPACE"),
}

OUTPUT = ROOT / "tools" / "codegen" / "r_exports.yaml"

# `export(name)` or `export("name")` -- not exportPattern / S3method / exportClasses.
_EXPORT_RE = re.compile(r'^\s*export\(\s*"?([A-Za-z._][A-Za-z0-9._]*)"?\s*\)')


def mine_namespace(path: Path) -> list[str]:
    """Return the sorted exported function names declared in *path*."""
    names: set[str] = set()
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        print(f"  WARNING: cannot read {path}", file=sys.stderr)
        return []
    for line in text.splitlines():
        m = _EXPORT_RE.match(line)
        if m is not None:
            names.add(m.group(1))
    return sorted(names)


def main() -> None:
    out: dict[str, list[str]] = {}
    for pkg, ns in NAMESPACES.items():
        if not ns.exists():
            print(f"  WARNING: NAMESPACE not found: {ns}", file=sys.stderr)
            out[pkg] = []
            continue
        names = mine_namespace(ns)
        out[pkg] = names
        print(f"  {pkg}: {len(names)} exports")

    header = (
        "# Exported function names of the sister SDV R packages (from each NAMESPACE).\n"
        "# Drives the per-league Python<->R parity table in generate.py.\n"
        "# Regenerate with:\n"
        "#   python tools/codegen/build_r_exports.py\n"
    )
    yaml_str = yaml.dump(out, allow_unicode=True, default_flow_style=False, sort_keys=True, width=120)
    OUTPUT.write_text(header + yaml_str, encoding="utf-8", newline="\n")
    print(f"\nWrote {OUTPUT}")


if __name__ == "__main__":
    main()
