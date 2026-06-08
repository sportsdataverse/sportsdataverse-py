"""Mine SDV R packages' @return column descriptions into a YAML dictionary.

Parses each of the 4 R packages' R/*.R files for @return markdown tables that
include a ``description`` column, extracts col_name -> description mappings,
deduplicates by most-frequent description (longest on tie), and emits
``tools/codegen/r_column_descriptions.yaml``.

Run:
    python tools/codegen/build_r_col_descriptions.py

Output YAML shape::

    cfbfastR:
      season: "Season (4-digit year) queried."
      ...
    hoopR: { ... }
    wehoop: { ... }
    baseballr: { ... }
    _merged: { ... }  # union across all packages, most-frequent globally
"""

from __future__ import annotations

import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Optional

import yaml

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parents[2]
_DEV = "C:/Users/saiem/Documents/GitHub-Data/sdv-dev"

PACKAGES: dict[str, Path] = {
    "cfbfastR": Path(f"{_DEV}/cfbfastR-dev/cfbfastR/R"),
    "hoopR": Path(f"{_DEV}/hoopR-dev/hoopR/R"),
    "wehoop": Path(f"{_DEV}/wehoop-dev/wehoop/R"),
    "baseballr": Path(f"{_DEV}/baseball-dev/baseballr/R"),
}

OUTPUT = ROOT / "tools" / "codegen" / "r_column_descriptions.yaml"

# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

# Active roxygen line: starts with optional whitespace, then "#'"
# (not "#  #'" or "## #'" which are commented-out roxygen blocks)
_ROXYGEN_RE = re.compile(r"^[^#]*#'(.*)$")


def _strip_roxygen(line: str) -> Optional[str]:
    """Return the payload of an active roxygen line, or None if not roxygen."""
    m = _ROXYGEN_RE.match(line)
    if m is None:
        return None
    # Exclude double-commented lines like "#  #'" or "## #'"
    # The regex above already anchors at start; check that the portion before
    # "#'" is only spaces (0 or more).
    prefix = line[: m.start(1) - 2]  # everything before the "#'"
    if prefix.strip().replace("#", "").strip() != "":
        return None
    # Extra guard: if the matched prefix (before "#'") contains a non-space
    # non-hash character, it's inside a code block or comment — skip.
    if re.search(r"[^#\s]", prefix):
        return None
    return m.group(1)


def _split_table_row(row: str) -> list[str]:
    """Split a markdown table row on '|', stripping leading/trailing pipes."""
    # Remove leading/trailing |, then split
    cells = row.strip().strip("|").split("|")
    return [c.strip() for c in cells]


def _is_separator_row(cells: list[str]) -> bool:
    """Return True if every non-empty cell matches ':---...' or '---...'."""
    non_empty = [c for c in cells if c]
    if not non_empty:
        return False
    return all(re.fullmatch(r":?-+:?", c) for c in non_empty)


def _is_table_row(payload: str) -> bool:
    """Return True if the roxygen payload looks like a markdown table row."""
    stripped = payload.strip()
    return stripped.startswith("|") and stripped.endswith("|") and stripped.count("|") >= 2


def _normalize(text: str) -> str:
    """Strip, collapse internal whitespace."""
    return re.sub(r"\s+", " ", text.strip())


# ---------------------------------------------------------------------------
# Per-file parser
# ---------------------------------------------------------------------------


def parse_r_file(path: Path) -> list[tuple[str, str]]:
    """Return a list of (col_name, description) pairs mined from *path*.

    Only 3+-column tables whose header contains both ``col_name`` and
    ``description`` are processed. 2-column tables (no description header)
    are silently skipped.
    """
    pairs: list[tuple[str, str]] = []

    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return pairs

    lines = text.splitlines()

    # State machine
    in_table = False
    desc_col_idx: int = -1  # index of the "description" header column
    col_name_idx: int = 0  # always 0 per spec, but find it dynamically

    i = 0
    while i < len(lines):
        payload = _strip_roxygen(lines[i])
        i += 1

        if payload is None:
            # Non-roxygen line resets any active table parse
            in_table = False
            desc_col_idx = -1
            continue

        if not _is_table_row(payload):
            # Plain roxygen text — reset table state
            in_table = False
            desc_col_idx = -1
            continue

        cells = _split_table_row(payload)

        if not in_table:
            # Potential header row: must contain "col_name" and "description"
            lower_cells = [c.lower() for c in cells]
            if "col_name" not in lower_cells or "description" not in lower_cells:
                # 2-column table or unrecognized header — skip
                in_table = False
                continue

            col_name_idx = lower_cells.index("col_name")
            desc_col_idx = lower_cells.index("description")
            in_table = True
            # Next line should be the separator — we'll consume it in the next
            # iteration as a table row and detect it via _is_separator_row.
            continue

        # We're inside a table
        if _is_separator_row(cells):
            # Separator row — skip, stay in table
            continue

        # Data row
        if len(cells) <= max(col_name_idx, desc_col_idx):
            # Row too short — probably end of table with blank trailing pipe
            in_table = False
            desc_col_idx = -1
            continue

        col_name = _normalize(cells[col_name_idx])
        description = _normalize(cells[desc_col_idx])

        # Skip empty, placeholder, or header-like values
        if not col_name or not description:
            continue
        if col_name.lower() == "col_name":
            continue
        if description.lower() in ("description", "-", "--", "—", "na", "n/a"):
            continue

        pairs.append((col_name, description))

    return pairs


# ---------------------------------------------------------------------------
# Per-package aggregation
# ---------------------------------------------------------------------------


def mine_package(r_dir: Path) -> dict[str, str]:
    """Mine all R/*.R files in *r_dir* and return col_name -> best description."""
    # freq[col_name][description] = count
    freq: dict[str, Counter] = defaultdict(Counter)

    r_files = sorted(r_dir.glob("*.R"))
    if not r_files:
        print(f"  WARNING: no .R files found in {r_dir}", file=sys.stderr)
        return {}

    for rf in r_files:
        for col_name, description in parse_r_file(rf):
            freq[col_name][description] += 1

    result: dict[str, str] = {}
    for col_name, counter in freq.items():
        # Most frequent; on tie pick the longest
        best = max(counter, key=lambda d: (counter[d], len(d)))
        result[col_name] = best

    return result


# ---------------------------------------------------------------------------
# Merged dictionary
# ---------------------------------------------------------------------------


def build_merged(per_package: dict[str, dict[str, str]]) -> dict[str, str]:
    """Build a global col_name -> best description across all packages."""
    global_freq: dict[str, Counter] = defaultdict(Counter)

    for pkg_dict in per_package.values():
        for col_name, description in pkg_dict.items():
            global_freq[col_name][description] += 1

    merged: dict[str, str] = {}
    for col_name, counter in global_freq.items():
        best = max(counter, key=lambda d: (counter[d], len(d)))
        merged[col_name] = best

    return merged


# ---------------------------------------------------------------------------
# YAML serialisation helpers
# ---------------------------------------------------------------------------


def _sorted_str_dict(d: dict[str, str]) -> dict[str, str]:
    """Return a new dict sorted by key."""
    return dict(sorted(d.items()))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    per_package: dict[str, dict[str, str]] = {}

    for pkg_name, r_dir in PACKAGES.items():
        print(f"Mining {pkg_name} from {r_dir} ...", flush=True)
        if not r_dir.exists():
            print(f"  WARNING: directory not found: {r_dir}", file=sys.stderr)
            per_package[pkg_name] = {}
            continue
        result = mine_package(r_dir)
        per_package[pkg_name] = _sorted_str_dict(result)
        print(f"  {pkg_name}: {len(result)} unique col_name descriptions")

    merged = _sorted_str_dict(build_merged(per_package))
    print(f"  _merged: {len(merged)} unique col_name descriptions")

    # Build the YAML document.  Use a custom representer so strings stay
    # single-line (no block scalars for short strings).
    output_data: dict = {}
    for pkg_name in PACKAGES:
        output_data[pkg_name] = per_package[pkg_name]
    output_data["_merged"] = merged

    header = (
        "# Mined from SDV R packages' @return tables (col_name -> description).\n"
        "# Source of truth for sdv-py codegen return-table descriptions.\n"
        "# Regenerate with:\n"
        "#   python tools/codegen/build_r_col_descriptions.py\n"
    )

    # Dump with default_flow_style=False so we get block style for readability,
    # but allow_unicode so non-ASCII descriptions survive.
    yaml_str = yaml.dump(
        output_data,
        allow_unicode=True,
        default_flow_style=False,
        sort_keys=False,
        width=120,
    )

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(header + yaml_str, encoding="utf-8")
    print(f"\nWrote {OUTPUT}")


if __name__ == "__main__":
    main()
