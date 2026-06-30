"""Walk the committed return-table schema YAMLs and emit the work-list of
columns that render BLANK in the docs (no captured description, no manual-dict
entry, no R-dict match). Input for description authoring + the coverage test."""

from __future__ import annotations

import glob
import json
import os

from tools.codegen.generate import _manual_col_desc, _r_col_desc

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SCHEMA_DIR = os.path.join(ROOT, "tools", "codegen", "schemas")


def _league_of(path: str) -> str | None:
    """Best-effort league slug from a schema path.

    Handles two cases:
    * ``schemas/autodoc/<league>/...``  → return ``<league>``
    * ``schemas/<name>/<league>.yaml``  where ``<name>`` is NOT ``autodoc`` or ``native``
      (e.g. ``schemas/news/nfl.yaml``, ``schemas/standings/nba.yaml``) → return file stem
      (the stem is the league slug, matching how ``_return_table`` passes ``league.prefix``).

    Top-level files (e.g. ``schemas/scoreboard.yaml``, depth==1) and ``native/<stem>/``
    subdirs (stem is an API family, not a league) remain ``None``.
    """
    rel = os.path.relpath(path, SCHEMA_DIR).replace("\\", "/").split("/")
    if rel[0] == "autodoc" and len(rel) >= 2:
        return rel[1]
    # schemas/<name>/<league>.yaml — two-segment relative path, <name> not autodoc/native
    if len(rel) == 2 and rel[0] not in ("autodoc", "native"):
        stem = os.path.splitext(rel[1])[0]
        return stem
    return None


def _bucket_of(path: str) -> str:
    rel = os.path.relpath(path, SCHEMA_DIR).replace("\\", "/").split("/")
    if rel[0] in ("autodoc", "native") and len(rel) >= 2:
        return f"{rel[0]}/{rel[1]}"
    return rel[0]


# Buckets whose blank columns are a TRACKED follow-up, not a coverage failure.
# The stats.nba.com / stats.wnba.com flat-API family wraps ~150 endpoints whose
# ~3k+ result-set columns are documented incrementally (the 5 pilot slugs are
# authored; the long tail is mined opportunistically via the R `_merged` dict).
# Capturing more endpoints (resolving "untested" -> "live") grows the column
# surface faster than descriptions are authored, so these buckets are exempted
# from the residual ratchet and surfaced via deferred_columns() instead. Authoring
# a column here (manual dict or R dict) still removes it from the deferred count.
_DEFERRED_BUCKETS = {"native/nba_stats", "native/wnba_stats"}


def iter_schema_columns() -> list[dict]:
    import yaml

    out: list[dict] = []
    for f in glob.glob(os.path.join(SCHEMA_DIR, "**", "*.yaml"), recursive=True):
        try:
            with open(f, encoding="utf-8") as fh:
                d = yaml.safe_load(fh)
        except (yaml.YAMLError, OSError):
            continue
        if not isinstance(d, dict):
            continue
        schema = d.get("schema") or os.path.splitext(os.path.basename(f))[0]
        league = _league_of(f)
        bucket = _bucket_of(f)
        frames = d.get("frames") if d.get("kind") == "frames" else [{"columns": d.get("columns") or []}]
        for blk in frames or []:
            cols = blk.get("columns") or []
            names = [c.get("name", "") for c in cols if isinstance(c, dict)]
            for c in cols:
                if not isinstance(c, dict):
                    continue
                out.append(
                    {
                        "schema": schema,
                        "league": league,
                        "bucket": bucket,
                        "col": c.get("name", ""),
                        "type": c.get("type", ""),
                        "blank": not (c.get("description") or "").strip(),
                        "siblings": [n for n in names if n != c.get("name", "")],
                    }
                )
    return out


def _uncovered(r: dict) -> bool:
    """A blank column with no manual-dict and no R-dict description."""
    return r["blank"] and not _manual_col_desc(r["schema"], r["col"]) and not _r_col_desc(r["league"], r["col"])


def residual_columns() -> list[dict]:
    """Uncovered blank columns OUTSIDE the deferred buckets (the ratchet target)."""
    return [r for r in iter_schema_columns() if _uncovered(r) and r["bucket"] not in _DEFERRED_BUCKETS]


def deferred_columns() -> list[dict]:
    """Uncovered blank columns INSIDE the deferred buckets (tracked follow-up)."""
    return [r for r in iter_schema_columns() if _uncovered(r) and r["bucket"] in _DEFERRED_BUCKETS]


def residual_by_bucket() -> dict:
    counts: dict[str, int] = {}
    for r in residual_columns():
        counts[r["bucket"]] = counts.get(r["bucket"], 0) + 1
    return dict(sorted(counts.items(), key=lambda kv: -kv[1]))


def main() -> None:
    residual = residual_columns()
    deferred = deferred_columns()
    print(
        json.dumps(
            {
                "residual": len(residual),
                "deferred": len(deferred),
                "by_bucket": residual_by_bucket(),
                "columns": residual,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
