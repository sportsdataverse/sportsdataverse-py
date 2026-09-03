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
# native/sports247_site_pages: the 247sports.com front-end page-model surface (17
# entity schemas, 372 columns). The semantically load-bearing columns (ids, FKs,
# ratings, ranks, status/commit fields — 89 columns) are authored from the OpenAPI
# `description:` fields; the plain long tail (names, dates, assets, tax rates) is a
# tracked follow-up, mirroring the nba_stats/wnba_stats decision above.
# native/on3 + native/pff: the On3 RDB and PFF Premium column tails are likewise
# authored as pilots with the remainder deferred (see those tracks' plans).
# loader_schemas: generated dataset-loader return tables gained a `description`
# column (previously they rendered name+type only), which exposed ~3k columns
# across 8 leagues in one step. The shared//id columns fill from the R dict for
# free; the per-dataset statistical tails are authored incrementally, mirroring
# the nba_stats/wnba_stats decision above. `deferred_columns()` reports the count.
_DEFERRED_BUCKETS = {
    "native/nba_stats",
    "native/wnba_stats",
    "native/sports247_site_pages",
    "native/on3",
    "native/pff",
    # NFL Pro (NGS): 1,036 columns whose descriptions are deliberately absent --
    # no authoritative label source was reachable, and guessing would manufacture
    # authority the capture does not carry. Tracked here rather than left invisible.
    "native/nflpro",
    "loader_schemas",
}


def _rendering_loaders() -> set[str]:
    """Loaders whose return table actually RENDERS a column/description table.

    Only loaders declared in ``releases.yaml`` reach ``loaders_page.md.jinja`` via
    ``_loader_schema_table``. Hand-written loaders are documented on the league's
    ``additional`` page, whose Returns section is prose -- they have no column table
    for a description to appear in. Counting their columns would let an authored
    description register as "covered" while rendering nowhere, so they are excluded
    from the accounting entirely rather than reported as blank.
    """
    import yaml

    path = os.path.join(ROOT, "tools", "codegen", "endpoints", "releases.yaml")
    try:
        with open(path, encoding="utf-8") as fh:
            raw = yaml.safe_load(fh) or {}
    except (yaml.YAMLError, OSError):
        return set()
    return {ld["fn"] for ld in raw.get("loaders", []) if "fn" in ld}


def _loader_schema_rows(d: dict) -> list[dict]:
    """Rows for ``loader_schemas.yaml`` — ``{loader_fn: [{name, type}]}``.

    The league is taken from the ``load_<league>_*`` function name (matching what
    ``_loader_schema_table`` passes as ``league``), so the R-dict fallback resolves
    the same way here as it does at render time. These entries carry no stored
    description, so ``blank`` is always True and coverage is decided purely by the
    manual dict + R dict.
    """
    rendering = _rendering_loaders()
    rows: list[dict] = []
    for fn, cols in (d or {}).items():
        if not isinstance(cols, list) or fn not in rendering:
            continue
        parts = fn.split("_")
        league = parts[1] if len(parts) > 2 and parts[0] == "load" else None
        names = [c.get("name", "") for c in cols if isinstance(c, dict)]
        for c in cols:
            if not isinstance(c, dict):
                continue
            rows.append(
                {
                    "schema": fn,
                    "league": league,
                    "bucket": "loader_schemas",
                    "col": c.get("name", ""),
                    "type": c.get("type", ""),
                    "blank": True,
                    "siblings": [n for n in names if n != c.get("name", "")],
                }
            )
    return rows


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
        # loader_schemas.yaml is a flat {loader_fn: [{name, type}]} map, not the
        # `kind: dataframe` shape. It is globbed like any other schema file, so
        # without this branch its columns silently contribute nothing -- and since
        # loader return tables now render a description column, they would be blank
        # cells invisible to the ratchet.
        if os.path.basename(f) == "loader_schemas.yaml":
            out.extend(_loader_schema_rows(d))
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
