"""Walk the committed return-table schema YAMLs and emit the work-list of
columns that render BLANK in the docs (no captured description, no manual-dict
entry, no R-dict match). Input for description authoring + the coverage test."""

from __future__ import annotations

import functools
import glob
import json
import os

from tools.codegen.generate import ENDPOINTS, FLAT_APIS, _manual_col_desc, _r_col_desc, _r_dict_applies

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SCHEMA_DIR = os.path.join(ROOT, "tools", "codegen", "schemas")


@functools.lru_cache(maxsize=1)
def _flat_leagues() -> dict[str, str]:
    """``{returns_schema: league prefix}`` over the flat-API endpoint YAMLs.

    A flat family's reference page renders each table with ``_return_table(returns_schema,
    <FLAT_APIS prefix>)``, so that prefix is the league its R-dict fallback text resolves with.
    No returns_schema is referenced from two leagues, so the map is exact.
    """
    import yaml

    out: dict[str, str] = {}
    for stem, prefix in FLAT_APIS:
        p = ENDPOINTS / f"{stem}.yaml"
        if p.exists():
            for ep in (yaml.safe_load(p.read_text(encoding="utf-8")) or {}).get("endpoints") or []:
                if ep.get("returns_schema"):
                    out.setdefault(ep["returns_schema"], prefix)
    return out


def _league_of(path: str) -> str | None:
    """League the docs render a schema's fallback text with (best effort).

    Handles three cases:
    * ``schemas/autodoc/<league>/...``  → return ``<league>``
    * ``schemas/<name>/<league>.yaml``  where ``<name>`` is NOT ``autodoc`` or ``native``
      (e.g. ``schemas/news/nfl.yaml``, ``schemas/standings/nba.yaml``) → return file stem
      (the stem is the league slug, matching how ``_return_table`` passes ``league.prefix``).
    * ``schemas/native/<stem>/<name>.yaml`` → the ``FLAT_APIS`` league of the endpoints that
      return it (``native/pff/*`` → ``nfl``). The path names an API family, not a league, and
      reading ``None`` here gave the check no R text where the page shows the league's own.

    Top-level files (e.g. ``schemas/scoreboard.yaml``, depth==1) and native schemas no endpoint
    returns remain ``None``.
    """
    rel = os.path.relpath(path, SCHEMA_DIR).replace("\\", "/").split("/")
    if rel[0] == "autodoc" and len(rel) >= 2:
        return rel[1]
    if rel[0] == "native":
        return _flat_leagues().get(os.path.splitext("/".join(rel))[0])
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
#
# nba_stats / wnba_stats / sports247_site_pages / on3 / pff / loader_schemas were
# once here for the same reason: each family's column surface was captured faster
# than descriptions could be authored, so the backlog was exempted from the hard
# residual ratchet and reported separately via deferred_columns(). All six have
# since been fully backfilled (0 genuinely-uncovered columns as of 2026-09-03) and
# were promoted OUT of this set -- their coverage is now enforced by the same hard
# gate as everything else, so a future regression (e.g. capturing a new nba_stats
# endpoint before its columns are described) is caught immediately instead of
# quietly reappearing as "deferred." If a bucket like this grows a large new
# backlog again, re-add it here deliberately rather than letting the gate go red.
#
# native/nflpro is deferred for a different, durable reason (not "not yet authored"
# but "not authorable"): the Next Gen Stats field names have no reachable authoritative
# label source. gen_nflpro_descriptions.py (2026-10-07) authored the 1,006 columns that
# a nflverse value crosswalk, an arithmetic identity on the capture or the envelope
# confirms; the 30 it cannot confirm (avgTTP, bhPct, twfPct, ...) stay blank and capped.
# See sdv-internal-refs/nfl/nflpro/catalogs/nfl_pro_secured_returns.md.
#
# native/nba_stats and native/wnba_stats are deferred AGAIN (2026-10-05; native/on3 was
# fully authored 2026-10-07 by gen_on3_descriptions.py and promoted out)
# because their tables were regenerated from what the parsers emit on real captures
# instead of from the canonical catalog / OpenAPI spec. That renamed columns
# (fg3m -> fg3_m, leagueid -> league_id; descriptions re-keyed where the rename was
# unambiguous), documented every result set of a multi-set stats endpoint instead of
# one representative set, and replaced On3's single object columns with the
# json_normalize-flattened ones the parser actually returns. The newly surfaced
# columns had no description to carry over. Each is CAPPED at the count measured
# that day, so a newly blank column still fails the gate (test_manual_descriptions);
# lower a cap as columns are authored, and promote the bucket out at 0.
#
# {bucket: max uncovered cells, or None for no cap}
_DEFERRED_BUCKETS: dict[str, int | None] = {
    # The ESPN generic tables captured 2026-10-07 by tools/codegen/capture_fixtures.py (one live
    # payload per schema-less Site v2 / Core v2 / Web v3 endpoint, parsed by the wrapper's own
    # parser). Their names and types are real; ESPN publishes no field dictionary to describe
    # them from. Each is CAPPED at its measured count: lower a cap as columns are authored.
    "athlete_contracts.yaml": 1,
    "athlete_core.yaml": 54,
    "athlete_gamelog.yaml": 26,
    "athlete_hotzones.yaml": 5,
    "athlete_overview.yaml": 21,
    "athlete_seasons.yaml": 1,
    "athlete_splits.yaml": 7,
    "athlete_statisticslog.yaml": 2,
    "athletes_index.yaml": 1,
    "award.yaml": 5,
    "awards.yaml": 1,
    "coach.yaml": 10,
    "coach_record.yaml": 8,
    "conferences.yaml": 8,
    "event.yaml": 13,
    "event_broadcasts.yaml": 21,
    "event_competition.yaml": 81,
    "event_competitor.yaml": 14,
    "event_competitor_roster.yaml": 5,
    "event_competitors.yaml": 14,
    "event_odds.yaml": 175,
    "event_official_detail.yaml": 10,
    "event_officials.yaml": 10,
    "event_play.yaml": 26,
    "event_plays.yaml": 27,
    "event_powerindex.yaml": 5,
    "event_predictor.yaml": 7,
    "event_probabilities.yaml": 16,
    "event_situation.yaml": 14,
    "event_status.yaml": 11,
    "events.yaml": 1,
    "franchise.yaml": 22,
    "franchises.yaml": 1,
    "league_root.yaml": 60,
    "position.yaml": 7,
    "positions.yaml": 1,
    "recruiting_athletes.yaml": 34,
    "recruiting_rankings.yaml": 1,
    "recruiting_years.yaml": 1,
    "season_athletes.yaml": 1,
    "season_awards.yaml": 1,
    "season_coaches.yaml": 1,
    "season_futures.yaml": 6,
    "season_group.yaml": 14,
    "season_groups.yaml": 1,
    "season_info.yaml": 33,
    "season_pointer.yaml": 32,
    "season_powerindex.yaml": 9,
    "season_qbr_week.yaml": 7,
    "season_recruits.yaml": 1,
    "season_team.yaml": 40,
    "season_teams.yaml": 1,
    "season_type.yaml": 16,
    "season_types.yaml": 1,
    "season_week.yaml": 6,
    "season_week_rankings.yaml": 1,
    "season_weeks.yaml": 1,
    "seasons.yaml": 1,
    "team.yaml": 41,
    "team_core.yaml": 40,
    "tournaments.yaml": 1,
    "transactions.yaml": 11,
    "venue.yaml": 7,
    "venues.yaml": 1,
    "native/nflpro": 30,  # the un-authorable NGS fields only (2026-10-07); was None
    "native/nba_stats": 312,
    "native/wnba_stats": 269,
    "native/fotmob": 283,
    "native/uefa": 841,
    "native/sleeper": 459,
    # Buckets whose blank cells held CROSS-SPORT R text until the fallback was scoped to the
    # league's own sport (2026-10-07): the earlier zero residual counted baseballr's "Inning
    # number." on a jersey number and cfbfastR's SP+ on an NFL rating as coverage, so these are
    # re-based at the measured count, not loosened. Lower a cap as columns are authored.
    "autodoc/ahl": 2,
    "autodoc/ajhl": 1,
    "autodoc/cchl": 1,
    "autodoc/cfb": 92,
    "autodoc/chl": 2,
    "autodoc/global": 3,
    "autodoc/gojhl": 1,
    "autodoc/mbb": 6,
    "autodoc/mlb": 31,
    "autodoc/nba": 4,
    "autodoc/nfl": 248,
    "autodoc/nhl": 51,
    "autodoc/nojhl": 1,
    "autodoc/odds": 22,
    "autodoc/ohl": 2,
    "autodoc/pwhl": 4,
    "autodoc/qmjhl": 4,
    "autodoc/sjhl": 1,
    "autodoc/ushl": 2,
    "autodoc/wbb": 6,
    "autodoc/whl": 1,
    "autodoc/wnba": 5,
    "cdn_scoreboard.yaml": 38,
    "loader_schemas": 371,
    "native/cbs_napi": 42,
    "native/mlb_api": 10,
    "native/mls_api": 8,
    "native/nhl_api_web": 6,
    "native/nhl_edge": 12,
    "native/nhl_records": 19,
    "native/nhl_stats_rest": 2,
    "native/nwsl_api": 9,
    "native/sports247_site_pages": 22,
    "native/yahoo_shangrila": 729,
    "scoreboard": 105,
    "scoreboard.yaml": 38,
    "standings": 8,
    "summary": 211,
    "team_roster": 24,
    "team_schedule": 4,
    # wave-2 intake families, same situation: the specs are capture-derived and these providers
    # publish no property docs, so the generators have nothing to describe from. espn-content
    # additionally keys its returns-doc rows by dotted JSON path, which never matches a
    # snake_cased parser column. football-data is the exception: its glossary describes 224 of
    # 251 columns, and only the exchange-odds codes are blank upstream too. Measured 2026-10-06;
    # re-measured 2026-10-07 once this check applied the same no-R-dict gate the renderer does
    # (the first measurement counted cross-sport R text the page never showed as coverage).
    "native/espn_content": 88,
    "native/thesportsdb": 486,
    "native/football_data": 27,
    "native/openligadb": 139,
    "native/polymarket": 292,
    "native/kalshi": 157,
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
    if not r["blank"] or _manual_col_desc(r["schema"], r["col"]):
        return False
    # Same gate as render: a family with no R counterpart (``_NO_R_DICT_FAMILIES``) gets no fill.
    key = f"{r['bucket']}/{r['schema']}" if r["bucket"].startswith("native/") else r["schema"]
    return not (_r_dict_applies(key) and _r_col_desc(r["league"], r["col"], r["schema"]))


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
