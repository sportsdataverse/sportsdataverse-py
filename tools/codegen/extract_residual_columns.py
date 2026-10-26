"""Walk the committed return-table schema YAMLs and emit the work-list of
columns that render BLANK in the docs (no captured description, no manual-dict
entry, no R-dict match). Input for description authoring + the coverage test.

Run from the repo root, either way::

    uv run python tools/codegen/extract_residual_columns.py            # JSON work-list
    uv run python tools/codegen/extract_residual_columns.py --report   # per-bucket count / cap
"""

from __future__ import annotations

import functools
import glob
import json
import os
import sys

# Script mode (`python tools/codegen/extract_residual_columns.py`) puts this file's directory on
# sys.path, not the repo root, so the absolute ``tools.codegen`` import below would fail.
if not __package__:
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

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


# Buckets whose blank columns are a TRACKED follow-up, not a coverage failure: residual_columns()
# skips them and deferred_columns() reports them. Every other bucket is held at zero blank cells by
# test_manual_descriptions, and so is a bucket registered here at 0 (the intake families below).
#
# Each cap is the count measured when the bucket was (re-)based, so a NEW blank column in it still
# fails test_deferred_buckets_do_not_grow. Lower a cap as columns are authored; remove the bucket
# once it reaches 0. ``extract_residual_columns.py --report`` prints each bucket's count beside its cap.
#
# Why the large buckets are here:
# - native/nflpro: not authorable. gen_nflpro_descriptions.py authored every Next Gen Stats column a
#   nflverse crosswalk, an identity on the capture or the envelope confirms; the 30 left (avgTTP,
#   bhPct, twfPct, ...) have no reachable label source (sdv-internal-refs/nfl/nflpro/catalogs/
#   nfl_pro_secured_returns.md).
# - native/nba_stats, native/wnba_stats (and native/nbagl_stats, built the same way 2026-10-26): their tables were rebuilt from what the parser emits on real
#   captures (2026-10-05), which renamed columns and surfaced every result set; the new columns had
#   no description to carry over. (native/on3, rebuilt the same way, was fully authored 2026-10-07 and
#   removed from this table.)
# - the ESPN generic tables, Fox, KenPom and the intake families: captured from live payloads whose
#   providers publish no field dictionary; the per-group comments below give the dates and sources.
# - the autodoc buckets: re-based 2026-10-08 when every hand-written DataFrame function got a table.
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
    # Same 2026-10-07 sweep, second pass: captured in the league whose payload the parser reads
    # (CFB groups, NFL corrections/weeks/talent picks, MBB power-index leaders) and the two
    # power-index tables whose links had pointed at no schema since 0.0.67.
    "fpi.yaml": 41,
    "season_group_children.yaml": 1,
    "season_group_teams.yaml": 1,
    "season_powerindex_leaders.yaml": 3,
    "season_type_corrections.yaml": 7,
    "season_week_events.yaml": 1,
    "season_week_powerindex.yaml": 37,
    "talentpicks.yaml": 12,
    # Fox Sports and KenPom tables from the same capture sweep. KenPom's column names are
    # hoopR's kp_* @return vocabulary in snake case, so it is the source to author them from.
    "native/fox_api": 496,
    "native/kenpom": 444,
    "native/nflpro": 30,  # the un-authorable NGS fields only (2026-10-07); was None
    "native/nba_stats": 312,
    "native/wnba_stats": 269,
    # Measured 2026-10-26 when the G League stem was built from the same parser-on-capture tables
    # (hoopR's column docs fill the rest, as for native/nba_stats).
    "native/nbagl_stats": 4442,  # no R package documents the G League; hoopR's NBA (and wehoop's WNBA) text was wrong on its columns, so blank
    "native/fotmob": 283,
    "native/uefa": 841,
    "native/sleeper": 459,
    # The autodoc buckets were re-based 2026-10-08 at their measured counts when Task 26 captured
    # returns tables for every hand-written DataFrame function (295 new tables, ~4,900 undescribed
    # cells after pass-through transformers inherit their input loader's descriptions via
    # generate._DESC_INHERITS). Lower a cap as columns are authored.
    # Buckets whose blank cells held CROSS-SPORT R text until the fallback was scoped to the
    # league's own sport (2026-10-07): the earlier zero residual counted baseballr's "Inning
    # number." on a jersey number and cfbfastR's SP+ on an NFL rating as coverage, so these are
    # re-based at the measured count, not loosened. Lower a cap as columns are authored.
    "autodoc/ahl": 2,
    "autodoc/ajhl": 1,
    "autodoc/cchl": 1,
    "autodoc/cfb": 765,
    "autodoc/chl": 2,
    "autodoc/global": 204,
    "autodoc/gojhl": 1,
    "autodoc/mbb": 622,
    "autodoc/mlb": 107,
    "autodoc/nba": 427,
    "autodoc/nfl": 1097,
    "autodoc/nhl": 208,
    "autodoc/nojhl": 1,
    "autodoc/odds": 69,
    "autodoc/ohl": 2,
    "autodoc/pwhl": 13,
    "autodoc/qmjhl": 4,
    "autodoc/sjhl": 1,
    "autodoc/ushl": 2,
    "autodoc/wbb": 573,
    "autodoc/whl": 1,
    "autodoc/wnba": 558,
    "autodoc/college_baseball": 38,
    "autodoc/f1": 17,
    "autodoc/mch": 7,
    "autodoc/soccer": 83,
    "autodoc/ufl": 24,
    "cdn_scoreboard.yaml": 36,
    "loader_schemas": 370,
    "native/cbs_napi": 42,
    "native/mlb_api": 10,
    "native/mls_api": 8,
    "native/nhl_api_web": 17,  # was 6: right_rail / club_stats(_season) built 2026-10-07 (dict -> frames)
    "native/nhl_edge": 74,  # was 12: eight *_top_10 boards captured 2026-10-07 (fastRhockey describes them)
    "native/nhl_records": 19,
    "native/nhl_stats_rest": 2,
    "native/nwsl_api": 9,
    "native/sports247_site_pages": 22,
    "native/yahoo_shangrila": 729,
    "scoreboard": 102,
    "scoreboard.yaml": 36,
    "standings": 8,
    "summary": 211,
    "team_roster": 24,
    "team_schedule": 2,
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
    # European hockey intake families, pinned to their measured counts at integration (2026-10-12).
    # Only KHL has blanks (re-pinned after review): columns no source documents and the data does not settle.
    "native/del_esports": 0,
    "native/icehl_esports": 0,
    "native/eihl_esports": 0,
    "native/metal_esports": 0,
    "native/elh": 0,
    "native/liiga": 0,
    "native/shl": 0,
    "native/ha": 0,
    "native/nationalleague": 0,
    "native/magnus": 0,
    "native/khl_rest": 2,
    "native/khl_webcaster": 11,
    "native/vhl": 0,
    # International basketball intake families: registered at 0 so a new family's blank columns fail from the first run;
    # pin each to its measured count at integration if any column is genuinely undocumented.
    "native/fiba_gdap": 0,
    "native/fiba_gamepage": 0,
    "native/genius": 0,
    "native/atrium": 0,
    "native/nbl_rosetta": 0,
    "native/feb": 0,
    "native/scb": 0,
    "native/legabasket": 0,
    "native/acb": 0,
    "native/ibba": 0,
    "native/ibba_segev": 0,
    "native/wjbl": 0,
    "native/vtb_infobasket": 0,
    "native/vtb_league": 0,
    "native/kbl": 0,
    "native/bleague": 0,
    "native/kzs": 0,
    "native/lkl": 0,
    "native/tbf": 0,
    "native/cebl": 0,
    "native/aba": 0,
    "native/easl": 0,
    "native/sportpress": 0,
    "native/tpbl": 0,
    "native/pleague": 0,
    "native/pleague_preciser": 0,
    "native/cba": 0,
    "native/sportality": 0,
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
                        "section": blk.get("section") or "",
                        "type": c.get("type", ""),
                        "blank": not (c.get("description") or "").strip(),
                        "siblings": [n for n in names if n != c.get("name", "")],
                    }
                )
    return out


def _uncovered(r: dict) -> bool:
    """A blank column with no manual-dict and no R-dict description."""
    if not r["blank"] or _manual_col_desc(r["schema"], r["col"], r["league"]):
        return False
    # A dict-of-frames table may key a column by ``<section>.<col>`` when its meaning differs by
    # frame (a player's ``game_id`` vs an official's); generate._autodoc_groups reads that key first.
    if r.get("section") and _manual_col_desc(r["schema"], f"{r['section']}.{r['col']}", r["league"]):
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


def report() -> str:
    """Each deferred bucket's uncovered count beside its cap, then the totals (``--report``)."""
    counts: dict[str, int] = {}
    for r in deferred_columns():
        counts[r["bucket"]] = counts.get(r["bucket"], 0) + 1
    lines = [f"{b}: {counts.get(b, 0)} / {cap}" for b, cap in sorted(_DEFERRED_BUCKETS.items())]
    below = sum(1 for b, cap in _DEFERRED_BUCKETS.items() if cap is not None and counts.get(b, 0) < cap)
    lines += [
        f"residual (outside the deferred buckets): {len(residual_columns())}",
        f"deferred cells: {sum(counts.values())}; buckets below their cap (lower it): {below}",
        f"deferred buckets: {len(_DEFERRED_BUCKETS)}",
    ]
    return "\n".join(lines)


def main() -> None:
    if "--report" in sys.argv[1:]:
        print(report())
        return
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
