"""Generate nba_stats/wnba_stats endpoint YAML + returns-schemas.

Endpoints and params come from the enriched canonical catalog (Plans 1-2). Column
names and types come from what ``parse_nba_stats_result_sets`` emits on the
committed real capture of each endpoint (``capture_path``), so a table never names a
column the parser does not produce. Idempotent: same inputs -> byte-identical output.

Run: python tools/codegen/gen_nba_stats.py
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List

import polars as pl
import yaml

from sportsdataverse.nba.nba_stats_parsers import _result_sets, _to_frame

ROOT = Path(__file__).resolve().parents[2]
CAT = json.loads((ROOT / "tools/codegen/inputs/nba_canonical_catalog.json").read_text(encoding="utf-8"))
_CAT_BY_SLUG = {e["slug"]: e for e in CAT["endpoints"] if e["family"] == "stats"}

STEMS = {
    "nba_stats": {"host": "https://stats.nba.com", "league": "nba", "league_id": "00", "default_league": "00"},
    "wnba_stats": {"host": "https://stats.wnba.com", "league": "wnba", "league_id": "10", "default_league": "10"},
}
# catalog column type -> R-style returns-schema type
_DTYPE = {
    "integer": "integer",
    "number": "numeric",
    "string": "character",
    "boolean": "logical",
    "unknown": "character",
}
# only endpoints with committed live captures and no upstream deprecation marker
# are codegen-ready (drop untested/barren/dead/deprecated).
_APPLICABLE = ("live",)

# Each endpoint's schema is derived from one committed real capture: by default the
# 2026-08 sweep sample vendored by ``vendor_captures.py``. These use the earlier
# pilot capture instead -- the sweep sample is empty (video) or was taken with
# non-default params (homepagev2 PlayerOrTeam=Player; the wrapper default is Team).
CAPTURE_OVERRIDES = {
    ("nba_stats", "videodetailsasset"): "tests/nba/fixtures/cap_videodetailsasset_nba.json",
    ("nba_stats", "videoevents"): "tests/nba/fixtures/cap_videoevents_nba.json",
    ("nba_stats", "videoeventsasset"): "tests/nba/fixtures/cap_videoeventsasset_nba.json",
    ("wnba_stats", "homepagev2"): "tests/nba/fixtures/cap_homepagev2_wnba.json",
}
_POLARS_TO_R = {"Int64": "integer", "Float64": "numeric", "Boolean": "logical"}


# What actually propagates out of nba_stats_runtime._get. A non-200 / blank /
# undecodable body is NOT an exception there -- it returns {} and the parser
# yields a zero-row frame -- so only these two reach the caller.
_STATS_RAISES = [
    "ImportError: ``curl_cffi`` is not installed. stats.nba.com/stats.wnba.com "
    "TLS-fingerprint-block plain ``requests``, so the live transport requires it "
    "(``pip install curl_cffi``, or ``pip install sportsdataverse[all]``).",
    "curl_cffi.requests.errors.RequestsError: Connection-level failure (timeout, "
    "reset) raised by the transport once ``SDV_PY_NBA_STATS_RETRIES`` retries are "
    "exhausted. A non-200 or empty body does NOT raise -- ``_get`` returns ``{}`` "
    "and the parser yields a zero-row frame.",
]
_SIBLING = {"nba_stats": ("wnba_stats", "wnba", "10"), "wnba_stats": ("nba_stats", "nba", "00")}
_R_COMPANION = {
    "nba_stats": ("hoopR", "https://hoopR.sportsdataverse.org", "R sister package for the NBA stats API"),
    "wnba_stats": ("wehoop", "https://wehoop.sportsdataverse.org", "R sister package for the WNBA stats API"),
}
# Endpoints that opt into the extended docstring contract (Raises + See Also +
# an import-bearing Example). Declared PER ENDPOINT: the family-level
# ``docstring:`` block would rewrite all ~112 wrappers in this family.
_DOCSTRING_ENDPOINTS = ("drafthistory",)
# Curated example args for those endpoints, so the Example block is a real,
# copy-pasteable call rather than the whole draft history.
_EXAMPLE_ARGS = {"drafthistory": {"season_year_nullable": "2024"}}
# Parsed-return phrase override for endpoints whose parser returns a dict of
# frames rather than a single DataFrame (video envelope: {Meta:{videoUrls},playlist}).
_VIDEO_PARSED_DOC = (
    "A dict of two DataFrames keyed ``videoUrls`` (clip URLs, durations, "
    "thumbnails) and ``playlist`` (per-event game metadata)"
)
_PARSED_DOC_ENDPOINTS = {
    "videodetailsasset": _VIDEO_PARSED_DOC,
    "videoevents": _VIDEO_PARSED_DOC,
    "videoeventsasset": _VIDEO_PARSED_DOC,
}


def capture_path(stem: str, slug: str) -> Path:
    """The committed real capture an endpoint's returns-schema is derived from."""
    return ROOT / CAPTURE_OVERRIDES.get((stem, slug), f"tests/fixtures/{stem}/endpoints/{slug}.json")


def parsed_sets(stem: str, slug: str) -> Dict[str, pl.DataFrame]:
    """``{result-set name: frame}`` exactly as ``parse_nba_stats_result_sets`` builds it."""
    raw = json.loads(capture_path(stem, slug).read_text(encoding="utf-8"))
    return {rs.get("name", f"set_{i}"): _to_frame(rs) for i, rs in enumerate(_result_sets(raw))}


def observed_type(df: pl.DataFrame, col: str) -> str | None:
    """R-style type of ``col`` as parsed; ``None`` when the capture carries no signal
    (zero rows -- the parser types an empty set all-Utf8 -- or an all-null column)."""
    if df.height == 0 or df[col].null_count() == df.height:
        return None
    return _POLARS_TO_R.get(str(df.schema[col]), "character")


def _alnum(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", name.lower())


def _column_type(slug: str, section: str, col: str, df: pl.DataFrame, sibling: pl.DataFrame | None) -> str:
    """Parsed type; else the other league's capture; else the catalog column whose
    name matches ignoring case/punctuation (``fg3m`` == ``fg3_m``); else character."""
    t = observed_type(df, col)
    if t is None and sibling is not None and col in sibling.columns:
        t = observed_type(sibling, col)
    if t is None:
        cat = {_alnum(c["name"]): c["type"] for c in _CAT_BY_SLUG[slug]["result_sets"].get(section, [])}
        t = _DTYPE[cat[_alnum(col)]] if _alnum(col) in cat else "character"
    return t


def _schema(stem: str, slug: str, sets: Dict[str, pl.DataFrame], sibling: Dict[str, pl.DataFrame]) -> Dict[str, Any]:
    blocks = [
        {
            "section": name,
            "columns": [
                {"name": c, "type": _column_type(slug, name, c, df, sibling.get(name)), "description": ""}
                for c in df.columns
            ],
        }
        for name, df in sets.items()
    ]
    if not any(b["columns"] for b in blocks):
        rel = capture_path(stem, slug).relative_to(ROOT).as_posix()
        return {
            "schema": slug,
            "kind": "dataframe",
            "columns": [],
            "unverified": f"parse_nba_stats_result_sets emits no columns for the committed capture {rel}",
        }
    if len(blocks) == 1:
        return {"schema": slug, "kind": "dataframe", "columns": blocks[0]["columns"]}
    return {"schema": slug, "kind": "frames", "frames": blocks}


def _docstring_extras(slug: str, stem: str, sets: Dict[str, pl.DataFrame]) -> Dict[str, Any]:
    """Per-endpoint ``docstring:`` extras consumed by ``generate._build_docstring``."""
    if slug in _PARSED_DOC_ENDPOINTS:
        return {"parsed_doc": _PARSED_DOC_ENDPOINTS[slug]}
    if len(sets) > 1:  # the parser returns {result-set name: frame}, not one frame
        names = ", ".join(f"``{n}``" for n in sets)
        return {"parsed_doc": f"A dict of DataFrames keyed by result-set name ({names})"}
    if slug not in _DOCSTRING_ENDPOINTS:
        return {}
    sib_stem, sib_league, sib_league_id = _SIBLING[stem]
    r_name, r_url, r_note = _R_COMPANION[stem]
    return {
        "example_import": True,
        # sportsdataverse.{nba,wnba} does NOT re-export these wrappers -- they are
        # reachable only through their own module, so the Example imports from there.
        "example_import_from": f"sportsdataverse.{STEMS[stem]['league']}.{stem}",
        "raises": list(_STATS_RAISES),
        "see_also": [
            {
                "name": f"{sib_stem}_{slug}",
                "url": f"https://sportsdataverse-py.sportsdataverse.org/docs/{sib_league}/reference/{sib_stem}",
                "note": f"the {sib_league.upper()} sibling wrapper (same resultSets "
                f"envelope, LeagueID={sib_league_id})",
            },
            {"name": r_name, "url": r_url, "note": r_note},
            {"name": "nba_api", "url": "https://github.com/swar/nba_api", "note": "Python alternative client"},
        ],
    }


def _stats_eps(league_id: str) -> List[dict]:
    return sorted(
        (
            e
            for e in CAT["endpoints"]
            if e["family"] == "stats"
            and e["league_applicability"].get(league_id) in _APPLICABLE
            # A capture-confirmed-live endpoint ships regardless of source
            # opinions (hoopR/nba_api/wehoop "deprecated" flags are opinions,
            # not liveness -- the 2026-08 probe sweep found e.g. playbyplayv2,
            # boxscoresummaryv3, homepagev2 live under those flags). Only
            # source-deprecated endpoints the sweep did NOT confirm live drop.
            and not (
                e.get("deprecation", {}).get("capture") != "live"
                and any(status == "deprecated" for status in e.get("deprecation", {}).values())
            )
        ),
        key=lambda e: e["slug"],
    )


# RULE: a Season / SeasonYear param gets the latest season that has data, at call time, via the
# runtime ``season_latest_with_data`` transform (the per-league, per-endpoint and playoff rollover
# months live in ``nba_stats_runtime._FIRST_ROWS``) iff hoopR (nba_stats) / wehoop (wnba_stats)
# gives that endpoint's season a default season in its R signature, i.e. a call such as
# ``year_to_season(most_recent_nba_season() - 1)`` / ``most_recent_wnba_season()``. An R default of
# "" / NULL (no call in the catalog) keeps the API's own default. An endpoint the league's package
# does not wrap borrows the other package's call (see ``_endpoint_entry``). There is no exemption
# list: an audit of hoopR + wehoop on 2026-10-05 found a season-call default for every endpoint a
# 2026-10-05 sweep had exempted for answering without a season (wehoop's deprecated
# homepageleaders / homepagev2 / leaderstiles included), and the three it does not wrap
# (leaguedashptdefend, scheduleleaguev2, scheduleleaguev2int) are per-season. Without a season
# those 55 answered with EVERY season summed (leaguedashteamstats: SuperSonics and Bullets rows,
# GP up to 2,395), and drafthistory / the finders with all of history, as hoopR / wehoop never ask.
# Re-check when the catalog grows: tests/nba/test_nba_stats_season_defaults.py::
# test_live_defaults_return_rows covers a sample live.
_SEASON_KEYS = ("Season", "SeasonYear")
# The one season call the catalog lost: it credits scheduleleaguev2 to nba_api only, but hoopR's
# nbagl_schedule() calls it with season = year_to_season(most_recent_nba_season() - 1). wehoop does
# not wrap it (wnba_schedule() reads the CDN), so the WNBA wrapper borrows the call like any other.
_SEASON_CALL_NOT_IN_CATALOG = {"scheduleleaguev2": "=year_to_season(most_recent_nba_season() - 1)"}
# The documented example call passes a fixed season: the default itself depends on today's date,
# and a docs page rendered from it would drift (and fail generate.py --check) at every rollover.
_SEASON_EXAMPLE = {"nba_stats": "2024-25", "wnba_stats": "2024"}
# hoopR defaults that fail once a season is sent. Measured 2026-10-05: playerdashptshots with
# PlayerID 2544, Season 2025-26 and hoopR's TeamID "0" answered HTTP 500 (twice); with the player's
# own team, the Lakers, 27 rows. ponytail: tied to the default player's team; re-pick if he moves.
_DEFAULT_OVERRIDES = {("nba_stats", "playerdashptshots", "TeamID"): "1610612747"}
_SEASON_DOC = {
    # Never name the date helpers here: generate.py treats a whole-word mention on a reference
    # row as "already documented" and drops the helper's own section from the docs.
    "nba_stats": (
        "Season label, e.g. ``2024-25``. Defaults at call time to the latest season that has rows: "
        "an NBA season from the November after its late-October tip-off (``2025-26`` until October "
        "2026), a G League season from the January after, a Summer League from its August (July "
        "2026's is ``2026-27``), a draft combine from June, a draft (``drafthistory``, a year) from "
        "July, and with season type ``Playoffs`` / ``PlayIn`` (or ``commonplayoffseries``) a season "
        "from the May its playoffs start. A month table cannot follow a lockout or pandemic calendar "
        "(1998-99, 2011-12, 2020-21): pass a season then. Without one stats.nba.com answers an empty "
        "HTTP 500 or every season summed."
    ),
    "wnba_stats": (
        "Season year, e.g. ``2024``. Defaults at call time to the latest WNBA season that has rows: "
        "the current year from June (``2026`` from June 2026, ``2025`` before), a draft "
        "(``drafthistory``) from May, and with season type ``Playoffs`` (or ``commonplayoffseries``) "
        "from October. A month table cannot follow a lockout or pandemic calendar: pass a season "
        "then. Without one stats.wnba.com answers an empty HTTP 500 or every season summed."
    ),
}


_LEAGUE_SPECIFIC = re.compile(r"(?i)(id(s|list)?\d*|season(year)?)$")


def _league_default(p: dict, league: str) -> Any:
    """This stem's own R default: hoopR's for nba_stats (``"00"``), wehoop's for wnba_stats (``"10"``).

    The catalog's legacy ``default`` let wehoop's value overwrite hoopR's, so on its own it can name
    the OTHER league's entity (nba teaminfocommon defaulted to a WNBA team and 500s). It is used only
    when neither package set a value for this league and it did not come from the other league.
    A call default arrives as ``"=<R expression>"``.
    """
    per_league = p.get("league_defaults") or {}
    if league in per_league:
        return per_league[league]
    other = "10" if league == "00" else "00"
    legacy = p.get("default")
    # Only an entity id or a season is league-specific; '' / '0' / 'Totals' are neutral.
    leaked = (
        other in per_league
        and legacy == per_league[other]
        and legacy not in ("", "0")
        and _LEAGUE_SPECIFIC.search(p["query_key"]) is not None
    )
    # Measured 2026-10-05: no default beats a stand-in; wnba playerdashptshotdefend answers 1,326
    # league-wide rows without a PlayerID and HTTP 500 for wehoop's usual example player.
    return None if leaked else legacy


def _clean_default(name: str, query_key: str, default: Any) -> Any:
    """Nullable entity-id filters must default to UNFILTERED, never to an entity.

    The catalog's defaults are mined from hoopR/wehoop roxygen examples, and for
    the league-wide log/detail endpoints those examples pin a specific entity:
    ``teamgamelogs.team_id_nullable`` defaulted to a G-League team id, so every
    NBA call silently filtered a league-wide endpoint to a team from ANOTHER
    league and returned a valid zero-row envelope -- while the same default on
    WNBA (where the id exists) silently narrowed the "league" dataset to one
    team. A default that changes which league answers is a bug, not an example.

    The API's own convention (from its 400 error text) is "pass 0 for all
    teams"; player filters accept empty. Only NULLABLE filters are touched --
    entity-KEYED endpoints (teamgamelog, playerprofilev2, the dashboards) keep
    their example defaults, since they cannot answer without an entity at all.
    """
    if "nullable" in name and ("team_id" in name or "player_id" in name):
        return "0" if "team_id" in name else ""
    return default


def _endpoint_entry(
    ep: dict, stem: str, default_league: str, parser_name: str, sets: Dict[str, pl.DataFrame]
) -> Dict[str, Any]:
    extra: List[Dict[str, Any]] = []
    has_league = False
    for p in ep["params"]:
        if p["query_key"] == "LeagueID":
            # normalize the routing param to a clean, uniform python name + pin the stem default
            extra.append({"name": "league_id", "query_key": "LeagueID", "type": "str", "default": default_league})
            has_league = True
        else:
            default = _league_default(p, default_league)
            if default is None and p["query_key"] in _SEASON_KEYS:
                # one package wraps the endpoint without a season default; the other's season call
                # still says the API wants one (wnba playerdashptshotdefend: only hoopR has it)
                per_league = p.get("league_defaults") or {}
                default = next(
                    (v for _, v in sorted(per_league.items()) if str(v).startswith("=")),
                    _SEASON_CALL_NOT_IN_CATALOG.get(ep["slug"]),
                )
            param: Dict[str, Any] = {
                # the catalog spells GameID ``gameid`` on boxscoresummaryv3/boxscorehustlev2;
                # every per-game wrapper takes ``game_id`` (#640)
                "name": "game_id" if p["name"] == "gameid" else p["name"],
                "query_key": p["query_key"],
                "type": "str",
            }
            if isinstance(default, str) and default.startswith("="):
                if p["query_key"] in _SEASON_KEYS:
                    # the latest season with rows, re-dated per league/endpoint by the runtime
                    param["transform"] = "season_latest_with_data"
                    param["description"] = _SEASON_DOC[stem]
                default = None  # an R call has no literal; the transform (if any) resolves it per call
            param["default"] = _DEFAULT_OVERRIDES.get(
                (stem, ep["slug"], p["query_key"]), _clean_default(p["name"], p["query_key"], default)
            )
            extra.append(param)
    example_args: Dict[str, Any] = {"league_id": default_league} if has_league else {}
    example_args.update(
        {p["name"]: _SEASON_EXAMPLE[stem] for p in extra if p.get("transform") == "season_latest_with_data"}
    )
    example_args.update(_EXAMPLE_ARGS.get(ep["slug"], {}))
    entry: Dict[str, Any] = {
        "short": ep["slug"],
        "summary": f"GET /stats/{ep['slug']}",
        "path": f"/stats/{ep['slug']}",
        "extra_params": extra,
        "parser": parser_name,
        "returns_schema": f"native/{stem}/{ep['slug']}",
        "example_args": example_args,
    }
    extras = _docstring_extras(ep["slug"], stem, sets)
    if extras:
        entry["docstring"] = extras
    return entry


def _write_yaml(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as fh:
        yaml.safe_dump(obj, fh, sort_keys=True, default_flow_style=False, allow_unicode=True)


def _clean_generated_schema_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    for schema_path in path.glob("*.yaml"):
        schema_path.unlink()


def main() -> None:
    eps = {stem: _stats_eps(cfg["league_id"]) for stem, cfg in STEMS.items()}
    sets = {stem: {e["slug"]: parsed_sets(stem, e["slug"]) for e in eps[stem]} for stem in STEMS}
    for stem, cfg in STEMS.items():
        parser_name = f"parse_{cfg['league']}_stats_result_sets"
        doc = {
            "api": stem,
            "host": cfg["host"],
            "name_pattern": f"{stem}_{{short}}",
            "module": stem,
            "parser_module": f"{cfg['league']}.{stem}_parsers",
            "getter_module": f"sportsdataverse.{cfg['league']}.{stem}_runtime",
            "endpoints": [
                _endpoint_entry(e, stem, cfg["default_league"], parser_name, sets[stem][e["slug"]]) for e in eps[stem]
            ],
        }
        _write_yaml(ROOT / f"tools/codegen/endpoints/{stem}.yaml", doc)
        schema_dir = ROOT / f"tools/codegen/schemas/native/{stem}"
        _clean_generated_schema_dir(schema_dir)
        sibling = sets[_SIBLING[stem][0]]
        for slug, ep_sets in sets[stem].items():
            _write_yaml(schema_dir / f"{slug}.yaml", _schema(stem, slug, ep_sets, sibling.get(slug, {})))
        print(f"{stem}: {len(eps[stem])} endpoints")


if __name__ == "__main__":
    main()
