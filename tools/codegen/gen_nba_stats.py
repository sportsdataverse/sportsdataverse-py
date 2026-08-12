"""Generate nba_stats/wnba_stats endpoint YAML + returns-schemas from the enriched
canonical catalog (Plans 1-2). Idempotent: same catalog -> byte-identical output.

Run: python tools/codegen/gen_nba_stats.py
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import yaml

ROOT = Path(__file__).resolve().parents[2]
CAT = json.loads((ROOT / "tools/codegen/inputs/nba_canonical_catalog.json").read_text(encoding="utf-8"))

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

# Representative result-set used for the generated returns-table schema. The
# runtime parser still exposes every result set; this only controls which table
# shape appears in reference docs for multi-set endpoints.
_REPRESENTATIVE_RESULT_SETS = {
    "boxscoreadvancedv2": "PlayerStats",
    "boxscoreadvancedv3": "PlayerStats",
    "boxscoredefensivev2": "PlayerStats",
    "boxscorefourfactorsv2": "sqlPlayersFourFactors",
    "boxscorefourfactorsv3": "PlayerStats",
    "boxscorehustlev2": "PlayerStats",
    "boxscoremiscv2": "sqlPlayersMisc",
    "boxscoremiscv3": "PlayerStats",
    "boxscoreplayertrackv3": "PlayerStats",
    "boxscorescoringv2": "sqlPlayersScoring",
    "boxscorescoringv3": "PlayerStats",
    "boxscoresummaryv2": "LineScore",
    "boxscoresummaryv3": "OtherStats",
    "boxscoretraditionalv2": "PlayerStats",
    "boxscoretraditionalv3": "PlayerStats",
    "boxscoreusagev2": "sqlPlayersUsage",
    "boxscoreusagev3": "PlayerStats",
    "commonplayerinfo": "CommonPlayerInfo",
    "commonteamroster": "CommonTeamRoster",
    "cumestatsplayer": "TotalPlayerStats",
    "cumestatsteam": "GameByGameStats",
    "franchisehistory": "FranchiseHistory",
    "gamerotation": "HomeTeam",
    "homepageleaders": "HomePageLeaders",
    "leaderstiles": "LeadersTiles",
    "playbyplay": "PlayByPlay",
    "playbyplayv2": "PlayByPlay",
    "playbyplayv3": "PlayByPlay",
    "playercareerstats": "SeasonTotalsRegularSeason",
    "playerprofilev2": "SeasonTotalsRegularSeason",
    "playoffpicture": "EastConfStandings",
    "scheduleleaguev2": "SeasonGames",
    "scheduleleaguev2int": "SeasonGames",
    "scoreboardv2": "LineScore",
    "shotchartdetail": "Shot_Chart_Detail",
    "shotchartlineupdetail": "ShotChartLineupDetail",
    "teamdetails": "TeamBackground",
    "teamplayerdashboard": "PlayersSeasonTotals",
    "winprobabilitypbp": "WinProbPBP",
}


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


def _docstring_extras(slug: str, stem: str) -> Dict[str, Any]:
    """Per-endpoint ``docstring:`` extras consumed by ``generate._build_docstring``."""
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
            and not any(status == "deprecated" for status in e.get("deprecation", {}).values())
        ),
        key=lambda e: e["slug"],
    )


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


def _endpoint_entry(ep: dict, stem: str, default_league: str, parser_name: str) -> Dict[str, Any]:
    extra: List[Dict[str, Any]] = []
    has_league = False
    for p in ep["params"]:
        if p["query_key"] == "LeagueID":
            # normalize the routing param to a clean, uniform python name + pin the stem default
            extra.append({"name": "league_id", "query_key": "LeagueID", "type": "str", "default": default_league})
            has_league = True
        else:
            extra.append(
                {
                    "name": p["name"],
                    "query_key": p["query_key"],
                    "type": "str",
                    "default": _clean_default(p["name"], p["query_key"], p.get("default")),
                }
            )
    example_args: Dict[str, Any] = {"league_id": default_league} if has_league else {}
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
    extras = _docstring_extras(ep["slug"], stem)
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


def _representative_result_set(ep: dict) -> str | None:
    result_sets = ep.get("result_sets") or {}
    if not result_sets:
        return None
    preferred = _REPRESENTATIVE_RESULT_SETS.get(ep["slug"])
    if preferred in result_sets:
        return preferred
    return max(result_sets, key=lambda name: (len(result_sets[name]), name))


def main() -> None:
    for stem, cfg in STEMS.items():
        eps = _stats_eps(cfg["league_id"])
        parser_name = f"parse_{cfg['league']}_stats_result_sets"
        doc = {
            "api": stem,
            "host": cfg["host"],
            "name_pattern": f"{stem}_{{short}}",
            "module": stem,
            "parser_module": f"{cfg['league']}.{stem}_parsers",
            "getter_module": f"sportsdataverse.{cfg['league']}.{stem}_runtime",
            "endpoints": [_endpoint_entry(e, stem, cfg["default_league"], parser_name) for e in eps],
        }
        _write_yaml(ROOT / f"tools/codegen/endpoints/{stem}.yaml", doc)
        schema_dir = ROOT / f"tools/codegen/schemas/native/{stem}"
        _clean_generated_schema_dir(schema_dir)
        for e in eps:
            primary = _representative_result_set(e)
            cols = e["result_sets"].get(primary, []) if primary else []
            schema = {
                "schema": e["slug"],
                "kind": "dataframe",
                "columns": [
                    {
                        "name": c["name"],
                        "type": _DTYPE.get(c["type"], "character"),
                        "description": c.get("description", ""),
                    }
                    for c in cols
                ],
            }
            _write_yaml(schema_dir / f"{e['slug']}.yaml", schema)
        print(f"{stem}: {len(eps)} endpoints")


if __name__ == "__main__":
    main()
