"""Generate the ``espn_fantasy`` flat-API endpoint YAML + returns-schemas (ESPN Fantasy read API, public data only).

Idempotent: same tables + same fixtures -> byte-identical output. Reads only committed files:
:data:`ENDPOINTS` here and the fixtures under ``tests/fixtures/espn_fantasy`` (byte copies of the
redacted reference captures), and emits:

* ``tools/codegen/endpoints/espn_fantasy.yaml`` -- one wrapper per tidy table. Several wrappers share
  one route and differ by their fixed ``view``.
* ``tools/codegen/schemas/native/espn_fantasy/<short>.yaml`` -- one returns-schema per wrapper, the
  documented columns of its parser (``espn_fantasy_parsers.SCHEMAS``), schema key
  ``espn_fantasy_<short>``. The run first parses each wrapper's fixture and fails when the frame's
  columns or dtypes differ from the documented ones. Descriptions live in
  ``manual_column_descriptions.yaml``; every ``description`` here is ``''``.

Run: ``python tools/codegen/gen_espn_fantasy.py``
"""

from __future__ import annotations

import copy
import json
from typing import Any, Dict, List

import polars as pl
from gen_soccer_common import ROOT, rewrite_schema_dir, write_yaml

from sportsdataverse.providers.espn_fantasy import espn_fantasy_parsers as P

STEM = "espn_fantasy"
HOST = "https://lm-api-reads.fantasy.espn.com/apis/v3"
NEWS_HOST = "https://site.api.espn.com/apis/fantasy/v3"
FIXTURES = ROOT / "tests" / "fixtures" / STEM
FILTER = "X-Fantasy-Filter"
GAMES = ["ffl", "fba", "flb", "fhl", "wfba"]

GAME = {
    "name": "game",
    "type": "str",
    "required": True,
    "choices": GAMES,
    "description": "Fantasy game: ffl (football), fba (men's basketball), flb (baseball), fhl (hockey), "
    "wfba (women's basketball).",
}
SEASON = {
    "name": "season",
    "type": "int",
    "required": True,
    "description": "ESPN's season id: the START year for ffl (2026 = the fall 2026 season), the END year for "
    "fba / fhl (2026-27 = 2027); flb and wfba fit in one year. espn_fantasy_games gives each game's current one.",
}
LEAGUE = {
    "name": "league_id",
    "type": "int",
    "required": True,
    "description": "Public league id (league_id in espn_fantasy_league_directory). A private league answers "
    "HTTP 401 without its owner's cookies and is out of scope.",
}
PERIOD = {
    "name": "scoring_period_id",
    "query_key": "scoringPeriodId",
    "type": "int",
    "description": "Scoring period: the NFL week for ffl, one day of the season for the other games. "
    "Omitted = the league's current period.",
}


def _filter(default: str | None, note: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "name": "fantasy_filter",
        "query_key": FILTER,
        "type": "str",
        "description": f"JSON object sent as the {FILTER} request header. {note} A limit needs a sort, or the "
        "host answers 400.",
    }
    if default is not None:
        out["default"] = default
    return out


POOL_FILTER = '{"players":{"limit":50,"sortPercOwned":{"sortPriority":1,"sortAsc":false}}}'
ACTIVE_FILTER = '{"filterActive":{"value":true}}'

SEASON_PATH = "/games/{game}/seasons/{season}"
LEAGUE_PATH = SEASON_PATH + "/segments/0/leagues/{league_id}"
EX = {"game": "fba", "season": 2027}
EX_LEAGUE = {**EX, "league_id": 1535462463}

#: short -> wrapper definition. ``fixture`` is the body under tests/fixtures/espn_fantasy that the
#: parser is checked against; ``view`` is the fixed ``view=`` the wrapper always sends.
ENDPOINTS: Dict[str, Dict[str, Any]] = {
    "games": {
        "path": "/games",
        "parser": "parse_espn_fantasy_games",
        "fixture": "games.json",
        "summary": "Every ESPN fantasy game (ffl, fba, flb, fhl, wfba) with its current season and scoring period.",
        "path_params": [],
        "example_args": {},
    },
    "seasons": {
        "path": "/games/{game}/seasons",
        "parser": "parse_espn_fantasy_seasons",
        "fixture": "ffl/seasons.json",
        "summary": "Every season of one fantasy game, newest first.",
        "path_params": [GAME],
        "example_args": {"game": "ffl"},
    },
    "pro_teams": {
        "path": SEASON_PATH,
        "view": "proTeamSchedules_wl",
        "parser": "parse_espn_fantasy_pro_teams",
        "fixture": "ffl/pro_team_schedules.json",
        "summary": "The pro teams of a fantasy season, with bye weeks (football) and the free-agent pool (id 0).",
        "path_params": [GAME, SEASON],
        "example_args": {"game": "ffl", "season": 2026},
    },
    "pro_schedule": {
        "path": SEASON_PATH,
        "view": "proTeamSchedules_wl",
        "parser": "parse_espn_fantasy_pro_schedule",
        "fixture": "fba/pro_team_schedules.json",
        "summary": "The pro-game schedule of a fantasy season: one row per pro game, keyed to its scoring period.",
        "path_params": [GAME, SEASON],
        "example_args": EX,
    },
    "players": {
        "path": SEASON_PATH + "/players",
        "view": "players_wl",
        "parser": "parse_espn_fantasy_players",
        "fixture": "fba/players.json",
        "summary": "The player universe of a fantasy season: ids, names, pro team, position and ownership.",
        "path_params": [GAME, SEASON],
        "extra_params": [_filter(ACTIVE_FILTER, "The default keeps active players only; pass '{}' for every player.")],
        "example_args": EX,
    },
    "league_directory": {
        "path": SEASON_PATH + "/leaguedirectory/{league_sub_type}",
        "parser": "parse_espn_fantasy_league_directory",
        "fixture": "fba/league_directory.json",
        "summary": "ESPN's public-league directory: the public leagues anyone can join, readable by league id.",
        "path_params": [
            GAME,
            SEASON,
            {
                "name": "league_sub_type",
                "type": "str",
                "required": False,
                "default": "DRAFT_LOBBY",
                "choices": ["DRAFT_LOBBY", "MOCKDRAFT_LOBBY"],
                "description": "DRAFT_LOBBY: ESPN-run public leagues with an upcoming or running draft. "
                "MOCKDRAFT_LOBBY: mock-draft rooms.",
            },
        ],
        "example_args": EX,
    },
    "default_scoring": {
        "path": SEASON_PATH + "/segments/0/leaguedefaults/{league_default_id}",
        "view": "mSettings",
        "parser": "parse_espn_fantasy_scoring",
        "fixture": "ffl/default_settings.json",
        "summary": "Scoring items of one of ESPN's default league-settings templates (points per stat).",
        "path_params": [
            GAME,
            SEASON,
            {
                "name": "league_default_id",
                "type": "int",
                "required": True,
                "description": "Default-settings template id (league_default_id in espn_fantasy_league_directory).",
            },
        ],
        "example_args": {"game": "ffl", "season": 2026, "league_default_id": 1},
    },
    "player_news": {
        "path": "/games/{game}/news/players",
        "host": NEWS_HOST,
        "parser": "parse_espn_fantasy_player_news",
        "fixture": "fba/player_news.json",
        "summary": "Fantasy news items for one player (site.api.espn.com).",
        "path_params": [GAME],
        "extra_params": [
            {
                "name": "player_id",
                "query_key": "playerId",
                "type": "int",
                "required": True,
                "description": "ESPN athlete id (player_id in espn_fantasy_players).",
            }
        ],
        "example_args": {"game": "fba", "player_id": 5104157},
    },
    "league_teams": {
        "path": LEAGUE_PATH,
        "view": "mTeam",
        "parser": "parse_espn_fantasy_league_teams",
        "fixture": "fba/league_mTeam.json",
        "summary": "The teams of a public league: overall record, points, ranks and transaction counters.",
        "path_params": [GAME, SEASON, LEAGUE],
        "example_args": EX_LEAGUE,
    },
    "league_rosters": {
        "path": LEAGUE_PATH,
        "view": "mRoster",
        "parser": "parse_espn_fantasy_league_rosters",
        "fixture": "fba/league_mRoster.json",
        "summary": "The rosters of a public league: one row per rostered player, with lineup slot and acquisition.",
        "path_params": [GAME, SEASON, LEAGUE],
        "example_args": EX_LEAGUE,
    },
    "league_matchups": {
        "path": LEAGUE_PATH,
        "view": "mMatchupScore",
        "parser": "parse_espn_fantasy_league_matchups",
        "fixture": "fba/league_mMatchupScore.json",
        "summary": "Every head-to-head matchup of a public league's season, with points and winner.",
        "path_params": [GAME, SEASON, LEAGUE],
        "example_args": EX_LEAGUE,
    },
    "league_boxscores": {
        "path": LEAGUE_PATH,
        "view": "mBoxscore",
        "parser": "parse_espn_fantasy_league_boxscores",
        "fixture": "fba/league_mBoxscore.json",
        "summary": "Box scores of a public league for one scoring period: each player's fantasy points by matchup.",
        "path_params": [GAME, SEASON, LEAGUE],
        "extra_params": [PERIOD],
        "example_args": {**EX_LEAGUE, "scoring_period_id": 37},
    },
    "league_scoring": {
        "path": LEAGUE_PATH,
        "view": "mSettings",
        "parser": "parse_espn_fantasy_scoring",
        "fixture": "fba/league_mSettings.json",
        "summary": "Scoring items of a public league (points per stat).",
        "path_params": [GAME, SEASON, LEAGUE],
        "example_args": EX_LEAGUE,
    },
    "league_draft": {
        "path": LEAGUE_PATH,
        "view": "mDraftDetail",
        "parser": "parse_espn_fantasy_league_draft",
        "fixture": "fba/league_mDraftDetail.json",
        "summary": "The draft of a public league: one row per pick, in pick order.",
        "path_params": [GAME, SEASON, LEAGUE],
        "example_args": EX_LEAGUE,
    },
    "league_transactions": {
        "path": LEAGUE_PATH,
        "view": "mTransactions2",
        "parser": "parse_espn_fantasy_league_transactions",
        "fixture": "fba/league_mTransactions2.json",
        "summary": "Recent transactions of a public league (adds, drops, trades, lineup moves), one row per item.",
        "path_params": [GAME, SEASON, LEAGUE],
        "extra_params": [
            {
                **PERIOD,
                "description": "Scoring period. Honoured for past periods; a recent or omitted period returns the "
                "league's current activity (pending and future moves). Not a complete ledger.",
            }
        ],
        "example_args": EX_LEAGUE,
    },
    "player_pool": {
        "path": LEAGUE_PATH,
        "view": "kona_player_info",
        "parser": "parse_espn_fantasy_player_pool",
        "fixture": "fba/league_kona_player_info.json",
        "summary": "A public league's player pool: free agents, waivers and rostered players with ownership.",
        "path_params": [GAME, SEASON, LEAGUE],
        "extra_params": [
            _filter(
                POOL_FILTER,
                'The default returns the 50 most-owned players; add "filterStatus":{"value":["FREEAGENT"]} '
                "inside players for free agents only.",
            )
        ],
        "example_args": EX_LEAGUE,
    },
}

_TYPE = {pl.Int64: "integer", pl.Float64: "numeric", pl.Boolean: "logical", pl.String: "character"}


def _type(dtype: Any) -> str:
    return "datetime" if isinstance(dtype, pl.Datetime) else _TYPE[dtype]


def _table(parser: str) -> str:
    return parser.removeprefix("parse_espn_fantasy_")  # parse_espn_fantasy_<table>


def _check(short: str, d: Dict[str, Any]) -> None:
    """The fixture parses to exactly the documented columns and dtypes, with at least one row."""
    df = getattr(P, d["parser"])(json.loads((FIXTURES / d["fixture"]).read_text(encoding="utf-8")))
    want = P.SCHEMAS[_table(d["parser"])]
    if dict(df.schema) != want or df.height == 0:
        raise SystemExit(f"espn_fantasy/{short}: {d['fixture']} parses to {dict(df.schema)} ({df.height} rows)")


def _entry(short: str, d: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "short": short,
        "summary": d["summary"],
        "path": d["path"],
        "parser": d["parser"],
        "returns_schema": f"native/{STEM}/{short}",
        "example_args": d["example_args"],
    }
    if d.get("host"):
        out["host"] = d["host"]
    if d["path_params"]:
        out["path_params"] = d["path_params"]
    if d.get("extra_params"):
        out["extra_params"] = d["extra_params"]
    if d.get("view"):
        out["fixed_params"] = {"view": d["view"]}
    return out


def main() -> None:
    for short, d in ENDPOINTS.items():
        _check(short, d)
    doc = {
        "api": STEM,
        "host": HOST,
        "name_pattern": "espn_fantasy_{short}",
        "module": STEM,
        "parser_module": "providers.espn_fantasy.espn_fantasy_parsers",
        "getter_module": "sportsdataverse.providers.espn_fantasy.espn_fantasy_runtime",
        "qualifier": "",
        "passthrough_query": False,
        "schema_prefix": "espn_fantasy_",
        "runtime_imports": ["_get"],
        "docstring": {
            "example_import": True,
            "notes": [
                "Public data only. No cookie is ever sent, so these wrappers read every game's league-agnostic "
                "routes and the PUBLIC leagues ESPN lists in espn_fantasy_league_directory. A private league "
                "answers HTTP 401: reading one needs its owner's espn_s2 / SWID cookies, which this package "
                "does not take.",
                "Season ids are ESPN's: the start year for football (2026 = fall 2026), the end year for basketball "
                "and hockey (2026-27 = 2027). Ids are Int64; team_id is unique only inside "
                "one league. No member or owner account id is returned.",
            ],
            "raises": [
                "sportsdataverse.errors.NoDataError: ESPN answered 404 (unknown game, season or id).",
                "AssetFetchError: also raised for a private league (HTTP 401).",
            ],
            "see_also": [
                {
                    "name": "ESPN Fantasy",
                    "url": "https://fantasy.espn.com/",
                    "note": "the fantasy platform these routes serve; public leagues are listed in its draft lobby",
                },
            ],
        },
        "endpoints": [
            _entry(s, copy.deepcopy(d)) for s, d in sorted(ENDPOINTS.items())
        ],  # no shared nodes: no &id anchors
    }
    write_yaml(ROOT / f"tools/codegen/endpoints/{STEM}.yaml", doc)
    schema_dir = ROOT / f"tools/codegen/schemas/native/{STEM}"
    rewrite_schema_dir(schema_dir)
    for short, d in ENDPOINTS.items():
        cols: List[Dict[str, str]] = [
            {"name": c, "type": _type(t), "description": ""} for c, t in P.SCHEMAS[_table(d["parser"])].items()
        ]
        write_yaml(
            schema_dir / f"{short}.yaml", {"schema": f"espn_fantasy_{short}", "kind": "dataframe", "columns": cols}
        )
    print(f"{STEM}: {len(ENDPOINTS)} wrappers")


if __name__ == "__main__":
    main()
