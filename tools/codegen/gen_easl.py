"""Generate the ``easl`` flat-API endpoint YAML + returns-schemas (East Asia Super League, via an UNOFFICIAL proxy).

Idempotent: same spec + same fixtures -> byte-identical output. Modeled on ``gen_kbl.py`` with the shared plumbing in
``gen_soccer_common.py``. Reads the frozen spec, the returns md and the presence sidecars from the ``sdv-internal-refs``
checkout (``$SDV_INTERNAL_REFS_REPO``, else the sibling checkout) and emits:

* ``tools/codegen/endpoints/easl.yaml`` -- 18 wrappers over the spec's 18 paths (one parser per route).
* ``tools/codegen/schemas/native/easl/<short>.yaml`` -- one returns-schema per wrapper: the columns of its parser
  (``easl_parsers.SPECS``, which also types an empty frame) checked against the parser run over its committed fixtures
  (the FULL bodies in ``tests/fixtures/easl/full/`` plus the refs captures in ``tests/fixtures/easl/captures/``).
  Schema key ``easl_<short>`` (``schema_prefix``); every ``description`` is ``''``.
* ``tools/codegen/notes/easl_descriptions.yaml`` -- the column descriptions (committed: the generator owns the text),
  keyed ``easl_<short>`` -> column, for ``manual_column_descriptions.yaml``. Each comes from :data:`ROUTE`
  (route-specific), the route's own ``easl-returns.md`` row (:data:`SECTIONS`), else :data:`LOCAL` (columns the parser
  derives). A filler or missing description stops the run.
* ``tests/fixtures/easl/spec_paths.json`` -- the spec's routes and the route each wrapper reads, for the tests (CI has
  no refs checkout).

It also checks the statistic blocks of ``easl_parsers`` against the refs ``captures/presence`` sidecars (computed on the
FULL bodies): a statistic the sidecars have and the parser lacks, or one the parser types Int64 that was seen as a
non-integral number, stops the run.

Run: ``python tools/codegen/gen_easl.py``
"""

from __future__ import annotations

import copy
import glob
import json
import re
import sys
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import polars as pl

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from gen_soccer_common import load_spec, refs_dir, rewrite_schema_dir, write_yaml  # noqa: E402

from sportsdataverse.basketball.easl import easl_parsers as P  # noqa: E402
from sportsdataverse.dl_utils import underscore  # noqa: E402

HOST = "https://easl.joshricha.com/api/v1"
SPEC = "easl.openapi.yaml"
FIXTURES = ROOT / "tests" / "fixtures" / "easl"
SCHEMA_PREFIX = "easl_"
SNIPPET = ROOT / "tools" / "codegen" / "notes" / "easl_descriptions.yaml"
SPEC_PATHS = FIXTURES / "spec_paths.json"
_BANNED = re.compile(r"^(the\s+)?\w+(\s+\w+)?\s+(column|field|value|id|name)\.?$", re.IGNORECASE)

S24, S25, S26, S27 = (
    "c24ce68a-6420-11ee-9daf-6b6188281bd0",
    "57cefc32-5ace-11ef-81a0-3d1f6018efb7",
    "77441bb3-9759-11f0-b659-0561c09a0222",
    "1409787d-a683-11f1-aea7-b596f55be5ed",
)
#: fixture id of each banked game: the 2025-26 final, the 2024-25 final, the 2023-24 final, the active season's game.
GAMES = {
    "@2025": "10af8fd4-106e-11f1-960b-5798087c7aa3",
    "@2024": "bde3aaa1-da50-11ef-97fd-97d4eecf418b",
    "@2023": "6e3c321b-d078-11ee-bd9b-cfe0b28e3077",
    "": "e123336e-a6d0-11f1-9d9e-dd8ab99a2c59",
}
#: tag -> (END year, seasonId) of each banked season; the bare file is the active season (2026-27).
SEASONS = {"@2025": (2026, S26), "@2024": (2025, S25), "@2023": (2024, S24), "": (2027, S27)}
TEAM = "865489e8-b0f5-11ed-8b3f-c199a9dea184"
PLAYER = "85c99dab-9f67-11f0-8780-a731edb5aad5"

FIXTURE_ID = (
    "DataCore fixture (game) UUID v1, e.g. '10af8fd4-106e-11f1-960b-5798087c7aa3' (the 2025-26 EASL Championship): the "
    "'id' of easl_schedule rows. Only finished (CONFIRMED) games are served; a malformed id is a ValueError."
)
TEAM_ID = (
    "DataCore team (entity) UUID v1, e.g. '865489e8-b0f5-11ed-8b3f-c199a9dea184' (Utsunomiya Brex): the team_id of "
    "easl_teams, easl_standings and the box scores. easl_team resolves the 12 historical teams easl_teams does not list."
)
PLAYER_ID = (
    "DataCore person UUID v1, e.g. '85c99dab-9f67-11f0-8780-a731edb5aad5': the player_id of easl_players, the box "
    "scores, leaders and play-by-play. A malformed id is a ValueError."
)
SEASON = (
    "Season END year (2026 = EASL 25-26; the four editions are 2024 to 2027). easl_runtime reads easl_seasons and maps "
    "it to the DataCore seasonId. Default: the ACTIVE season."
)

Fixture = Tuple[str, Dict[str, Any]]
# short -> (spec path, parser, fixtures, summary, endpoint fields)
Entry = Tuple[str, Callable[..., Any], List[Fixture], str, Dict[str, Any]]


def _p(name: str, desc: str) -> Dict[str, Any]:
    return {"name": name, "type": "str", "required": True, "description": desc}


def _q(name: str, wire: str, desc: str, *, typ: str = "str", **extra: Any) -> Dict[str, Any]:
    return {"name": name, "query_key": wire, "type": typ, "description": desc, **extra}


def _game_fields() -> Dict[str, Any]:
    return {"path_params": [_p("fixture_id", FIXTURE_ID)], "example_args": {"fixture_id": GAMES["@2025"]}}


def _team_fields(season: bool = False) -> Dict[str, Any]:
    out: Dict[str, Any] = {"path_params": [_p("team_id", TEAM_ID)], "example_args": {"team_id": TEAM}}
    if season:
        out["extra_params"] = [_q("season", "season", SEASON, typ="int")]
        out["example_args"]["season"] = 2026
    return out


def _season_fields() -> Dict[str, Any]:
    return {"extra_params": [_q("season", "season", SEASON, typ="int")], "example_args": {"season": 2026}}


def _per_game(captures: str, full: Optional[str] = None) -> List[Fixture]:
    """The refs capture of a game route in each of the four seasons, plus the full body of the 2025-26 final."""
    out: List[Fixture] = []
    if full:
        out.append((f"full/{full}@2025.json", {"fixture_id": GAMES["@2025"]}))
    out += [(f"captures/{captures}{tag}.json", {"fixture_id": gid}) for tag, gid in GAMES.items()]
    return out


def _per_season(captures: str, full: Optional[str] = None) -> List[Fixture]:
    out: List[Fixture] = []
    if full:
        out.append((f"full/{full}@2025.json", {"season": 2026, "season_id": S26}))
    out += [(f"captures/{captures}{tag}.json", {"season": y, "season_id": sid}) for tag, (y, sid) in SEASONS.items()]
    return out


ENDPOINTS: Dict[str, Entry] = {
    "health": (
        "/health",
        P.parse_easl_health,
        [("captures/health.json", {})],
        "Liveness probe of the unofficial sync server: status, message and the server's own timestamp.",
        {"example_args": {}},
    ),
    "seasons": (
        "/metadata/seasons",
        P.parse_easl_seasons,
        [("full/metadata__seasons.json", {}), ("captures/metadata__seasons.json", {})],
        "Every configured EASL season (the four editions 2023-24 to 2026-27) with its DataCore season id and END year.",
        {"example_args": {}},
    ),
    "schedule": (
        "/matches",
        P.parse_easl_schedule,
        _per_season("matches", "matches"),
        "Every fixture of one season: status, tip-off, stage, pool, venue and both teams with their scores.",
        _season_fields(),
    ),
    "game": (
        "/matches/{fixture_id}",
        P.parse_easl_game,
        _per_game("matches__fixtureId"),
        "Header of one finished fixture: teams, final score, status, stage, pool, venue and attendance.",
        _game_fields(),
    ),
    "player_box": (
        "/matches/{fixture_id}/boxscore",
        P.parse_easl_player_box,
        _per_game("matches__fixtureId__boxscore", "matches__fixtureId__boxscore"),
        "Player box score of a finished game, one row per rostered player (non-players have null counting stats).",
        _game_fields(),
    ),
    "pbp": (
        "/matches/{fixture_id}/playbyplay",
        P.parse_easl_pbp,
        _per_game("matches__fixtureId__playbyplay", "matches__fixtureId__playbyplay"),
        "Play-by-play of a finished game in delivered order, with the running score and the points each event scored.",
        _game_fields(),
    ),
    "team_box": (
        "/matches/{fixture_id}/teamstats",
        P.parse_easl_team_box,
        _per_game("matches__fixtureId__teamstats", "matches__fixtureId__teamstats"),
        "Team totals of a finished game, one row per team, with the opponent's totals as *_against columns.",
        _game_fields(),
    ),
    "team_box_by_period": (
        "/matches/{fixture_id}/teamstats/by/period",
        P.parse_easl_team_box_by_period,
        _per_game("matches__fixtureId__teamstats__by__period", "matches__fixtureId__teamstats__by__period"),
        "Team totals of a finished game by period, one row per team and period (overtimes after the fourth).",
        _game_fields(),
    ),
    "standings": (
        "/standings",
        P.parse_easl_standings,
        _per_season("standings", "standings"),
        "Group-stage standings of one season: rank within the pool, record, splits, points and the last five results.",
        _season_fields(),
    ),
    "leaders": (
        "/leaders",
        P.parse_easl_leaders,
        _per_season("leaders", "leaders"),
        "Tournament top-5 player leaders in nine categories (points, rebounds, assists, steals, blocks, shooting).",
        _season_fields(),
    ),
    "teams": (
        "/teams",
        P.parse_easl_teams,
        [("full/teams.json", {}), ("captures/teams.json", {})],
        "The 12 ACTIVE EASL teams (historical teams resolve only through easl_team).",
        {"example_args": {}},
    ),
    "team": (
        "/teams/{team_id}",
        P.parse_easl_team,
        [("captures/teams__teamId.json", {"team_id": TEAM})],
        "One team's directory entry; resolves the historical teams easl_teams does not list.",
        _team_fields(),
    ),
    "team_schedule": (
        "/teams/{team_id}/matches",
        P.parse_easl_team_schedule,
        [("captures/teams__teamId__matches@2025.json", {"team_id": TEAM, "season": 2026, "season_id": S26})],
        "A team's fixtures in one season, shaped like easl_schedule.",
        _team_fields(season=True),
    ),
    "team_stats": (
        "/teams/{team_id}/stats",
        P.parse_easl_team_stats,
        [("captures/teams__teamId__stats@2025.json", {"team_id": TEAM, "season": 2026, "season_id": S26})],
        "A team's cumulative season statistics: totals, per-game rates and the opponent's totals (*_against).",
        _team_fields(season=True),
    ),
    "team_leaders": (
        "/teams/{team_id}/leaders",
        P.parse_easl_team_leaders,
        [("captures/teams__teamId__leaders@2025.json", {"team_id": TEAM, "season": 2026, "season_id": S26})],
        "A team's top-5 player leaders per category in one season, shaped like easl_leaders.",
        _team_fields(season=True),
    ),
    "players": (
        "/players",
        P.parse_easl_players,
        [
            ("full/players@roster.json", {"season": 2026, "season_id": S26, "team_id": TEAM}),
            ("captures/players@roster.json", {"season": 2026, "season_id": S26, "team_id": TEAM}),
            ("captures/players.json", {}),
        ],
        "Every EASL player of all seasons; with season and team_id a roster that adds jersey number and position.",
        {
            "extra_params": [
                _q(
                    "season",
                    "season",
                    SEASON.replace(" Default: the ACTIVE season.", " Default: no season filter."),
                    typ="int",
                ),
                _q("team_id", "teamId", TEAM_ID + " Needs season for a roster (jersey number and position)."),
            ],
            "example_args": {"season": 2026, "team_id": TEAM},
        },
    ),
    "player": (
        "/players/{player_id}",
        P.parse_easl_player,
        [("captures/players__playerId.json", {"player_id": PLAYER})],
        "One player's biography: names, nationality, birth date, height, weight.",
        {"path_params": [_p("player_id", PLAYER_ID)], "example_args": {"player_id": PLAYER}},
    ),
    "player_stats": (
        "/players/{player_id}/stats",
        P.parse_easl_player_stats,
        [("captures/players__playerId__stats.json", {"player_id": PLAYER})],
        "A player's cumulative statistics over ALL seasons (the server ignores a season filter).",
        {"path_params": [_p("player_id", PLAYER_ID)], "example_args": {"player_id": PLAYER}},
    ),
}

#: short -> (returns md section, wire-path prefix in that section). A column's md row is ``<prefix><wire path>``.
SECTIONS: Dict[str, Tuple[str, str]] = {
    "health": ("/health", ""),
    "seasons": ("/metadata/seasons", "[]."),
    "schedule": ("/matches", "[]."),
    "game": ("/matches/{fixtureId}", ""),
    "player_box": ("/matches/{fixtureId}/boxscore", "boxScore.[]."),
    "pbp": ("/matches/{fixtureId}/playbyplay", "playbyplay.[]."),
    "team_box": ("/matches/{fixtureId}/teamstats", "[]."),
    "team_box_by_period": ("/matches/{fixtureId}/teamstats/by/period", "[]."),
    "standings": ("/standings", "[]."),
    "leaders": ("/leaders", "pointsPerGame.[]."),
    "teams": ("/teams", "[]."),
    "team": ("/teams/{teamId}", ""),
    "team_schedule": ("/teams/{teamId}/matches", "[]."),
    "team_stats": ("/teams/{teamId}/stats", ""),
    "team_leaders": ("/teams/{teamId}/leaders", "pointsPerGame.[]."),
    "players": ("/players", "[]."),
    "player": ("/players/{playerId}", ""),
    "player_stats": ("/players/{playerId}/stats", ""),
}

#: Wrappers whose own md section lacks a column that a sibling section documents.
FALLBACK = {"game": "schedule", "team_schedule": "schedule", "player": "players", "team": "teams"}

_UNOBSERVED = (
    "Not yet populated upstream: null in every full body observed (2026-10-12), so its meaning is unverified; the "
    "2026-27 bodies start filling some of these columns, so it is typed from its family ({kind})."
)
_KIND = {pl.Int64: "Int64", pl.Float64: "Float64", pl.String: "String", pl.Boolean: "Boolean", pl.Date: "Date"}

#: Descriptions of columns the parser derives or renames (apply to every wrapper that has the column).
LOCAL: Dict[str, str] = {
    "season": "Season END year (2026 = EASL 25-26): stamped by easl_runtime from the request, null when the body is parsed bare.",
    "season_id": "DataCore season UUID v1 (one per edition), from the request when the row carries none. Joins easl_seasons season_id.",
    "fixture_id": "DataCore fixture (game) UUID v1: the id of every game route; joins easl_schedule fixture_id.",
    "team_id": "DataCore team (entity) UUID v1; joins easl_teams team_id, the fixtures' home_team_id / away_team_id and the box scores.",
    "player_id": "DataCore person UUID v1 (the box score calls it personId); joins easl_players player_id, leaders and play-by-play.",
    "name": "Display name: despite the wire name nameFullLocal this is the English / Latin-script name used on the league site.",
    "name_native": "Native-script name of some Japanese clubs (nameFullLatin on the wire, despite its name), blank for most clubs.",
    "team_name": "Team display name (English / Latin script, the wire's nameFullLocal).",
    "team_name_native": "Team name in native script (the wire's nameFullLatin; blank for most clubs).",
    "player_name": "Player display name in 'FAMILY Given' form with the family name upper-cased (the wire's nameFullLocal).",
    "player_name_latin": "The wire's nameFullLatin: equal to player_name for about half the players, otherwise null.",
    "fixture_name": "Name of the fixture where it has one ('EASL Championship'); null for group-stage games.",
    "category": "Leader category, the wire key of the group: pointsPerGame, reboundsPerGame, assistsPerGame, stealsPerGame, blocksPerGame, pointsThreeMade, pointsThreePercentage, fieldGoalsPercentage or freeThrowsPercentage.",
    "rank": "Rank inside the category, 1 = best, in the delivered order (the server lists the top 5).",
    "value": "The leader's value in the category: a per-game average, a count (pointsThreeMade) or a percentage (0-100). Float64.",
    "event_number": "1-based position of the event in the delivered array, which is the only usable order (sequence is not monotonic).",
    "period_label": "Q1-Q4 for periods 1-4; OT1, OT2 ... for overtimes (the server numbers the first overtime 11 in team totals; periods 5-10 are read as OT{n-4}, unverified).",
    "period": "Period number: 1-4 for regulation, overtimes after (the wire's period_{n} key).",
    "team_score": "The event team's running score as of this event (the running score lags the true score on a minority of events); null on events with no team.",
    "opponent_score": "The other team's running score as of this event; null on events with no team.",
    "points_scored": "Points the event team gained since the previous event (change of its running score): the made shot's value. The description always says 'missed', so count points here, not strings; the running score lags on some games, which shifts points to a later event.",
    "team1_id": "First team of the body's teams array (the fixture's competitors order, arbitrary home/away); team1_score belongs to it.",
    "team1_score": "Running score of team1_id as of this event.",
    "team2_id": "Second team of the body's teams array; team2_score belongs to it.",
    "team2_score": "Running score of team2_id as of this event.",
    "played": "True when the player has playing time (minutes present); False for the rostered players who did not play (their counting stats are null).",
    "minutes_played": "Playing time in minutes (decimal), parsed from the ISO 8601 minutes statistic (PT35M11S = 35.1833).",
    "last_five": "Results of the team's last five games as a comma-separated string, most recent first (W win, L loss).",
    "standing_id": "DataCore UUID v1 of the (season, pool, team) standings row; joins nothing else.",
    "season_row_id": "The sync server's own integer row id of the season (insertion order, not chronological); NOT a DataCore id.",
    "team_row_id": "The sync server's own integer row id of the team; NOT a DataCore id.",
    "player_row_id": "The sync server's own integer row id of the player; NOT a DataCore id.",
    "team_code_latin": "Three-letter club code (the wire's codeLatin); null for some clubs.",
    "team_code_local": "Club code in native script (the wire's codeLocal); null for most clubs.",
    "code_latin": "Three-letter club code (the wire's codeLatin); null for some clubs.",
    "code_local": "Club code in native script (the wire's codeLocal); null for most clubs.",
    "player_status": "The player's registration status in the season (ACTIVE, INACTIVE or UNREGISTERED).",
    "year": "START year of the season (2025 = EASL 25-26); the sdv season (END year) is year + 1.",
    "jersey_number": "Jersey number as a string ('00' and '0' are distinct).",
    "height_cm": "Height in centimetres; null for some players.",
    "weight_kg": "Weight in kilograms; null for some players.",
    "height_feet": "Height in feet as the server sends it (a string); null for most players.",
    "weight_lbs": "Weight in pounds; null for most players.",
    "dob": "Date of birth (a public biography field); null for some players.",
    "status": "Registration or run status as sent by the server (see the route's returns table).",
    "gender": "Competition gender of the team: MALE (the EASL is a men's competition).",
    "stage_id": "Stage reference '<season UUID>:<stage code>' on team-fixture rows only (its first half scopes the fixture to a season); null on the other fixture routes, which send the stage_code, stage_name and stage_order trio.",
    "stage_code": "Stage code of the fixture (a 30-character hex id); filled on season fixture lists, null on the game header and team fixtures, which send no stage trio.",
    "stage_name": "Stage name of the fixture ('Group Stage', 'Playoffs'); filled on season fixture lists, null on the game header and team fixtures.",
    "stage_order": "Order of the stage in the season (1 = group stage); filled on season fixture lists, null on the game header and team fixtures.",
    "venue_name": "Arena name (the wire's venue.nameLocal), e.g. 'Okinawa Suntory Arena'.",
    "venue_state": "State or province of the venue (venue.address.state); sent for some venues only.",
    "free_throws_made": "Free throws made: a game count on the box-score and team-total routes, a season total on the season statistics routes.",
    "points": "Points scored: a game count on the box-score and team-total routes, a season total on the season statistics routes.",
    "wins": "Wins: all group-stage games on the standings route, all games on the season statistics routes.",
    "wins_home": "Wins in the team's home games.",
    "wins_away": "Wins in the team's away games.",
    "losses": "Losses: all group-stage games on the standings route, all games on the season statistics routes.",
    "losses_home": "Losses in the team's home games.",
    "losses_away": "Losses in the team's away games.",
    "points_for": "Points the team scored over its group-stage games (points_against is the points it conceded).",
    "position": "Playing position (G, F, C, PG, SG, SF, PF or GF); filled on a roster (season and team filter), null on the directory and the biography.",
}

#: Route-specific descriptions (keyed by wrapper short) that replace a returns-md row or fill a derived column.
_SCORES = {
    "home_score": "Home team final points (the wire sends a numeric string; cast to Int64); null before the game is played.",
    "away_score": "Away team final points (the wire sends a numeric string; cast to Int64); null before the game is played.",
}
ROUTE: Dict[str, Dict[str, str]] = {
    "seasons": {
        "season": "Season END year (2026 = EASL 25-26), derived as year + 1.",
        "name": "Season label ('EASL 24-25' = 2024-25).",
        "name_latin": "Season label in Latin script; null on some seasons.",
        "status": "COMPLETE for a finished season, ACTIVE for the current one (the default of every season route).",
    },
    "schedule": {
        **_SCORES,
        "season": "Season END year (2026 = EASL 25-26): stamped by easl_runtime from the request, null when parsed bare.",
        "season_id": "DataCore season UUID v1 of the fixture (the row's own seasonId).",
        "status": "CONFIRMED for a finished game, SCHEDULED for a future one, DRAFT for an unscheduled placeholder.",
    },
    "game": {
        **_SCORES,
        "season": "Always null here: the game route carries no season; join season_id to easl_seasons.",
        "season_id": "Always null here: the game route carries no season.",
    },
    "team_schedule": {
        **_SCORES,
        "season": "Season END year (2026 = EASL 25-26): stamped by easl_runtime from the request.",
        "season_id": "Season UUID: the rows carry no seasonId (the season sits in the first half of stage_id), so this is the request's.",
    },
    "player_box": {
        "fixture_id": "DataCore fixture (game) UUID v1 of the box row.",
        "team_id": "The player's team: DataCore team (entity) UUID v1 (the wire's entityId).",
        "team_name": "The player's team display name, joined from the body's teams array.",
        "team_name_native": "The player's team name in native script, joined from the body's teams array; blank for most clubs.",
        "position": "Playing position: G, F, C, PG, SG, SF, PF or GF; null on some rows.",
        "bib": "Jersey number as a string ('00' and '0' are distinct).",
    },
    "pbp": {
        "fixture_id": "DataCore fixture (game) UUID v1 from the request.",
        "team_id": "DataCore team UUID v1 of the acting team; null on events with no team.",
        "player_id": "DataCore person UUID v1 of the acting player; null for team events (team rebounds, timeouts).",
    },
    "team_box": {"team_id": "DataCore team (entity) UUID v1 (the wire's entityId)."},
    "team_box_by_period": {
        "fixture_id": "DataCore fixture (game) UUID v1 from the request.",
        "team_id": "DataCore team UUID v1 (the wire's team.teamId).",
    },
    "standings": {
        "season": "Season END year (2026 = EASL 25-26), from the request.",
        "season_id": "DataCore season UUID v1, from the request (the standings rows carry none).",
    },
    "team_stats": {
        "team_id": "DataCore team UUID v1, from the request (the body carries none).",
        "season": "Season END year (2026 = EASL 25-26), from the request.",
        "season_id": "DataCore season UUID v1, from the request.",
    },
    "players": {
        "season": "Season END year of the filter, from the request; null when no season was passed.",
        "season_id": "DataCore season UUID v1 of the filter; null when no season was passed.",
    },
    "player_stats": {"player_id": "DataCore person UUID v1, from the request (the body carries none)."},
    "leaders": {"season_id": "DataCore season UUID v1 of the leader row."},
    "team_leaders": {"season_id": "DataCore season UUID v1 of the leader row."},
}


def _unescape(text: str) -> str:
    return text.replace("&lt;", "<").replace("&gt;", ">").replace("\\|", "|").strip()


def _md_sections(path: Path) -> Dict[str, Dict[str, str]]:
    """``{section path: {raw column path: description}}`` from the returns md."""
    out: Dict[str, Dict[str, str]] = {}
    current: Optional[str] = None
    head = re.compile(r"^## `([^`]+)`")
    row = re.compile(r"^\| `([^`]+)` \| [^|]+ \| (.*) \|$")
    for line in path.read_text(encoding="utf-8").splitlines():
        h = head.match(line)
        if h:
            current = h.group(1).replace(HOST, "")
            out.setdefault(current, {})
            continue
        m = row.match(line)
        if m and current is not None:
            out[current].setdefault(m.group(1), _unescape(m.group(2)))
    return out


def _filler(name: str, desc: str) -> bool:
    d = desc.strip()
    if len(d) < 15 or d.lower() == name.lower() or _BANNED.match(d):
        return True
    words = "".join(re.findall(r"[a-z0-9]+", d.lower().split(":")[-1]))
    return bool(words) and name.replace("_", "").endswith(words)


_SIDE_WIRE = {c: w for c, w, _t in P._FIXTURE_SIDE}


def _md_key(short: str, wire: str) -> str:
    prefix = SECTIONS[short][1]
    if short == "team_box_by_period":
        return prefix + wire.replace("stats.", "period_{n}.", 1)
    return prefix + wire


def _describe(short: str, col: str, wire: str, dtype: Any, md: Dict[str, Dict[str, str]]) -> str:
    """ROUTE, then the route's md row for the column's wire key, then LOCAL."""
    route = ROUTE.get(short, {})
    if col in route:
        return route[col]
    section = md.get(SECTIONS[short][0], {})
    if wire:
        hit = section.get(_md_key(short, wire))
        if hit and hit.startswith("Unobserved"):
            return _UNOBSERVED.format(kind=_KIND[dtype])
        if hit and not _filler(col, hit):
            return hit
    if wire and short in FALLBACK:
        hit = md.get(SECTIONS[FALLBACK[short]][0], {}).get(_md_key(FALLBACK[short], wire))
        if hit and not _filler(col, hit):
            return hit
    m = re.match(r"^(home|away)_(.+)$", col)
    if m and m.group(2) in _SIDE_WIRE:
        base = section.get(SECTIONS[short][1] + "competitors.[]." + _SIDE_WIRE[m.group(2)], "")
        if base and not _filler(col, base):
            return f"{m.group(1).capitalize()} team: {base[0].lower()}{base[1:]}"
    return LOCAL.get(col, "")


_TYPE = {pl.String: "character", pl.Int64: "integer", pl.Float64: "numeric", pl.Boolean: "logical", pl.Date: "date"}


def load_fixture(rel: str, ctx: Dict[str, Any]) -> Any:
    """A fixture body as the runtime hands it to a parser: the body plus the ``_request`` stamp."""
    data = json.loads((FIXTURES / rel).read_text(encoding="utf-8"))
    out: Dict[str, Any] = dict(data) if isinstance(data, dict) else {"items": data}
    out["_request"] = ctx
    return out


def _check_frames(short: str, entry: Entry) -> None:
    """Run the parser over every fixture: the frame must carry the documented schema exactly."""
    spec = {c: d for c, _w, d in P.SPECS[entry[1].__name__]}
    rows = 0
    for rel, ctx in entry[2]:
        df = entry[1](load_fixture(rel, ctx))
        if dict(df.schema) != spec:
            raise SystemExit(f"easl: {short}: {rel} parses to {dict(df.schema)}, not the documented schema")
        rows += df.height
    if rows == 0:
        raise SystemExit(f"easl: {short}: no fixture parses to a row")


def _check_presence(refs: Path) -> None:
    """The statistic blocks against the refs presence sidecars (computed on the FULL bodies)."""
    blocks = [
        ("matches__fixtureId__boxscore*.json", r"boxScore\.\[\]\.statistics\.(\w+)", P._BOX_STATS),
        ("matches__fixtureId__teamstats*.json", r"\[\]\.statistics\.(\w+)", P._TEAM_GAME_STATS),
        ("matches__fixtureId__teamstats__by__period*.json", r"\[\]\.period_\{n\}\.(\w+)", P._PERIOD_STATS),
        ("teams__teamId__stats*.json", r"(\w+)", P._TEAM_SEASON_STATS),
        ("players__playerId__stats*.json", r"(\w+)", P._PLAYER_SEASON_STATS),
    ]
    bad: List[str] = []
    for pattern, rx, block in blocks:
        have = {k: v for k, _t in (p.rsplit(":", 1) for p in block.split()) for v in [_t]}
        seen: Dict[str, set] = {}
        for f in sorted(glob.glob(str(refs / "captures" / "presence" / pattern))):
            for key, info in json.loads(Path(f).read_text(encoding="utf-8")).items():
                m = re.fullmatch(rx, key)
                if m and set(info["types"]) - {"null"}:
                    seen.setdefault(m.group(1), set()).update(info["types"])
        for key, types in seen.items():
            if key not in have:
                bad.append(f"{pattern}: statistic {key} is in the full bodies but not in the parser block")
            elif have[key] == "i" and "number" in types:
                bad.append(f"{pattern}: {key} is Int64 in the parser but was seen as a non-integral number")
            elif have[key] == "s" and types - {"string", "null"}:
                bad.append(f"{pattern}: {key} is String in the parser but was seen as {sorted(types)}")
    if bad:
        raise SystemExit("easl: statistic blocks out of step with the presence sidecars:\n" + "\n".join(bad))


def _snake_path(path: str) -> str:
    return re.sub(r"\{([^}]+)\}", lambda m: "{" + underscore(m.group(1)) + "}", path)


def _endpoint(short: str, entry: Entry) -> Dict[str, Any]:
    spec_path, parser, _fixtures, summary, fields = entry
    fields = copy.deepcopy(fields)
    tokens = re.findall(r"\{([^}]+)\}", spec_path)
    assert len(fields.get("path_params", [])) == len(tokens), short
    return {
        "short": short,
        "summary": summary,
        "path": _snake_path(spec_path),
        "parser": parser.__name__,
        "returns_schema": f"native/easl/{short}",
        **fields,
    }


NOTES = [
    "WARNING: UNOFFICIAL THIRD-PARTY INTERMEDIARY. These wrappers read easl.joshricha.com/api/v1, an Express server run "
    "by an individual. It is not EASL's and not Sportradar's: it mirrors Sportradar DataCore (organisation b1e0z) into "
    "SQLite and serves it keyless, and it can change or vanish without notice. Its routes, field names (steak, "
    "facebook_username) and sync freshness are the proxy author's. A first-party DataCore feed needs credentials this "
    "package does not have. The host serves no robots.txt (404) and states no terms; the runtime waits at least 1 s "
    "before every request, retries included, and honours Retry-After. Payloads are not redistributed as release assets.",
    "Seasons are END years (2026 = EASL 25-26). The server's season key is the START year; easl_runtime reads the "
    "season list, maps the END year to the DataCore seasonId and defaults to the ACTIVE season. Four editions exist "
    "(2023-24 to 2026-27). The server defaults to the active season on /matches, /standings and /leaders; the runtime "
    "always sends the resolved seasonId. A season is two calendar years: group by season_id, not by the year of the "
    "tip-off.",
    "Finished games only: the game routes first read the fixture and refuse (AssetFetchError) one that is not CONFIRMED "
    "with two scores, a box score whose player points do not add up to the score (the server serves one-team, four-row "
    "boxes for some games), a play-by-play that does not end on the final score in its highest period (4 or more, and "
    "the last period of the period table), and team totals that disagree with the score. For the ACTIVE season's "
    "fixture lists the runtime reads /health: a server clock more than a day from yours, or a game still SCHEDULED more "
    "than a day after its tip-off, means the mirror has stalled (AssetFetchError). That proves the mirror kept up with "
    "the games played, not that a game is final upstream.",
    "Ids: every DataCore id is a UUID v1 string. team_id = entityId, player_id = personId; fixture_number (YYSSNNN) is "
    "null on every 2023-24 fixture; *_row_id columns are the sync server's own integers, not DataCore ids. The "
    "fixture's competitors come in arbitrary order: read home_* / away_*, which the parsers take from isHome.",
    "No shot x/y, no game clock and no lineups exist on this server. The play-by-play text always says 'missed' for "
    "field goals and 'Free throw K of M made' for free throws, made or not: use points_scored (the change of the "
    "event team's running score), which lags on a minority of events. 301 of 2,444 sampled box rows are players who "
    "did not play (null minutes and counting stats; points are 0); player_box.played tells them apart. "
    "easl_player_stats ignores a season filter: it is the career line over every season. Names: nameFullLocal is the "
    "English display name and nameFullLatin the native-script name, so the columns are name and name_native.",
]


def main() -> None:
    refs = refs_dir("easl", SPEC)
    spec = load_spec(refs, SPEC)
    wanted = {re.sub(r"\{[^}]+\}", "{}", r) for r in spec["paths"]}
    mapped = {re.sub(r"\{[^}]+\}", "{}", e[0]) for e in ENDPOINTS.values()}
    if mapped != wanted:
        raise SystemExit(f"easl: spec paths and ENDPOINTS differ: {sorted(mapped ^ wanted)}")
    if set(SECTIONS) != set(ENDPOINTS):
        raise SystemExit("easl: SECTIONS out of step with ENDPOINTS")
    _check_presence(refs)
    for short, entry in ENDPOINTS.items():
        names = set(re.findall(r"\{([^}]+)\}", entry[0]))
        assert not {"league", "sport"} & names, short  # the renderer substitutes those as ESPN slugs
        for rel, _ctx in entry[2]:
            assert (FIXTURES / rel).exists(), f"{short}: missing fixture {rel}"
        _check_frames(short, entry)
    doc = {
        "api": "easl",
        "host": HOST,
        "name_pattern": "easl_{short}",
        "module": "easl",
        "parser_module": "basketball.easl.easl_parsers",
        "getter_module": "sportsdataverse.basketball.easl.easl_runtime",
        "qualifier": "",
        "passthrough_query": False,
        "parser_columns": False,
        "schema_prefix": SCHEMA_PREFIX,
        "runtime_imports": ["_get"],
        "docstring": {
            "example_import": True,
            "notes": NOTES,
            "raises": [
                "sportsdataverse.errors.NoDataError: the server has nothing for the request (an empty list or leaders "
                "object, a team or player without games, a 404).",
                "sportsdataverse.errors.AssetFetchError: the fetch failed or the answer is unknown or partial: any "
                "non-2xx (an unknown fixture is the server's HTTP 500), an error envelope, a game that is not "
                "finished or whose box score or play-by-play is incomplete, a stalled sync on the active season, or a "
                "body for another game, season, team or player than the one asked for.",
                "ValueError: a malformed id or a season that is not one of the server's END years (raised before the "
                "request that would use it).",
            ],
            "see_also": [
                {
                    "name": "East Asia Super League",
                    "url": "https://www.easl.basketball",
                    "note": "the league whose data these wrappers read through an unofficial proxy",
                },
                {
                    "name": "hoopR",
                    "url": "https://hoopR.sportsdataverse.org",
                    "note": "SportsDataverse men's basketball package (R)",
                },
            ],
        },
        "endpoints": [_endpoint(short, entry) for short, entry in ENDPOINTS.items()],
    }
    write_yaml(ROOT / "tools/codegen/endpoints/easl.yaml", doc)

    md = _md_sections(refs / "easl-returns.md")
    unknown = {sec for sec, _ in SECTIONS.values()} - set(md)
    if unknown:
        raise SystemExit(f"easl: SECTIONS name sections the returns md lacks: {sorted(unknown)}")
    schema_dir = ROOT / "tools/codegen/schemas/native/easl"
    rewrite_schema_dir(schema_dir)
    snippet: Dict[str, Dict[str, str]] = {}
    bad: List[str] = []
    for short, entry in ENDPOINTS.items():
        key = f"{SCHEMA_PREFIX}{short}"
        snippet[key] = {}
        cols = []
        for col, wire, dtype in P.SPECS[entry[1].__name__]:
            desc = _describe(short, col, wire, dtype, md)
            if _filler(col, desc):
                bad.append(f"{key}.{col}: {desc!r}")
            snippet[key][col] = desc
            # descriptions live in manual_column_descriptions.yaml (via the snippet), never here
            cols.append({"name": col, "type": _TYPE[dtype], "description": ""})
        write_yaml(schema_dir / f"{short}.yaml", {"schema": key, "kind": "dataframe", "columns": cols})
    if bad:
        raise SystemExit("easl: filler or missing descriptions:\n" + "\n".join(bad))
    write_yaml(SNIPPET, snippet)
    SPEC_PATHS.write_text(
        json.dumps({"paths": sorted(spec["paths"]), "wrappers": {s: e[0] for s, e in ENDPOINTS.items()}}, indent=1)
        + "\n",
        encoding="utf-8",
        newline="\n",
    )
    print(f"easl: {len(ENDPOINTS)} endpoints")


if __name__ == "__main__":
    main()
