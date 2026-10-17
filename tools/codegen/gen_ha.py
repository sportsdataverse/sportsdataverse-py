"""Generate the ``ha`` flat-API endpoint YAML + returns-schemas (HockeyAllsvenskan, ``hockeyallsvenskan.se``).

Idempotent: same spec + same captures -> byte-identical output. Modeled on ``gen_liiga.py`` (itself
``gen_asa.py`` + ``gen_soccer_common.py``). Reads the frozen spec from the ``sdv-internal-refs``
checkout (``$SDV_INTERNAL_REFS_REPO``, else the sibling checkout) and emits:

* ``tools/codegen/endpoints/ha.yaml`` -- 11 wrappers over the spec's 10 routes (the goalkeeper
  table also feeds ``ha_teams``, the StatNet id <-> display code crosswalk). The table
  :data:`ENDPOINTS` is the source of truth.
* ``tools/codegen/schemas/native/ha/<short>.yaml`` -- one returns-schema per wrapper: the columns
  of its parser run over its committed fixture, stamped the way the runtime stamps a live body.
  The games table uses the FULL body in ``tests/fixtures/ha/full/`` (``efficiency`` arrives as
  ``"8.82"``, ``0`` or null, which a 3-row capture cannot show); every other wrapper uses its
  refs capture. Columns are written with ``description: ''`` (re-capture clobbers schemas).
* ``tools/codegen/ha_column_descriptions.snippet.yaml`` -- the column descriptions, keyed by
  schema key (``ha_<short>``), merged into ``manual_column_descriptions.yaml``. They come from
  :data:`LOCAL`, then the spec, then the leaf of each ``hockeyallsvenskan-returns.md`` row.
* ``sportsdataverse/hockey/ha/_ha_schema.py`` -- column -> returns-schema type, which the
  parsers cast to, so an empty frame carries the dtypes of a populated one.
* ``tests/fixtures/ha/spec_snapshot.json`` -- the spec's routes, POST request examples and
  ``categories`` enums, so the tests check the runtime and the YAML without the refs checkout.

**What the flat-API YAML cannot express**, and how this family works around it: an endpoint has
no HTTP method and no body, so every wrapper renders as ``_get(url, params=...)``. The YAML's
``extra_params`` carry the wire names of the JSON body fields, and the family runtime
(``getter_module: sportsdataverse.hockey.ha.ha_runtime``) POSTs them as the body for every
route but ``/api/game``, and decodes the text/html ``/pages/matcher`` into its embedded game
list. Nothing in ``generate.py`` changes.

Run: ``python tools/codegen/gen_ha.py``
"""

from __future__ import annotations

import copy
import json
import re
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from gen_soccer_common import (
    ROOT,
    columns_from_frame,
    load_spec,
    refs_dir,
    rewrite_schema_dir,
    spec_descriptions,
    write_yaml,
)

from sportsdataverse.dl_utils import underscore
from sportsdataverse.hockey.ha import ha_parsers as P
from sportsdataverse.hockey.ha.ha_runtime import BODIES, CATEGORIES, HOST, PHASES, build_body

SPEC = "hockeyallsvenskan.openapi.yaml"
FIXTURES = ROOT / "tests" / "fixtures" / "ha"
SNAPSHOT = FIXTURES / "spec_snapshot.json"
SNIPPET = ROOT / "tools" / "codegen" / "ha_column_descriptions.snippet.yaml"
SCHEMA_MODULE = ROOT / "sportsdataverse" / "hockey" / "ha" / "_ha_schema.py"

SEASON = (
    "Season END year (2026 = 2025/26); the site's START-year key is sent as season - 1. "
    "Omitted = the season in progress. Stats and standings start in 2005/06 (2006)."
)
_PHASE = "Game type: HA regular season, HAPlayoff playoffs, HAkval playout (relegation series)."


def _q(name: str, wire: str, desc: str, *, typ: str = "str", required: bool = False, **extra: Any) -> Dict[str, Any]:
    out: Dict[str, Any] = {"name": name, "query_key": wire, "type": typ, "description": desc}
    if required:
        out["required"] = True
    out.update(extra)
    return out


def _season() -> Dict[str, Any]:
    return _q("season", "season", SEASON, typ="int")


def _phase() -> Dict[str, Any]:
    return _q("phase", "phase", _PHASE, choices=list(PHASES), default="HA")


def _category(route: str) -> Dict[str, Any]:
    allowed = list(CATEGORIES[route])
    desc = f"Stats category (column set), one of {', '.join(allowed)}. Anything else answers zero rows."
    return _q("category", "categories", desc, choices=allowed, default=allowed[0])


_FILTERS = [
    _q("team_side", "team_side", "Home / away split.", choices=["all", "home", "away"], default="all"),
    _q("team", "team", "Team filter: all, or one team of the site's filter list (values not probed).", default="all"),
    _q("against_team", "against_team", "Opponent filter: all, or one team (values not probed).", default="all"),
    _q(
        "all_results",
        "all_results",
        "Result filter: W win, T tie, L loss.",
        choices=["all", "W", "T", "L"],
        default="all",
    ),
    _q(
        "game_status",
        "game_status",
        "Game status filter: played drops the season's unplayed games.",
        choices=["all", "played"],
        default="all",
    ),
    _q(
        "decided_in",
        "decided_in",
        "How the game was decided: ft full time, ot overtime, so shootout, wo walkover.",
        choices=["all", "ft", "ot", "so", "wo"],
        default="all",
    ),
]

# short -> (spec path, parser, fixture, example request params or None, summary, endpoint fields)
Entry = Tuple[str, Callable[..., Any], str, Optional[Dict[str, Any]], str, Dict[str, Any]]
ENDPOINTS: Dict[str, Entry] = {
    "schedule": (
        "/pages/matcher",
        P.parse_ha_schedule,
        "pages__matcher.json",
        None,
        "The current season's games (CMS records, decoded from the schedule page): slug, StatNet "
        "game number and team ids, start time, score.",
        {},
    ),
    "game": (
        "/api/game",
        P.parse_ha_schedule,
        "api__game.json",
        None,
        "One game's CMS record by slug (2025/26 onward): StatNet game number and team ids, scores by "
        "period, venue, attendance.",
        {
            "extra_params": [
                _q(
                    "slug",
                    "slug",
                    "Game slug yyyymmdd-home-away (slug in ha_schedule), e.g. 20260918-aik-modo.",
                    required=True,
                )
            ],
            "example_args": {"slug": "20260918-aik-modo"},
        },
    ),
    "pbp": (
        "/api/play-by-play",
        P.parse_ha_pbp,
        "api__play-by-play.json",
        {
            "statNetGameNumber": "23401",
            "homeStatNetId": "AIK",
            "awayStatNetId": "MODO",
            "scheduledDateTime": "2026-09-18T17:00:00.000Z",
        },
        "One game's events (2025/26 onward): shots with outcome, goals with assists and strength, "
        "penalties, goalkeeper changes, timeouts. No coordinates, no on-ice players.",
        {
            "extra_params": [
                _q(
                    "statnet_game_number",
                    "statNetGameNumber",
                    "StatNet game number (statnet_game_number in ha_schedule / ha_game), e.g. 23401.",
                    typ="int|str",
                    required=True,
                ),
                _q("home_statnet_id", "homeStatNetId", "StatNet team id of the home team, e.g. AIK.", required=True),
                _q("away_statnet_id", "awayStatNetId", "StatNet team id of the away team, e.g. MODO.", required=True),
                _q(
                    "scheduled_date_time",
                    "scheduledDateTime",
                    "Scheduled start (scheduled_date_time in ha_schedule), ISO 8601 UTC. Required by the site.",
                    required=True,
                ),
                _q("played_date_time", "playedDateTime", "Actual start (played_date_time), ISO 8601 UTC. Optional."),
            ],
            "example_args": {
                "statnet_game_number": 23401,
                "home_statnet_id": "AIK",
                "away_statnet_id": "MODO",
                "scheduled_date_time": "2026-09-18T17:00:00.000Z",
            },
        },
    ),
    "games": (
        "/api/all-time-games-stats",
        P.parse_ha_games,
        "full/all-time-games-stats__2026__played.json",
        {"season": 2027, "game_status": "played"},
        "Every game of a season, played or scheduled (is_played): date, display team codes, score, "
        "periods, arena, attendance, shots and power play. No game id.",
        {
            "extra_params": [_season(), _phase(), *_FILTERS],
            "example_args": {"season": 2026},
        },
    ),
    "standings": (
        "/api/league-standings-all-time",
        P.parse_ha_standings,
        "api__league-standings-all-time.json",
        {"season": 2026},
        "Standings of a season. Wins and losses are REGULATION only; overtime counts include shootouts.",
        {
            "extra_params": [
                _season(),
                _phase(),
                _q(
                    "location",
                    "location",
                    "Home / away standings split.",
                    choices=["all", "home", "away"],
                    default="all",
                ),
            ],
            "example_args": {"season": 2026},
        },
    ),
    "skater_stats": (
        "/api/all-time-skaters-stats",
        P.parse_ha,
        "api__all-time-skaters-stats.json",
        {"season": 2026},
        "Skater stats of a season, one category (column set) per call. Names and StatNet team ids, no player ids.",
        {
            "extra_params": [
                _season(),
                _phase(),
                _category("/api/all-time-skaters-stats"),
                _q(
                    "position",
                    "position",
                    "Position filter (back = defence).",
                    choices=["all", "forward", "back", "goalkeeper"],
                    default="all",
                ),
                _q("ages", "ages", "Age-group filter.", choices=["all", "junior", "rookie"], default="all"),
                *_FILTERS,
            ],
            "example_args": {"season": 2026, "category": "summary"},
        },
    ),
    "goalie_stats": (
        "/api/all-time-goalkeepers-stats",
        P.parse_ha,
        "api__all-time-goalkeepers-stats.json",
        {"season": 2026},
        "Goalkeeper stats of a season, one category per call. Names and StatNet team ids, no player ids.",
        {
            "extra_params": [_season(), _phase(), _category("/api/all-time-goalkeepers-stats"), *_FILTERS],
            "example_args": {"season": 2026, "category": "summary"},
        },
    ),
    "team_stats": (
        "/api/all-time-teams-stats",
        P.parse_ha_team_stats,
        "api__all-time-teams-stats.json",
        {"season": 2026},
        "Team stats of a season, one category per call. W / L are totals including overtime and "
        "shootout (wins_total / losses_total).",
        {
            "extra_params": [_season(), _phase(), _category("/api/all-time-teams-stats"), *_FILTERS],
            "example_args": {"season": 2026, "category": "results"},
        },
    ),
    "teams": (
        "/api/all-time-goalkeepers-stats",
        P.parse_ha_teams,
        "api__all-time-goalkeepers-stats.json",
        {"season": 2026},
        "Teams of a season with both team keys: StatNet team id <-> display code (from the stats tables' teams list).",
        {
            "extra_params": [_season(), _phase()],
            "example_args": {"season": 2026},
        },
    ),
    "leaders": (
        "/api/player-leaderboard",
        P.parse_ha_leaders,
        "api__player-leaderboard.json",
        {"metric": "TP"},
        "Current-season top 25 of one metric, with the StatNet player id (the only stats route that has one).",
        {
            "extra_params": [
                _q(
                    "metric",
                    "metric",
                    "Leaderboard metric. Seen on the site: TP, G, A, PlusMinus, Hits, FOPerc, TOI/GP (skaters).",
                    required=True,
                ),
                _phase(),
                _q("team", "team", "Team filter; omitted = the whole league (values not probed)."),
                _q("is_goalkeeper", "isGoalkeeper", "True for a goalkeeper metric.", typ="bool"),
            ],
            "example_args": {"metric": "TP"},
        },
    ),
    "players": (
        "/api/all-players",
        P.parse_ha_players,
        "api__all-players.json",
        {"teamShortName": "AIK"},
        "One team's players from the CMS directory: CMS ids, position, jersey, birth date, size, "
        "headshots. One team per call (league-wide paging is unreliable).",
        {
            "extra_params": [
                _q(
                    "team",
                    "teamShortName",
                    "Team display code (CMS short name: AIK, MORA, ÖIK, ...). Required: the directory pages "
                    "unreliably league-wide.",
                    required=True,
                ),
                _q(
                    "order_by",
                    "orderBy",
                    "Sort: familyName or firstName.",
                    choices=["familyName", "firstName"],
                    default="familyName",
                ),
                _q("search_query", "searchQuery", "Free-text name search; omitted = everyone."),
            ],
            "example_args": {"team": "AIK"},
        },
    ),
}

#: Descriptions of the columns the parsers name or derive (first source consulted).
LOCAL: Dict[str, str] = {
    "season": "Season END year (2026 = 2025/26).",
    "cms_season": "CMS season key as sent (season_2026 = 2026/27: a START year).",
    "game_cms_id": "CMS (Strapi) record id of the game.",
    "game_document_id": "CMS (Strapi) document id of the game.",
    "statnet_game_number": "StatNet game number (23401 = 2026/27 game 1); the play-by-play request key.",
    "home_statnet_id": "StatNet team id of the home team (AIK, MODO, MIK).",
    "away_statnet_id": "StatNet team id of the away team.",
    "team_statnet_id": "StatNet team id (AIK, MODO, MIK, NYB, OSIK), not the display code.",
    "home_team_cms_id": "CMS (Strapi) record id of the home team (81 = AIK).",
    "away_team_cms_id": "CMS (Strapi) record id of the away team.",
    "team_cms_id": "CMS (Strapi) record id of the team (81 = AIK).",
    "home_team_code": "Display code of the home team (AIK, MoDo, MORA, NVIF, ÖIK), not the StatNet id.",
    "away_team_code": "Display code of the away team, not the StatNet id.",
    "team_code": "Display code of the team (AIK, MoDo, MORA, NVIF, ÖIK), not the StatNet id.",
    "team_code_mixed": "Upstream standings teamCode: the StatNet id for some clubs (MODO), the display code for "
    "others (MORA, NVIF, ÖIK). Join on team_code instead.",
    "player_statnet_id": "StatNet player id (the same id space as SHL's isa id, verified on one player).",
    "player_cms_id": "CMS (Strapi) record id of the player (not the StatNet id).",
    "player_document_id": "CMS (Strapi) document id of the player.",
    "player_name": "Player name (the table carries no player id).",
    "is_played": "True when the game has been played (decided_in is not empty).",
    "decided_in": "How the game was decided (FT, OT, SO); empty for a game not yet played.",
    "date": "Game date (YYYY-MM-DD).",
    "arena": "Arena.",
    "attendance": "Attendance; null for an unplayed game (0 for a played one means unknown).",
    "home_score": "Home goals, final.",
    "away_score": "Away goals, final.",
    "period_scores": "Goals per period, JSON list of {Home, Away}; null for an unplayed game.",
    "most_points": "Top point scorers of the game, JSON list of {player, team (StatNet id), points}.",
    "regulation_wins": "Regulation wins only (the team table's wins_total also counts OT and shootout wins).",
    "regulation_losses": "Regulation losses only.",
    "overtime_and_shootout_wins": "Overtime wins, shootout wins included.",
    "overtime_and_shootout_losses": "Overtime losses, shootout losses included.",
    "shootouts_wins": "Shootout wins as sent (0 in every sampled row: they are counted in overtime_and_shootout_wins).",
    "shootouts_losses": "Shootout losses as sent (0 in every sampled row).",
    "wins_total": "Wins in total, overtime and shootout wins included (otw and sow are subsets).",
    "losses_total": "Losses in total, overtime and shootout losses included (otl and sol are subsets).",
    "event_type": "Event type: Shot, Goal, Penalty, GoalkeeperEvent or Timeout.",
    "period": "Period number the event belongs to.",
    "time": "Seconds into the period.",
    "event_description": "Shot outcome, goal strength (EQ), penalty text or timeout note.",
    "is_entering": "Goalkeeper event: true when the goalkeeper enters, false when he leaves.",
    "assist1_name": "First assist, by name (no id).",
    "assist2_name": "Second assist, by name (no id).",
    "assist1_jersey_today": "First assist's jersey number in this game.",
    "assist2_jersey_today": "Second assist's jersey number in this game.",
    "assist1_season_assists": "First assist's season assists after this goal.",
    "assist2_season_assists": "Second assist's season assists after this goal.",
    "scorer_season_goals": "Scorer's season goals after this goal.",
    "running_score": "Score after the goal, home-away.",
    "is_penalty_shot": "Goal scored on a penalty shot.",
    "metric_value": "Value of the metric (a count, a percentage or a clock, as text).",
    "home_team_logo_url": "Logo URL of the home team.",
    "away_team_logo_url": "Logo URL of the away team.",
    "team_logo_url": "Logo URL of the team.",
    "rank": "Rank in the table.",
    "goals": "Goals scored by the team (goals for).",
    "ties": "Ties (0 in the overtime era).",
    "goal_difference": "Goal difference.",
    "height": "Player height in centimetres.",
    "name": "Short club name (the teams list's name).",
    "full_name": "Full club name (the teams list's fullName).",
    "arena": "Arena the game is played in, as the games table names it.",
    "venue": "Arena name on the CMS game record.",
    "round": "Round number of the game in the season schedule.",
    "games_played": "Games played in the season and phase.",
    "gp": "Games played in the season and phase.",
    "goals_against": "Goals conceded by the team.",
    "ga": "Goals against: goals allowed in the season and phase.",
    "total_points": "Standings points of the team.",
    "otw": "Overtime wins (a subset of wins_total).",
    "sow": "Shootout wins (a subset of wins_total).",
    "a": "Assists credited to the skater.",
    "sog": "Shots on goal by the skater.",
    "svs": "Saves made by the goalkeeper.",
    "so": "Shutouts recorded by the goalkeeper.",
    "w": "Wins credited to the goalkeeper.",
    "l": "Losses credited to the goalkeeper.",
    "t": "Ties credited to the goalkeeper (0 in the overtime era).",
    "is_rookie": "True when the CMS directory flags the player as a rookie.",
    "weight": "Player weight in kilograms.",
    "team_logo": "Logo URL of the player's team.",
    "created_at": "CMS creation time of the record (ISO 8601 UTC).",
    "updated_at": "CMS update time of the record (ISO 8601 UTC).",
    "published_at": "CMS publish time of the record (ISO 8601 UTC).",
    "team_document_id": "CMS (Strapi) document id of the team.",
    "team_main_color_hex": "Primary colour of the team (hex).",
    **{f"headshots_{size}": f"Headshot URL ({size} rendition)." for size in ("large", "medium", "small", "thumb")},
    **{
        f"{side}_{col}": f"{side.capitalize()} team: {text}"
        for side in ("home", "away")
        for col, text in (
            ("shots_total", "shots on goal."),
            ("shots_goal", "goals."),
            ("shots_efficiency", "shooting percentage (goals per 100 shots); sent as text, 0 or null."),
            ("powerplay_goals", "power-play goals."),
            ("powerplay_num_pp", "power plays."),
            ("powerplay_efficiency", "power-play percentage; sent as text, 0 or null."),
        )
    },
}
#: Per-wrapper overrides, where one leaf means different things by route.
LOCAL_BY_SHORT: Dict[Tuple[str, str], str] = {
    ("leaders", "slug"): "Player slug; joins ha_players.",
    ("players", "slug"): "Player slug; joins ha_leaders.",
    ("skater_stats", "g"): "Goals scored by the skater.",
    ("team_stats", "g"): "Goals scored by the team.",
}
#: Wire-type remarks in the refs descriptions that the parsers make untrue (they cast the value).
_WIRE_NOTE = re.compile(r",? decimal string| \(string\)| as a string")

#: The refs md describes CMS team fields under the away team; a prefixed column names its own side.
_SIDE_NOTE = re.compile(r" of the (home|away) team| \(the (home|away) team's [^)]*\)")

_PREFIXES = (
    ("home_team_", "Home team: "),
    ("away_team_", "Away team: "),
    ("home_", "Home team: "),
    ("away_", "Away team: "),
    ("player_", "Player: "),
    ("team_", "Team: "),
    ("logos_", "Logo: "),
    ("headshots_", "Headshot: "),
)
_ROW = re.compile(r"^\s*\|\s*`([^`]+)`\s*\|[^|]*\|(.*)\|\s*$")


def _md_leaves(path: Path) -> Dict[str, str]:
    """``{underscored leaf: description}`` from the returns md rows (first definition wins)."""
    out: Dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        m = _ROW.match(line)
        if not m:
            continue
        leaf = m.group(1).split(".")[-1]
        if leaf in ("[]", "<key>") or not leaf:
            continue
        out.setdefault(underscore(leaf).replace("stat_net", "statnet"), m.group(2).strip())
    return out


def _describe(name: str, sources: List[Dict[str, str]]) -> str:
    for src in sources:
        if src.get(name):
            return src[name]
    for prefix, label in _PREFIXES:
        if name.startswith(prefix) and len(name) > len(prefix):
            inner = _SIDE_NOTE.sub("", _describe(name[len(prefix) :], sources))
            if inner:
                head = inner[:1] if inner[1:2].isupper() else inner[:1].lower()
                return label + head + inner[1:]
    return ""


def _frame(entry: Entry) -> Any:
    spec_path, parser, fixture, request, _summary, _fields = entry
    body = json.loads((FIXTURES / fixture).read_text(encoding="utf-8"))
    if request is not None:
        _body, stamp = build_body(spec_path, request)
        if stamp:
            body["_request"] = stamp
    return parser(body)


def _endpoint(short: str, entry: Entry) -> Dict[str, Any]:
    path, parser, _fixture, _request, summary, fields = entry
    names = [p["name"] for p in fields.get("extra_params", [])]
    assert not {"league", "sport"} & set(names), short  # the renderer substitutes those as ESPN slugs
    assert "{" not in path, path
    return {
        "short": short,
        "summary": summary,
        "path": path,
        "parser": parser.__name__,
        "returns_schema": f"native/ha/{short}",
        # The runtime POSTs every route but these two (docstrings say POST / body field).
        **({} if path in ("/api/game", "/pages/matcher") else {"method": "post", "body": "json"}),
        **copy.deepcopy(fields),  # no shared objects, so the YAML carries no anchors
    }


#: Body fields every call sets itself (the spec example carries a sample value).
_PER_CALL = frozenset({"season", "teamShortName"})


def _json_request(ops: Dict[str, Any]) -> Dict[str, Any]:
    return ((ops.get("post") or {}).get("requestBody") or {}).get("content", {}).get("application/json", {})


def spec_snapshot(spec: Dict[str, Any]) -> Dict[str, Any]:
    """What the tests check against the spec: routes, POST request examples, ``categories`` enums."""
    paths = spec["paths"]
    examples = {p: _json_request(ops).get("example") for p, ops in paths.items()}
    categories = {
        p: ((_json_request(ops).get("schema") or {}).get("properties") or {}).get("categories", {}).get("enum")
        for p, ops in paths.items()
    }
    return {
        "routes": sorted(paths),
        "request_examples": {p: v for p, v in sorted(examples.items()) if v},
        "categories": {p: v for p, v in sorted(categories.items()) if v},
    }


def _check_bodies(examples: Dict[str, Dict[str, Any]]) -> None:
    """The runtime's default bodies are the spec's request examples (minus the per-call fields)."""
    for path, example in examples.items():
        example = {k: v for k, v in example.items() if k not in _PER_CALL}
        if example and path != "/api/play-by-play":
            default = {k: v for k, v in BODIES[path].items() if k not in _PER_CALL}
            assert default == example, (path, default, example)


def _write_schema_module(types: Dict[str, str]) -> None:
    lines = [
        '"""Column -> returns-schema type for the ``ha`` parsers (written by tools/codegen/gen_ha.py)."""',
        "",
        "from typing import Dict",
        "",
        "DTYPES: Dict[str, str] = {",
        *(f"    {json.dumps(name, ensure_ascii=False)}: {json.dumps(typ)}," for name, typ in sorted(types.items())),
        "}",
        "",
    ]
    SCHEMA_MODULE.write_text("\n".join(lines), encoding="utf-8", newline="\n")


def main() -> None:
    refs = refs_dir("hockeyallsvenskan", SPEC)
    spec = load_spec(refs, SPEC)
    missing = {e[0] for e in ENDPOINTS.values()} ^ set(spec["paths"])
    if missing:
        raise SystemExit(f"ha: spec paths and ENDPOINTS differ: {sorted(missing)}")
    snapshot = spec_snapshot(spec)
    _check_bodies(snapshot["request_examples"])
    SNAPSHOT.write_text(json.dumps(snapshot, indent=2, ensure_ascii=False) + "\n", encoding="utf-8", newline="\n")
    doc = {
        "api": "ha",
        "host": HOST,
        "name_pattern": "ha_{short}",
        "module": "ha",
        "parser_module": "hockey.ha.ha_parsers",
        "getter_module": "sportsdataverse.hockey.ha.ha_runtime",
        "qualifier": "",
        "passthrough_query": False,
        "parser_columns": True,
        "schema_prefix": "ha_",  # bare shorts (standings, games, players) name other families' descriptions
        "runtime_imports": ["_get"],
        "docstring": {
            "example_import": True,
            "raises": [
                "sportsdataverse.errors.NoDataError: the site has nothing for the request (a season before "
                "2005/06, an unknown or pre-2025/26 game, a phase not played).",
                "ValueError: a value outside its enumerated set, or a field the site requires is missing.",
            ],
            "see_also": [
                {
                    "name": "HockeyAllsvenskan",
                    "url": "https://hockeyallsvenskan.se/",
                    "note": "the site whose own API routes these wrappers read",
                },
                {
                    "name": "fastRhockey",
                    "url": "https://fastRhockey.sportsdataverse.org",
                    "note": "SportsDataverse hockey package (R)",
                },
            ],
        },
        "endpoints": [_endpoint(short, entry) for short, entry in ENDPOINTS.items()],
    }
    write_yaml(ROOT / "tools/codegen/endpoints/ha.yaml", doc)

    sources = [LOCAL, spec_descriptions(spec), _md_leaves(refs / "hockeyallsvenskan-returns.md")]
    schema_dir = ROOT / "tools/codegen/schemas/native/ha"
    rewrite_schema_dir(schema_dir)
    snippet: Dict[str, Dict[str, str]] = {}
    types: Dict[str, str] = {}
    for short, entry in ENDPOINTS.items():
        cols = columns_from_frame(_frame(entry), [], leaf_fallback=False)
        texts = snippet.setdefault(f"ha_{short}", {})
        for col in cols:
            desc = LOCAL_BY_SHORT.get((short, col["name"])) or _describe(col["name"], sources)
            texts[col["name"]] = _WIRE_NOTE.sub("", desc) if col["type"] != "character" else desc
            col["description"] = ""
            assert types.setdefault(col["name"], col["type"]) == col["type"], (short, col)  # one dtype per name
        write_yaml(schema_dir / f"{short}.yaml", {"schema": f"ha_{short}", "kind": "dataframe", "columns": cols})
    write_yaml(SNIPPET, snippet)
    _write_schema_module(types)
    print(f"ha: {len(ENDPOINTS)} endpoints, {sum(map(len, snippet.values()))} descriptions")


if __name__ == "__main__":
    main()
