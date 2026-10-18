"""Generate the two KHL flat-API families from the sdv-internal-refs ``khl/`` specs.

Idempotent: same specs + same fixtures -> byte-identical output. Emits:

* ``tools/codegen/endpoints/khl_rest.yaml`` -- the khl.ru site REST (``POST /rest/<route>/``, form
  bodies). ``/game/text/`` feeds four wrappers (game info, skater box score, play-by-play, shot map).
* ``tools/codegen/endpoints/khl_webcaster.yaml`` -- the webcaster mobile API (``GET <route>.json``).
* ``tools/codegen/schemas/native/khl_{rest,webcaster}/<short>.yaml`` -- one returns-schema per
  endpoint, from running its parser over the committed fixture (``tests/fixtures/khl_*``): the FULL
  body under ``full/`` when there is one (dtypes from complete data), else the refs capture.
  Descriptions are left blank there on purpose.
* ``sportsdataverse/hockey/khl/khl_schemas.py`` -- the same schemas as name:dtype specs, so the
  parsers type a zero-row frame (and backfill a missing column) exactly like a populated one without
  reading ``tools/`` at runtime.
* ``tests/fixtures/khl_{rest,webcaster}/spec_routes.json`` -- the spec's route list, so the YAML
  route test needs no sdv-internal-refs checkout (CI has none).

The column descriptions (refs ``khl-returns.md`` plus the columns the parsers rename or derive) were
merged into ``manual_column_descriptions.yaml`` at integration; edit them there. ``main`` still
builds them, only to report the described count.

The REST wrappers send their form fields as the getter's ``params``: the codegen always renders
``_get(url, params=...)``, and ``sportsdataverse.hockey.khl.khl_runtime._get`` POSTs them as the form.
Every route's wire fields come from the spec's ``requestBody``; a spec route this table does not
wrap (or a wrapped route the spec lacks) fails the build.

Run: ``python tools/codegen/gen_khl.py``
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import yaml

from gen_soccer_common import ROOT, columns_from_frame, load_spec, refs_dir, rewrite_schema_dir, write_yaml

from sportsdataverse.dl_utils import underscore
from sportsdataverse.hockey.khl import khl_rest_parsers as rest
from sportsdataverse.hockey.khl import khl_webcaster_parsers as wc

FIXTURES = ROOT / "tests" / "fixtures"
REST_WIRE = (
    "The documented curl request is schematic: the runtime primes a session (adds the form field ``sessid``), "
    "maps ``season`` / ``stage`` to the form field ``values[tournament]`` (the tournament id) and sends the "
    "X-Requested-With, Origin and Referer headers."
)
SANCTIONS = (
    "Owner flag (sdv-internal-refs/khl/README.md): the KHL and its webcaster platform are run by Russian "
    "operators, some clubs by sanctioned state companies. Fetching on your own machine is fine; any "
    "published data release, hosted mirror or redistribution needs owner (sanctions / legal) sign-off first."
)
SEE_ALSO = [
    {"name": "KHL", "note": "the league site whose front end these routes feed", "url": "https://www.khl.ru/"},
    {
        "name": "fastRhockey",
        "note": "SportsDataverse hockey package (R)",
        "url": "https://fastRhockey.sportsdataverse.org",
    },
]

# --- parameters ---------------------------------------------------------------------------------

SEASON = {
    "name": "season",
    "query_key": "season",
    "type": "int",
    "description": "Season by its END year (2026 = 2025-26), mapped to the KHL tournament id (tnId) through "
    "the webcaster season table (khl_webcaster_stages). None = the site's current season.",
}
STAGE = {
    "name": "stage",
    "query_key": "stage",
    "type": "str",
    "default": "regular",
    "choices": ["regular", "playoff"],
    "description": "Which stage of the season: regular or playoff.",
}
TOURNAMENT = {
    "name": "tournament_id",
    "query_key": "values[tournament]",
    "type": "int",
    "kw_only": True,
    "description": "khl.ru tournament id (tnId), sent as is instead of season / stage (1369 = 2025-26 regular "
    "season, 1370 its playoff; the schedule's tournament_id column). The All-Star games have their own.",
}


def _required(name: str, key: str, desc: str) -> Dict[str, Any]:
    return {"name": name, "query_key": key, "type": "int", "required": True, "description": f"Required. {desc}"}


GAME = _required("game_id", "values[gameid]", "khl.ru game id (the schedule's game_id; equals webcaster event khl_id).")
GAME_SEASON = {**SEASON, "description": SEASON["description"].replace("None = the site's current season.", "")}
GAME_SEASON["description"] += "The game's season; required unless tournament_id is given."
GAME_PARAMS = [GAME, GAME_SEASON, STAGE, TOURNAMENT]
SEASON_PARAMS = [SEASON, STAGE, TOURNAMENT]

# --- khl.ru REST --------------------------------------------------------------------------------

REST_HOST = "https://www.khl.ru/rest"
#: path -> [(short, parser, summary, params, fixed form fields, example args, fixture)]
REST: Dict[str, List[Tuple[str, str, str, List[Dict[str, Any]], Dict[str, Any], Dict[str, Any], str]]] = {
    "/calendar/list/": [
        (
            "schedule",
            "parse_khl_schedule",
            "Season schedule and results of a tournament, one row per game (All-Star games dropped).",
            SEASON_PARAMS,
            {},
            {"season": 2026},
            "calendar__list.json",
        )
    ],
    "/game/header/": [
        (
            "game_header",
            "parse_khl_game_header",
            "Game header: status, teams, score, arena (one row).",
            GAME_PARAMS,
            {"page": "resume"},
            {"game_id": 898236, "season": 2026},
            "game__header.json",
        )
    ],
    "/game/text/": [
        (
            "game_info",
            "parse_khl_game_info",
            "Game info: teams, score, arena, officials, starting goalies (one row).",
            GAME_PARAMS,
            {},
            {"game_id": 898236, "season": 2026},
            "full/game__text.json",
        ),
        (
            "player_box",
            "parse_khl_player_box",
            "Skater box score by period: one row per period, side and jersey, with the player id.",
            GAME_PARAMS,
            {},
            {"game_id": 898236, "season": 2026},
            "full/game__text.json",
        ),
        (
            "pbp",
            "parse_khl_pbp",
            "Play-by-play: shots, faceoffs, hits, blocks, giveaways, takeaways, penalties and goals (2008-09 on).",
            GAME_PARAMS,
            {},
            {"game_id": 898236, "season": 2026},
            "full/game__text.json",
        ),
        (
            "shots",
            "parse_khl_shots",
            "Shot map: one row per shot with its x / y on the 1000 x 500 rink grid (2024-25 on).",
            GAME_PARAMS,
            {},
            {"game_id": 898236, "season": 2026},
            "full/game__text.json",
        ),
    ],
    "/game/protocol/": [
        (
            "game_protocol",
            "parse_khl_game_protocol",
            "Game protocol: one row per goal with scorer, assistants, strength and the jerseys on the ice.",
            GAME_PARAMS,
            {},
            {"game_id": 898236, "season": 2026},
            "game__protocol.json",
        )
    ],
    "/standings/regular/": [
        (
            "standings",
            "parse_khl_standings",
            "Regular-season standings: one row per team per table (league, conferences, divisions).",
            [SEASON, TOURNAMENT],
            {"values[type]": "regular"},
            {"season": 2026},
            "standings__regular.json",
        )
    ],
    "/standings/playoff/": [
        (
            "playoff_bracket",
            "parse_khl_playoff_bracket",
            "Playoff bracket: one row per team per series.",
            [
                {
                    **SEASON,
                    "description": SEASON["description"].replace(
                        "None = the site's current season.", "Required unless tournament_id is given."
                    ),
                },
                TOURNAMENT,
            ],
            {"values[type]": "playoff", "stage": "playoff"},
            {"season": 2026},
            "standings__playoff.json",
        )
    ],
    "/stat/players/": [
        (
            "player_stats",
            "parse_khl_player_stats",
            "League player stats of a tournament, one page (one row per player).",
            [
                *SEASON_PARAMS,
                {
                    "name": "position",
                    "query_key": "values[amplua_full]",
                    "type": "str",
                    "choices": ["goalies", "defenses", "forwards", "skaters", "all"],
                    "description": "Position group: goalies, defenses, forwards, skaters or all. None = the site's default.",
                },
                {
                    "name": "club",
                    "query_key": "values[clubs_full]",
                    "type": "str",
                    "description": "Club slug (khl_stat_filters, filter clubs_full) or all. None = every club.",
                },
                {
                    "name": "page",
                    "query_key": "nav[page]",
                    "type": "int",
                    "default": 1,
                    "description": "1-based page. Loop until a page comes back shorter than per_page.",
                },
                {
                    "name": "per_page",
                    "query_key": "nav[count]",
                    "type": "int",
                    "default": 20,
                    "kw_only": True,
                    "description": "Rows per page: 12, 20, 24 or 36. Keep 20: the site computes page counts and ranks for 20.",
                },
            ],
            {},
            {"season": 2026, "position": "forwards"},
            "stat__players.json",
        )
    ],
    "/players/profile/": [
        (
            "player_profile",
            "parse_khl_player_profile",
            "Player profile: bio, contract, club (one row).",
            [_required("player_id", "values[id]", "khl.ru player id (the box score / roster player_id).")],
            {},
            {"player_id": 41928},
            "players__profile.json",
        )
    ],
    "/players/stats/": [
        (
            "player_career",
            "parse_khl_player_career",
            "Player career: one row per season tournament.",
            [_required("player_id", "values[id]", "khl.ru player id (the box score / roster player_id).")],
            {},
            {"player_id": 41928},
            "players__stats.json",
        )
    ],
    "/clubs/team/": [
        (
            "roster",
            "parse_khl_roster",
            "Club roster (current season only): one row per player.",
            [
                _required(
                    "club_id",
                    "values[club_id]",
                    "khl.ru CLUB id (7 = Spartak; schedule home_club_id), not a team-season id.",
                )
            ],
            {"page": "team"},
            {"club_id": 7},
            "clubs__team.json",
        )
    ],
    "/filters/statplayers/": [
        (
            "stat_filters",
            "parse_khl_stat_filters",
            "Stats filter options: seasons, tournaments, position groups and clubs (one row per option).",
            SEASON_PARAMS,
            {},
            {"season": 2026},
            "filters__statplayers.json",
        )
    ],
}

# --- webcaster ----------------------------------------------------------------------------------

WC_HOST = "https://khl.api.webcaster.pro/api/khl_mobile"
LOCALE = {
    "name": "locale",
    "query_key": "locale",
    "type": "str",
    "default": "en",
    "choices": ["en", "ru"],
    "kw_only": True,
    "description": "Response language: en or ru.",
}
WC_STAGE_ID = {
    "name": "stage_id",
    "query_key": "stage_id",
    "type": "int",
    "kw_only": True,
    "description": "Webcaster stage id, sent as is instead of season / stage (370 = 2025-26 regular season; "
    "khl_webcaster_stages lists them). Not a khl.ru tournament id.",
}
WC_SEASON = {
    **SEASON,
    "description": SEASON["description"]
    .replace("tournament id (tnId)", "webcaster stage id")
    .replace("the site's current season", "the current stage"),
}
WC_SEASON_PARAMS = [WC_SEASON, STAGE, WC_STAGE_ID]
#: path -> (short, parser, summary, params, example args, fixture)
WEBCASTER: Dict[str, Tuple[str, str, str, List[Dict[str, Any]], Dict[str, Any], str]] = {
    "/data.json": (
        "stages",
        "parse_khl_webcaster_stages",
        "Season table: every stage (season END year, regular / playoff) with its khl.ru tournament id and webcaster stage id.",
        [],
        {},
        "full/data.json",
    ),
    "/events_v2.json": (
        "events",
        "parse_khl_webcaster",
        "Games of a stage, 16 per page (one row per game; khl_id is the khl.ru game id).",
        [
            *WC_SEASON_PARAMS,
            {
                "name": "page",
                "query_key": "page",
                "type": "int",
                "default": 1,
                "description": "1-based page of 16 games; an empty page ends the stage.",
            },
            {
                "name": "team_ids",
                "query_key": "q[team_a_or_team_b_in][]",
                "type": "int|List[int]",
                "kw_only": True,
                "description": "Webcaster team id(s) (khl_webcaster_teams id, not a khl.ru club id): games of these teams.",
            },
            {
                "name": "start_after",
                "query_key": "q[start_at_gt_time_from_unixtime]",
                "type": "int",
                "kw_only": True,
                "description": "Games starting after this unix time (seconds). Needs a stage.",
            },
            {
                "name": "start_before",
                "query_key": "q[start_at_lt_time_from_unixtime]",
                "type": "int",
                "kw_only": True,
                "description": "Games starting before this unix time (seconds). Needs a stage.",
            },
        ],
        {"season": 2026},
        "events_v2.json",
    ),
    "/event_v2.json": (
        "event",
        "parse_khl_webcaster",
        "One game: officials, team totals, per-player match stats, goals, penalties (one row; lists as JSON text).",
        [_required("event_id", "id", "Webcaster event id (khl_webcaster_events id), not the khl.ru game id.")],
        {"event_id": 2786714},
        "event_v2.json",
    ),
    "/teams_v2.json": (
        "teams",
        "parse_khl_webcaster",
        "Teams of a stage (one row per team; khl_id is the khl.ru club id).",
        WC_SEASON_PARAMS,
        {"season": 2026},
        "teams_v2.json",
    ),
    "/team_v2.json": (
        "team",
        "parse_khl_webcaster",
        "One team with roster, stats and news feed (one row; lists as JSON text).",
        [
            _required("team_id", "id", "Webcaster team id (khl_webcaster_teams id), not the khl.ru club id."),
            *WC_SEASON_PARAMS,
        ],
        {"team_id": 18, "season": 2026},
        "team_v2.json",
    ),
    "/players_v2.json": (
        "players",
        "parse_khl_webcaster",
        "Players with stage-scoped stats (one row per player; khl_id is the khl.ru player id).",
        [
            {
                "name": "player_ids",
                "query_key": "q[id_in][]",
                "type": "int|List[int]",
                "description": "Webcaster player id(s), not khl.ru ids. None = every player of the stage (a large body).",
            },
            *WC_SEASON_PARAMS,
        ],
        {"player_ids": [33405, 1144], "season": 2026},
        "players_v2.json",
    ),
    "/players_v2_light.json": (
        "players_light",
        "parse_khl_webcaster",
        "Every current player: webcaster id, khl.ru id, jersey and team (one row per player).",
        [],
        {},
        "players_v2_light.json",
    ),
    "/leaders_v2.json": (
        "leaders",
        "parse_khl_webcaster_leaders",
        "Stat leaders of a stage: one row per player per category.",
        WC_SEASON_PARAMS,
        {"season": 2026},
        "leaders_v2.json",
    ),
    "/tables_v2.json": (
        "tables",
        "parse_khl_webcaster_tables",
        "League table of every season (one row per season per team, from the championship table).",
        [],
        {},
        "tables_v2.json",
    ),
}

# --- descriptions -------------------------------------------------------------------------------

#: Columns the parsers rename or derive (the returns doc names the wire field, not these).
DERIVED = {
    "game_id": "khl.ru game id; equals webcaster event khl_id / match_id",
    "tournament_id": "khl.ru tournament (stage) id, tnId: 1369 = 2025-26 regular season, 1370 its playoff; equals webcaster stage khl_id",
    "home_club_id": "Home CLUB id (stable across seasons; joins webcaster team khl_id)",
    "away_club_id": "Visitor CLUB id (stable across seasons; joins webcaster team khl_id)",
    "home_team_season_id": "Home team-season id (one per tournament; joins nothing in the webcaster API)",
    "away_team_season_id": "Visitor team-season id (one per tournament; joins nothing in the webcaster API)",
    "arena_id": "khl.ru arena id",
    "home_score": "Home goals (final, shootout winner counted)",
    "away_score": "Visitor goals (final, shootout winner counted)",
    "home_name": "Home team name (Russian on www.khl.ru)",
    "away_name": "Visitor team name (Russian on www.khl.ru)",
    "arena_name": "Arena (city) name, from data.ARENAS",
    "decision": "OT or SO when decided in overtime / a shootout; null in regulation",
    "period_scores": "Per-period home:away scores, comma-joined",
    "winner_club_id": "Club id of the winner",
    "gk_a_jn": "Home starting goalie's JERSEY number (gameItem.gkA), not an id",
    "gk_b_jn": "Visitor starting goalie's JERSEY number (gameItem.gkB), not an id",
    "gk_a_player_id": "khl.ru player id of the home starting goalie (gk_a_jn resolved through players)",
    "gk_b_player_id": "khl.ru player id of the visitor starting goalie (gk_b_jn resolved through players)",
    "period": "Period number (box score: 0 = the whole game; 4+ = overtime)",
    "team_side": "A = home, B = visitor",
    "jn": "Jersey number (not an id; 0 on a team event)",
    "player_id": "khl.ru player id; equals webcaster player khl_id",
    "player_name": "Player name as the site prints it (Russian on www.khl.ru)",
    "position": "Position code (Russian letter on www.khl.ru)",
    "event_index": "0-based order of the event in the game, oldest first",
    "event_type": "period (start / end marker), faceoff, shot, block, action, penalty or goal",
    "time": "Game clock mm:ss (game-elapsed); period marker rows keep their wall-clock HH:MM",
    "time_seconds": "Game-elapsed seconds (null on period marker rows)",
    "penalty_minutes": "Penalty minutes (penalty events)",
    "score_a": "Home goals after the goal",
    "score_b": "Visitor goals after the goal",
    "won_side": "Faceoff winner's side (A / B)",
    "won_jn": "Faceoff winner's jersey",
    "won_player_id": "Faceoff winner's khl.ru player id",
    "lost_side": "Faceoff loser's side (A / B)",
    "lost_jn": "Faceoff loser's jersey",
    "lost_player_id": "Faceoff loser's khl.ru player id",
    "won_zone": "Zone of the faceoff from the winner's view: offensive, neutral or defensive",
    "shooter_side": "Blocked shooter's side (A / B)",
    "shooter_jn": "Blocked shooter's jersey",
    "shooter_player_id": "Blocked shooter's khl.ru player id",
    "period_action": "Period marker: 0 = start, 1 = end (inferred)",
    "result": "Event result code (shots: goal / saved / miss; actions: hit, gva = giveaway, tka = takeaway, intc = interception)",
    "result_text": "Event result label",
    "shot_type": "1 = goal, 2 = saved (inferred upstream; unverified)",
    "x": "Shot x on the site's 1000-wide rink grid (indent_left = x/10 %); whether shots are normalised to one "
    "attacking direction is UNVERIFIED",
    "y": "Shot y on the site's 500-high rink grid (indent_top = y/5 %); orientation UNVERIFIED",
    "indent_left": "Shot position as percent from the left edge",
    "indent_top": "Shot position as percent from the top edge",
    "indent_right": "Shot position as percent from the right edge",
    "indent_bottom": "Shot position as percent from the bottom edge",
    "goal_number": "Goal number in the game",
    "strength": "Strength code as the site prints it (Russian: РАВ even, БОЛ power play, МЕН short-handed)",
    "strength_text": "Strength label as the site prints it",
    "on_ice_a": "Home jerseys on the ice, comma-separated",
    "on_ice_b": "Visitor jerseys on the ice, comma-separated",
    **{
        f"{who}_{col}": f"{label} {text}"
        for who, label in (
            ("scorer", "Scorer's"),
            ("assist_1", "First assistant's"),
            ("assist_2", "Second assistant's"),
        )
        for col, text in (
            ("player_id", "khl.ru player id"),
            ("name", "name with the season count in brackets"),
            ("jn", "jersey"),
        )
    },
    "table": "Standings table: league, a conference (west / east) or a division key",
    "rank": "1-based position in its table / list",
    "club_id": "khl.ru CLUB id (stable across seasons; joins webcaster team khl_id)",
    "goals_for": "Goals scored by the team",
    "goals_against": "Goals allowed by the team",
    "round": "Playoff round number",
    "wins_to_advance": "Wins needed to win a series of the round",
    "pair": "Series number within the round",
    "conference": "Conference (west / east)",
    "seed": "Conference seed",
    "series_wins": "Wins in the series so far",
    "game_ids": "khl.ru game ids of the series, comma-joined",
    "group": "List the row comes from (position group / roster section)",
    "season_label": "Season label as sent (26/27, 2025/2026)",
    "team_season_id": "Team-season id (tid; one per tournament; joins nothing in the webcaster API)",
    "team_name": "Team name as the site prints it",
    "filter": "Filter code: season, tournament, amplua_full or clubs_full",
    "filter_name": "Filter label as the site prints it",
    "value": "Option code to send for the filter (a tournament id, a season code such as s19, a club slug)",
    "season": "Season END year (2026 = 2025-26)",
    "stage_id": "Webcaster stage id (its own id space; not a khl.ru tournament id)",
    "category": "Leader category",
}
#: The quality bar of tests/codegen/test_manual_descriptions.py::test_no_filler_descriptions.
_FILLER = re.compile(r"^(the\s+)?\w+(\s+\w+)?\s+(column|field|value|id|name)\.?$", re.I)


def _useful(col: str, desc: str) -> bool:
    text = (desc or "").strip()
    return len(text) >= 15 and text.lower() != col.lower() and not _FILLER.match(text)


assert all(_useful(c, d) for c, d in DERIVED.items()), [c for c, d in DERIVED.items() if not _useful(c, d)]
_SECTION = re.compile(r"^## `(?:GET|POST) (\S+)`")
_ROW = re.compile(r"^\|\s*`([^`]+)`\s*\|[^|]*\|\s*(.*?)\s*\|\s*$")


def returns_descriptions(path: Path) -> Dict[str, Dict[str, str]]:
    """``{route: {snake suffix: description}}`` from ``khl-returns.md``.

    Every suffix of a documented path is a key (``[].event.team_a.khl_id`` -> ``team_a_khl_id`` and
    ``khl_id``), so a flattened column finds its own row before a bare leaf of another object does.
    """
    out: Dict[str, Dict[str, str]] = {}
    route: Optional[str] = None
    for line in path.read_text(encoding="utf-8").splitlines():
        sec = _SECTION.match(line)
        if sec:
            route = sec.group(1)
            out[route] = {}
            continue
        row = _ROW.match(line)
        if not route or not row or not row.group(2):
            continue
        segs = [s for s in row.group(1).split(".") if s and not s.startswith(("[", "<"))]
        for i in range(len(segs)):
            key = "_".join(underscore(s) for s in segs[i:])
            out[route].setdefault(key, row.group(2))
    return out


def _schema(
    fixture: Path, parser: Callable[..., Any], key: str, route_docs: Dict[str, str], notes: Dict[str, Dict[str, str]]
) -> Dict[str, Any]:
    frame = parser(json.loads(fixture.read_text(encoding="utf-8")))
    assert frame.height, f"{fixture.name}: {parser.__name__} parsed no rows"
    cols = columns_from_frame(frame, [], leaf_fallback=False)
    descs = {c["name"]: DERIVED.get(c["name"]) or route_docs.get(c["name"], "") for c in cols}
    notes[key] = {k: v for k, v in descs.items() if _useful(k, v)}
    return {"schema": key, "kind": "dataframe", "columns": cols}


SCHEMA_MODULE = ROOT / "sportsdataverse" / "hockey" / "khl" / "khl_schemas.py"
_TYPE_CODE = {"integer": "i", "numeric": "f", "character": "s", "logical": "b"}


def write_schema_module() -> None:
    """``khl_schemas.py``: every returns schema as ``"col:code ..."`` (codes i / f / s / b)."""
    out = [
        '"""KHL returns schemas as ``"column:code"`` specs (i Int64, f Float64, s Utf8, b Boolean).',
        "",
        "Generated by ``tools/codegen/gen_khl.py`` from ``tools/codegen/schemas/native/khl_*``; do not edit.",
        "The parsers type zero-row frames and backfilled columns from these, so an empty answer concatenates",
        "with a populated one.",
        '"""',
        "",
        "from typing import Dict",
        "",
        "SCHEMAS: Dict[str, str] = {",
    ]
    for path in sorted(ROOT.glob("tools/codegen/schemas/native/khl_*/*.yaml")):
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
        tokens = [f"{c['name']}:{_TYPE_CODE[c['type']]}" for c in doc["columns"]]
        lines, line = [], ""
        for tok in tokens:
            if line and len(line) + len(tok) + 1 > 96:
                lines.append(line + " ")
                line = tok
            else:
                line = f"{line} {tok}" if line else tok
        lines.append(line)
        out.append(f'    "{doc["schema"]}": (')
        out.extend(f'        "{chunk}"' for chunk in lines)
        out.append("    ),")
    out.append("}")
    SCHEMA_MODULE.write_text("\n".join(out) + "\n", encoding="utf-8", newline="\n")
    subprocess.run([sys.executable, "-m", "ruff", "format", "-q", str(SCHEMA_MODULE)], check=True)


def write_spec_routes(family: str, spec: dict) -> None:
    """The spec's route list, committed next to the fixtures (the tests read it, not the refs)."""
    path = FIXTURES / family / "spec_routes.json"
    path.write_text(json.dumps(sorted(spec["paths"]), indent=1) + "\n", encoding="utf-8", newline="\n")


def _docstring(raw_doc: str, raises: List[str], headers_doc: str = "", extra_notes: tuple = ()) -> Dict[str, Any]:
    doc: Dict[str, Any] = {
        "example_import": True,
        "raw_doc": raw_doc,
        "raises": raises,
        "see_also": SEE_ALSO,
        "notes": [SANCTIONS, *extra_notes],
    }
    if headers_doc:
        doc["headers_doc"] = headers_doc
    return doc


def _spec_paths(spec: dict, method: str) -> List[str]:
    return [p for p, ops in spec["paths"].items() if method in ops]


def build_rest(refs: Path, docs: Dict[str, Dict[str, str]], notes: Dict[str, Dict[str, str]]) -> int:
    spec = load_spec(refs, "khl-rest.openapi.yaml")
    assert sorted(_spec_paths(spec, "post")) == sorted(REST), "khl-rest spec routes != gen_khl.REST"
    write_spec_routes("khl_rest", spec)
    schema_dir = ROOT / "tools/codegen/schemas/native/khl_rest"
    rewrite_schema_dir(schema_dir)
    endpoints = []
    for path in sorted(REST):
        fields = spec["paths"][path]["post"]["requestBody"]["content"]["application/x-www-form-urlencoded"]["schema"][
            "properties"
        ]
        for short, parser, summary, params, fixed, example, fixture in REST[path]:
            sent = {p["query_key"] for p in params} | set(fixed)
            unknown = sent - set(fields) - {"season", "stage"}
            assert not unknown, f"{path}: {sorted(unknown)} not in the spec's form fields"
            assert not {"league", "sport"} & {p["name"] for p in params}, path
            entry: Dict[str, Any] = {
                "short": short,
                "summary": summary,
                "path": path,
                "parser": parser,
                "returns_schema": f"native/khl_rest/{short}",
                "method": "post",
                "body": "form",  # khl_runtime._get POSTs ``data=`` as the form
                "example_args": example,
            }
            if params:
                entry["extra_params"] = params
            if fixed:
                entry["fixed_params"] = fixed
            endpoints.append(entry)
            write_yaml(
                schema_dir / f"{short}.yaml",
                _schema(
                    FIXTURES / "khl_rest" / fixture, getattr(rest, parser), f"khl_{short}", docs.get(path, {}), notes
                ),
            )
    doc = {
        "api": "khl_rest",
        "host": REST_HOST,
        "name_pattern": "khl_{short}",
        "module": "khl_rest",
        "parser_module": "hockey.khl.khl_rest_parsers",
        "getter_module": "sportsdataverse.hockey.khl.khl_runtime",
        "auth": True,
        "qualifier": "",
        "passthrough_query": False,
        "parser_columns": True,
        "schema_prefix": "khl_",
        "runtime_imports": ["_get"],
        "docstring": _docstring(
            "the raw khl.ru envelope ``Dict`` (``{status, data, errors}``)",
            [
                "sportsdataverse.errors.AssetFetchError: khl.ru answered an error envelope (status != success), "
                "a 307 again after re-priming the session, or a page for another game / player / tournament than "
                "the one requested.",
                "sportsdataverse.errors.NoDataError: khl.ru answered no rows, or the season has no such stage.",
            ],
            "extra request headers merged over the defaults (X-Requested-With, Origin, Referer); the session "
            "cookie jar and sessid are kept in-process, so nothing needs reusing across calls.",
            extra_notes=(REST_WIRE,),
        ),
        "endpoints": endpoints,
    }
    write_yaml(ROOT / "tools/codegen/endpoints/khl_rest.yaml", json.loads(json.dumps(doc)))  # no YAML anchors
    return len(endpoints)


def build_webcaster(refs: Path, docs: Dict[str, Dict[str, str]], notes: Dict[str, Dict[str, str]]) -> int:
    spec = load_spec(refs, "khl-webcaster.openapi.yaml")
    assert sorted(_spec_paths(spec, "get")) == sorted(WEBCASTER), "khl-webcaster spec routes != gen_khl.WEBCASTER"
    write_spec_routes("khl_webcaster", spec)
    schema_dir = ROOT / "tools/codegen/schemas/native/khl_webcaster"
    rewrite_schema_dir(schema_dir)
    endpoints = []
    for path in sorted(WEBCASTER):
        short, parser, summary, params, example, fixture = WEBCASTER[path]
        wire = {p["name"] for p in spec["paths"][path]["get"].get("parameters", [])}
        unknown = {p["query_key"] for p in params} - wire - {"season", "stage"}
        assert not unknown, f"{path}: {sorted(unknown)} not in the spec's parameters"
        entry: Dict[str, Any] = {
            "short": short,
            "summary": summary,
            "path": path,
            "extra_params": [*params, LOCALE],
            "parser": parser,
            "returns_schema": f"native/khl_webcaster/{short}",
            "example_args": example,
        }
        endpoints.append(entry)
        write_yaml(
            schema_dir / f"{short}.yaml",
            _schema(
                FIXTURES / "khl_webcaster" / fixture,
                getattr(wc, parser),
                f"khl_webcaster_{short}",
                docs.get(path, {}),
                notes,
            ),
        )
    doc = {
        "api": "khl_webcaster",
        "host": WC_HOST,
        "name_pattern": "khl_webcaster_{short}",
        "module": "khl_webcaster",
        "parser_module": "hockey.khl.khl_webcaster_parsers",
        "getter_module": "sportsdataverse.hockey.khl.khl_webcaster_runtime",
        "qualifier": "",
        "passthrough_query": False,
        "parser_columns": True,
        "schema_prefix": "khl_webcaster_",
        "runtime_imports": ["_get"],
        "docstring": _docstring(
            "the raw JSON body (betting fields and the caller's IP removed)",
            [
                "sportsdataverse.errors.NoDataError: an error envelope with code 404 (unknown id), an empty "
                "object, or an unknown season.",
                "sportsdataverse.errors.AssetFetchError: a body for another id / stage / player than the one "
                "requested, or any other error envelope.",
            ],
        ),
        "endpoints": endpoints,
    }
    write_yaml(ROOT / "tools/codegen/endpoints/khl_webcaster.yaml", json.loads(json.dumps(doc)))  # no YAML anchors
    return len(endpoints)


def main() -> None:
    refs = refs_dir("khl", "khl-rest.openapi.yaml")
    docs = returns_descriptions(refs / "khl-returns.md")
    notes: Dict[str, Dict[str, str]] = {}
    n_rest = build_rest(refs, docs, notes)
    n_wc = build_webcaster(refs, docs, notes)
    write_schema_module()
    described = sum(1 for cols in notes.values() for v in cols.values() if v)
    print(f"khl: {n_rest} khl_rest + {n_wc} khl_webcaster endpoints, {described} columns described")


if __name__ == "__main__":
    main()
