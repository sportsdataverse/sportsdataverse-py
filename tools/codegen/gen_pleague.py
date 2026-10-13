"""Generate the ``pleague`` and ``pleague_preciser`` flat-API endpoint YAMLs + returns-schemas (P. LEAGUE+, Taiwan).

Idempotent: same specs + same fixtures -> byte-identical output. Modeled on ``gen_aba.py``. One script writes BOTH
stems (the site and the Preciser vendor share the clubs table, the descriptions and the fixtures layout). Reads the two
frozen specs from the ``sdv-internal-refs`` checkout (``$SDV_INTERNAL_REFS_REPO``, else the sibling checkout) and emits:

* ``tools/codegen/endpoints/pleague.yaml`` -- 11 wrappers over the site's 10 routes (the four schedule routes are one
  wrapper with a ``stage`` argument; the game page feeds ``game``, ``periods`` and ``officials``; the box feeds
  ``box_players`` and ``box_teams``; the player page feeds ``player`` and ``player_seasons``).
* ``tools/codegen/endpoints/pleague_preciser.yaml`` -- 7 wrappers over the vendor's 4 routes (the summary feeds four).
* ``tools/codegen/schemas/native/{pleague,pleague_preciser}/<short>.yaml`` -- one returns-schema per wrapper: the
  columns of its parser run over its committed fixture. Each parser emits a fixed column list with dtypes pinned by
  name (``SPECS``), so a fixture cannot mistype a rate. The schemas carry ``description: ''`` (descriptions never live
  in ``schemas/**``).
* ``tools/codegen/notes/plg_descriptions.yaml`` -- ``{pleague_<short>: {column: text}}`` and
  ``{pleague_preciser_<short>: ...}``, the block for ``manual_column_descriptions.yaml`` (Integration merges it).
  Traditional Chinese values are translated there; the ``action`` / ``action_zh`` columns carry the 82 observed
  play-by-play phrases with their counts. A filler or missing description stops the run.
* ``tests/fixtures/{pleague,pleague_preciser}/spec_paths.json`` -- the specs' routes, for the tests (CI has no refs).

Run: ``python tools/codegen/gen_pleague.py``
"""

from __future__ import annotations

import copy
import json
import re
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple

import polars as pl
from gen_soccer_common import ROOT, columns_from_frame, load_spec, refs_dir, rewrite_schema_dir, write_yaml

from sportsdataverse.basketball.plg import pleague_parsers as P
from sportsdataverse.basketball.plg import pleague_preciser_parsers as V
from sportsdataverse.basketball.plg.pleague_preciser_runtime import HOST as PRECISER_HOST
from sportsdataverse.basketball.plg.pleague_runtime import HOST as SITE_HOST

SITE_SPEC = "pleague.openapi.yaml"
PRECISER_SPEC = "pleague-preciser.openapi.yaml"
FIX = ROOT / "tests" / "fixtures"
SITE_FIX = FIX / "pleague"
VENDOR_FIX = FIX / "pleague_preciser"
#: Column descriptions, keyed ``pleague_<short>`` / ``pleague_preciser_<short>`` -> column.
NOTES = ROOT / "tools" / "codegen" / "notes" / "plg_descriptions.yaml"
#: What tests/codegen/test_manual_descriptions.py rejects as filler.
_BANNED = re.compile(r"^(the\s+)?\w+(\s+\w+)?\s+(column|field|value|id|name)\.?$", re.IGNORECASE)

# ---------------------------------------------------------------------------------------------- parameters
SEASON = (
    "Required. Season END year (2025 = the site's slug '2024-25'). ``pleague_runtime`` sends the slug "
    "``f'{season - 1}-{season % 100:02d}'``. The first season is 2020-21 (``season=2021``); earlier slugs redirect."
)
GID = (
    "Required. P. LEAGUE+ game id on the site: ONE global integer sequence over every stage and season "
    "(197 = the 2022-23 opener, 662 = the 2025-26 opener), e.g. 197; ``gid`` in pleague_schedule. Only finished games "
    "are served."
)
_SEASON_P = {"name": "season", "type": "int", "required": True, "description": SEASON}
_GID_P = {"name": "gid", "type": "int", "required": True, "description": GID}
_GID_Q = {"name": "gid", "query_key": "gid", "type": "int", "required": True, "description": GID}
_STAGE_Q = {
    "name": "stage",
    "query_key": "stage",
    "type": "str",
    "default": "regular-season",
    "choices": ["pre-season", "regular-season", "playoffs", "finals"],
    "description": (
        "Stage: pre-season (熱身賽, exhibition games), regular-season (例行賽), playoffs (季後賽) or finals "
        "(總冠軍賽). A stage a season does not have (2025-26 pre-season) answers NoDataError."
    ),
}
_STAGE_SN_P = {
    "name": "stage_sn",
    "type": "int",
    "required": True,
    "description": "Required. Stage select value of the player table: 2 = regular season, 3 = playoffs, 4 = finals.",
}
_PID_P = {
    "name": "player_page_id",
    "type": "str",
    "required": True,
    "description": (
        "Required. The 8-character page id of /player/{id} (e.g. '1JbPozLB' = Lai Ting-En); ``player_page_id`` in "
        "pleague_stat_player, ``random_id`` in pleague_box_players from 2022-23 (unreliable before)."
    ),
}
_UUID_P = {
    "name": "uuid",
    "type": "str",
    "required": True,
    "description": (
        "Required. The Preciser game uuid the site's game page embeds (``uuid`` of pleague_game; empty before "
        "2022-23, which have no vendor data), e.g. 'e6f98675-3e5b-11ed-949e-5d17350689cf' (gid 197)."
    ),
}
_VERIFY_Q = {
    "name": "gid",
    "query_key": "gid",
    "type": "int",
    "description": (
        "Optional. The site's game id for this uuid. When given the runtime also fetches the site's game page and box "
        "(cached) and checks the vendor body against them (uuid, final score, made shots, roster and minutes); the "
        "uuid must be the game page's. Without it only the checks the body allows by itself run."
    ),
}


def _fields(path_params: List[Dict[str, Any]], extra: List[Dict[str, Any]], example: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {"example_args": example}
    if path_params:
        out["path_params"] = [dict(p) for p in path_params]
    if extra:
        out["extra_params"] = [dict(p) for p in extra]
    return out


# ---------------------------------------------------------------------------------------------- fixtures
def _json(root: Path, name: str) -> Any:
    return json.loads((root / name).read_text(encoding="utf-8"))


def _html(name: str) -> str:
    return (SITE_FIX / name).read_text(encoding="utf-8")


def _record(kind: str, name: str, **want: Any) -> Callable[[], Dict[str, Any]]:
    return lambda: P.record_for(kind, _html(name), **want)


def _stamped(name: str, gid: int) -> Callable[[], Dict[str, Any]]:
    def load() -> Dict[str, Any]:
        body = _json(SITE_FIX, name)
        body[P._REQUEST] = {"gid": gid}
        return body

    return load


def _vendor_fixture(name: str, uuid: str = "e6f98675-3e5b-11ed-949e-5d17350689cf") -> Callable[[], Dict[str, Any]]:
    def load() -> Dict[str, Any]:
        body = _json(VENDOR_FIX, name)
        return {"data": body[0], "_request": {"kind": "x", "uuid": uuid, "verified": False, "gid": None}}

    return load


# short -> (spec paths, wrapper path, parser, fixture loader, summary, fields)
Entry = Tuple[Tuple[str, ...], str, Callable[..., Any], Callable[[], Dict[str, Any]], str, Dict[str, Any]]
_SCHEDULES = tuple(f"/schedule-{s}/{{season}}" for s in ("finals", "playoffs", "pre-season", "regular-season"))
_GAME_SPEC = ("/game/{gid}",)
_BOX_SPEC = ("/api/boxscore.preciser.php",)
_PLAYER_SPEC = ("/player/{player_page_id}",)


def _game206() -> Dict[str, Any]:
    return P.record_for("game", _html("game_206.html"), gid=206)


SITE: Dict[str, Entry] = {
    "schedule": (
        _SCHEDULES,
        "/schedule/{season}",
        P.parse_pleague_schedule,
        _record("schedule", "schedule_regular_2022-23.html", stage="regular-season", season="2022-23"),
        "Schedule and results of one season and stage (pre-season, regular season, playoffs or finals): one row per "
        "game, away team first.",
        _fields([_SEASON_P], [_STAGE_Q], {"season": 2025}),
    ),
    "game": (
        _GAME_SPEC,
        "/game/{gid}",
        P.parse_pleague_game,
        _game206,
        "Header of one finished game: date, venue, both clubs with their score and the Preciser game uuid.",
        _fields([_GID_P], [], {"gid": 197}),
    ),
    "periods": (
        _GAME_SPEC,
        "/game/{gid}",
        P.parse_pleague_periods,
        _game206,
        "Points each team scored in each quarter and overtime of one finished game.",
        _fields([_GID_P], [], {"gid": 197}),
    ),
    "officials": (
        _GAME_SPEC,
        "/game/{gid}",
        P.parse_pleague_officials,
        _game206,
        "The referee and the two umpires printed under the header of one finished game (names only).",
        _fields([_GID_P], [], {"gid": 197}),
    ),
    "box_players": (
        _BOX_SPEC,
        "/api/boxscore.preciser.php",
        P.parse_pleague_box_players,
        _stamped("box_197.json", 197),
        "Player box score of one finished game: minutes, points, shooting splits, rebounds, assists, plus/minus, and "
        "the ratings from 2022-23; did-not-play rows included.",
        _fields([], [_GID_Q], {"gid": 197}),
    ),
    "box_teams": (
        _BOX_SPEC,
        "/api/boxscore.preciser.php",
        P.parse_pleague_box_teams,
        _stamped("box_197.json", 197),
        "Team totals of one finished game: the box score's total row, home then away.",
        _fields([], [_GID_Q], {"gid": 197}),
    ),
    "pbp": (
        ("/api/playbyplay.php",),
        "/api/playbyplay.php",
        P.parse_pleague_pbp,
        _stamped("full/pbp_197.json", 197),
        "Play-by-play of one finished game (2022-23 on) in game order, with shot x / y and the clip references.",
        _fields([], [_GID_Q], {"gid": 197}),
    ),
    "standings": (
        ("/standings/{season}",),
        "/standings/{season}",
        P.parse_pleague_standings,
        _record("standings", "standings_2022-23.html", season="2022-23"),
        "Regular-season standings of one season: record, scoring averages, home / away and recent form.",
        _fields([_SEASON_P], [], {"season": 2025}),
    ),
    "stat_player": (
        ("/stat-player/{season}/{stage_sn}",),
        "/stat-player/{season}/{stage_sn}",
        P.parse_pleague_stat_player,
        _record("stat_player", "stat_player_2022-23_2.html", season="2022-23", stage_sn=2),
        "Player season averages and totals of one season and stage: shooting, rebounds, assists and fouls.",
        _fields([_SEASON_P, _STAGE_SN_P], [], {"season": 2025, "stage_sn": 2}),
    ),
    "player": (
        _PLAYER_SPEC,
        "/player/{player_page_id}",
        P.parse_pleague_player,
        _record("player", "player_1JbPozLB.html", pid="1JbPozLB"),
        "One player's profile: names, club, jersey, position, height, weight and birthplace.",
        _fields([_PID_P], [], {"player_page_id": "1JbPozLB"}),
    ),
    "player_seasons": (
        _PLAYER_SPEC,
        "/player/{player_page_id}",
        P.parse_pleague_player_seasons,
        _record("player", "player_1JbPozLB.html", pid="1JbPozLB"),
        "One player's season totals and per-game averages, league and East Asia Super League, with a career row.",
        _fields([_PID_P], [], {"player_page_id": "1JbPozLB"}),
    ),
}

_CHART = "/v1/chart-data/id/{uuid}"
_SUMMARY = "/post/game/summary/o/plg/gameId/{uuid}"
_ROTATION = "/game/rotation/chart/o/plg/gameId/{uuid}"
_INJURY = "/game/injury/list/o/plg/gameId/{uuid}"
_EXAMPLE_UUID = {"uuid": "e6f98675-3e5b-11ed-949e-5d17350689cf"}
_VENDOR_FIELDS = _fields([_UUID_P], [_VERIFY_Q], _EXAMPLE_UUID)
VENDOR: Dict[str, Entry] = {
    "shots": (
        (_CHART,),
        _CHART,
        V.parse_pleague_preciser_shots,
        _vendor_fixture("full/chart_data_197.json"),
        "Every field-goal attempt of one finished game with court x / y (vendor frame), zone, style and result.",
        _VENDOR_FIELDS,
    ),
    "summary_teams": (
        (_SUMMARY,),
        _SUMMARY,
        V.parse_pleague_preciser_summary_teams,
        _vendor_fixture("summary_197.json"),
        "Post-game team comparison of one finished game: totals, situational points, lead tracker and top performer.",
        _VENDOR_FIELDS,
    ),
    "score_log": (
        (_SUMMARY,),
        _SUMMARY,
        V.parse_pleague_preciser_score_log,
        _vendor_fixture("summary_197.json"),
        "Scoring log of one finished game (scoring events only, with the running score).",
        _VENDOR_FIELDS,
    ),
    "lead_flow": (
        (_SUMMARY,),
        _SUMMARY,
        V.parse_pleague_preciser_lead_flow,
        _vendor_fixture("summary_197.json"),
        "Score at each minute mark of one finished game, with the score difference (the lead tracker).",
        _VENDOR_FIELDS,
    ),
    "win_probability": (
        (_SUMMARY,),
        _SUMMARY,
        V.parse_pleague_preciser_win_probability,
        _vendor_fixture("summary_197.json"),
        "The vendor's home-team win probability by minute for one finished game.",
        _VENDOR_FIELDS,
    ),
    "rotation": (
        (_ROTATION,),
        _ROTATION,
        V.parse_pleague_preciser_rotation,
        _vendor_fixture("rotation_197.json"),
        "On-court stints of every player of one finished game (the rotation chart).",
        _VENDOR_FIELDS,
    ),
    "injury": (
        (_INJURY,),
        _INJURY,
        V.parse_pleague_preciser_injury,
        _vendor_fixture("injury_197.json"),
        "Injury report of one finished game: one row per team.",
        _VENDOR_FIELDS,
    ),
}

# ---------------------------------------------------------------------------------------------- docstrings
CLUBS = [  # (team_id, Preciser uuid, 中文, English)
    (1, "e86394aa-3d88-11ed-8ba8-7f44ba46b0b1", "勇士", "Taipei Fubon Braves"),
    (2, "e865bb20-3d88-11ed-a956-7f44ba46b0b1", "領航猿", "Taoyuan Pauian Pilots (Taoyuan Pilots from 2024-25)"),
    (3, "e8674343-3d88-11ed-98b4-7f44ba46b0b1", "攻城獅", "Hsinchu Jko Lioneers (Toplus Lioneers from 2023-24)"),
    (4, "e869743f-3d88-11ed-925e-7f44ba46b0b1", "夢想家", "Formosa Taishin Dreamers (Formosa Dreamers from 2022-23)"),
    (5, "e86cac11-3d88-11ed-8637-7f44ba46b0b1", "鋼鐵人", "Kaohsiung 17LIVE Steelers"),
    (6, "e86b0ecc-3d88-11ed-bed5-7f44ba46b0b1", "國王", "New Taipei Kings"),
    (7, "3c3a601a-358a-11ed-ab92-711c34ed1298", "獵鷹", "TSG Ghosthawks"),
    (8, "bf957f5a-9389-11f0-812c-a38482ec4d30", "洋基工程", "Yankey Ark"),
]
CLUB_TABLE = "; ".join(f"{i} = {u} ({zh}, {en})" for i, u, zh, en in CLUBS)

SITE_NOTES = [
    "Unofficial use of a public source: these wrappers read pleagueofficial.com (P. LEAGUE+, Taiwan's men's league), "
    "keyless, behind Cloudflare (the runtime needs curl_cffi: pip install curl_cffi). Release flag: the data release "
    "needs owner sign-off. The site's /user-policy (members' terms, section 11) reserves all content on the site to "
    "the service and its rights holders and forbids use, copying, adaptation or distribution without authorisation "
    "(it says nothing about automated access); robots.txt holds only Cloudflare content-signal comment lines with no "
    "Allow, Disallow or Crawl-delay rule. Use for reference only; payloads are not redistributed as release assets. "
    "The runtime waits at least 1 s before every request, retries included.",
    "Coverage: schedules, standings and player tables from 2020-21 (season=2021); box scores for every finished game "
    "from 2020-21; play-by-play (with shot x / y) from 2022-23 (gid 191 on). 2020-21 and 2021-22 pages embed an empty "
    "game uuid, so they have no play-by-play and no Preciser data. A game is keyed by gid: ONE global integer sequence "
    "over every stage and season. Only finished games are served (status 已完賽): an unplayed game, or a box or "
    "play-by-play that does not match its game page (the routes echo no id, so the runtime checks scores, quarters "
    "and points), raises AssetFetchError.",
    "Seasons are END years (2025 = the slug '2024-25'). Translation: 客隊 / 主隊 = away / home, 已完賽 = finished, "
    "熱身賽 / 例行賽 / 季後賽 / 總冠軍賽 = pre-season / regular season / playoffs / finals, 主裁判 = referee, "
    "第一檢查員 / 第二檢查員 = first / second umpire, 洋將 = import, 本土 = local, 勝 / 敗 = win / loss. Names stay as "
    "printed. Ids: team_id (1-8) is stable across renames; box player_id is the player-team membership; the box "
    "random_id is unreliable before 2022-23 (join on player_id, and box to Preciser shots on person_id). The date of "
    "birth is dropped from the player frame; officials appear by name only.",
]
VENDOR_NOTES = [
    "Unofficial use of a THIRD-PARTY host: api.preciser.io is the analytics vendor behind the P. LEAGUE+ game page, "
    "not the league's own API. It is keyless, CORS-open, has no robots.txt (404) and no terms were found; it serves the "
    "data the league's own game page displays. Release flag: the data release needs owner sign-off (and the league's "
    "/user-policy reserves its content). The runtime waits at least 1 s before every request, retries included.",
    "Wrappers take the Preciser game uuid (the uuid column of pleague_game; empty before 2022-23, which have no vendor "
    "data). Pass the optional gid to verify the body against the site's game page and box: the shot list and the "
    "rotation echo nothing about the game, so only then is the made-shot count, the roster and the court time checked "
    "(rotation minutes within 30 s of the box). An unknown uuid answers 200 [] (NoDataError) or HTTP 500 "
    "(AssetFetchError).",
    "Shot x / y are in the VENDOR frame (percent of the court, the home team always attacks x = 95) while the site's "
    "play-by-play x / y are in the physical court frame (teams swap ends at half-time): x_vendor = 100 - x_pbp and "
    "y_vendor = 100 - y_pbp in the halves where the home team attacks x < 50. The court is 28.75 x 15.25 m. Clip urls "
    "carry a vendor access key (?key=); the runtime scrubs it from every body and no frame keeps a url.",
    "Clubs, site team_id -> Preciser team uuid (the file name of the club logo): " + CLUB_TABLE + ".",
]

# ---------------------------------------------------------------------------------------------- descriptions
_ACTION_ROWS = P.action_table()
ACTION_TABLE = "; ".join(f"{code} = {en} [{zh or '(blank)'}, {n:,}]" for zh, code, en, n in _ACTION_ROWS)
ACTION_ZH_TABLE = "; ".join(
    f"{zh or '(blank)'} = {code} ({n:,})" for zh, code, _en, n in sorted(_ACTION_ROWS, key=lambda r: -r[3])
)
_N_EVENTS = sum(r[3] for r in _ACTION_ROWS)

_TEAM_ID = (
    "P. LEAGUE+ club id on the site (stable across seasons AND renames): 1 = Taipei Fubon Braves, 2 = Taoyuan Pauian "
    "Pilots, 3 = Hsinchu Jko / Toplus Lioneers, 4 = Formosa Taishin Dreamers, 5 = Kaohsiung 17LIVE Steelers, 6 = New "
    "Taipei Kings, 7 = TSG Ghosthawks, 8 = Yankey Ark. NOT a Preciser team uuid (that joins through the standings "
    "team_uuid)."
)
_NAMES = "Printed names change by season (short and long forms, renames): key on the id, not the name."
LOCAL: Dict[str, str] = {
    "gid": "P. LEAGUE+ game id on the site: ONE global integer sequence over every stage and season (1-12 = 2020-21 "
    "pre-season, 13 = the 2020-21 opener, 197 = the 2022-23 opener, 662 = the 2025-26 opener), with gaps; a row id, not "
    "a count. The {gid} of /game/{gid}.",
    "season": "Season END year (2025 = the site's slug '2024-25'): the first year of the slug plus one.",
    "season_slug": "The site's season slug as printed: the START year, then the END year's last two digits ('2024-25').",
    "stage": "Stage slug: pre-season (熱身賽, exhibition games before the season), regular-season (例行賽), playoffs "
    "(季後賽) or finals (總冠軍賽, the championship series).",
    "side": "Which team the row belongs to: home or away. The box lists home first, the play-by-play and the schedule "
    "list away first.",
    "uuid": "Preciser game uuid (key space: the analytics vendor's games, a version-1 uuid), embedded in the game page "
    "as getPlayByPlay('uuid'); keys the play-by-play and every pleague_preciser wrapper. Null before 2022-23 (the page "
    "embeds an empty string: no play-by-play or vendor data).",
    "date": "Game date, Asia/Taipei (UTC+8) local time.",
    "time": "Scheduled tip-off 'HH:MM', Asia/Taipei (UTC+8) local time.",
    "venue_zh": "Venue (arena) name in Traditional Chinese.",
    "away_team_id": _TEAM_ID,
    "home_team_id": _TEAM_ID,
    "team_id": _TEAM_ID,
    "away_name_zh": "Away team's name on that date in Traditional Chinese (short or long form; 勇士 = Braves, 領航猿 = "
    "Pilots, 攻城獅 = Lioneers, 夢想家 = Dreamers, 鋼鐵人 = Steelers, 國王 = Kings, 獵鷹 = Ghosthawks, 洋基工程 = Yankey "
    "Ark). " + _NAMES,
    "home_name_zh": "Home team's name on that date in Traditional Chinese (short or long form). " + _NAMES,
    "away_name_en": "Away team's English name on that date; renames change it. Null on 2020-21 game pages (no English "
    "name is printed).",
    "home_name_en": "Home team's English name on that date; renames change it. Null on 2020-21 game pages (no English "
    "name is printed).",
    "away_logo": "Away team logo url as printed by the page: protocol-relative ('//d36fypkbmmogz6.cloudfront.net/upload/"
    "p_team/logo_{team}_{epoch}.png'); some game pages print a malformed 'https:////d36...'. The trailing number is the "
    "upload time.",
    "home_logo": "Home team logo url as printed by the page: protocol-relative ('//d36fypkbmmogz6.cloudfront.net/upload/"
    "p_team/logo_{team}_{epoch}.png'); some game pages print a malformed 'https:////d36...'. The trailing number is the "
    "upload time.",
    "away_score": "Away team's final points (0 on a future game).",
    "home_score": "Home team's final points (0 on a future game).",
    "period": "Period label: Q1-Q4, then OT1, OT2 for overtime.",
    "period_no": "1-based position of the period: 1-4 for Q1-Q4, then 5 for OT1, 6 for OT2.",
    "player_id": "P. LEAGUE+ player-team membership id on the site (key space: the member id the player page passes to "
    "trackPlayer(), e.g. 40 = Lai Ting-En), cast to an integer. NOT the 8-character page id. Join box rows on it, never "
    "on the printed name.",
    "person_id": "Preciser person uuid (key space: the analytics vendor's persons): the join between a box row and a "
    "pleague_preciser_shots row. Present on played rows from 2022-23; null on 2020-21 and 2021-22 rows and did-not-play "
    "rows (the refs notes report some 2025-26 finals boxes without it; gid 716 has it).",
    "random_id": "The 8-character page id of /player/{id} (same space as player_page_id). Reliable from 2022-23 only: on "
    "2020-21 and 2021-22 rows it is wrong for most players (gid 13 gives Lai Ting-En the page of Kao Kuo-Hao), so never "
    "join across eras on it.",
    "player_page_id": "The 8-character page id of /player/{id} (e.g. '1JbPozLB'). The box random_id on 2022-23+ rows; "
    "NOT the box player_id.",
    "name_zh": "Player name as printed (Traditional Chinese, or a transliteration for foreign players).",
    "jersey": "Shirt number as printed, a string ('0', '00' and '10' differ).",
    "team_zh": "Team name in Traditional Chinese as the page prints it (a traded player shows the current team).",
}
_BOX_NOTE = " Null on did-not-play rows."
SITE_BY: Dict[str, Dict[str, str]] = {
    "schedule": {
        "weekday_zh": "Weekday as one Chinese character: 日 Sun, 一 Mon, 二 Tue, 三 Wed, 四 Thu, 五 Fri, 六 Sat.",
        "starred": "True when the date cell carries the site's star icon (a highlighted game).",
        "game_label": "Game label as printed: 'G01' = game 1 of the stage, 'PLAYOFFS Game1' / 'PLAYOFFS A G1' = playoff "
        "game (series A / B), 'FINALS G1' / 'Final Game1' = finals game, '第一站G1' = game 1 of the first 'stop' (站, a "
        "round-robin hub weekend; 2020-21).",
        "attendance": "Spectators: the first number of the page's 'N / M' seat cell; 0 on future games.",
        "capacity": "Seats: the second number of the 'N / M' seat cell (the venue capacity as the site records it); 0 on "
        "future games.",
        "phase": "Row state from the page's CSS class: before_today = the date is past (the score is the result), "
        "is_future = not yet played (both scores 0), is_today. A past game printing 0-0 may be a series game that was "
        "never played: check the game page's status.",
        "gid": LOCAL["gid"] + " Cross-checked with the page's trackGame() call.",
    },
    "game": {
        "status_zh": "Status badge: 已完賽 = finished. The only value the wrappers serve (anything else raises "
        "AssetFetchError).",
        "finished": "True when the status badge is 已完賽 (finished); always true for a served game.",
        "game_label_zh": "Stage and game number as printed in the header: '例行賽G1' = regular season game 1 ('例行賽G01' "
        "on 2020-21 pages); 季後賽 = playoffs, 總冠軍賽 = finals.",
        "overtimes": "Number of overtime periods played (overtime cells with points on the page).",
        "date": "Game date from the header, Asia/Taipei (UTC+8) local time.",
        "time": "Tip-off 'HH:MM' from the header, Asia/Taipei (UTC+8) local time.",
    },
    "periods": {
        "home_points": "Home points scored IN the period (not cumulative).",
        "away_points": "Away points scored IN the period (not cumulative).",
    },
    "officials": {
        "official_no": "1-based position of the official in the printed line (referee first).",
        "role": "Role translated: referee (主裁判, the crew chief), first_umpire (第一檢查員, literally 'first inspector'), "
        "second_umpire (第二檢查員). Null for a role this table does not know.",
        "role_zh": "Official's role as printed: 主裁判 = referee, 第一檢查員 = first umpire, 第二檢查員 = second umpire.",
        "name_zh": "Official's name in Traditional Chinese. Names only: the page carries no ids or contact data.",
    },
    "box_players": {
        "starter": "True for a starter (the site's '〇' marker). Null on did-not-play rows.",
        "played": "True when the player has court time; false on a 'DNP' (did not play) row, whose statistics are null.",
        "minutes": "Minutes played 'mm:ss'." + _BOX_NOTE,
        "seconds": "Playing time in whole seconds: the site's seconds column in 2020-21 and 2021-22, derived from "
        "minutes from 2022-23." + _BOX_NOTE,
        "points": "Points scored." + _BOX_NOTE,
        "reb_off": "Offensive rebounds." + _BOX_NOTE,
        "reb_def": "Defensive rebounds." + _BOX_NOTE,
        "reb": "Total rebounds (offensive plus defensive)." + _BOX_NOTE,
        "ast": "Assists." + _BOX_NOTE,
        "stl": "Steals." + _BOX_NOTE,
        "blk": "Blocks made." + _BOX_NOTE,
        "tov": "Turnovers committed." + _BOX_NOTE,
        "pf": "Personal fouls committed." + _BOX_NOTE,
        "plus_minus": "Plus/minus: the team's points minus the opponent's while the player was on court (signed)."
        + _BOX_NOTE,
        "eff": "Efficiency: points + rebounds + assists + steals + blocks - missed field goals - missed free throws - "
        "turnovers (verified on every 2020-21 and 2021-22 row)." + _BOX_NOTE,
        "fg2_made": "2-point field goals made, from the site's 'made-attempted' string." + _BOX_NOTE,
        "fg2_att": "2-point field goals attempted, from the site's 'made-attempted' string." + _BOX_NOTE,
        "fg2_pct": "2-point field-goal percentage, 0-100 (the site sends a number or a string)." + _BOX_NOTE,
        "fg3_made": "3-point field goals made, from the site's 'made-attempted' string." + _BOX_NOTE,
        "fg3_att": "3-point field goals attempted, from the site's 'made-attempted' string." + _BOX_NOTE,
        "fg3_pct": "3-point field-goal percentage, 0-100." + _BOX_NOTE,
        "ft_made": "Free throws made, from the site's 'made-attempted' string." + _BOX_NOTE,
        "ft_att": "Free throws attempted, from the site's 'made-attempted' string." + _BOX_NOTE,
        "ft_pct": "Free-throw percentage, 0-100." + _BOX_NOTE,
        "efg_pct": "Effective field-goal percentage 0-100, (FGM + 0.5 x 3PM) / FGA x 100 (verified). 2020-21 and "
        "2021-22 rows only; null from 2022-23.",
        "ts_pct": "True-shooting percentage 0-100, PTS / (2 x (FGA + 0.44 x FTA)) x 100 (verified). 2020-21 and "
        "2021-22 rows only; null from 2022-23.",
        "usage_pct": "Usage percentage as the site prints it, 0-100; the standard usage formula reproduces it on only "
        "685 of 3,932 rows, so the formula is unverified. 2020-21 and 2021-22 rows only.",
        "position": "Position code on some 2020-21 and 2021-22 rows ('G' guard, 'F' forward, 'C' centre); null on the "
        "rest and from 2022-23.",
        "ortg": "Offensive rating: the team's points scored per 100 offensive possessions while the player was on court "
        "(2022-23 on; null before)." + _BOX_NOTE,
        "drtg": "Defensive rating: the opponent's points per 100 of its possessions while the player was on court, "
        "lower is better (2022-23 on)." + _BOX_NOTE,
        "netrtg": "Net rating, ortg minus drtg (exact on every checked row; 2022-23 on)." + _BOX_NOTE,
        "pace": "Pace: 48 x the team's offensive possessions / minutes on court, the team's possessions per 48 minutes "
        "with this player on court (verified on 1,124 of 1,149 rows; 2022-23 on)." + _BOX_NOTE,
        "ast_ratio": "Assist ratio 0-100: assists per 100 of the player's own plays. The common formula AST / (FGA + "
        "0.5 x FTA + AST + TOV) reproduces it on 919 of 1,174 rows, so the exact formula is unverified (2022-23 on).",
        "to_ratio": "Turnover ratio 0-100: turnovers per 100 of the player's own plays; same unverified formula family "
        "as ast_ratio (917 of 1,174 rows; 2022-23 on).",
        "off_possessions": "The team's offensive possessions while the player was on court (the base of ortg; 2022-23 "
        "on)." + _BOX_NOTE,
        "opp_off_possessions": "The opponent's offensive possessions while the player was on court (the base of drtg; "
        "2022-23 on)." + _BOX_NOTE,
        "opp_pts_second_chance": "Opponent second-chance points while the player was on court (2022-23 on)."
        + _BOX_NOTE,
        "opp_pts_fast_break": "Opponent fast-break points while the player was on court (2022-23 on)." + _BOX_NOTE,
        "opp_pts_off_turnover": "Opponent points off turnovers while the player was on court (2022-23 on)." + _BOX_NOTE,
        "opp_pts_paint": "Opponent points in the paint while the player was on court (2022-23 on)." + _BOX_NOTE,
    },
    "box_teams": {
        "minutes": "Team minutes 'mm:ss' ('240:00' = 5 players x 48 min; more with overtime).",
        "seconds": "Team seconds on court (14400 = 240 minutes): the site's value in 2020-21 and 2021-22, derived from "
        "minutes from 2022-23.",
        "points": "Team points; equals the final score (the runtime checks it).",
        "fg2_made": "Team 2-point field goals made.",
        "fg2_att": "Team 2-point field goals attempted.",
        "fg2_pct": "Team 2-point percentage, 0-100.",
        "fg3_made": "Team 3-point field goals made.",
        "fg3_att": "Team 3-point field goals attempted.",
        "fg3_pct": "Team 3-point percentage, 0-100.",
        "ft_made": "Team free throws made.",
        "ft_att": "Team free throws attempted.",
        "ft_pct": "Team free-throw percentage, 0-100.",
        "reb": "Team total rebounds, including team rebounds that no player row carries (so it can exceed the sum of the "
        "player rows).",
        "reb_off": "Team offensive rebounds.",
        "reb_def": "Team defensive rebounds.",
        "ast": "Team assists in the game (the total row).",
        "stl": "Team steals in the game.",
        "blk": "Team blocks made in the game.",
        "tov": "Team turnovers.",
        "pf": "Team personal fouls.",
        "plus_minus": "Sum of the players' plus/minus (2020-21 and 2021-22 only; null from 2022-23).",
    },
    "pbp": {
        "seq": "Position of the play in game order, 1-based (the fragment lists plays oldest first within the "
        "fragment).",
        "clock": "Game clock REMAINING in the period 'mm:ss' (counts down; Q1 opens at 12:00, OT at 05:00).",
        "seconds_left": "The clock as whole seconds remaining in the period (08:34 = 514), derived by the parser.",
        "side": "Side of the acting team: away or home (the fragment places the play in the away or home column).",
        "jersey": "Shirt number of the acting player as printed; null on team rows and where the name is printed "
        "without a number.",
        "player_zh": "Acting player's name as printed; null on team rows (a team rebound, timeout or shot-clock "
        "violation). The page prints no player ids: join to the box on side, jersey and name_zh.",
        "action": f"Action code from the vocabulary observed on {_N_EVENTS:,} plays of 404 finished 2022-23..2025-26 "
        f"games (82 phrases plus the blank row; an unseen phrase is refused as layout drift). The code = meaning [Traditional Chinese "
        f"phrase, events]: {ACTION_TABLE}.",
        "action_zh": f"The action phrase as printed, Traditional Chinese (灌籃得分 = dunk, made; '#35 辛巴 灌籃得分' = "
        f"'#35 {{player}} dunk, made'); blank on an unlabelled row. The 82 observed phrases = action code (events): "
        f"{ACTION_ZH_TABLE}.",
        "action_en": "English meaning of the action code (see action); null for a code outside the observed table.",
        "text_zh": "The whole printed text of the play, e.g. '#35 辛巴 灌籃得分' = '#35 {player} dunk, made'.",
        "ft_number": "Which free throw of the trip this is (1, 2 or 3), from 第一罰 / 第二罰 / 第三罰; null on every other "
        "play.",
        "ft_awarded": "Free throws awarded in the trip (1, 2 or 3) from 得到一次罰球 / 得到兩次罰球 / 得到三次罰球; set only on "
        "the FIRST free throw's row, null otherwise.",
        "points": "Points scored by the play from the '+2' / '+3' / '+1' tag; null when the tag is a bare '+'.",
        "score_away": "Away score after the play, from the 'A - H' cell (AWAY first, unlike the box); null on plays that "
        "print no score (substitutions, rebounds, jump balls).",
        "score_home": "Home score after the play, from the 'A - H' cell; null when the play prints no score.",
        "x": "Shot / play x: PERCENT (0-100) of the court LENGTH in the PHYSICAL court frame (not normalised per team): "
        "baskets near x = 5 and x = 95, teams swap ends at half-time. The Preciser shot list uses a home-attacks-95 "
        "frame: x_vendor = 100 - x in the halves where the home team attacks x < 50. Null on plays without a "
        "location. Court 28.75 m long.",
        "y": "Shot / play y: PERCENT (0-100) of the court WIDTH in the physical frame (baskets at y = 50); "
        "y_vendor = 100 - y in the halves where the home team attacks x < 50. Which sideline is y = 0 is unverified. "
        "Court 15.25 m wide.",
        "nice_play": "True when the site stars the play (a nice_play row class and a starred clip).",
        "video_uuid": "Clip uuid (key space: the clip vendor tryspot); null on plays without a clip (about 3 in 4). "
        "The clip urls are never kept: they carry a vendor access key.",
        "synergy_play_id": "Synergy Sports play uuid of the clip (the site links Synergy); a string on every one of "
        "the 50,862 clips in the corpus.",
        "video_stat_category": "The clip's own stat label (ast, blk, ft_made, ft_miss, pfoul, reb_d, reb_o, stl, "
        "three_made, three_miss, timeout, turnover, two_made, two_miss): it names the PLAY the clip shows, not always "
        "this row's action (a rebound row carries the missed shot's clip).",
        "tryspot_player_id": "Player id in the clip vendor's key space (an integer such as 59476); NOT the box "
        "player_id.",
        "video_created_at": "When the clip was created, UTC ISO 8601 with milliseconds.",
    },
    "standings": {
        "rank": "Rank on the page (1 = best); ties are not shared.",
        "team_zh": "Team short name in Traditional Chinese.",
        "team_uuid": "Preciser team uuid cut from the logo url (photonew.preciser.io/plg/teams/{uuid}.png): the join "
        "between the site team_id and the vendor's home_id / away_id / team_id. Clubs: " + CLUB_TABLE + ".",
        "team_logo": "Team logo url (photonew.preciser.io/plg/teams/{team uuid}.png).",
        "games_played": "Games played (已賽 GP).",
        "wins": "Games won so far this season (勝 W).",
        "losses": "Games lost so far this season (敗 L).",
        "win_pct": "Win percentage 0-100 as a whole percent ('68%' -> 68.0; 勝率 PCT).",
        "games_behind": "Games behind the leader (勝差).",
        "points_for_avg": "Points scored per game (平均得分).",
        "points_against_avg": "Points allowed per game (平均失分).",
        "point_diff_avg": "Average point margin (平均分差).",
        "home_record": "Home record 'W-L' (主場).",
        "away_record": "Away record 'W-L' (客場).",
        "streak": "Current streak: W{n} wins or L{n} losses (連勝連敗).",
        "last_n": "Games in the 'last games' column (近10場比賽 = the last 10), read from the header.",
        "last_n_record": "Record 'W-L' over the last last_n games (近N場比賽).",
    },
    "stat_player": {
        "stage_sn": "Stage select value: 2 = regular season, 3 = playoffs, 4 = finals (echo of the request, checked "
        "against the page's selected option).",
        "jersey": "Shirt number as a string (the player's number in the latest game).",
        "games": "Games played (出賽).",
        "minutes_avg": "Minutes per game 'mm:ss' (上場時間).",
        "minutes_total": "Total minutes 'mmm:ss' (上場時間).",
        "fg2_pct": "2-point percentage 0-100 (兩分命中率; the cell holds the same value as average and total).",
        "fg3_pct": "3-point percentage 0-100 (三分命中率; same value as average and total).",
        "ft_pct": "Free-throw percentage 0-100 (罰球命中率; same value as average and total).",
        **{
            f"{k}_{agg}": f"{'Per-game average' if agg == 'avg' else 'Season total'} of {label} ({zh})."
            for k, label, zh in (
                ("fg2_made", "2-point makes", "兩分命中"),
                ("fg2_att", "2-point attempts", "兩分出手"),
                ("fg3_made", "3-point makes", "三分命中"),
                ("fg3_att", "3-point attempts", "三分出手"),
                ("ft_made", "free-throw makes", "罰球命中"),
                ("ft_att", "free-throw attempts", "罰球出手"),
                ("points", "points", "得分"),
                ("reb_off", "offensive rebounds", "攻板"),
                ("reb_def", "defensive rebounds", "防板"),
                ("reb", "total rebounds", "籃板"),
                ("ast", "assists", "助攻"),
                ("stl", "steals", "抄截"),
                ("blk", "blocks", "阻攻"),
                ("tov", "turnovers", "失誤"),
                ("pf", "personal fouls", "犯規"),
            )
            for agg in ("avg", "total")
        },
    },
    "player": {
        "player_id": "P. LEAGUE+ player-team membership id (the box player_id), read from the page's trackPlayer() "
        "call, e.g. 40 = Lai Ting-En. NOT the 8-character page id.",
        "name_en": "Player name in Latin letters.",
        "team_zh": "Current team's short name in Traditional Chinese.",
        "team_en": "Current team's English name.",
        "position": "Position letter: G guard, F forward, C centre.",
        "nickname": "Nickname (綽號); null when blank.",
        "tenure": "Years of service (年資) as the page prints it, with a prime character ('0‘').",
        "height_cm": "Height in centimetres (身高).",
        "weight_kg": "Weight in kilograms (體重).",
        "birthplace_zh": "Birthplace in Traditional Chinese (出生地; a country for foreign players). The date of birth "
        "is dropped on purpose.",
        "status_zh": "Player status (身份): 洋將 = import (foreign player), 本土 = local (Taiwanese) player.",
        "education": "Education lines (學歷) as printed, joined with '; '.",
    },
    "player_seasons": {
        "competition": "Table block: plg (the league's tables) or easl (the East Asia Super League block, present only "
        "for players with games there).",
        "table": "totals (the 累計 table: season totals) or averages (the 平均 table: per game).",
        "season": "Season END year of the slug; null on the career row.",
        "season_slug": "Season slug as printed ('2025-26'), or 'career' for the final totals / averages row.",
        "games": "Games played (出賽).",
        "minutes": "Minutes 'mm:ss' (a season total in the totals table, a per-game average in the averages table).",
        "points": "Points (total in the totals table, per game in the averages table; 得分).",
        "reb": "Rebounds (total / per game; 籃板).",
        "ast": "Assists (total / per game; 助攻).",
        "stl": "Steals (total / per game; 抄截).",
        "blk": "Blocks (total / per game; 阻攻).",
        "tov": "Turnovers (total / per game; 失誤).",
        "pf": "Personal fouls (total / per game; 犯規).",
        "fg2_made": "2-point makes: the first number of the page's '56 - 103' cell (total / per game).",
        "fg2_att": "2-point attempts: the second number of the cell.",
        "fg2_pct": "2-point percentage 0-100 ('54.4%' -> 54.4).",
        "fg3_made": "3-point makes (total / per game).",
        "fg3_att": "3-point attempts (total / per game).",
        "fg3_pct": "3-point percentage 0-100.",
        "ft_made": "Free-throw makes (total / per game).",
        "ft_att": "Free-throw attempts (total / per game).",
        "ft_pct": "Free-throw percentage 0-100.",
    },
}

_TEAM_UUID = (
    "Preciser team uuid (key space: the vendor's teams; the file name of the club logo). Joins the site team_id "
    "through pleague_standings.team_uuid: " + CLUB_TABLE + "."
)
VENDOR_LOCAL: Dict[str, str] = {
    "game_uuid": "Preciser game uuid (key space: the vendor's games, a version-1 uuid), the uuid asked for; "
    "``uuid`` of pleague_game.",
    "side": "Which team the row belongs to: home or away.",
    "team_uuid": _TEAM_UUID,
    "home_team_uuid": _TEAM_UUID,
    "away_team_uuid": _TEAM_UUID,
    "color": "Team colour as a CSS hex string.",
    "logo_url": "Team logo url (photonew.preciser.io/plg/teams/{team uuid}.png).",
    "team_name_zh": "Team full name in Traditional Chinese (臺北富邦勇士 = Taipei Fubon Braves, 桃園璞園領航猿 = Taoyuan "
    "Pauian Pilots, 新竹御頂攻城獅 = Hsinchu Toplus Lioneers, 福爾摩沙夢想家 = Formosa Dreamers, 高雄鋼鐵人 = Kaohsiung "
    "17LIVE Steelers, 新北國王 = New Taipei Kings, 臺南台鋼獵鷹 = TSG Ghosthawks).",
    "period": "Period number: 1-4, 5 and up for overtime (OT1 = 5).",
    "period_no": "1-based position of the period: 1-4 for Q1-Q4, 5 for OT1, 6 for OT2.",
    "clock": "Game clock REMAINING in the period 'mm:ss' (counts down).",
    "seconds_left": "The clock as whole seconds remaining in the period (08:34 = 514), derived by the parser.",
    "home_score": "Home score after the event.",
    "away_score": "Away score after the event.",
    "person_id": "Preciser person uuid (key space: the vendor's persons): equal to the site box person_id of the same "
    "player, the join between this row and pleague_box_players (2022-23 on).",
}
VENDOR_BY: Dict[str, Dict[str, str]] = {
    "shots": {
        "shot_no": "1-based position in the order the vendor sent the shots, which is NOT guaranteed game order: sort "
        "on period and seconds_left.",
        "period_label": "Period label: Q1-Q4, then OT1, OT2.",
        "side": "Which team took the shot: home or away, from the shooter's team uuid against home_id / away_id.",
        "player_name": "Shooter's name as printed (Traditional Chinese or a transliteration); equals the box name_zh.",
        "type": "Shot value: 2pt or 3pt.",
        "subtype": "Shot style: jumpShot, layup, drivingLayup, floatingJumpShot (floater), tipIn (putback), "
        "pullUpJumpShot, fadeAway, dunk, turnAroundJumpShot, hookShot, stepBackJumpShot, alleyOop; null on a few shots "
        "(7 of 2,566).",
        "area": "Shot zone, English label '{side} {distance band}': 'Center 8ft' (restricted-area band, within 8 ft), "
        "'Center 8-16ft', 'Left 8-16ft', 'Right 8-16ft', 'Left 16-23ft', 'Center 16-23ft', 'Right 16-23ft', "
        "'Left Center 16-23ft', ... and the three-point zones; null on some shots.",
        "success": "True when the shot was made, false when missed.",
        "x": "Shot x: PERCENT (0-100) of the court LENGTH in the VENDOR frame, in which the HOME team always attacks the "
        "basket at x = 95 and the away team the basket at x = 5 (the frame does not flip at half-time). x_vendor = "
        "100 - x_pbp in the halves where the home team attacks x < 50 (pleague_pbp.x is the physical frame). Court "
        "28.75 m long: distance = hypot((x - 5) x 0.2875, (y - 50) x 0.1525) m (mean error 0.010 m on 1,893 shots).",
        "y": "Shot y: PERCENT (0-100) of the court WIDTH in the vendor frame (baskets at y = 50); y_vendor = 100 - y_pbp "
        "in the halves where the home team attacks x < 50 (held for 166 of the 168 shots of gid 197 matched to the "
        "play-by-play; one shot differs in y only). Which sideline is y = 0 is unverified. Court 15.25 m wide.",
        "message_zh": "Play text in Traditional Chinese, '{player}(未)投(進){distance}公尺遠的{shot}' = '{player} made / "
        "missed a {distance} m {shot}'; the distance is in metres and matches x / y.",
        "has_video": "True when the shot has a clip. The clip url is never kept: it carries a vendor access key.",
    },
    "summary_teams": {
        "points": "Team points (the comparison block).",
        "rebounds": "Team total rebounds (the comparison block).",
        "assists": "Team assists (the comparison block).",
        "steals": "Team steals (the comparison block).",
        "blocks": "Team blocks made (the comparison block).",
        "turnovers": "Team turnovers.",
        "fg_made": "Team field goals MADE: the vendor's key is field_goal_percentage but the value is a count of makes.",
        "fg2_pct": "Team 2-point percentage 0-100.",
        "fg3_pct": "Team 3-point percentage 0-100.",
        "ft_pct": "Team free-throw percentage 0-100.",
        "fast_break_points": "Team fast-break points.",
        "paint_points": "Team points in the paint.",
        "second_chance_points": "Team second-chance points.",
        "biggest_lead": "The team's largest lead in points.",
        "biggest_scoring_run": "The team's longest unanswered scoring run, in points.",
        "time_leading_seconds": "Seconds the team led.",
        "lead_changes": "Number of lead changes in the game (the same for both rows).",
        "top_player_name": "Name of the team's top performer (Traditional Chinese or a transliteration).",
        "top_player_jersey": "Shirt number of the top performer as a string.",
        "top_player_points": "Points of the top performer in the game.",
        "top_player_rebounds": "Total rebounds of the top performer in the game.",
        "top_player_assists": "Assists of the top performer in the game.",
    },
    "score_log": {
        "seq": "1-based position of the scoring event in game order.",
        "period": "Period label: Q1-Q4, then OT1, OT2 (the vendor sends it in lower case).",
    },
    "lead_flow": {
        "seq": "1-based position of the minute mark in game order.",
        "period": "Period label: Q1-Q4, then OT1, OT2 (the vendor sends it in lower case).",
        "score_difference": "home_score minus away_score (positive = home leads).",
    },
    "win_probability": {
        "minute": "Minute mark: 0 is the tip-off (50 percent); 49 entries for a 48-minute game.",
        "home_win_pct": "The home team's win probability in percent (0-100) at that minute; the vendor documents no "
        "method.",
    },
    "rotation": {
        "name_zh": "Player name (Traditional Chinese); equals the site box name_zh, which is how the runtime checks "
        "the roster.",
        "jersey": "Shirt number (an integer here; the box prints it as a string).",
        "started": "True when the player started the game.",
        "stint_no": "1-based position of the stint among the player's stints, in game order.",
        "start_period": "Period in which the stint starts (Q1-Q4, OT1, ...).",
        "start_clock": "Clock REMAINING in start_period when the stint starts, 'm:ss'.",
        "start_seconds": "Seconds elapsed on the game clock when the stint starts (cumulative across periods).",
        "end_period": "Period in which the stint ends.",
        "end_clock": "Clock REMAINING in end_period when the stint ends, 'm:ss'.",
        "end_seconds": "Seconds elapsed on the game clock when the stint ends (cumulative across periods).",
    },
    "injury": {
        "team_name_zh": "Team short name in Traditional Chinese ('攻城獅' = Lioneers).",
        "team_name_en": "Team short English name ('Lioneers').",
        "injury_report": "The team's injury report as a JSON string; null when the vendor sent null. It was null on all "
        "16 sampled games, so its shape is unverified.",
    },
}


def _describe(table: Dict[str, Dict[str, str]], local: Dict[str, str], short: str, col: str) -> str:
    return table.get(short, {}).get(col) or local.get(col) or ""


def _frame(entry: Entry) -> pl.DataFrame:
    frame = entry[2](entry[3]())
    assert isinstance(frame, pl.DataFrame)
    return frame


def _endpoint(short: str, entry: Entry, stem: str) -> Dict[str, Any]:
    _spec, path, parser, _fix, summary, fields = entry
    names = [p["name"] for p in fields.get("path_params", [])]
    assert not {"league", "sport"} & set(names), path  # the renderer substitutes those as ESPN slugs
    assert set(re.findall(r"\{([^}]+)\}", path)) == set(names), path
    return {
        "short": short,
        "summary": summary,
        "path": path,
        "parser": parser.__name__,
        "returns_schema": f"native/{stem}/{short}",
        **copy.deepcopy(fields),  # no shared objects, so the YAML carries no anchors
    }


def _doc(stem: str, host: str, endpoints: Dict[str, Entry], notes: List[str], raw_doc: str, see: List[Dict[str, str]]):
    return {
        "api": stem,
        "host": host,
        "name_pattern": stem + "_{short}",
        "module": stem,
        "parser_module": f"basketball.plg.{stem}_parsers",
        "getter_module": f"sportsdataverse.basketball.plg.{stem}_runtime",
        "qualifier": "",
        "passthrough_query": False,
        "parser_columns": True,
        # manual_column_descriptions.yaml keys: bare shorts (game, pbp, shots) name other families
        "schema_prefix": stem + "_",
        "runtime_imports": ["_get"],
        "docstring": {
            "example_import": True,
            "raw_doc": raw_doc,
            "raises": [
                "sportsdataverse.errors.NoDataError: the source has nothing for the request (an unknown game, a "
                "stage the season does not have, a game with no play-by-play or vendor data).",
                "sportsdataverse.errors.AssetFetchError: the fetch failed or the answer is not the one asked for "
                "(a page or body for another game, an unfinished game, a body that does not match its game page, "
                "layout drift, a play-by-play phrase outside the observed vocabulary).",
            ],
            "notes": notes,
            "see_also": see,
        },
        "endpoints": [_endpoint(short, entry, stem) for short, entry in endpoints.items()],
    }


_SEE_R = [
    {"name": "hoopR", "url": "https://hoopR.sportsdataverse.org", "note": "SportsDataverse men's basketball (R)"},
    {"name": "wehoop", "url": "https://wehoop.sportsdataverse.org", "note": "SportsDataverse women's basketball (R)"},
]


def _write_family(
    stem: str,
    spec_name: str,
    refs: Path,
    fix_dir: Path,
    host: str,
    endpoints: Dict[str, Entry],
    specs: Dict[str, Dict[str, pl.DataType]],
    notes: List[str],
    raw_doc: str,
    see: List[Dict[str, str]],
    by: Dict[str, Dict[str, str]],
    local: Dict[str, str],
    out_notes: Dict[str, Dict[str, str]],
    bad: List[str],
) -> None:
    spec = load_spec(refs, spec_name)
    differs = {p for e in endpoints.values() for p in e[0]} ^ set(spec["paths"])
    if differs:
        raise SystemExit(f"{stem}: spec paths and ENDPOINTS differ: {sorted(differs)}")
    assert set(specs) == set(endpoints), "every parser spec needs an endpoint and the reverse"
    write_yaml(ROOT / f"tools/codegen/endpoints/{stem}.yaml", _doc(stem, host, endpoints, notes, raw_doc, see))
    schema_dir = ROOT / f"tools/codegen/schemas/native/{stem}"
    rewrite_schema_dir(schema_dir)
    for short, entry in endpoints.items():
        cols: List[Dict[str, str]] = columns_from_frame(_frame(entry), [], leaf_fallback=False)
        block = out_notes.setdefault(f"{stem}_{short}", {})
        for col in cols:
            text = " ".join(_describe(by, local, short, col["name"]).split())
            if len(text) < 15 or text.lower() == col["name"].lower() or _BANNED.match(text):
                bad.append(f"{stem}_{short}.{col['name']}: {text!r}")
            block[col["name"]] = text
            col["description"] = ""
        write_yaml(schema_dir / f"{short}.yaml", {"schema": f"{stem}_{short}", "kind": "dataframe", "columns": cols})
    (fix_dir / "spec_paths.json").write_text(
        json.dumps(sorted(spec["paths"]), indent=1) + "\n", encoding="utf-8", newline="\n"
    )


def main() -> None:
    refs = refs_dir("pleague", SITE_SPEC)
    notes: Dict[str, Dict[str, str]] = {}
    bad: List[str] = []
    _write_family(
        "pleague",
        SITE_SPEC,
        refs,
        SITE_FIX,
        SITE_HOST,
        SITE,
        P.SPECS,
        SITE_NOTES,
        "the raw record (``dict``): the page record (the refs capture shape), the box body as the site sends it, or the "
        "parsed play-by-play record, plus ``_request`` (kind and the season, stage, game or player asked for)",
        [
            {
                "name": "P. LEAGUE+",
                "url": "https://pleagueofficial.com",
                "note": "the league site whose pages and PHP routes these wrappers read",
            },
            *_SEE_R,
        ],
        SITE_BY,
        LOCAL,
        notes,
        bad,
    )
    _write_family(
        "pleague_preciser",
        PRECISER_SPEC,
        refs,
        VENDOR_FIX,
        PRECISER_HOST,
        VENDOR,
        V.SPECS,
        VENDOR_NOTES,
        "the raw record (``dict``): ``{'data': <the vendor's game object>, '_request': {...}}`` with the clip access "
        "key scrubbed",
        [
            {
                "name": "Preciser",
                "url": "https://api.preciser.io",
                "note": "the analytics vendor (third party) behind the P. LEAGUE+ game page",
            },
            {
                "name": "P. LEAGUE+",
                "url": "https://pleagueofficial.com",
                "note": "the league site that embeds the vendor's widgets and holds the game uuid",
            },
            *_SEE_R,
        ],
        VENDOR_BY,
        VENDOR_LOCAL,
        notes,
        bad,
    )
    if bad:
        raise SystemExit(f"pleague: empty or filler descriptions (add them to LOCAL / SITE_BY / VENDOR_BY): {bad}")
    write_yaml(NOTES, notes)
    print(
        f"pleague: {len(SITE)} + {len(VENDOR)} endpoints, {sum(map(len, notes.values()))} descriptions -> {NOTES.name}"
    )


if __name__ == "__main__":
    main()
