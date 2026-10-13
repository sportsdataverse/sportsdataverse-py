"""Generate the ``bleague`` flat-API endpoint YAML + returns-schemas (Japan B.LEAGUE: B1, B2, B3).

Idempotent: same spec + same fixtures -> byte-identical output. Reads the frozen spec from the
``sdv-internal-refs`` checkout (``$SDV_INTERNAL_REFS_REPO``, else the sibling checkout) and emits:

* ``tools/codegen/endpoints/bleague.yaml`` -- 14 wrappers. One game context feeds nine frames (``game_info``,
  ``period_scores``, ``player_box``, ``team_box``, ``team_summary``, ``pbp``, ``shots``, ``leaders``,
  ``score_track``), one schedule page feeds four (``schedule``, ``schedule_clubs``, ``schedule_events``,
  ``schedule_days``) and the standings page one. :data:`ENDPOINTS` is the source of truth.
* ``tools/codegen/schemas/native/bleague/<short>.yaml`` -- one returns-schema per wrapper: the columns of its parser
  run over its committed fixture (the FULL v2 / v1 contexts in ``tests/fixtures/bleague/full``, stamped the way
  ``bleague_runtime`` stamps a live body). Dtypes are pinned by name in ``bleague_parsers.SPECS``. The schemas carry
  ``description: ''`` (descriptions never live in ``schemas/**``).
* ``tools/codegen/notes/bleague_descriptions.yaml`` -- ``{bleague_<short>: {column: text}}``, the block for
  ``manual_column_descriptions.yaml`` (Integration merges it). Japanese terms are translated. An empty, short or
  filler text fails the run.
* ``tests/fixtures/bleague/spec_paths.json`` -- the spec's routes, for the tests (CI has no refs checkout).

Run: ``python tools/codegen/gen_bleague.py``
"""

from __future__ import annotations

import copy
import gzip
import json
import re
from typing import Any, Callable, Dict, List, Tuple

import polars as pl
from gen_soccer_common import ROOT, columns_from_frame, load_spec, refs_dir, rewrite_schema_dir, write_yaml

from sportsdataverse.basketball.bleague import bleague_parsers as P

HOST = "https://b-league.s3.amazonaws.com/web_json"
SITE = "https://www.bleague.jp"
SPEC = "bleague.openapi.yaml"
FIXTURES = ROOT / "tests" / "fixtures" / "bleague"
SCHEMA_PREFIX = "bleague_"
NOTES = ROOT / "tools" / "codegen" / "notes" / "bleague_descriptions.yaml"
SPEC_PATHS = FIXTURES / "spec_paths.json"
_BANNED = re.compile(r"^(the\s+)?\w+(\s+\w+)?\s+(column|field|value|id|name)\.?$", re.IGNORECASE)

UNOFFICIAL = (
    "Unofficial and unsupported: the game contexts are JSON objects in B.LEAGUE's own public S3 bucket "
    "(b-league.s3.amazonaws.com, the store its game pages read), and schedule / standings / game_detail are pages "
    "of www.bleague.jp parsed here. Neither host publishes terms for this use (the site's /terms/ answers 404; "
    "robots.txt sets a Crawl-delay only for msnbot and bingbot), and ``bleague_runtime`` spaces requests at least "
    "1 s apart. SportsDataverse has not ruled on the site's terms, so payloads are not redistributed as release "
    "assets."
)
SEASONS = (
    "Seasons: the site's ``Year`` is the season START year; every wrapper takes ``season``, the END year "
    "(2026 = 2025-26), and ``bleague_runtime`` sends ``season - 1``. Game wrappers take the global "
    "``schedule_key`` (one integer sequence across B1, B2, B3 and every season: 1 = the 2016-17 opener, 5856 in "
    "2020-21, 500017 in 2022-23, 504734 / 505508 B1 / B2 and 10693 B3 in 2025-26); list a day's keys with "
    "``bleague_schedule``. The runtime reads the feed at run time: v2 (B1/B2 from 2020-21), then v1 (B3 and "
    "B1/B2 2018-2020), then the game page's embedded copy (the only source of 2016-17 and 2017-18)."
)
COVERAGE = (
    "Coverage: game wrappers serve FINISHED games only (an unfinished game raises AssetFetchError; 2019-20 game 4153 "
    "is an unfinished feed). The two feeds differ: v2 ids are strings, v1 ids integers (both become text here); v1 "
    "adds leaders and the score track; the team-rebound codes 18 / 19 are swapped between them and a v1 "
    "substitution is one row where v2 has two (see ``bleague_pbp``). Shot x / y exist from 2022-23 on v2 only, "
    "``side`` from 2025-26; before that ``bleague_shots`` is empty. Player and team names are Japanese (English "
    "names only on the box score and header); the code tables are in ``bleague_codes``."
)
SEASON_ARG = (
    "Season END year (2026 = the 2025-26 season). The site's own year is the START year; ``bleague_runtime`` "
    "sends ``season - 1``."
)
KEY_ARG = (
    "Required. B.LEAGUE ScheduleKey (integer, one global sequence across B1, B2, B3 and every season), e.g. "
    "504734 = B1 2025-26 sec 1, Alvark Tokyo v Utsunomiya Brex; list a day's keys with bleague_schedule."
)
GUARD_ARG = (
    "Optional guard: the season END year the game must belong to. A context whose Year differs raises "
    "AssetFetchError. Omitted = no check (the key alone selects the game)."
)
TAB_ARG = "Required. Division tab: 1 = B1, 2 = B2, 3 = B3."


def _full(name: str) -> Dict[str, Any]:
    with gzip.open(FIXTURES / "full" / f"{name}.json.gz", "rt", encoding="utf-8") as fh:
        body = json.load(fh)
    feed = "v2" if name.startswith("v2") else "v1"
    body["_request"] = {"schedule_key": int(name.split("_")[1]), "feed": feed}
    return body


def _capture(name: str) -> Dict[str, Any]:
    return json.loads((FIXTURES / "captures" / f"{name}.json").read_text(encoding="utf-8"))


def _game_fields() -> Dict[str, Any]:
    return {
        "path_params": [{"name": "schedule_key", "type": "int", "required": True, "description": KEY_ARG}],
        "extra_params": [
            {"name": "season", "query_key": "season", "type": "int", "required": False, "description": GUARD_ARG}
        ],
        "example_args": {"schedule_key": 504734},
    }


def _sched_fields() -> Dict[str, Any]:
    return {
        "host": SITE,
        "extra_params": [
            {
                "name": "season",
                "query_key": "season",
                "type": "int",
                "required": True,
                "description": "Required. " + SEASON_ARG,
            },
            {"name": "tab", "query_key": "tab", "type": "int", "required": True, "description": TAB_ARG},
            {
                "name": "month",
                "query_key": "month",
                "type": "int",
                "required": True,
                "description": "Required. Calendar month 1-12 of the game day (seasons run September to June).",
            },
            {
                "name": "day",
                "query_key": "day",
                "type": "int",
                "required": True,
                "description": "Required. Day of month of the game day.",
            },
            {
                "name": "event",
                "query_key": "event",
                "type": "int",
                "required": False,
                "description": "Competition type: 2 B1 league, 3 B1 Championship, 4 B1 relegation playoffs, 5 all-star, 7 B2 league, 8 B2 playoffs, 11 B1-B2 promotion/relegation, 15 B3 league, 32 B3 playoffs. Omitted = the league competition of the tab (2 / 7 / 15).",
            },
        ],
        "example_args": {"season": 2026, "tab": 1, "month": 10, "day": 4},
    }


# short -> (spec path, wrapper path, parser, fixture loader, summary, fields)
Entry = Tuple[str, str, Callable[..., Any], Callable[[], Dict[str, Any]], str, Dict[str, Any]]
_V2 = lambda: _full("v2_504734")  # noqa: E731
_V1 = lambda: _full("v1_10693")  # noqa: E731
_SCH = lambda: _capture("schedule")  # noqa: E731
ENDPOINTS: Dict[str, Entry] = {
    "game_info": (
        "/game_detail/",
        "/contexts/{schedule_key}",
        P.parse_bleague_game_info,
        _V1,
        "Game header: competition, round, venue, officials, tip-off, both teams and the scores.",
        _game_fields(),
    ),
    "period_scores": (
        "/game_detail/",
        "/contexts/{schedule_key}",
        P.parse_bleague_period_scores,
        _V2,
        "Points each team scored in each period of a game, overtime included.",
        _game_fields(),
    ),
    "player_box": (
        "/game_detail/",
        "/contexts/{schedule_key}",
        P.parse_bleague_player_box,
        _V2,
        "Player box score per period category: minutes, points, shooting, rebounds, assists, efficiency.",
        _game_fields(),
    ),
    "team_box": (
        "/game_detail/",
        "/contexts/{schedule_key}",
        P.parse_bleague_team_box,
        _V2,
        "Team rows of the box score (team-credited stats and team totals) per period category.",
        _game_fields(),
    ),
    "team_summary": (
        "/game_detail/",
        "/contexts/{schedule_key}",
        P.parse_bleague_team_summary,
        _V2,
        "Team summary per period category in long form: shooting, rebounds, paint, fast-break and second-chance points.",
        _game_fields(),
    ),
    "pbp": (
        "/game_detail/",
        "/contexts/{schedule_key}",
        P.parse_bleague_pbp,
        _V2,
        "Play-by-play of a finished game in game order, with decoded action codes and x / y from 2022-23.",
        _game_fields(),
    ),
    "shots": (
        "/game_detail/",
        "/contexts/{schedule_key}",
        P.parse_bleague_shots,
        _V2,
        "Field-goal attempts with court position (2022-23 onward, v2 feed).",
        _game_fields(),
    ),
    "leaders": (
        "/game_detail/",
        "/contexts/{schedule_key}",
        P.parse_bleague_leaders,
        _V1,
        "Team leaders per statistic and period category (v1 feed only).",
        _game_fields(),
    ),
    "score_track": (
        "/game_detail/",
        "/contexts/{schedule_key}",
        P.parse_bleague_score_track,
        _V1,
        "Running score after each scoring event (v1 feed only).",
        _game_fields(),
    ),
    "schedule": (
        "/schedule/",
        "/schedule/",
        P.parse_bleague_schedule,
        _SCH,
        "Games of one day of one competition and division: schedule keys, clubs, scores, venue, tip-off.",
        _sched_fields(),
    ),
    "schedule_clubs": (
        "/schedule/",
        "/schedule/",
        P.parse_bleague_schedule_clubs,
        _SCH,
        "Clubs of the season and division from the schedule page's club filter, with their team ids.",
        _sched_fields(),
    ),
    "schedule_events": (
        "/schedule/",
        "/schedule/",
        P.parse_bleague_schedule_events,
        _SCH,
        "Competitions the schedule page offers for the season and division.",
        _sched_fields(),
    ),
    "schedule_days": (
        "/schedule/",
        "/schedule/",
        P.parse_bleague_schedule_days,
        _SCH,
        "Game days of the requested month for the competition.",
        _sched_fields(),
    ),
    "standings": (
        "/standings/",
        "/standings/",
        P.parse_bleague_standings,
        lambda: _capture("standings"),
        "Standings of one season and division, per conference / division table and wild card.",
        {
            "host": SITE,
            "extra_params": [
                {
                    "name": "season",
                    "query_key": "season",
                    "type": "int",
                    "required": True,
                    "description": "Required. " + SEASON_ARG,
                },
                {"name": "tab", "query_key": "tab", "type": "int", "required": True, "description": TAB_ARG},
            ],
            "example_args": {"season": 2026, "tab": 1},
        },
    ),
}

# ------------------------------------------------------------------------------ descriptions
KEYS = (
    "B.LEAGUE ScheduleKey (key space: B.LEAGUE games, ONE global integer sequence across B1, B2, B3 and every "
    "season). The {schedule_key} every game wrapper takes and the schedule page's list-item id."
)
TEAM_ID = (
    "B.LEAGUE TeamID as text (key space: B.LEAGUE clubs, stable across seasons AND club renames: 703 = Tochigi Brex "
    "in 2016-17 = Utsunomiya Brex in 2025-26). The feed sends a string on v2 and an integer on v1; both are text "
    "here. Not a player id."
)
PLAYER_ID = (
    "B.LEAGUE PlayerID as text (key space: B.LEAGUE persons, stable across seasons and clubs: Makoto Hiejima = "
    "'8589'). A string on v2, an integer on v1; both are text here. Null where the row names no player."
)
COMMON: Dict[str, str] = {
    "schedule_key": KEYS,
    "season": "Season END year (2026 = 2025-26), the feed's Year (the START year) plus one.",
    "feed": "Source feed of the row: v2 (v2_genius_contexts, B1/B2 from 2020-21) or v1 (v1_contexts: B3, B1/B2 2018-2020, "
    "and the 2016-17 / 2017-18 game-page embed). The feeds differ in id types, dates and some codes.",
    "team_id": TEAM_ID,
    "team_side": "Side of the game the row belongs to: home or away.",
    "team_name_j": "Team name in Japanese (official name, e.g. アルバルク東京 = Alvark Tokyo).",
    "team_name_e": "Team name in English, uppercase ('ALVARK TOKYO').",
    "player_id": PLAYER_ID,
    "player_no": "Shirt number as text, so '00' and '0' stay distinct; null on team and game-level rows.",
    "player_name_j": "Player name in Japanese (katakana for foreign players); null on team and game-level rows.",
    "period": "Period number: 1-4 regulation; overtime numbering is unverified (no overtime sample).",
    "period_category": "Period category: 1-4 = quarters 1-4, 15 = first half, 16 = second half, 18 = whole game. "
    "Overtime codes are unverified.",
    "x": "Event x: percent (0-100) of the court LENGTH from the 'left' baseline (baskets near 5 and 95). Metres "
    "(x / 100 x 28) are an inference. 2022-23 onward on v2 only.",
    "y": "Event y: percent (0-100) of the court WIDTH (baskets at 50). Which sideline is y = 0 is unverified; metres "
    "(y / 100 x 15) are an inference. 2022-23 onward on v2 only.",
    "area_code": "Shot-zone code 1-13 read from the site's shot-chart SVG: 1 restricted area, 2 rest of the paint, "
    "3 / 7 baseline mid-range, 4 / 6 wing mid-range, 5 top-of-the-key mid-range, 8 / 12 corner 3, 9 / 11 wing 3, "
    "10 top 3, 13 deep band. Of each pair the first is the side far along the baseline. Also set on turnover and "
    "foul rows; 2022-23 onward.",
    "area": "English zone slug for area_code (restricted_area, paint, baseline_mid_range, wing_mid_range, "
    "top_of_key_mid_range, corner_3, wing_3, top_3, deep_3); mirrored codes share a slug.",
    "side": "Basket the play is charted at: 'left' (x near 0) or 'right' (x near 100); flips at half-time. Present "
    "from 2025-26 only; earlier seasons leave it null and the end must be inferred from x.",
    "rest_time": "Game clock REMAINING in the period as the feed's 'mm:ss' text (counts down from 10:00).",
    "clock_seconds": "Game clock remaining in the period in whole seconds (8:50 = 530), derived from rest_time.",
    "score": "Running score 'home-away' after the event (e.g. '2-4'); null on player-in rows and v1 substitutions.",
}

BOX_STATS: Dict[str, str] = {
    "points": "Points scored. The v2 feed sends '' (a placeholder, not 0) on the team row of quarters; null here.",
    "fg3_made": "Three-point field goals made (v2 placeholder '' on quarter team rows becomes null).",
    "fg3_attempted": "Three-point field goals attempted.",
    "fg2_made": "Two-point field goals made.",
    "fg2_attempted": "Two-point field goals attempted.",
    "ft_made": "Free throws made.",
    "ft_attempted": "Free throws attempted.",
    "fouls": "Personal fouls committed (F).",
    "fouls_drawn": "Fouls drawn (被ファウル, FD).",
    "reb_off": "Offensive rebounds (OR).",
    "reb_def": "Defensive rebounds (DR).",
    "reb": "Total rebounds.",
    "turnovers": "Turnovers (TO).",
    "assists": "Assists credited to the player or team (AS).",
    "steals": "Steals credited to the player or team (ST).",
    "blocks": "Blocks made (BS).",
    "blocks_received": "Shots of this player or team that were blocked (被ブロック, BSON).",
    "ptfb": "Fast-break figure (ファストブレイク); equals the summary's fast-break points on the one game checked, but "
    "whether it counts points or baskets is unverified.",
    "pt2in": "Inside-the-paint figure: on v2 a COUNT of made inside-the-paint 2PT (the summary holds 2x that in "
    "points); on v1 points in the paint (0 on B3, whose 2PT shots are never split by paint).",
    "pt2nd": "Second-chance figure (セカンドチャンス); smaller than the summary's second-chance points, so likely made "
    "baskets; unverified.",
    "ptpft": "Points off turnovers (ポインツオフターンオーバー); v1 box rows only, null on v2.",
    "dunks": "Dunks made (PT2DUNK); v1 box rows only, null on v2.",
    "efficiency": "Efficiency rating (貢献度), era-dependent: v2 = points + rebounds + assists + steals + blocks + fouls "
    "drawn - missed FG - missed FT - turnovers - fouls - shots blocked (PIR); v1 omits fouls drawn, fouls and "
    "shots blocked.",
    "plus_minus": "Plus/minus: team point difference while on court. v2 from 2022-23 only; null on totals and "
    "half rows.",
    "ast_to": "Assist-to-turnover ratio (0 when no turnovers). v2 from 2022-23 only; null on half rows.",
    "efg": "Effective field-goal percentage as a FRACTION 0-1 (3PT weighted 1.5x); the site multiplies by 100. v2 "
    "from 2022-23 only.",
    "ts": "True-shooting percentage as a FRACTION 0-1; the site multiplies by 100. v2 from 2022-23 only.",
    "usg": "Usage rate in PERCENT 0-100. v2 from 2022-23 only; absent on totals rows.",
    "off_rtg": "Offensive rating. v2 from 2022-23 only; on team and totals rows 0 in every sample, never filled.",
    "def_rtg": "Defensive rating. v2 from 2022-23 only; on team and totals rows 0 in every sample, never filled.",
    "net_rtg": "Net rating. v2 from 2022-23 only; on team and totals rows 0 in every sample, never filled.",
    "poss": "Possessions. v2 from 2022-23 only; on team and totals rows 0 in every sample, never filled.",
}
BOX: Dict[str, str] = {
    **BOX_STATS,
    "category": "Row category: 1 = player, 2 = team / coaches row (team-credited stats; on v1 it names the head coach), "
    "3 = team totals (合計). One row per (category, player, period category).",
    "display_order": "Display order of the row (v1 only), integer; null on v2.",
    "player_name_e": "Player name in English ('Makoto Hiejima'); null on team and totals rows.",
    "play_time": "Minutes played as the feed's 'mm:ss'; 'DNP' = did not play. Totals rows hold 5 x the period length "
    "('200:00' for the game).",
    "seconds_played": "Time on court in seconds, parsed from play_time (50:00 = 3000); null for DNP and blank.",
    "on_court": "True for the five players on court when the context was last written (for a final game, at the "
    "final buzzer); the same value on every period row of the player. Null on v2 team and totals rows.",
    "starter": "True when the player started; false for the bench; null on team and totals rows. (The feed sends 1 / "
    "null on v2 and a boolean on v1.)",
}


def _game_info() -> Dict[str, str]:
    d: Dict[str, str] = {
        "year": "Season START year as the site stores it (2025 = 2025-26); season is the END year.",
        "code": "Feed/organisation code repeated on every row: 546 on v2, 6 on v1; meaning unverified.",
        "event": "Competition type (v1 header only): 2 B1 league, 3 B1 Championship, 4 B1 relegation playoffs, "
        "5 all-star, 7 B2 league, 8 B2 playoffs, 11 B1-B2 promotion/relegation, 15 B3 league, 32 B3 playoffs; null on v2.",
        "convention_key": "ConventionKey as text (key space: competition-seasons, 大会): v2 '514' = B1 2025-26, v1 102 = "
        "B3 2025-26. Key on (feed, convention_key); the feeds may number independently.",
        "convention_class": "Competition class code (1 on every sample); meaning unverified.",
        "round": "Round (節, Setu) within the competition-season; playoffs use 100+ (101). Integer.",
        "game_no": "Game number within the competition (v1 header only); null on v2.",
        "schedule_state": "Schedule state code (v1 header only; 2 on the final-game sample); meaning unverified.",
        "schedule_conference": "Conference code (v1 header only); meaning unverified.",
        "interleague": "True for an inter-league game (v1 header only).",
        "stadium_cd": "StadiumCD as text (key space: B.LEAGUE venues; stability unverified).",
        "attendance": "Announced attendance.",
        "game_datetime": "Scheduled tip-off as a UTC instant (the feed's JST wall clock minus nine hours).",
        "game_start_time": "Actual start as a UTC instant (v1 header only; epoch seconds on the 2016-17 embed).",
        "game_end_time": "Actual end as a UTC instant (v1 header only).",
        "max_period": "Number of periods the game had (4 plus overtimes).",
        "current_period": "Period the context was last written in (the last period for a final game).",
        "rest_time": "Clock remaining when last written (v1 header only; 0 for a final game).",
        "game_ended": "True when the game has ended; the runtime serves only games where this and record_fixed are true.",
        "record_fixed": "True when the record is finalised (no more corrections expected).",
        "box_exists": "True when the box score exists.",
        "pbp_exists": "True when play-by-play exists.",
        "picture_exists": "True when photos exist (v1 header only).",
        "report_exists": "True when a game report text exists (v1 header only).",
        "home_win": "True when the home team won (v1 header only); null on v2.",
        "commissioner_id": "Commissioner (コミッショナー) id as text; null when none is assigned.",
        "referee_id": "Crew-chief referee RefereeID as text (key space: officials; stability unverified).",
        "sub_referee_id1": "Second referee RefereeID as text.",
        "sub_referee_id2": "Third referee RefereeID as text.",
        "temperature": "Temperature; null in every observed body, so its dtype is an assumption.",
        "humidity": "Humidity; null in every observed body, so its dtype is an assumption.",
        "convention_name_j": "Competition name in Japanese (e.g. りそなグループ B.LEAGUE 2025-26 B1リーグ戦 = B1 regular season).",
        "convention_name_e": "Competition name in English; empty (null) on early v1 bodies.",
        "convention_title_j": "Competition title in Japanese (v1 header only).",
        "convention_title_e": "Competition title in English (v1 header only).",
        "stadium_name_j": "Venue name in Japanese (e.g. TOYOTA ARENA TOKYO, AGF鈴鹿体育館 = AGF Suzuka Gymnasium).",
        "stadium_name_e": "Venue name in English; often empty (null).",
        "court": "Court number at the venue (v1 header only).",
        "report": "Game report text (v1 header only); usually empty (null).",
        "weather": "Weather text (v1 header only); null in every observed body.",
        "schedule_class_name_j": "Schedule class name in Japanese (v1 header only); usually empty.",
        "schedule_class_name_e": "Schedule class name in English (v1 header only); usually empty.",
        "schedule_class_short_name_j": "Schedule class short name in Japanese (v1 header only); usually empty.",
        "schedule_class_short_name_e": "Schedule class short name in English (v1 header only); usually empty.",
        "commissioner_name_j": "Commissioner name in Japanese (names only; no contact data is published).",
        "commissioner_name_e": "Commissioner name in English (v1 header only).",
        "referee_name_j": "Crew-chief referee name in Japanese (名 only; no contact data is published).",
        "referee_name_e": "Crew-chief referee name in English (v1 header only).",
        "sub_referee_name_j1": "Second referee name in Japanese.",
        "sub_referee_name_e1": "Second referee name in English (v1 header only).",
        "sub_referee_name_j2": "Third referee name in Japanese.",
        "sub_referee_name_e2": "Third referee name in English (v1 header only).",
    }
    for side in ("home", "away"):
        s = side.capitalize()
        d.update(
            {
                f"{side}_team_id": f"{s} team id. " + TEAM_ID,
                f"{side}_team_name_j": f"{s} team name in Japanese (official name).",
                f"{side}_team_name_e": f"{s} team name in English, uppercase.",
                f"{side}_team_short_name_j": f"{s} team short name in Japanese (e.g. A東京).",
                f"{side}_team_short_name_e": f"{s} team short name in English (e.g. A TOKYO).",
                f"{side}_team_sub_name_j": f"{s} team sub name in Japanese (v1 header only); usually empty.",
                f"{side}_team_sub_name_e": f"{s} team sub name in English (v1 header only); usually empty.",
                f"{side}_score": f"{s} team final score.",
                f"{side}_winning_count": f"{s} team winning streak (v1 header only); null in every observed body.",
                f"{side}_losing_count": f"{s} team losing streak (v1 header only); null in every observed body.",
            }
        )
        for p in range(1, 15):
            d[f"{side}_score_p{p}"] = (
                f"{s} team points scored IN period {p} (not cumulative). v2 headers carry four slots, v1 fourteen; "
                "an unplayed period is 0 on v1 and null on v2."
            )
    return d


PBP: Dict[str, str] = {
    "event_no": "Event sequence number, ascending in game order. v2 counts in steps of 1000 (inserted corrections may "
    "fall in between); v1 counts 1, 2, 3...",
    "action_code": "ActionCD1, the primary action code: 1 / 2 3PT made / missed, 3 / 5 2PT outside the paint made / "
    "missed, 4 / 6 inside the paint, 7 / 8 free throw, 9 / 10 defensive / offensive rebound, 11 block, 12 assist, "
    "13 turnover, 14 steal, 15 foul drawn, 16 and-one, 17 team turnover, 18 / 19 team rebounds (SWAPPED between "
    "feeds), 22-25 fouls, 80-85 game and clock events, 86 / 87 player in / out, 88 timeout, 89 substitution (v1).",
    "action": "English slug for action_code (fg3_made, ft_missed, rebound_def, foul_personal, player_in, ...). The "
    "team-rebound codes 18 / 19 follow the feed column; null for a code no sample contains.",
    "qualifier_code2": "ActionCD2, a secondary qualifier (shot type, free throws awarded on a fouls, scoring context, "
    "24 s violation); one code space shared with qualifier_code3. Null when the row has none.",
    "qualifier2": "English slug for qualifier_code2 (jump_shot, layup, dunk, free_throws_awarded_N, fast_break, "
    "second_chance, shooting_foul, ...); null when unknown.",
    "qualifier_code3": "ActionCD3, a tertiary qualifier (scoring context on made shots, shooting foul / foul-out on "
    "fouls); same code space as qualifier_code2.",
    "qualifier3": "English slug for qualifier_code3; null when unknown.",
    "team_side": "Side of the acting team from HomeAway (1 home, 2 away); null on game-level rows and official timeouts.",
    "team_name_j": "Acting team name in Japanese; null on game-level rows and official timeouts.",
    "player_id": "Primary actor PlayerID as text (key space: B.LEAGUE persons). On a v1 substitution it is the player "
    "going out. Null on team rows and game-level rows.",
    "player2_id": "Second actor PlayerID as text; on a v1 substitution the player coming in. v1 / embed only.",
    "player2_no": "Shirt number of the second actor (v1 / embed only).",
    "player2_name_j": "Second actor's name in Japanese (v1 / embed only).",
    "home_score": "Home running score after the event, split from score.",
    "away_score": "Away running score after the event, split from score.",
    "period_end": "True on the closing row of a period (ActionCD1 83).",
    "game_end": "True on the game-end row on v1 and the embed; false on EVERY v2 row, the game-end row (81) "
    "included, so use action instead.",
    "success": "Outcome flag: true = made / successful, false = missed; true on non-shot rows. v2 from 2022-23 only.",
    "play_text": "Play description in Japanese: '#{shirt} {surname} {action} ({running count})'. ○ = made, × = missed, "
    "フリースロー = free throw, ブロック = block, ディフェンス / オフェンスリバウンド = defensive / offensive rebound, "
    "→ = substitution out to in (v1).",
    "recorded_at": "When the event was logged, as a UTC instant (v2 stores UTC text, v1 a JST-offset .NET date, the "
    "embed epoch seconds).",
    "edited_at": "When the event was last edited, as a UTC instant; null when never edited.",
    "code": "Feed/organisation code repeated on every row: 546 on v2, 6 on v1; meaning unverified.",
}
SHOTS: Dict[str, str] = {
    "shot_type": "3pt or 2pt, from ActionCD1 (1-2 three-point, 3-6 two-point).",
    "in_paint": "True for a two-point shot inside the paint, false outside; null for threes and on the B3 feed, "
    "whose 2PT codes do not split by paint.",
    "made": "True when the shot was made (ActionCD1 odd for threes and 3 / 4), false when missed.",
    "qualifier2": "English slug for the shot's ActionCD2 (jump_shot, layup, dunk); null when unknown.",
    "qualifier3": "English slug for the shot's ActionCD3 (fast_break, points_off_turnover, second_chance, ...); null "
    "when none.",
}
SUMMARY: Dict[str, str] = {
    "fg_made": "Field goals made (PTM).",
    "fg_attempted": "Field goals attempted (PTA).",
    "fg_pct": "Field-goal percentage 0-100, one decimal.",
    "fg2_made": "Two-point field goals made.",
    "fg2_attempted": "Two-point field goals attempted.",
    "fg2_pct": "Two-point percentage 0-100.",
    "fg3_made": "Three-point field goals made.",
    "fg3_attempted": "Three-point field goals attempted.",
    "fg3_pct": "Three-point percentage 0-100.",
    "ft_made": "Free throws made.",
    "ft_attempted": "Free throws attempted.",
    "ft_pct": "Free-throw percentage 0-100.",
    "reb_off": "Offensive rebounds.",
    "reb_def": "Defensive rebounds.",
    "reb": "Total rebounds.",
    "assists": "Assists credited to the team in the period category.",
    "turnovers": "Turnovers committed by the team in the period category.",
    "steals": "Steals made by the team in the period category.",
    "blocks": "Blocks made by the team in the period category.",
    "blocks_received": "Shots blocked by the opponent (被ブロック).",
    "fouls": "Fouls committed.",
    "fouls_drawn": "Fouls drawn (被ファウル).",
    "paint_points": "Points in the paint (inside-the-paint 2PT points; 2x the players' PT2IN count on v2).",
    "points_off_turnover": "Points off turnovers (ポインツオフターンオーバー).",
    "second_chance_points": "Second-chance points (セカンドチャンス).",
    "fast_break_points": "Fast-break points (ファストブレイク).",
    "max_lead": "Largest lead of the team in the period category (0 if it never led).",
    "lead_last": "Lead statistic (LeadLast), integer; meaning unverified (7 for the home side on all checked games).",
    "lead_last_home": "Home score-like value tied to lead_last; meaning unverified. A string on v2 quarter / game rows.",
    "lead_last_away": "Away score-like value tied to lead_last; meaning unverified.",
    "lead_changes": "Lead changes in the period category (LeadCount); the same on both side rows.",
    "ties": "Times the score was tied in the period category (TieCount); the same on both side rows.",
    "team_name_j": "Team name in Japanese.",
    "team_name_e": "Team name in English, uppercase.",
}
LEADERS: Dict[str, str] = {
    "category": "Leader category code, decoded by matching count to the box score: 1 points, 2 assists, 3 steals, "
    "4 blocks, 5 rebounds, 8 shots blocked.",
    "category_name": "English name of category (points, assists, steals, blocks, rebounds, blocks_received); null "
    "for an unknown code.",
    "rank": "Rank within the team for the category (1 = best; ties share a rank).",
    "tie_order": "Order within a tied rank (1 = first listed).",
    "count": "The player's value in the category for the period category.",
    "reb_off_count": "Offensive-rebound count carried on the row (filled for the rebounds category).",
    "reb_def_count": "Defensive-rebound count carried on the row (filled for the rebounds category).",
}
TRACK: Dict[str, str] = {
    "event_index": "Index of the scoring event, 0 = the 0-0 start row, then 1, 2... (the score track's x axis; NOT a clock).",
    "home_points": "Home running total after the event.",
    "away_points": "Away running total after the event.",
}
PERIOD: Dict[str, str] = {
    "points": "Points the team scored IN this period (not cumulative); from the header's score slots."
}
SCHED: Dict[str, str] = {
    "tab": "Division tab: 1 = B1, 2 = B2, 3 = B3.",
    "event": "Competition type of the page: 2 B1 league, 3 B1 Championship, 4 B1 relegation playoffs, 5 all-star, "
    "7 B2 league, 8 B2 playoffs, 11 B1-B2 promotion/relegation, 15 B3 league, 32 B3 playoffs.",
    "date": "Local game date, derived from the requested month and day (September to June; months 7-12 fall in the "
    "season's START year).",
    "round_label": "Round label as printed, 第N節 (round N); playoffs print other labels.",
    "round": "Round number parsed from round_label; null when the label is not 第N節.",
    "home_team_id": "Home club TeamID as text, mapped from the page's club filter. " + TEAM_ID,
    "home_team": "Home club short name in Japanese as the page prints it (大阪 = Osaka).",
    "home_score": "Home final score; null for a game without one.",
    "away_team_id": "Away club TeamID as text, mapped from the page's club filter. " + TEAM_ID,
    "away_team": "Away club short name in Japanese as the page prints it (三遠 = San-en).",
    "away_score": "Away final score; null for a game without one.",
    "prefecture": "Prefecture of the venue in Japanese (大阪府 = Osaka Prefecture); null when the page gives none.",
    "venue": "Venue name in Japanese, abbreviated by the page (おおきに = Osaka Edion Arena).",
    "tip_time": "Tip-off time 'HH:MM' in JST (no offset on the page); null when absent.",
    "state": "Score-state label on the page (FINAL for a finished game); null when absent.",
    "team_id": TEAM_ID,
    "name": "Club short name in Japanese as the schedule page prints it (北海道 = Hokkaido).",
    "event_id": "Competition type code (see event); the page's event filter value.",
    "label": "Competition label in Japanese (B1リーグ = B1 league, オールスターゲーム = all-star game, "
    "B1チャンピオンシップ = B1 Championship).",
    "selected": "True for the competition the page was asked for.",
}
STAND: Dict[str, str] = {
    "tab": "Division tab: 1 = B1, 2 = B2, 3 = B3.",
    "season_label": "Season label of the selected option (2025-26).",
    "section": "Table heading in Japanese: 東地区 East, 中地区 Central, 西地区 West, ワイルドカード wild card.",
    "section_en": "English name of section for the four known headings; null for any other heading.",
    "rank": "Rank in that table (順位); a club in a division table also appears in the wild-card table.",
    "team_id": TEAM_ID,
    "team": "Club name in Japanese as the standings page prints it (千葉ジェッツ = Chiba Jets).",
    "team_short": "Club short name in Japanese (千葉J).",
    "wins": "Games won (勝) so far this season in the table.",
    "losses": "Games lost (負) so far this season in the table.",
    "win_pct": "Win percentage (勝率) as a 0-1 fraction.",
    "games_behind": "Games behind the table leader (差).",
    "points_for": "Points scored (得点) over the season to date.",
    "points_against": "Points allowed (失点) over the season to date.",
    "point_diff": "Point differential (得失点差).",
    "home_record": "Home record W-L (ホーム).",
    "away_record": "Away record W-L (アウェー).",
    "last_n": "Length of the recent-form window the page shows: 5 or 10 games (過去5試合 / 過去10試合).",
    "last_n_record": "Record W-L over the last_n games.",
    "streak": "Current streak (連勝/連敗) as printed: W5 = five straight wins, L1 = one loss.",
    "division_title": "Division-title mark (地区優勝) as printed; null when none ('--').",
}

SHORT_DESC: Dict[str, Dict[str, str]] = {
    "game_info": _game_info(),
    "period_scores": PERIOD,
    "player_box": BOX,
    "team_box": BOX,
    "team_summary": SUMMARY,
    "pbp": PBP,
    "shots": {**PBP, **SHOTS},
    "leaders": LEADERS,
    "score_track": TRACK,
    "schedule": SCHED,
    "schedule_clubs": SCHED,
    "schedule_events": SCHED,
    "schedule_days": {**SCHED, "date": "Local date of a game day in the requested month (a date strip entry)."},
    "standings": STAND,
}


def _describe(short: str, col: str) -> str:
    return SHORT_DESC[short].get(col) or COMMON.get(col) or ""


def _frame(entry: Entry) -> pl.DataFrame:
    frame = entry[2](entry[3]())
    assert isinstance(frame, pl.DataFrame)
    return frame


def _endpoint(short: str, entry: Entry) -> Dict[str, Any]:
    _sp, path, parser, _fix, summary, fields = entry
    names = [p["name"] for p in fields.get("path_params", [])]
    assert not {"league", "sport"} & set(names), path  # the renderer substitutes those as ESPN slugs
    assert set(re.findall(r"\{([^}]+)\}", path)) == set(names), path
    return {
        "short": short,
        "summary": summary,
        "path": path,
        "parser": parser.__name__,
        "returns_schema": f"native/bleague/{short}",
        **copy.deepcopy(fields),
    }


def main() -> None:
    refs = refs_dir("bleague", SPEC)
    spec = load_spec(refs, SPEC)
    missing = {e[0] for e in ENDPOINTS.values() if e[0] != "/game_detail/"} - set(spec["paths"])
    if missing or "/game_detail/" not in spec["paths"]:
        raise SystemExit(f"bleague: spec lacks routes {sorted(missing)} (or /game_detail/)")
    assert set(P.SPECS) == set(ENDPOINTS), "every parser spec needs an endpoint and the reverse"
    doc = {
        "api": "bleague",
        "host": HOST,
        "name_pattern": "bleague_{short}",
        "module": "bleague",
        "parser_module": "basketball.bleague.bleague_parsers",
        "getter_module": "sportsdataverse.basketball.bleague.bleague_runtime",
        "qualifier": "",
        "passthrough_query": False,
        "parser_columns": True,
        "schema_prefix": SCHEMA_PREFIX,
        "runtime_imports": ["_get"],
        "docstring": {
            "example_import": True,
            "raw_doc": "the raw body (``dict``): the game context (``Game``, ``PlayByPlays``, ``HomeBoxscores``, "
            "``AwayBoxscores``, ``Summaries`` and on v1 ``Leaders``, ``Points``) stamped ``_request`` "
            "(``schedule_key``, ``feed``), or the schedule / standings page record",
            "raises": [
                "sportsdataverse.errors.NoDataError: B.LEAGUE has nothing for the request (a ScheduleKey no source "
                "holds, a schedule day without games, HTTP 404).",
                "sportsdataverse.errors.AssetFetchError: the fetch failed or the answer is not the one asked for "
                "(an unfinished game, a context or page for another game, season, tab or day, an error or changed "
                "body, a truncated schedule list).",
                "ValueError: a non-integer or out-of-range argument, or HTTP 400 / 422.",
            ],
            "notes": [UNOFFICIAL, SEASONS, COVERAGE],
            "see_also": [
                {
                    "name": "B.LEAGUE",
                    "url": "https://www.bleague.jp",
                    "note": "the Japan Professional Basketball League site whose pages and game store these wrappers read",
                },
                {
                    "name": "wehoop",
                    "url": "https://wehoop.sportsdataverse.org",
                    "note": "SportsDataverse women's basketball (R)",
                },
                {
                    "name": "hoopR",
                    "url": "https://hoopR.sportsdataverse.org",
                    "note": "SportsDataverse men's basketball (R)",
                },
            ],
        },
        "endpoints": [_endpoint(short, entry) for short, entry in ENDPOINTS.items()],
    }
    write_yaml(ROOT / "tools/codegen/endpoints/bleague.yaml", doc)

    schema_dir = ROOT / "tools/codegen/schemas/native/bleague"
    rewrite_schema_dir(schema_dir)
    notes: Dict[str, Dict[str, str]] = {}
    bad: List[str] = []
    for short, entry in ENDPOINTS.items():
        cols = columns_from_frame(_frame(entry), [], leaf_fallback=False)
        block = notes.setdefault(f"{SCHEMA_PREFIX}{short}", {})
        for col in cols:
            text = _describe(short, col["name"]).strip()
            if len(text) < 15 or text.lower() == col["name"].lower() or _BANNED.match(text):
                bad.append(f"{SCHEMA_PREFIX}{short}.{col['name']}: {text!r}")
            block[col["name"]] = text
            col["description"] = ""
        write_yaml(
            schema_dir / f"{short}.yaml", {"schema": f"{SCHEMA_PREFIX}{short}", "kind": "dataframe", "columns": cols}
        )
    if bad:
        raise SystemExit(f"bleague: empty or filler descriptions (add them to the tables): {bad}")
    write_yaml(NOTES, notes)
    SPEC_PATHS.write_text(json.dumps(sorted(spec["paths"]), indent=1) + "\n", encoding="utf-8", newline="\n")
    print(f"bleague: {len(ENDPOINTS)} endpoints, {sum(map(len, notes.values()))} descriptions -> {NOTES.name}")


if __name__ == "__main__":
    main()
