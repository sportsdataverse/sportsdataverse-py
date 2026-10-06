"""Generate the ``euroleague`` flat-API endpoint YAML + returns-schemas from the
EuroLeague OpenAPI spec (three keyless hosts, one spec with per-path ``servers``).

Idempotent: same spec + same captures -> byte-identical output. Modeled on
``gen_asa.py``, with the shared plumbing in ``gen_soccer_common.py``.

The generator reads the frozen spec from the ``sdv-internal-refs`` checkout
(``$SDV_INTERNAL_REFS_REPO``, else the sibling checkout) and emits:

* ``tools/codegen/endpoints/euroleague.yaml`` -- the family host is
  ``https://api-live.euroleague.net/v2``; a path whose spec entry carries its own
  ``servers`` (the v3 standings / season stats / game report, the
  ``live.euroleague.net/api`` per-game routes) gets an endpoint-level ``host``
  (the ``gen_fotmob.py`` / ``gen_mls.py`` mechanism). The getter
  ``sportsdataverse.euroleague.euroleague_runtime._get`` picks the body contract
  by host: ``Accept: application/json`` for api-live (it answers XML otherwise),
  and ``{}`` for the live API's empty-200 "no such game" sentinel.
* ``tools/codegen/schemas/native/euroleague/<short>.yaml`` -- returns-schema per
  endpoint, columns taken from running the parser over the committed capture.

Sibling routes that differ only in their last path segment are **one wrapper**:
the segment becomes a python argument with a default (a path token) and the
returns-schema one table per value (``kind: frames`` + ``frames_by``, the
``gen_pff_api.py`` mechanism) -- ``standings`` (``kind`` = basicstandings /
calendarstandings / streaks / aheadbehind), ``player_stats`` and ``team_stats``
(``mode`` = traditional / advanced).

Path tokens are emitted snake_cased (``{competitionCode}`` -> ``{competition_code}``)
because the renderer substitutes a path token by the python argument of the same
name. The spec uses no ``{league}`` / ``{sport}`` token; the build asserts it, since
the renderer would substitute those as ESPN slugs and blank the segment.

Run: ``python tools/codegen/gen_euroleague.py``
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from gen_soccer_common import (
    ROOT,
    columns_from_frame,
    load_spec,
    markdown_descriptions,
    parse_capture,
    refs_dir,
    rewrite_schema_dir,
    spec_descriptions,
    write_yaml,
)

from sportsdataverse.dl_utils import underscore
from sportsdataverse.euroleague import euroleague_parsers

HOST = "https://api-live.euroleague.net/v2"
LIVE_HOST = "https://live.euroleague.net/api"
SPEC = "euroleague.openapi.yaml"
STEM = "euroleague"

# The committed samples are pinned to one season + game + round (sdv-internal-refs
# euroleague/tools/capture.py SEASON / GAME); captures live under captures/<SEASON>/.
SEASON = "E2025"
GAME = 1
_CAPTURE_VALUES = {"competitionCode": "E", "seasonCode": SEASON, "gameCode": str(GAME), "round": "1"}

# Live-verified example arguments for the docs (E = EuroLeague, E2025 = 2025-26).
_EXAMPLE: Dict[str, Any] = {"competition_code": "E", "season_code": SEASON, "game_code": GAME, "round": 1}

# operationId prefixes the shorts drop: every api-live route hangs off a competition.
_OP_PREFIXES = ("competitions_competitionCode_seasons_seasonCode_", "competitions_competitionCode_")

# The live API's operationIds are its bare route names.
_LIVE_SHORTS = {"Points": "game_points", "PlayByPlay": "game_pbp", "Boxscore": "game_boxscore", "Header": "game_header"}
# One parser per live route, each carrying its documented schema for the empty-body case
# (``_euroleague_schemas.py``, generated below from the captures).
_LIVE_PARSERS = {
    "game_points": "parse_euroleague_points",
    "game_header": "parse_euroleague_header",
    "game_pbp": "parse_euroleague_pbp",
    "game_boxscore": "parse_euroleague_boxscore",
}
_SCHEMAS_MODULE = ROOT / "sportsdataverse" / "euroleague" / "_euroleague_schemas.py"

# One wrapper over sibling routes: ``members`` are the (unmerged) shorts in the order
# the values are documented, the first being the default of the ``token`` argument.
_MERGED: Dict[str, Dict[str, Any]] = {
    "standings": {
        "token": "kind",
        "members": [
            "rounds_round_basicstandings",
            "rounds_round_calendarstandings",
            "rounds_round_streaks",
            "rounds_round_aheadbehind",
        ],
        "summary": "Standings as of a round: basic (W-L, points, home/away/last-10 records), "
        "calendar (per-round result streaks), streaks (longest win/loss runs) or aheadbehind "
        "(records when ahead/behind/tied after Q1, the half and Q3).",
        "token_doc": "Standings table: basicstandings (default), calendarstandings, streaks or aheadbehind; "
        "the columns depend on it (see Returns).",
    },
    "player_stats": {
        "token": "mode",
        "members": ["statistics_players_traditional", "statistics_players_advanced"],
        "summary": "Season player stats, traditional (box-score totals or per-game averages) or advanced "
        "(eFG%, TS%, rebound / assist / turnover rates), one row per player.",
        "token_doc": "traditional (default) or advanced; the columns depend on it (see Returns).",
    },
    "team_stats": {
        "token": "mode",
        "members": ["statistics_teams_traditional", "statistics_teams_advanced"],
        "summary": "Season team stats, traditional (box-score totals or per-game averages) or advanced "
        "(eFG%, TS%, four-factor style rates), one row per team.",
        "token_doc": "traditional (default) or advanced; the columns depend on it (see Returns).",
    },
}
_MEMBER_OF = {m: short for short, grp in _MERGED.items() for m in grp["members"]}

_TOKEN = re.compile(r"\{([^}]+)\}")

# Query keys ``underscore`` cannot split, mapped to the python name the v2 path
# params already use so one ``season_code`` / ``game_code`` serves every host.
_QUERY_NAMES = {"gamecode": "game_code", "seasoncode": "season_code"}
# The spec's paging params carry no description.
_QUERY_FALLBACK = {"limit": "Page size (number of rows to return).", "offset": "Row offset into the full list."}
# Required query params with a capture-verified default, so a stats call needs only
# the competition + season (other values are unverified -- see the recon README).
_QUERY_DEFAULTS = {"SeasonMode": "Single", "statisticMode": "PerGame"}

# Capture-derived spec: no property docs. Curated prose for the stable EuroLeague
# vocabulary, keyed by the UNPREFIXED leaf so one entry covers ``code`` /
# ``season_code`` / ``local_club_code`` alike (container prefixes are stripped
# recursively by :class:`_Prefixed`).
_LEAF: Dict[str, str] = {
    "code": "EuroLeague code of the entity (competition, season, club, person or venue; Utf8 join key).",
    "name": "Display name.",
    "alias": "Short display alias.",
    "competition_code": "Competition code (E = EuroLeague, U = EuroCup; Utf8 join key).",
    "season_code": "Season code: competition code + start year, e.g. E2025 for 2025-26 (Utf8 join key).",
    "game_code": "Game number within the season (1-based; Utf8 join key).",
    "id": "Provider identifier for the entity (Utf8 join key).",
    "identifier": "Season-qualified game identifier, e.g. E2025_1.",
    "external_id": "External (statistics-provider) identifier of the person (Utf8 join key).",
    "tv_code": "Three-letter broadcast abbreviation of the club.",
    "tv_codes": "Three-letter broadcast abbreviation(s) of the club.",
    "venue_code": "Code of the venue the club or game plays at (Utf8 join key).",
    "year": "Start year of the season.",
    "start_date": "Start date (ISO 8601).",
    "end_date": "End date (ISO 8601).",
    "activation_date": "Date the season was activated in the engine (ISO 8601).",
    "round": "Round number within the season.",
    "round_name": "Display name of the round.",
    "round_alias": "Short alias of the round.",
    "index": "Ordinal position of the round within the season.",
    "phase_type_code": "Phase type code (RS = regular season, PO = playoffs, ...).",
    "dates_formmated": "Human-readable date range of the round (sic: the API spells it this way).",
    "min_game_start_date": "Earliest game start in the round (ISO 8601).",
    "max_game_start_date": "Latest game start in the round (ISO 8601).",
    "abbreviated_name": "Abbreviated display name.",
    "editorial_name": "Editorial (long-form) display name.",
    "club_permanent_name": "Permanent club name independent of sponsor naming.",
    "club_permanent_alias": "Permanent club alias independent of sponsor naming.",
    "is_virtual": "Whether the club is a placeholder rather than a real club.",
    "images_crest": "URL of the club crest image.",
    "image_url": "URL of the image.",
    "country_code": "ISO country code.",
    "country_name": "Country name.",
    "partials1": "Points scored in the 1st quarter.",
    "partials2": "Points scored in the 2nd quarter.",
    "partials3": "Points scored in the 3rd quarter.",
    "partials4": "Points scored in the 4th quarter.",
    "extra_periods": "Points scored per overtime period, JSON-encoded.",
    "city": "City the club is based in.",
    "address": "Street address.",
    "phone": "Contact phone number.",
    "president": "Club president.",
    "sponsor": "Club sponsor.",
    "tickets_url": "Ticketing URL.",
    "twitter_account": "Twitter / X handle.",
    "website": "Official website URL.",
    "active": "Whether the record is currently active.",
    "dorsal": "Jersey number as displayed.",
    "dorsal_raw": "Jersey number as stored by the engine.",
    "position": "Position code of the player (engine integer).",
    "position_name": "Position name of the player.",
    "type": "Person type code (J = player, E = coach, ...).",
    "type_name": "Person type name.",
    "order": "Display order within the roster.",
    "last_team": "Previous club of the person.",
    "date": "Scheduled tip-off (ISO 8601, venue local time).",
    "local_date": "Scheduled tip-off in venue local time (ISO 8601).",
    "utc_date": "Scheduled tip-off in UTC (ISO 8601).",
    "local_time_zone": "UTC offset of the venue, in hours.",
    "played": "Whether the game has been played.",
    "game_status": "Game status (scheduled, live, result).",
    "confirmed_date": "Whether the game date is confirmed.",
    "confirmed_hour": "Whether the tip-off time is confirmed.",
    "is_neutral_venue": "Whether the game is played at a neutral venue.",
    "audience": "Attendance.",
    "audience_confirmed": "Whether the attendance figure is confirmed.",
    "social_feed": "Social-media hashtag or feed tag for the game.",
    "operations_code": "Engine operations code of the game.",
    "score": "Final score of the side.",
    "standings_score": "Score of the side as counted for the standings.",
    "capacity": "Seating capacity of the venue.",
    "notes": "Venue notes.",
    "images_medium": "URL of the medium-size venue image.",
    "images_vertical_small": "URL of the small portrait image.",
    "is_group_phase": "Whether the phase is played in groups.",
    "raw_name": "Group name as stored by the engine.",
    "players": "Per-player box-score rows for the side, JSON-encoded.",
    "alias_raw": "Short alias as stored by the engine.",
    "passport_name": "Given name as on the passport.",
    "passport_surname": "Surname as on the passport.",
    "jersey_name": "Name printed on the jersey.",
    "height": "Height in centimetres.",
    "weight": "Weight in kilograms.",
    "birth_date": "Date of birth (ISO 8601).",
    "instagram_account": "Instagram handle.",
    "facebook_account": "Facebook handle.",
    "is_referee": "Whether the person is a referee.",
    "referee4": "Fourth referee (null unless a fourth official is assigned).",
    "winner": "Winning club of the season (null while in progress).",
    "last5_form": "Results of the last five games, oldest first (W / L), JSON-encoded.",
    # --- v3 standings (one row per team, as of the requested round) ---
    "position_change": "Movement since the previous round (Up, Down, Equal).",
    "games_played": "Games played.",
    "games_won": "Games won.",
    "games_lost": "Games lost.",
    "qualified": "Whether the team has clinched qualification for the next phase.",
    "win_percentage": "Win percentage, formatted (e.g. 100%).",
    "wins_percentage": "Win percentage, formatted (e.g. 100%).",
    "points_difference": "Points for minus points against, signed and formatted (e.g. +19).",
    "points_for": "Points scored.",
    "points_against": "Points conceded.",
    "home_record": "Home win-loss record (W-L).",
    "away_record": "Away win-loss record (W-L).",
    "neutral_record": "Neutral-venue win-loss record (W-L).",
    "overtime_record": "Overtime win-loss record (W-L).",
    "last_ten_record": "Win-loss record over the last ten games (W-L).",
    "last10": "Win-loss record over the last ten games (W-L).",
    "home_last5": "Win-loss record over the last five home games (W-L).",
    "away_last5": "Win-loss record over the last five away games (W-L).",
    "streaks": "Per-round result streaks: start date, end date and W-L record of each run, JSON-encoded.",
    "longest_wins_streak_current_season": "Longest winning streak this season.",
    "longest_loses_streak_current_season": "Longest losing streak this season.",
    "longest_wins_streak_any_season": "Longest winning streak in any season.",
    "longest_loses_streak_any_season": "Longest losing streak in any season.",
    # --- v3 season stats (traditional + advanced) ---
    "player_ranking": "Rank of the player on the requested statistic mode.",
    "team_ranking": "Rank of the team on the requested statistic mode.",
    "player_age": "Age of the player in years.",
    "team_code": "EuroLeague club code (Utf8 join key).",
    "team_name": "Club display name.",
    "team_tv_codes": "Three-letter broadcast abbreviation(s) of the club.",
    "team_image_url": "URL of the club crest image.",
    "games_started": "Games started.",
    "minutes_played": "Minutes played.",
    "points_scored": "Points scored.",
    "two_pointers_made": "Two-point field goals made.",
    "two_pointers_attempted": "Two-point field goals attempted.",
    "two_pointers_percentage": "Two-point field-goal percentage, formatted.",
    "three_pointers_made": "Three-point field goals made.",
    "three_pointers_attempted": "Three-point field goals attempted.",
    "three_pointers_percentage": "Three-point field-goal percentage, formatted.",
    "free_throws_percentage": "Free-throw percentage, formatted.",
    "assists": "Assists.",
    "blocks": "Blocks made.",
    "fouls_drawn": "Fouls drawn.",
    "pir": "Performance index rating (PIR).",
    "effective_field_goal_percentage": "Effective field-goal percentage, formatted.",
    "true_shooting_percentage": "True shooting percentage, formatted.",
    "offensive_rebounds_percentage": "Offensive rebound percentage, formatted.",
    "defensive_rebounds_percentage": "Defensive rebound percentage, formatted.",
    "rebounds_percentage": "Total rebound percentage, formatted.",
    "assists_to_turnovers_ratio": "Assist-to-turnover ratio.",
    "assists_ratio": "Assist ratio (assists per 100 possessions used), formatted.",
    "turnovers_ratio": "Turnover ratio (turnovers per 100 possessions used), formatted.",
    "two_point_attempts_ratio": "Share of field-goal attempts that are two-pointers, formatted.",
    "three_point_attempts_ratio": "Share of field-goal attempts that are three-pointers, formatted.",
    "two_point_rate": "Share of field-goal attempts that are two-pointers, formatted.",
    "three_point_rate": "Share of field-goal attempts that are three-pointers, formatted.",
    "free_throws_rate": "Free-throw rate (free-throw attempts per field-goal attempt), formatted.",
    "possesions": "Possessions (sic: the API spells it this way).",
    "points_from_two_pointers_percentage": "Share of points from two-pointers, formatted.",
    "points_from_three_pointers_percentage": "Share of points from three-pointers, formatted.",
    "points_from_free_throws_percentage": "Share of points from free throws, formatted.",
    # --- live API (live.euroleague.net/api): shots, play-by-play, box score, header ---
    "live": "Whether the game is in progress.",
    "num_anot": "Sequence number of the scoring annotation within the game.",
    "team": "EuroLeague club code of the team (Utf8 join key; space padding stripped).",
    "id_player": "EuroLeague player code (Utf8 join key; space padding stripped).",
    "player_id": "EuroLeague player code (Utf8 join key; space padding stripped; blank on team rows).",
    "player": "Player display name (SURNAME, GIVEN NAME).",
    "id_action": "Shot type code: 2FGM / 2FGA / 3FGM / 3FGA (made / attempted field goal) or FTM (made free throw).",
    "action": "Shot type label (Two Pointer, Three Pointer, Free Throw In, ...).",
    "points": "Points.",
    "coord_x": (
        "Shot x in integer centimeters from the hoop, signed left/right of it (-683 to 696 cm "
        "measured): both teams are mapped onto one basket; 3-point zone H is x < 0 and I is x > 0; "
        "which sideline is positive (from the shooter's view or from the scorer's table) is "
        "UNVERIFIED from the data alone. Made free throws (FTM) carry the -1 sentinel."
    ),
    "coord_y": (
        "Shot y in integer centimeters from the hoop, growing away from the baseline toward the court "
        "(-6 to 865 cm measured; 3FG rows at 414-865): both teams are mapped onto one basket, negative "
        "y is behind the hoop. Made free throws (FTM) carry the -1 sentinel."
    ),
    "zone": "Court zone letter: A rim, B/C close left/right, D/E mid, F/G long two, H/I three; blank on free throws.",
    "fastbreak": "Whether the shot came on a fast break (0 / 1 as a string).",
    "second_chance": "Whether the shot was a second-chance attempt (0 / 1 as a string).",
    "points_off_turnover": "Whether the shot came off a turnover (0 / 1 as a string).",
    "minute": "Game minute of the event (1-based; 41+ in overtime).",
    "console": "Game clock at the event (mm:ss remaining in the period).",
    "points_a": "Running score of team A (= the home side, measured on one game) after the event.",
    "points_b": "Running score of team B (= the away side, measured on one game) after the event.",
    "utc": "UTC timestamp of the event (yyyymmddHHMMSS).",
    "quarter": "Period of the play: 1-4, or 5 for every overtime period.",
    "team_a": "Display name of team A (= the home side, measured on one game).",
    "team_b": "Display name of team B (= the away side, measured on one game).",
    "code_team_a": "EuroLeague club code of team A (= the home side, measured on one game); Utf8 join key.",
    "code_team_b": "EuroLeague club code of team B (= the away side, measured on one game); Utf8 join key.",
    "tv_code_a": "Three-letter broadcast abbreviation of team A.",
    "tv_code_b": "Three-letter broadcast abbreviation of team B.",
    "numberofplay": "Sequence number of the play within the game.",
    "codeteam": "EuroLeague club code of the team on the play (Utf8 join key; blank on administrative plays).",
    "playtype": "Play type code (BP = begin period, 2FGM, 3FGA, FTM, AS = assist, TO, RV, CM, ...).",
    "markertime": "Game clock at the play (mm:ss remaining in the period).",
    "comment": "Free-text annotation of the play.",
    "playinfo": "Play description.",
    "row_type": "Row kind: player, team (team-only rebounds) or total (side totals).",
    "coach": "Head coach name.",
    "attendance": "Attendance, as reported by the box score (repeated on every row).",
    "referees": "Referees of the game, comma-separated SURNAME, GIVEN NAME (repeated on every row).",
    "is_starter": "Whether the player started (1 / 0).",
    "is_playing": "Whether the player appeared in the game (1 / 0).",
    "minutes": "Minutes played (mm:ss).",
    "plusminus": "Plus/minus.",
    "hour": "Scheduled tip-off time (venue local, HH:MM).",
    "stadium": "Venue name.",
    "im_a": "Crest image file name of team A.",
    "im_b": "Crest image file name of team B.",
    "score_a": "Score of team A (= the home side, measured on one game).",
    "score_b": "Score of team B (= the away side, measured on one game).",
    "coach_a": "Head coach of team A.",
    "coach_b": "Head coach of team B.",
    "game_time": "Elapsed game time (mm:ss).",
    "remaining_partial_time": "Time remaining in the current period (mm:ss).",
    "wid": "Live-feed widget identifier of the game.",
    "foults_a": "Team fouls of team A in the current period (sic: the API spells it this way).",
    "foults_b": "Team fouls of team B in the current period (sic: the API spells it this way).",
    "timeouts_a": "Timeouts used by team A.",
    "timeouts_b": "Timeouts used by team B.",
    "score_extra_time_a": "Points scored by team A in overtime (0 when none).",
    "score_extra_time_b": "Points scored by team B in overtime (0 when none).",
    "phase": "Phase name (Regular Season, Playoffs, ...).",
    "phase_reduced_name": "Short phase name.",
    "competition": "Competition name.",
    "competition_reduced_name": "Short competition name.",
    "pcom": "Competition code of the live feed (E = EuroLeague, U = EuroCup).",
    "referee1": "First referee.",
    "referee2": "Second referee.",
    "referee3": "Third referee.",
}
for _q in (1, 2, 3, 4):
    for _side, _label in (("a", "team A"), ("b", "team B")):
        _LEAF[f"score_quarter{_q}_{_side}"] = f"Score of {_label} at the end of quarter {_q} (cumulative)."
    _LEAF[f"by_quarter_q{_q}"] = f"Points the row's team scored in quarter {_q} (from the box score's ByQuarter block)."
    _LEAF[f"end_of_quarter_q{_q}"] = (
        f"Score of the row's team at the end of quarter {_q} (cumulative; from the box score's EndOfQuarter block)."
    )
for _when, _label in (
    ("quater1", "after the 1st quarter"),
    ("half1", "at the half"),
    ("quater3", "after the 3rd quarter"),
):
    for _state in ("ahead", "behind", "tied"):
        _LEAF[f"{_when}_{_state}"] = (
            f"Win-loss record when {_state} {_label} (W-L; sic: the API spells quarter this way)."
        )

# Box-score stat leaves; they appear as ``local_team_*`` / ``local_total_*`` /
# ``road_team_*`` / ``road_total_*`` columns and resolve through :class:`_Prefixed`.
_STAT: Dict[str, str] = {
    "time_played": "Seconds played.",
    "valuation": "Performance index rating (PIR).",
    "points": "Points.",
    "field_goals_made2": "Two-point field goals made.",
    "field_goals_attempted2": "Two-point field goals attempted.",
    "field_goals_made3": "Three-point field goals made.",
    "field_goals_attempted3": "Three-point field goals attempted.",
    "free_throws_made": "Free throws made.",
    "free_throws_attempted": "Free throws attempted.",
    "field_goals_made_total": "Field goals made.",
    "field_goals_attempted_total": "Field goals attempted.",
    "accuracy_made": "Shots made (accuracy numerator).",
    "accuracy_attempted": "Shots attempted (accuracy denominator).",
    "assistances": "Assists.",
    "steals": "Steals.",
    "turnovers": "Turnovers.",
    "blocks_favour": "Blocks made.",
    "blocks_against": "Shots blocked by the opponent.",
    "fouls_commited": "Personal fouls committed.",
    "fouls_received": "Fouls drawn.",
    "offensive_rebounds": "Offensive rebounds.",
    "defensive_rebounds": "Defensive rebounds.",
    "total_rebounds": "Total rebounds.",
    "plus_minus": "Plus/minus.",
    "start_five": "Whether the player started.",
    "coach_code": "Head coach code (Utf8 join key).",
    "coach_name": "Head coach name.",
}

# A leaf whose meaning differs on one route wins there over the shared prose.
_PER_SHORT: Dict[str, Dict[str, str]] = {
    "game_pbp": {
        "type": "Play type (engine integer).",
        "team": "Display name of the team on the play (null on administrative plays).",
        "points": "Points scored on the play.",
    },
    "game_points": {"points": "Points the shot is worth (1, 2 or 3)."},
    "game_header": {
        "round": "Round label (e.g. Round 1).",
        "capacity": "Capacity as reported by the API (equals the box score's attendance on the captured game; semantics unverified).",
        "quarter": "Current period of the game.",
        "date": "Game date (venue local, dd/mm/yyyy).",
    },
}

# Container prefixes ``json_normalize`` bolts onto the nested EuroLeague objects,
# longest first; the label qualifies the leaf prose. Resolved recursively, so
# ``local_total_points`` -> "Home side: totals: points."
_PREFIXES = [
    ("birth_country_", "Birth country: "),
    ("phase_type_", "Phase type: "),
    ("referee1_", "First referee: "),
    ("referee2_", "Second referee: "),
    ("referee3_", "Third referee: "),
    ("referee4_", "Fourth referee: "),
    ("country_", "Country: "),
    ("partials_", "Quarter scores: "),
    ("winner_", "Winning club: "),
    ("person_", "Person: "),
    ("player_", "Player: "),
    ("season_", "Season: "),
    ("venue_", "Venue: "),
    ("local_", "Home side: "),
    ("group_", "Group: "),
    ("total_", "Totals: "),
    ("team_", "Team-only (not attributed to a player): "),
    ("road_", "Away side: "),
    ("club_", "Club: "),
]


class _Prefixed(dict):
    """Description map that strips known container prefixes recursively on a miss."""

    def get(self, name: str, default: Any = None) -> Any:
        try:
            return self[name]
        except KeyError:
            return default

    def __getitem__(self, name: str) -> str:
        if dict.__contains__(self, name):
            return dict.__getitem__(self, name)
        for prefix, label in _PREFIXES:
            if name.startswith(prefix) and len(name) > len(prefix):
                inner = self.get(name[len(prefix) :])
                if inner:
                    # keep an acronym's capital (``ISO``), lower an ordinary first word
                    head = inner if inner[:2].isupper() else inner[:1].lower() + inner[1:]
                    return label + head
        raise KeyError(name)


def _descriptions() -> _Prefixed:
    return _Prefixed({**_LEAF, **_STAT})


def _short(op: dict) -> str:
    """``competitions_competitionCode_seasons_seasonCode_games_gameCode_stats`` -> ``game_stats``."""
    short = op["operationId"]
    if short in _LIVE_SHORTS:
        return _LIVE_SHORTS[short]
    for prefix in _OP_PREFIXES:
        if short.startswith(prefix) and len(short) > len(prefix):
            short = short[len(prefix) :]
            break
    return short.replace("games_gameCode_", "game_")


def _snake_path(path: str) -> str:
    return _TOKEN.sub(lambda m: "{" + underscore(m.group(1)) + "}", path)


def _host(spec: dict, path: str) -> Optional[str]:
    """The path's own server (per-path ``servers``) when it is not the family host."""
    servers = spec["paths"][path].get("servers") or []
    url = str(servers[0]["url"]).rstrip("/") if servers else HOST
    return None if url == HOST else url


def _capture_for(refs: Path, path: str, host: Optional[str]) -> Path:
    """The committed capture: ``capture.py``'s slug rule (server tail + route) over the pinned season/game."""
    route = _TOKEN.sub(lambda m: _CAPTURE_VALUES[m.group(1)], path)
    slug = route.strip("/").replace("/", "__") or "root"
    prefix = f"{host.rsplit('/', 1)[-1]}__" if host else ""
    return refs / "captures" / SEASON / f"{prefix}{slug}.json"


def _path_params(op: dict) -> List[Dict[str, Any]]:
    return [
        {
            "name": underscore(prm["name"]),
            "type": "str",
            "required": True,
            "description": str(prm.get("description") or ""),
        }
        for prm in op.get("parameters", [])
        if prm.get("in") == "path"
    ]


def _query_params(op: dict, host: Optional[str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for prm in op.get("parameters", []):
        if prm.get("in") != "query":
            continue
        wire = prm["name"]
        desc = str(prm.get("description") or _QUERY_FALLBACK.get(wire, "")).strip()
        if prm.get("required"):
            desc = f"Required. {desc}".strip()
        entry: Dict[str, Any] = {
            "name": _QUERY_NAMES.get(wire) or underscore(wire),
            "query_key": wire,
            "type": "str",
            "description": desc,
        }
        if host == LIVE_HOST and prm.get("required"):
            # positional, not Optional: an omitted code answers a silent empty 200
            entry["required"] = True
        if wire in _QUERY_DEFAULTS:
            entry["default"] = _QUERY_DEFAULTS[wire]
            entry["description"] = f"{desc} Default {_QUERY_DEFAULTS[wire]}."
        out.append(entry)
    return out


def _docstring(host: Optional[str]) -> Dict[str, Any]:
    """The family docstring block; the live-API routes also name their ``{}`` sentinel."""
    doc: Dict[str, Any] = {
        "example_import": True,
        "raises": [
            "sportsdataverse.errors.NoDataError: the EuroLeague API returned 404 "
            "(unknown competition, season, round or game code).",
        ],
        "see_also": [
            {
                "name": "EuroLeague Basketball",
                "url": "https://www.euroleaguebasketball.net/",
                "note": "the site the Competition Engine and live APIs serve (competitions, seasons, clubs, games)",
            },
            {
                "name": "euroleague-api",
                "url": "https://github.com/giasemidis/euroleague_api",
                "note": "community Python client over the same v2 / v3 / live APIs",
            },
        ],
    }
    if host == LIVE_HOST:
        doc["raw_doc"] = (
            "the raw JSON ``Dict`` (``{}`` when the live API answers its empty-body "
            '"no such game" sentinel, which the parser turns into a zero-row frame)'
        )
    return doc


def _endpoint_entry(spec: dict, path: str, op: dict) -> Dict[str, Any]:
    short = _short(op)
    host = _host(spec, path)
    pps = _path_params(op)
    qps = _query_params(op, host)
    names = {p["name"] for p in pps}
    assert not {"league", "sport"} & names, path
    assert {underscore(t) for t in _TOKEN.findall(path)} == names, path
    entry: Dict[str, Any] = {
        "short": short,
        "summary": (op.get("summary") or f"Fetch {path}").rstrip(".") + ".",
        "path": _snake_path(path),
        "parser": _LIVE_PARSERS.get(short, "parse_euroleague"),
        "returns_schema": f"native/{STEM}/{short}",
        "example_args": {k: v for k, v in _EXAMPLE.items() if k in names | {q["name"] for q in qps}},
    }
    if host:
        entry["host"] = host
        entry["docstring"] = _docstring(host)
    if pps:
        entry["path_params"] = pps
    if qps:
        entry["extra_params"] = qps
    return entry


def _merge(
    short: str, members: List[Tuple[str, Dict[str, Any], Path]]
) -> Tuple[Dict[str, Any], List[Tuple[str, Path]]]:
    """One entry over sibling routes: the trailing segment becomes the ``token`` argument."""
    grp = _MERGED[short]
    token = grp["token"]
    values = [m.rsplit("_", 1)[-1] for m in grp["members"]]
    by_member = {m: (entry, capture) for m, entry, capture in members}
    assert set(by_member) == set(grp["members"]), f"{short}: members {sorted(by_member)} != {grp['members']}"
    entry, _ = by_member[grp["members"][0]]
    entry = dict(entry)
    assert entry["path"].endswith("/" + values[0]), entry["path"]
    entry["path"] = entry["path"][: -len(values[0])] + "{" + token + "}"
    entry["short"] = short
    entry["summary"] = grp["summary"]
    entry["returns_schema"] = f"native/{STEM}/{short}"
    entry["path_params"] = [
        *entry.get("path_params", []),
        {
            "name": token,
            "type": "str",
            "required": False,
            "default": values[0],
            "choices": values,
            "description": grp["token_doc"],
        },
    ]
    return entry, [(value, by_member[m][1]) for m, value in zip(grp["members"], values)]


def main() -> None:
    refs = refs_dir(STEM, SPEC)
    spec = load_spec(refs, SPEC)
    ops = sorted(((p, o["get"]) for p, o in spec["paths"].items() if "get" in o), key=lambda pv: _short(pv[1]))
    shorts = [_short(op) for _, op in ops]
    dupes = sorted({s for s in shorts if shorts.count(s) > 1})
    if dupes:
        raise SystemExit(f"duplicate {STEM} shorts: {dupes}")

    entries: List[Dict[str, Any]] = []
    # short -> [(value, capture)]: a plain route documents one table, a merged one per value
    captures: Dict[str, List[Tuple[Optional[str], Path]]] = {}
    pending: Dict[str, List[Tuple[str, Dict[str, Any], Path]]] = {}
    for path, op in ops:
        short = _short(op)
        entry = _endpoint_entry(spec, path, op)
        capture = _capture_for(refs, path, _host(spec, path))
        if short in _MEMBER_OF:
            pending.setdefault(_MEMBER_OF[short], []).append((short, entry, capture))
            continue
        entries.append(entry)
        captures[short] = [(None, capture)]
    for short in _MERGED:
        entry, by_value = _merge(short, pending[short])
        entries.append(entry)
        captures[short] = list(by_value)
    entries.sort(key=lambda e: e["short"])

    doc = {
        "api": STEM,
        "host": HOST,
        "name_pattern": "euroleague_{short}",
        "module": STEM,
        "parser_module": "euroleague.euroleague_parsers",
        "getter_module": "sportsdataverse.euroleague.euroleague_runtime",
        "qualifier": "",
        "passthrough_query": False,
        "docstring": _docstring(None),
        "runtime_imports": ["_get"],
        "endpoints": entries,
    }
    write_yaml(ROOT / f"tools/codegen/endpoints/{STEM}.yaml", doc)

    shared = [spec_descriptions(spec), markdown_descriptions(refs / f"{STEM}-returns.md"), _descriptions()]
    schema_dir = ROOT / f"tools/codegen/schemas/native/{STEM}"
    rewrite_schema_dir(schema_dir)
    live_schemas: Dict[str, Dict[str, str]] = {}
    for entry in entries:
        short = entry["short"]
        parser = getattr(euroleague_parsers, entry["parser"])
        descriptions = [_PER_SHORT.get(short, {}), *shared]
        tables = []
        missing = []
        for value, capture in captures[short]:
            if capture.exists():
                frame = parse_capture(capture, parser)
                tables.append((value, columns_from_frame(frame, descriptions)))
                if short in _LIVE_PARSERS:
                    live_schemas[short] = {c: str(t) for c, t in frame.schema.items()}
            else:
                missing.append(capture.name)
        schema: Dict[str, Any] = {"schema": short}
        if short in _MERGED:
            schema["kind"] = "frames"
            schema["frames_by"] = _MERGED[short]["token"]
            schema["frames"] = [{"section": value, "columns": cols} for value, cols in tables]
        else:
            schema["kind"] = "dataframe"
            schema["columns"] = tables[0][1] if tables else []
        if missing:
            schema["unverified"] = (
                f"no committed capture in sdv-internal-refs/{STEM}/captures/{SEASON} ({', '.join(missing)})"
            )
        write_yaml(schema_dir / f"{short}.yaml", schema)
    _write_schemas_module(live_schemas)
    print(f"{STEM}: {len(entries)} endpoints ({len(ops)} routes)")


def _write_schemas_module(schemas: Dict[str, Dict[str, str]]) -> None:
    """The live parsers' documented schemas, so an empty body yields a zero-row frame WITH columns."""
    lines = [
        "# GENERATED by tools/codegen/gen_euroleague.py -- DO NOT EDIT.",
        '"""Documented polars schema per live-API route (the columns its parser emits on the',
        'committed capture), so the empty-body "no such game" answer parses to a zero-row frame',
        'that still carries the returns table."""',
        "",
        "from __future__ import annotations",
        "",
        "from typing import Dict",
        "",
        "SCHEMAS: Dict[str, Dict[str, str]] = {",
    ]
    for short in sorted(schemas):
        lines.append(f'    "{short}": {{')
        lines += [f'        "{col}": "{dtype}",' for col, dtype in schemas[short].items()]
        lines.append("    },")
    lines.append("}")
    _SCHEMAS_MODULE.write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")


if __name__ == "__main__":
    main()
