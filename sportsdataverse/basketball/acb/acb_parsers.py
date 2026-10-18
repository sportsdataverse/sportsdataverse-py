"""Parsers for the generated ``acb`` wrappers (Spain's ACB: Liga Endesa, Copa del Rey, Supercopa, Minicopa).

One JSON host (``api2.acb.com``), 33 routes, 45 frames. Each public ``parse_acb_*`` takes the dict the wrapper
returns with ``return_parsed=False`` (the body, with the request stamped under ``_request`` by
``acb_runtime``) and follows the package parser contract: a polars frame by default, pandas with
``return_as_pandas=True``, snake_case columns, and a zero-row frame with the documented columns and dtypes,
never an exception, for an empty or unrecognised input. Several wrappers read one route (the match header feeds
``acb_match_header`` and ``acb_period_scores``; the box score feeds ``acb_player_box``, ``acb_team_box`` and
``acb_box_info``), so fetch it once and parse it as many ways as needed.

**Dtypes** are pinned by column name in :data:`FIELDS`, never sampled: ids and counts ``Int64``; rates,
averages, per-40 values and percentages ``Float64`` (the API sends whole-number rates as integers, so a sample
would mistype them); flags ``Boolean``; instants ``Datetime("us")`` in UTC (``...Z`` strings, naive); birth
dates ``Date``. Shirt numbers stay strings (``"0"`` and ``"00"`` differ).

**Id key spaces.** Every id is ``Int64``.

* ``match_id``: one global sequence for every competition. Legacy ids (up to 2019-20) are below about
  20000, from 2020-21 they start around 100000. The switch also changes the shot coordinate system.
* ``team_id``: a SEASON-TEAM entry (one club in one competition edition), NEW every season (Barca = 4467 in
  Liga 2026-27). ``club_id`` is the stable club (2 = Barca, 9 = Real Madrid): the ``club_id`` request argument
  and the only id that follows a club across seasons. Never join teams of different seasons on ``team_id``.
* ``player_id``: the ACB licence number, one key across the box score, play-by-play, shots, leaders, rosters
  and the player routes (``playerLicenceId`` / ``playerLicense`` request params). Older licences start
  ``2020...``, recent ones ``3000...``. ``person_id`` (rosters, search) is the same number for players and
  coaches.
* ``edition_id``: season START year - 1935 (91 = 2026-27). ``season`` is the END year (``edition_id + 1936``,
  2027 = 2026-27) and is the key the wrappers take.
* ``round_id`` / ``week_id``: global ids; ``round_number`` is the number within the phase.

**Time.** The API has four time formats: ``play_time`` / ``time_played`` are ``'mm:ss'`` strings (parsed to
``seconds_played`` where a column exists), season totals are integer seconds, season averages seconds with
decimals, and ``minute`` / ``second`` of an event are the clock REMAINING in a 10-minute period (5 in
overtime). ``clock_seconds`` is that clock as whole seconds. ``start`` / ``date`` instants are UTC.

**Spanish.** Positions (``game_role``: Base, Escolta, Alero, Ala-pivot, Pivot), phases, weeks, the ``licensing``
codes and nationalities are Spanish as sent; ``nationality_iso`` maps the 31 observed countries to ISO 3166-1
alpha-2. Comparison bars send display strings with a decimal comma (``'79,17%'``); they are parsed to numbers.
"""

from __future__ import annotations

import math
import re
from datetime import date, datetime
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple, Union

import pandas as pd
import polars as pl

from sportsdataverse.basketball.acb.acb_codes import COUNTRIES, PLAY_TAGS, PLAY_TYPES
from sportsdataverse.dl_utils import underscore

__all__ = [
    "parse_acb_bracket",
    "parse_acb_bracket_games",
    "parse_acb_box_info",
    "parse_acb_calendar",
    "parse_acb_career_stints",
    "parse_acb_career_trophies",
    "parse_acb_club_data",
    "parse_acb_competition_current",
    "parse_acb_competition_editions",
    "parse_acb_competition_rounds",
    "parse_acb_competition_weeks",
    "parse_acb_current_roster",
    "parse_acb_lead_stats",
    "parse_acb_lead_tracker",
    "parse_acb_league_zones",
    "parse_acb_lineup",
    "parse_acb_match_advanced_stats",
    "parse_acb_match_header",
    "parse_acb_match_leaders",
    "parse_acb_matchlist",
    "parse_acb_next_matches",
    "parse_acb_pbp",
    "parse_acb_period_scores",
    "parse_acb_player_advanced_stats",
    "parse_acb_player_box",
    "parse_acb_player_data",
    "parse_acb_player_matches",
    "parse_acb_player_stats",
    "parse_acb_player_totals",
    "parse_acb_quickfacts",
    "parse_acb_round_matches",
    "parse_acb_search",
    "parse_acb_season_leaders",
    "parse_acb_serie",
    "parse_acb_shots",
    "parse_acb_standings",
    "parse_acb_standings_rounds",
    "parse_acb_team_box",
    "parse_acb_team_comparison",
    "parse_acb_team_leaders",
    "parse_acb_team_matches",
    "parse_acb_team_records",
    "parse_acb_team_stats",
    "parse_acb_team_totals",
    "parse_acb_teams",
]

Frame = Union[pl.DataFrame, pd.DataFrame]

#: Where the runtime stamps the request (``acb_runtime.REQUEST_KEY``).
_REQUEST = "_request"
#: ``season`` (END year) = ``edition_id`` + this.
_SEASON_OFFSET = 1936
#: Match ids from 2020-21 on start around 100000 and use the millimetre shot grid; older ids use a 0-100 grid.
_NEW_MATCH_ID = 50000
_SIDES = ("home", "away")

INT, F, S, B, D = pl.Int64, pl.Float64, pl.Utf8, pl.Boolean, pl.Date
_DT = pl.Datetime("us")
#: Column type letters used in :data:`FIELDS`.
_DTYPES: Dict[str, Any] = {"i": INT, "f": F, "s": S, "b": B, "t": _DT, "d": D, "m": INT, "p": F, "j": S}


# ------------------------------------------------------------------------------------------- casts
def _i(v: Any) -> Optional[int]:
    """An id / count from an int, an integral float or a numeric string, else ``None``. Never stringifies a float."""
    if isinstance(v, bool) or v is None:
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v) if v.is_integer() else None
    if isinstance(v, str) and re.fullmatch(r"\s*[+-]?\d+\s*", v):
        return int(v)
    return None


def _f(v: Any) -> Optional[float]:
    """A number from a number or a numeric string (``'2,20'`` decimal comma accepted), else ``None``."""
    if isinstance(v, bool) or v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str) and re.fullmatch(r"\s*[+-]?\d+(?:[.,]\d+)?\s*", v):
        return float(v.strip().replace(",", "."))
    return None


def _s(v: Any) -> Optional[str]:
    """A stripped non-empty string, else ``None`` (numbers are stringified; containers are not)."""
    if v is None or isinstance(v, (dict, list)):
        return None
    if isinstance(v, float):  # 123.0 -> "123" (never "123.0"); NaN / inf are not values
        if not math.isfinite(v):
            return None
        v = int(v) if v.is_integer() else v
    t = str(v).strip()
    return t or None


def _b(v: Any) -> Optional[bool]:
    return v if isinstance(v, bool) else None


def _utc(v: Any) -> Optional[datetime]:
    """``'2026-10-11T17:00:00Z'`` -> a naive UTC datetime."""
    if not isinstance(v, str):
        return None
    try:
        return datetime.fromisoformat(v.strip().replace("Z", "")).replace(tzinfo=None)
    except ValueError:
        return None


def _dmy(v: Any) -> Optional[date]:
    """``'22-03-1992'`` (DD-MM-YYYY) -> a date."""
    m = re.fullmatch(r"\s*(\d{2})-(\d{2})-(\d{4})\s*", v) if isinstance(v, str) else None
    if not m:
        return None
    try:
        return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
    except ValueError:
        return None


def _mmss(v: Any) -> Optional[int]:
    """``'23:42'`` -> 1422 seconds (minutes may exceed 59: ``'8665:53'``)."""
    m = re.fullmatch(r"\s*(\d+):(\d{2})\s*", v) if isinstance(v, str) else None
    return int(m.group(1)) * 60 + int(m.group(2)) if m else None


def _pct(v: Any) -> Optional[float]:
    """A display value (``'79,17%'``, ``'35'``) -> a number; a ``a/b`` ratio or text -> ``None``."""
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return float(v)
    m = re.fullmatch(r"\s*([+-]?\d+(?:[.,]\d+)?)\s*%?\s*", v) if isinstance(v, str) else None
    return float(m.group(1).replace(",", ".")) if m else None


def _join(v: Any) -> Optional[str]:
    """A list of strings joined with ``'; '``, else ``None``."""
    if not isinstance(v, list):
        return None
    out = [t for t in (_s(x) for x in v) if t]
    return "; ".join(out) if out else None


_CAST: Dict[str, Callable[[Any], Any]] = {
    "i": _i,
    "f": _f,
    "s": _s,
    "b": _b,
    "t": _utc,
    "d": _dmy,
    "m": _mmss,
    "p": _pct,
    "j": _join,
}


def _dig(d: Any, path: str) -> Any:
    for k in path.split("."):
        if not isinstance(d, dict):
            return None
        d = d.get(k)
    return d


def _lst(v: Any) -> List[Any]:
    return v if isinstance(v, list) else []


def _dct(v: Any) -> Dict[str, Any]:
    return v if isinstance(v, dict) else {}


def _season(edition: Optional[int]) -> Optional[int]:
    return None if edition is None else edition + _SEASON_OFFSET


def _clock(rec: Dict[str, Any], minute: str = "minute", second: str = "second") -> Optional[int]:
    m, s = _i(rec.get(minute)), _i(rec.get(second))
    return None if m is None or s is None else m * 60 + s


# ------------------------------------------------------------------------------------ field tables
# (column, type letter, source path inside the record or None when the extractor supplies it)
Field = Tuple[str, str, Optional[str]]
Pairs = Iterable[Tuple[Dict[str, Any], Dict[str, Any]]]


def _f_(col: str, typ: str, path: Optional[str] = None) -> Field:
    return (col, typ, path)


def _team_cols(prefix: str = "") -> List[Field]:
    return [
        (f"{prefix}team_id", "i", None),
        (f"{prefix}club_id", "i", None),
        (f"{prefix}team", "s", None),
        (f"{prefix}team_abbr", "s", None),
    ]


def _team_from(prefix: str, path: str) -> List[Field]:
    return [
        (f"{prefix}team_id", "i", f"{path}.id"),
        (f"{prefix}club_id", "i", f"{path}.clubId"),
        (f"{prefix}team", "s", f"{path}.fullName"),
        (f"{prefix}team_abbr", "s", f"{path}.abbreviatedName"),
    ]


def _person(path: str, prefix: str = "player") -> List[Field]:
    p = f"{path}." if path else ""
    return [
        (f"{prefix}_id", "i", f"{p}id"),
        (f"{prefix}_name", "s", f"{p}nickname"),
        ("first_name", "s", f"{p}firstName"),
        ("last_name", "s", f"{p}lastName"),
        ("shirt_number", "s", f"{p}shirtNumber"),
        ("game_role", "s", f"{p}gameRole"),
    ]


_ED: List[Field] = [("edition_id", "i", None), ("season", "i", None)]
_MATCH: List[Field] = [("match_id", "i", None)]

BOX_STATS: Tuple[Tuple[str, str], ...] = (
    ("points", "points"),
    ("free_throws_made", "freeThrowsMade"),
    ("free_throws_attempted", "freeThrowsAttempted"),
    ("two_pointers_made", "twoPointersMade"),
    ("two_pointers_attempted", "twoPointersAttempted"),
    ("three_pointers_made", "threePointersMade"),
    ("three_pointers_attempted", "threePointersAttempted"),
    ("assists", "assists"),
    ("off_rebounds", "offRebounds"),
    ("def_rebounds", "defRebounds"),
    ("total_rebounds", "totalRebounds"),
    ("steals", "steals"),
    ("turnovers", "turnovers"),
    ("blocks", "blocks"),
    ("received_blocks", "receivedBlocks"),
    ("dunks", "dunks"),
    ("personal_fouls", "personalFouls"),
    ("fouls_drawn", "foulsDrawn"),
    ("plus_minus", "plusMinus"),
    ("rating", "rating"),
)
_RUN_STATS = tuple(s for s in BOX_STATS if s[0] not in ("received_blocks", "dunks", "plus_minus", "rating"))
SEASON_STATS: Tuple[str, ...] = (
    "numGames",
    "timePlayed",
    "starter",
    "points",
    "maxPoints",
    "threePointersMade",
    "threePointersAttempted",
    "threePointersPercentage",
    "twoPointersMade",
    "twoPointersAttempted",
    "twoPointersPercentage",
    "freeThrowsMade",
    "freeThrowsAttempted",
    "freeThrowsPercentage",
    "rebounds",
    "offensiveRebounds",
    "defensiveRebounds",
    "assists",
    "steals",
    "turnovers",
    "blocks",
    "shotsRejected",
    "dunks",
    "personalFouls",
    "foulsReceived",
    "rating",
    "plusMinus",
    "wins",
    "losses",
)
_LOG_STATS: Tuple[str, ...] = tuple(
    s for s in SEASON_STATS if s not in ("numGames", "starter", "maxPoints", "wins", "losses", "timePlayed")
)


def _wide(prefix_path: str = "") -> List[Field]:
    """Season totals / averages / per-40 side by side: ``total_*`` (counts Int64), ``avg_*`` and ``per40_*`` Float64."""
    out: List[Field] = []
    p = f"{prefix_path}." if prefix_path else ""
    for tag, block in (("total", "totals"), ("avg", "averages"), ("per40", "per40")):
        for base in SEASON_STATS:
            typ = "i" if tag == "total" and not base.endswith("Percentage") else "f"
            out.append((f"{tag}_{underscore(base)}", typ, f"{p}{block}.{base}"))
    return out


_MATCH_ROW: List[Field] = [
    ("match_id", "i", "id"),
    ("round_type", "s", "roundType"),
    ("round_id", "i", "roundId"),
    ("round_number", "i", "roundNumber"),
    ("group_id", "i", "groupId"),
    ("subphase_id", "i", "subphaseId"),
    ("subphase_number", "i", "subphaseNumber"),
    ("week_id", "i", "weekId"),
    ("status", "s", "matchStatus"),
    ("is_on_half_time", "b", "isOnHalfTime"),
    ("current_quarter", "i", "currentQuarter"),
    ("home_score", "i", "homeScore"),
    ("away_score", "i", "awayScore"),
    ("start", "t", "startDateTime"),
    ("home_wins", "i", "homeCurrentWins"),
    ("home_losses", "i", "homeCurrentLosses"),
    ("away_wins", "i", "awayCurrentWins"),
    ("away_losses", "i", "awayCurrentLosses"),
    *_team_from("home_", "homeTeam"),
    *_team_from("away_", "awayTeam"),
    ("competition_id", "i", "homeTeam.competitionId"),
    ("edition_id", "i", "homeTeam.editionId"),
    ("season", "i", None),
    ("is_selected_local", "b", "isSelectedLocal"),
]

_GAME_ROW: List[Field] = [  # a game inside a series, a round or a calendar (teams by season-team id)
    ("match_id", "i", "id"),
    ("home_team_id", "i", "homeTeamId"),
    ("away_team_id", "i", "awayTeamId"),
    ("home_score", "i", "homeScore"),
    ("away_score", "i", "awayScore"),
    ("start", "t", "startDateTime"),
    ("status", "s", "matchStatus"),
    ("is_on_half_time", "b", "isOnHalfTime"),
    ("current_quarter", "i", "currentQuarter"),
]
_LOOKUP: List[Field] = [
    ("home_club_id", "i", None),
    ("home_team", "s", None),
    ("home_team_abbr", "s", None),
    ("away_club_id", "i", None),
    ("away_team", "s", None),
    ("away_team_abbr", "s", None),
]
_ROUND_MATCH: List[Field] = [
    ("competition_id", "i", None),
    *_ED,
    ("round_id", "i", None),
    ("round_number", "i", None),
    ("subphase_id", "i", None),
    ("subphase_number", "i", None),
    ("week_id", "i", None),
    *_GAME_ROW,
    *_LOOKUP,
    ("ticketing_link", "s", "ticketingLink"),
    ("broadcasters", "j", None),
]
_SERIES_BASE: List[Field] = [
    ("phase_id", "i", None),
    ("subphase_id", "i", None),
    ("series_home_team_id", "i", None),
    ("series_home_club_id", "i", None),
    ("series_away_team_id", "i", None),
    ("series_away_club_id", "i", None),
    ("series_home_wins", "i", None),
    ("series_away_wins", "i", None),
    ("series_finished", "b", None),
    ("best_of", "i", None),
    *_ED,
]

FIELDS: Dict[str, List[Field]] = {
    "match_header": [
        ("match_id", "i", "matchId"),
        ("competition_id", "i", "competitionId"),
        ("start", "t", "start"),
        ("status", "s", "matchStatus"),
        ("is_on_half_time", "b", "isOnHalfTime"),
        ("time_left", "s", "timeLeft"),
        ("current_quarter", "i", "currentQuarter"),
        ("home_score", "i", "currentHomeScore"),
        ("away_score", "i", "currentAwayScore"),
        ("home_bonus", "i", "currentHomeBonus"),
        ("away_bonus", "i", "currentAwayBonus"),
        *_team_from("home_", "teams.home"),
        *_team_from("away_", "teams.away"),
        ("edition_id", "i", "teams.home.editionId"),
        ("season", "i", None),
        ("has_overview", "b", "availableContent.overview"),
        ("has_play_by_play", "b", "availableContent.playbyplay"),
        ("has_boxscore", "b", "availableContent.boxscore"),
        ("has_advanced", "b", "availableContent.advanced"),
        ("highlights_url", "s", "highLightsVideoURL"),
    ],
    "period_scores": [
        *_MATCH,
        ("quarter", "i", "quarter"),
        ("home_score", "i", "homeScore"),
        ("away_score", "i", "awayScore"),
    ],
    "player_box": [
        *_MATCH,
        *_ED,
        ("team_side", "s", None),
        *_team_cols(),
        ("quarter", "i", None),
        *_person("player"),
        ("on_court", "b", "onCourt"),
        ("play_time", "s", "playTime"),
        ("seconds_played", "m", "playTime"),
        ("is_starter", "b", "isStarted"),
        *[(c, "i", p) for c, p in BOX_STATS],
    ],
    "team_box": [
        *_MATCH,
        *_ED,
        ("team_side", "s", None),
        *_team_cols(),
        ("quarter", "i", None),
        ("scope", "s", None),
        *[(c, "i", p) for c, p in BOX_STATS],
    ],
    "box_info": [
        *_MATCH,
        *_ED,
        ("team_side", "s", None),
        *_team_cols(),
        ("head_coach", "s", "headCoach"),
        ("assistant_coaches", "j", "assistantCoaches"),
        ("arena", "s", None),
        ("attendance", "i", None),
        ("referees", "j", None),
    ],
    "pbp": [
        *_MATCH,
        *_ED,
        ("play_index", "i", None),
        ("order", "i", "order"),
        ("quarter", "i", "quarter"),
        ("minute", "i", "minute"),
        ("second", "i", "second"),
        ("clock_seconds", "i", None),
        ("play_type", "i", "playType"),
        ("play_name", "s", None),
        ("play_tag", "i", "playTag"),
        ("turnover_type", "s", None),
        ("is_home", "b", "local"),
        ("score_home", "i", "scoreHome"),
        ("score_away", "i", "scoreAway"),
        ("license_type", "i", "licenseType"),
        ("player_id", "i", "playerLicenseId"),
        ("player_name", "s", "playerName"),
        ("shirt_number", "s", "playerNumber"),
        *[(f"run_{c}", "i", f"playerStats.{p}") for c, p in _RUN_STATS],
        ("series_wins", "i", "matchesWon"),
        ("series_losses", "i", "matchesLost"),
    ],
    "shots": [
        *_MATCH,
        *_ED,
        ("shot_id", "i", "id"),
        ("pos_x", "i", "posX"),
        ("pos_y", "i", "posY"),
        ("coord_system", "s", None),
        ("x_basket_m", "f", None),
        ("y_basket_m", "f", None),
        ("quarter", "i", "quarter"),
        ("minute", "i", "minute"),
        ("second", "i", "second"),
        ("clock_seconds", "i", None),
        ("play_type", "i", "playType"),
        ("play_name", "s", None),
        ("is_home", "b", "local"),
        ("score_home", "i", "scoreHome"),
        ("score_away", "i", "scoreAway"),
        ("player_id", "i", "playerLicenseId"),
        ("player_name", "s", None),
        ("shirt_number", "s", None),
        ("zone", "s", "zone"),
        ("made", "b", "made"),
        ("points", "i", "points"),
    ],
    "lead_tracker": [
        *_MATCH,
        *_ED,
        ("quarter", "i", "quarter"),
        ("minute", "i", "minute"),
        ("second", "i", "second"),
        ("clock_seconds", "i", None),
        ("home_score", "i", "home"),
        ("away_score", "i", "away"),
        ("margin", "i", None),
    ],
    "lead_stats": [
        *_MATCH,
        ("stat", "s", None),
        ("side", "s", None),
        ("value", "s", "title"),
        ("bar_progress", "f", "progress"),
        ("is_highlighted", "b", "isBarHighLighted"),
    ],
    "lineup": [
        *_MATCH,
        *_ED,
        ("team_side", "s", None),
        *_team_cols(),
        ("lineup_order", "i", None),
        ("player_id", "i", "id"),
        ("player_name", "s", "nickname"),
        ("short_name", "s", "firstInitialAndLastName"),
        ("shirt_number", "s", "shirtNumber"),
        ("play_time", "s", "playtime"),
        ("seconds_played", "m", "playtime"),
        ("is_playing", "b", "isPlaying"),
        ("points", "i", "points"),
        ("rebounds", "i", "rebounds"),
        ("assists", "i", "assists"),
        ("fouls", "i", "fouls"),
        ("differential", "i", "differential"),
        ("rating", "i", "rating"),
    ],
    "team_comparison": [
        *_MATCH,
        ("quarter", "i", None),
        ("stat", "s", None),
        ("home_value", "p", None),
        ("away_value", "p", None),
        ("home_made", "i", None),
        ("home_attempted", "i", None),
        ("away_made", "i", None),
        ("away_attempted", "i", None),
        ("home_is_highlighted", "b", None),
        ("away_is_highlighted", "b", None),
    ],
    "serie": [("requested_match_id", "i", None), *_SERIES_BASE, *_GAME_ROW],
    "quickfacts": [
        *_MATCH,
        ("team_side", "s", None),
        *_team_from("", "team"),
        ("edition_id", "i", "team.editionId"),
        ("season", "i", None),
        ("standing", "i", "standing"),
        ("wins", "i", "currentWins"),
        ("losses", "i", "currentLoses"),
        ("last_results", "j", "lastResults"),
    ],
    "match_advanced_stats": [
        *_MATCH,
        *_ED,
        ("team_side", "s", None),
        *_team_cols(),
        ("group", "s", None),
        ("stat", "s", None),
        ("match_value", "f", "partido"),
        ("season_value", "f", "temporada"),
        ("win_value", "f", "win"),
        ("loss_value", "f", "loss"),
        ("season_diff", "f", "temporadaDiff"),
        ("win_diff", "f", "winDiff"),
        ("loss_diff", "f", "lossDiff"),
    ],
    "matchlist": [*_MATCH_ROW, ("match_grouping", "s", None)],
    "competition_editions": [
        ("competition_id", "i", None),
        ("competition_name", "s", None),
        ("edition_id", "i", "id"),
        ("season_start_year", "i", "seasonStartYear"),
        ("season", "i", "seasonEndYear"),
        ("n_rounds", "i", None),
        ("n_weeks", "i", None),
    ],
    "competition_rounds": [
        ("competition_id", "i", None),
        *_ED,
        ("round_id", "i", "id"),
        ("round_number", "i", "roundNumber"),
        ("subphase_id", "i", "subphaseId"),
        ("subphase_number", "i", "subphaseNumber"),
    ],
    "competition_weeks": [
        ("competition_id", "i", None),
        *_ED,
        ("week_id", "i", "id"),
        ("description", "s", "description"),
        ("start_date", "t", "startDate"),
    ],
    "competition_current": [
        ("competition_id", "i", "currentCompetitionId"),
        ("edition_id", "i", "currentEditionId"),
        ("season", "i", None),
        ("week_id", "i", "currentWeekId"),
        ("round_id", "i", "currentRoundId"),
        ("match_grouping", "s", "currentMatchGrouping"),
        ("match_id", "i", "currentMatch.id"),
        ("status", "s", "currentMatch.matchStatus"),
        ("start", "t", "currentMatch.startDateTime"),
        *_team_from("home_", "currentMatch.homeTeam"),
        *_team_from("away_", "currentMatch.awayTeam"),
    ],
    "round_matches": list(_ROUND_MATCH),
    "calendar": list(_ROUND_MATCH),
    "standings": [
        ("competition_id", "i", None),
        *_ED,
        ("round_id", "i", None),
        ("total_rounds", "i", None),
        ("team_id", "i", "teamId"),
        ("club_id", "i", None),
        ("team", "s", None),
        ("team_abbr", "s", None),
        ("position", "i", "position"),
        ("position_trend", "i", "positionTrend"),
        ("played", "i", "matchesPlayed"),
        ("wins", "i", "wins"),
        ("losses", "i", "loses"),
        ("win_pct", "f", "winPercentage"),
        ("points_for", "i", "pointsFor"),
        ("points_against", "i", "pointsAgainst"),
        ("plus_minus", "i", "plusMinus"),
        ("home_wins", "i", "localBalanceWins"),
        ("home_losses", "i", "localBalanceLosses"),
        ("away_wins", "i", "awayBalanceWins"),
        ("away_losses", "i", "awayBalanceLosses"),
    ],
    "standings_rounds": [
        ("competition_id", "i", None),
        *_ED,
        ("team_id", "i", None),
        ("club_id", "i", None),
        ("team", "s", None),
        ("team_abbr", "s", None),
        ("round_id", "i", "roundData.id"),
        ("round_number", "i", "roundData.roundNumber"),
        ("subphase_id", "i", "roundData.subphaseId"),
        ("subphase_number", "i", "roundData.subphaseNumber"),
        ("position", "i", "position"),
        ("match_id", "i", "matchData.id"),
    ],
    "bracket": [
        ("stage", "s", None),
        ("phase_id", "i", "phaseId"),
        ("subphase_id", "i", "subphaseId"),
        *_team_from("home_", "homeTeam"),
        *_team_from("away_", "awayTeam"),
        ("home_wins", "i", "homeScore"),
        ("away_wins", "i", "awayScore"),
        ("finished", "b", "finished"),
        ("best_of", "i", "bestOf"),
        ("n_games", "i", None),
        ("edition_id", "i", "homeTeam.editionId"),
        ("season", "i", None),
    ],
    "bracket_games": [("stage", "s", None), *_SERIES_BASE, *_GAME_ROW],
    "league_zones": [
        ("competition_id", "i", None),
        *_ED,
        ("phase_id", "i", None),
        ("zone", "s", "zone"),
        ("attempts", "i", "attempts"),
        ("made", "i", "made"),
        ("points_scored", "i", "pointsScored"),
    ],
    "teams": [
        *_team_from("", "selected"),
        ("competition_id", "i", "selected.competitionId"),
        ("edition_id", "i", "selected.editionId"),
        ("season", "i", None),
        ("latest_team_id", "i", "latest.id"),
        ("latest_team", "s", "latest.fullName"),
    ],
    "club_data": [
        *_team_from("", "team"),
        ("competition_id", "i", "team.competitionId"),
        ("edition_id", "i", "team.editionId"),
        ("season", "i", None),
        ("stadium_name", "s", "stadiumName"),
        ("stadium_address", "s", "stadiumAddress"),
        ("stadium_capacity", "i", "stadiumCapacity"),
        ("president", "s", "presidentName"),
        ("foundation_year", "i", "foundationYear"),
        ("web_url", "s", "webUrl"),
        ("ticketing_url", "s", "ticketingUrl"),
        ("merchandising_url", "s", "merchanUrl"),
        ("instagram_url", "s", "instagramUrl"),
        ("twitter_url", "s", "twitterUrl"),
        ("facebook_url", "s", "facebookUrl"),
        ("youtube_url", "s", "youtubeUrl"),
        ("tiktok_url", "s", "tikTokUrl"),
        ("validated", "b", "validated"),
        ("temporary_roster", "b", "temporaryRoster"),
    ],
    "current_roster": [
        ("club_id", "i", None),
        ("role", "s", None),
        ("person_id", "i", "person.id"),
        ("person_name", "s", "person.nickname"),
        ("first_name", "s", "person.firstName"),
        ("last_name", "s", "person.lastName"),
        ("shirt_number", "s", "person.shirtNumber"),
        ("game_role", "s", "person.gameRole"),
        ("licensing", "s", "licensing"),
        ("age", "i", "age"),
        ("nationality", "s", "nationalityCountry"),
        ("nationality_iso", "s", None),
        ("height_cm", "i", "height"),
        ("is_license_active", "b", "isLicenseActive"),
        ("edition_id", "i", "person.editionId"),
        ("season", "i", None),
        ("temporary_roster", "b", None),
    ],
    "team_leaders": [
        ("club_id", "i", None),
        ("competition_id", "i", None),
        *_ED,
        ("scope", "s", None),
        ("metric", "s", None),
        ("rank", "i", None),
        ("value", "f", "value"),
        *_person("player"),
    ],
    "team_stats": [
        ("club_id", "i", None),
        ("team_id", "i", None),
        *_ED,
        *_person("player"),
        *_wide(),
    ],
    "team_totals": [
        ("club_id", "i", None),
        ("team_id", "i", "teamEditionId"),
        *_ED,
        *_wide(),
    ],
    "team_records": [
        ("club_id", "i", None),
        ("stat", "s", None),
        ("value", "f", "value"),
        ("game_id", "i", "gameId"),
        ("game_date", "t", "gameDate"),
        *_person("player"),
        *_team_from("local_", "localTeam"),
        *_team_from("visitor_", "visitorTeam"),
    ],
    "player_data": [
        ("player_id", "i", "player.id"),
        ("player_name", "s", "player.nickname"),
        ("first_name", "s", "player.firstName"),
        ("last_name", "s", "player.lastName"),
        ("shirt_number", "s", "player.shirtNumber"),
        ("game_role", "s", "player.gameRole"),
        ("is_license_active", "b", "isLicenseActive"),
        ("licensing", "s", "licensing"),
        ("birth_date", "d", "birthDate"),
        ("birth_place", "s", "birthPlace"),
        ("birth_country", "s", "birthCountry"),
        ("birth_country_iso", "s", None),
        ("nationality", "s", "nationality"),
        ("nationality_iso", "s", None),
        ("height_m", "f", "height"),
        ("player_number", "i", "playerNumber"),
        *_team_from("", "currentTeam"),
        ("edition_id", "i", "player.editionId"),
        ("season", "i", None),
        ("instagram", "s", "instagram"),
        ("twitter", "s", "twitter"),
        ("facebook", "s", "facebook"),
        ("youtube", "s", "youtube"),
        ("tiktok", "s", "tiktok"),
        ("pronunciation_url", "s", "pronunciationUrl"),
    ],
    "player_stats": [
        ("player_id", "i", None),
        ("team_id", "i", "teamEditionId"),
        ("competition_id", "i", "competitionId"),
        ("edition_id", "i", "editionId"),
        ("season", "i", None),
        ("edition_year", "s", "editionYear"),
        ("club_id", "i", "team.selected.clubId"),
        ("team", "s", "team.selected.fullName"),
        ("team_abbr", "s", "team.selected.abbreviatedName"),
        *_wide(),
    ],
    "player_totals": [*_person("player"), *_wide()],
    "career_stints": [
        ("player_id", "i", None),
        ("initial_year", "i", "initialYear"),
        ("end_year", "i", "endYear"),
        *_team_from("", "team"),
        ("edition_id", "i", "team.editionId"),
        ("n_editions", "i", None),
        ("has_no_validated_data", "b", "hasNoValidatedData"),
    ],
    "career_trophies": [
        ("player_id", "i", None),
        ("competition_id", "i", "competitionId"),
        ("times_champion", "i", "timesChampion"),
        ("times_runner_up", "i", "timesRunnerUp"),
    ],
    "player_matches": [
        ("player_id", "i", None),
        ("match_id", "i", "id"),
        ("round_type", "s", "roundType"),
        ("round_number", "i", "roundNumber"),
        ("date", "t", "date"),
        *_team_from("home_", "homeTeam"),
        *_team_from("away_", "awayTeam"),
        ("edition_id", "i", "homeTeam.editionId"),
        ("season", "i", None),
        ("is_selected_local", "b", "isSelectedLocal"),
        ("has_won", "b", "hasWon"),
        ("play_time", "s", "stats.timePlayed"),
        ("seconds_played", "m", "stats.timePlayed"),
        *[(underscore(b), "f" if b.endswith("Percentage") else "i", f"stats.{b}") for b in _LOG_STATS],
    ],
    "search": [
        ("kind", "s", None),
        ("person_id", "i", "id"),
        ("name", "s", None),
        ("short_name", "s", "firstInitialAndLastName"),
        ("first_name", "s", "firstName"),
        ("last_name", "s", "lastName"),
        ("shirt_number", "s", "shirtNumber"),
        ("game_role", "s", "gameRole"),
        ("is_license_active", "b", "isLicenseActive"),
        ("club_id", "i", "clubId"),
        ("edition_id", "i", "editionId"),
        ("season", "i", None),
    ],
}
# leaders (match, season-preview) share one shape
_LEADER: List[Field] = [
    *_MATCH,
    *_ED,
    ("team_side", "s", None),
    *_team_cols(),
    ("metric", "s", None),
    ("rank", "i", None),
    ("value", "f", "value"),
    *_person("player"),
]
FIELDS["match_leaders"] = list(_LEADER)
FIELDS["season_leaders"] = list(_LEADER)
FIELDS["next_matches"] = list(FIELDS["matchlist"])
FIELDS["team_matches"] = list(FIELDS["matchlist"])
FIELDS["player_advanced_stats"] = [
    *_MATCH,
    ("player_id", "i", None),
    ("group", "s", None),
    ("stat", "s", None),
    *[f for f in FIELDS["match_advanced_stats"] if f[2] is not None],
]
FIELDS["next_matches"] = [f for f in FIELDS["next_matches"] if f[0] != "match_grouping"]
FIELDS["team_matches"] = [f for f in FIELDS["team_matches"] if f[0] != "match_grouping"]

#: Each parser's column dtypes, for the generator and the tests.
SPECS: Dict[str, Dict[str, Any]] = {
    short: {col: _DTYPES[typ] for col, typ, _ in fields} for short, fields in FIELDS.items()
}


# ----------------------------------------------------------------------------------------- frames
def _frame(
    rows: List[Dict[str, Any]], spec: Dict[str, Any], columns: Optional[Sequence[str]], as_pandas: bool
) -> Frame:
    """``rows`` as a frame with exactly ``spec``'s columns and dtypes (a zero-row frame when there are none).

    ``columns`` is the wrapper's documented column list; a name the spec lacks is added as a null string column.
    """
    full: Dict[str, Any] = dict(spec)
    for c in columns or ():
        full.setdefault(c, S)
    df = pl.DataFrame({c: [r.get(c) for r in rows] for c in full}, schema=full)
    return df.to_pandas() if as_pandas else df


def _emit(short: str, pairs: Pairs, columns: Optional[Sequence[str]], as_pandas: bool) -> Frame:
    """Build the ``short`` frame from ``(record, extra columns)`` pairs.

    Source-path columns are read from the record and cast by their type letter; the columns the extractor
    derives arrive in the second element already cast.
    """
    fields = FIELDS[short]
    rows: List[Dict[str, Any]] = []
    try:
        for rec, ctx in pairs:
            row = {col: _CAST[typ](_dig(rec, path)) for col, typ, path in fields if path is not None}
            row.update(ctx)
            rows.append(row)
    except (
        TypeError,
        AttributeError,
        KeyError,
        ValueError,
        IndexError,
    ):  # an odd body is an empty frame, never a crash
        rows = []
    return _frame(rows, SPECS[short], columns, as_pandas)


def _req(raw: Any) -> Dict[str, Any]:
    return _dct(_dct(raw).get(_REQUEST))


def _match_ctx(raw: Any) -> Dict[str, Any]:
    return {"match_id": _i(_req(raw).get("match_id"))}


def _ed_ctx(edition: Optional[int]) -> Dict[str, Any]:
    return {"edition_id": edition, "season": _season(edition)}


def _team_ctx(team: Any, prefix: str = "") -> Dict[str, Any]:
    t = _dct(team)
    return {
        f"{prefix}team_id": _i(t.get("id")),
        f"{prefix}club_id": _i(t.get("clubId")),
        f"{prefix}team": _s(t.get("fullName")),
        f"{prefix}team_abbr": _s(t.get("abbreviatedName")),
    }


def _body(raw: Any, *keys: str) -> Optional[Dict[str, Any]]:
    """``raw`` when it is a dict holding a non-empty value under one of ``keys``, else ``None``."""
    if not isinstance(raw, dict):
        return None
    return raw if any(raw.get(k) not in (None, [], {}) for k in keys) else None


def _home_edition(raw: Dict[str, Any], *paths: str) -> Optional[int]:
    for p in paths or ("homeTeam.editionId",):
        e = _i(_dig(raw, p))
        if e is not None:
            return e
    return None


# --------------------------------------------------------------------------------------- extractors
def _x_match_header(raw: Any) -> Pairs:
    if _body(raw, "teams", "quarterScores") is None:
        return
    yield raw, {**_ed_ctx(_i(_dig(raw, "teams.home.editionId")))}


def _x_period_scores(raw: Any) -> Pairs:
    if _body(raw, "quarterScores") is None:
        return
    for q in _lst(raw["quarterScores"]):
        yield _dct(q), _match_ctx(raw)


def _box_teams(raw: Any) -> Iterator[Tuple[str, Dict[str, Any], Dict[str, Any]]]:
    for idx, tb in enumerate(_lst(_dct(raw).get("teamBoxscores"))):
        tb = _dct(tb)
        yield _SIDES[idx] if idx < 2 else "", tb, _dct(tb.get("team"))


def _x_player_box(raw: Any) -> Pairs:
    for side, tb, team in _box_teams(raw):
        ctx0 = {**_match_ctx(raw), **_ed_ctx(_i(team.get("editionId"))), "team_side": side or None, **_team_ctx(team)}
        for per in _lst(tb.get("statsByPeriods")):
            for p in _lst(_dct(_dct(per).get("stats")).get("players")):
                yield _dct(p), {**ctx0, "quarter": _i(_dct(per).get("quarter"))}


def _x_team_box(raw: Any) -> Pairs:
    for side, tb, team in _box_teams(raw):
        ctx0 = {**_match_ctx(raw), **_ed_ctx(_i(team.get("editionId"))), "team_side": side or None, **_team_ctx(team)}
        for per in _lst(tb.get("statsByPeriods")):
            for scope in ("total", "team"):
                block = _dct(_dct(per).get("stats")).get(scope)
                if isinstance(block, dict):
                    yield block, {**ctx0, "quarter": _i(_dct(per).get("quarter")), "scope": scope}


def _x_box_info(raw: Any) -> Pairs:
    for side, tb, team in _box_teams(raw):
        yield (
            tb,
            {
                **_match_ctx(raw),
                **_ed_ctx(_i(team.get("editionId"))),
                "team_side": side or None,
                **_team_ctx(team),
                "arena": _s(raw.get("arena")),
                "attendance": _i(raw.get("attendance")),
                "referees": _join(raw.get("referees")),
            },
        )


def _x_pbp(raw: Any) -> Pairs:
    if _body(raw, "plays") is None:
        return
    ed = _ed_ctx(_home_edition(raw, "homeTeam.editionId", "awayTeam.editionId"))
    for n, p in enumerate(_lst(raw["plays"]), start=1):
        p = _dct(p)
        pt, tag = _i(p.get("playType")), _i(p.get("playTag"))
        yield (
            p,
            {
                **_match_ctx(raw),
                **ed,
                "play_index": n,
                "clock_seconds": _clock(p),
                "play_name": PLAY_TYPES.get(pt) if pt is not None else None,
                "turnover_type": PLAY_TAGS.get(tag) if tag is not None else None,
            },
        )


def _x_shots(raw: Any) -> Pairs:
    if _body(raw, "shotPoints") is None:
        return
    ed = _ed_ctx(_home_edition(raw, "homeTeam.editionId", "awayTeam.editionId"))
    mctx = _match_ctx(raw)
    mid = mctx["match_id"]
    names: Dict[int, Tuple[Optional[str], Optional[str]]] = {}
    for key in ("homePlayerStats", "awayPlayerStats"):
        for ps in _lst(raw.get(key)):
            pid = _i(_dct(ps).get("playerLicenseId"))
            if pid is not None:
                names[pid] = (_s(_dct(ps).get("playerName")), _s(_dct(ps).get("playerNumber")))
    for sh in _lst(raw["shotPoints"]):
        sh = _dct(sh)
        pt, pid = _i(sh.get("playType")), _i(sh.get("playerLicenseId"))
        system = None if mid is None else ("mm" if mid >= _NEW_MATCH_ID else "grid")
        free_throw = pt in (92, 96)
        x, y = _i(sh.get("posX")), _i(sh.get("posY"))
        basket = system == "mm" and not free_throw
        yield (
            sh,
            {
                **mctx,
                **ed,
                "coord_system": system,
                "x_basket_m": x / 1000 if basket and x is not None else None,
                "y_basket_m": y / 1000 if basket and y is not None else None,
                "clock_seconds": _clock(sh),
                "play_name": PLAY_TYPES.get(pt) if pt is not None else None,
                "player_name": names.get(pid, (None, None))[0] if pid is not None else None,
                "shirt_number": names.get(pid, (None, None))[1] if pid is not None else None,
            },
        )


def _x_lead_tracker(raw: Any) -> Pairs:
    if _body(raw, "leadTrackerInfo") is None:
        return
    ed = _ed_ctx(_home_edition(raw, "homeTeam.editionId", "awayTeam.editionId"))
    for r in _lst(raw["leadTrackerInfo"]):
        r = _dct(r)
        h, a = _i(r.get("home")), _i(r.get("away"))
        yield (
            r,
            {**_match_ctx(raw), **ed, "clock_seconds": _clock(r), "margin": None if h is None or a is None else h - a},
        )


def _x_lead_stats(raw: Any) -> Pairs:
    for stat, sides in _dct(_dct(raw).get("stats")).items():
        for side in _SIDES:
            if isinstance(_dct(sides).get(side), dict):
                yield sides[side], {**_match_ctx(raw), "stat": underscore(stat), "side": side}


def _x_lineup(raw: Any) -> Pairs:
    for idx, tl in enumerate(_lst(_dct(raw).get("teamLineUps"))):
        tl = _dct(tl)
        team = _dct(tl.get("team"))
        ctx0 = {
            **_match_ctx(raw),
            **_ed_ctx(_i(team.get("editionId"))),
            "team_side": _SIDES[idx] if idx < 2 else None,
            **_team_ctx(team),
        }
        for n, p in enumerate(_lst(tl.get("players")), start=1):
            yield _dct(p), {**ctx0, "lineup_order": n}


def _x_leaders(raw: Any) -> Pairs:
    for idx, tl in enumerate(_lst(_dct(raw).get("teamLeaders"))):
        tl = _dct(tl)
        team = _dct(tl.get("team"))
        ctx0 = {
            **_match_ctx(raw),
            **_ed_ctx(_i(team.get("editionId"))),
            "team_side": _SIDES[idx] if idx < 2 else None,
            **_team_ctx(team),
        }
        for metric, rows in _dct(tl.get("leaders")).items():
            for rank, r in enumerate(_lst(rows), start=1):
                yield _dct(r), {**ctx0, "metric": underscore(metric), "rank": rank}


def _made(side: Dict[str, Any], k: int) -> Optional[int]:
    """Made (``k`` = 1) or attempted (``k`` = 2) from a bar subtitle such as ``'19/24'``."""
    m = re.fullmatch(r"\s*(\d+)/(\d+)\s*", side.get("subtitle") or "")
    return int(m.group(k)) if m else None


def _x_team_comparison(raw: Any) -> Pairs:
    for q in _lst(_dct(raw).get("statsPerQuarters")):
        q = _dct(q)
        for stat, sides in _dct(q.get("statsPerQuarters")).items():
            home, away = _dct(_dct(sides).get("home")), _dct(_dct(sides).get("away"))

            yield (
                {},
                {
                    **_match_ctx(raw),
                    "quarter": _i(q.get("quarter")),
                    "stat": underscore(stat),
                    "home_value": _pct(home.get("title")),
                    "away_value": _pct(away.get("title")),
                    "home_made": _made(home, 1),
                    "home_attempted": _made(home, 2),
                    "away_made": _made(away, 1),
                    "away_attempted": _made(away, 2),
                    "home_is_highlighted": _b(home.get("isBarHighLighted")),
                    "away_is_highlighted": _b(away.get("isBarHighLighted")),
                },
            )


def _series_ctx(s: Dict[str, Any]) -> Dict[str, Any]:
    ed = _i(_dig(s, "homeTeam.editionId"))
    return {
        "phase_id": _i(s.get("phaseId")),
        "subphase_id": _i(s.get("subphaseId")),
        "series_home_team_id": _i(_dig(s, "homeTeam.id")),
        "series_home_club_id": _i(_dig(s, "homeTeam.clubId")),
        "series_away_team_id": _i(_dig(s, "awayTeam.id")),
        "series_away_club_id": _i(_dig(s, "awayTeam.clubId")),
        "series_home_wins": _i(s.get("homeScore")),
        "series_away_wins": _i(s.get("awayScore")),
        "series_finished": _b(s.get("finished")),
        "best_of": _i(s.get("bestOf")),
        **_ed_ctx(ed),
    }


def _x_serie(raw: Any) -> Pairs:
    if _body(raw, "matches") is None:
        return
    for g in _lst(raw["matches"]):
        yield _dct(g), {"requested_match_id": _match_ctx(raw)["match_id"], **_series_ctx(raw)}


_STAGES = (
    ("quarterFinals", "quarter_finals"),
    ("semifinals", "semifinals"),
    ("final", "final"),
    ("otherPositions", "other_positions"),
)


def _series_list(raw: Any) -> Iterator[Tuple[str, Dict[str, Any]]]:
    data = _dct(_dct(raw).get("data"))
    for key, stage in _STAGES:
        for s in _lst(_dct(data.get(key)).get("series")):
            if isinstance(s, dict):
                yield stage, s


def _x_bracket(raw: Any) -> Pairs:
    for stage, s in _series_list(raw):
        ed = _i(_dig(s, "homeTeam.editionId"))
        yield s, {"stage": stage, "n_games": len(_lst(s.get("matches"))), "season": _season(ed)}


def _x_bracket_games(raw: Any) -> Pairs:
    for stage, s in _series_list(raw):
        for g in _lst(s.get("matches")):
            yield _dct(g), {"stage": stage, **_series_ctx(s)}


def _x_quickfacts(raw: Any) -> Pairs:
    for idx, f in enumerate(_lst(_dct(raw).get("facts"))):
        f = _dct(f)
        yield (
            f,
            {
                **_match_ctx(raw),
                "team_side": _SIDES[idx] if idx < 2 else None,
                "season": _season(_i(_dig(f, "team.editionId"))),
            },
        )


def _adv_leaves(block: Any) -> Iterator[Tuple[str, str, Dict[str, Any]]]:
    for group, stats in _dct(block).items():
        for stat, leaf in _dct(stats).items():
            if isinstance(leaf, dict):
                yield underscore(group), underscore(stat), leaf


def _x_match_advanced_stats(raw: Any) -> Pairs:
    for side in _SIDES:
        team = _dct(_dct(raw).get(f"{side}Team"))
        for group, stat, leaf in _adv_leaves(_dct(raw).get(f"{side}AdvancedStats")):
            yield (
                leaf,
                {
                    **_match_ctx(raw),
                    **_ed_ctx(_i(team.get("editionId"))),
                    "team_side": side,
                    **_team_ctx(team),
                    "group": group,
                    "stat": stat,
                },
            )


def _x_player_advanced_stats(raw: Any) -> Pairs:
    req = _req(raw)
    for group, stat, leaf in _adv_leaves({k: v for k, v in _dct(raw).items() if k != _REQUEST}):
        yield leaf, {**_match_ctx(raw), "player_id": _i(req.get("player_id")), "group": group, "stat": stat}


def _rich_matches(rows: Any, extra: Optional[Dict[str, Any]] = None) -> Pairs:
    for m in _lst(rows):
        m = _dct(m)
        yield m, {"season": _season(_i(_dig(m, "homeTeam.editionId"))), **(extra or {})}


def _x_matchlist(raw: Any) -> Pairs:
    return _rich_matches(_dct(raw).get("matches"), {"match_grouping": _s(_dct(raw).get("matchGrouping"))})


def _x_matches_plain(raw: Any) -> Pairs:  # next_matches, team_matches
    return _rich_matches(_dct(raw).get("matches"))


def _x_competition_editions(raw: Any) -> Pairs:
    for c in _lst(_dct(raw).get("competitions")):
        c = _dct(c)
        for e in _lst(c.get("editions")):
            e = _dct(e)
            yield (
                e,
                {
                    "competition_id": _i(c.get("id")),
                    "competition_name": _s(c.get("name")),
                    "n_rounds": len(_lst(e.get("rounds"))),
                    "n_weeks": len(_lst(e.get("weeks"))),
                },
            )


def _x_competition_sub(key: str) -> Callable[[Any], Pairs]:
    def run(raw: Any) -> Pairs:
        for c in _lst(_dct(raw).get("competitions")):
            c = _dct(c)
            for e in _lst(c.get("editions")):
                e = _dct(e)
                end = _i(e.get("seasonEndYear"))
                for r in _lst(e.get(key)):
                    yield _dct(r), {"competition_id": _i(c.get("id")), "edition_id": _i(e.get("id")), "season": end}

    return run


def _x_competition_current(raw: Any) -> Pairs:
    if _body(raw, "currentCompetitionId", "competitions") is None:
        return
    yield raw, {"season": _season(_i(raw.get("currentEditionId")))}


def _team_index(raw: Dict[str, Any]) -> Dict[int, Dict[str, Any]]:
    out: Dict[int, Dict[str, Any]] = {}
    for t in _lst(raw.get("teams")):
        tid = _i(_dct(t).get("id"))
        if tid is not None:
            out[tid] = _dct(t)
    return out


def _lookup_ctx(game: Dict[str, Any], teams: Dict[int, Dict[str, Any]]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for side in _SIDES:
        t = teams.get(_i(game.get(f"{side}TeamId")) or -1, {})
        out[f"{side}_club_id"] = _i(t.get("clubId"))
        out[f"{side}_team"] = _s(t.get("fullName"))
        out[f"{side}_team_abbr"] = _s(t.get("abbreviatedName"))
    return out


def _broadcasters(game: Dict[str, Any]) -> Optional[str]:
    return _join([_dct(b).get("broadcaster") for b in _lst(game.get("broadcastChannels"))])


def _x_round_matches(raw: Any) -> Pairs:
    if _body(raw, "matches") is None:
        return
    teams = _team_index(raw)
    sel = _dct(raw.get("selectedFilters"))
    rid = _i(sel.get("round"))
    rounds = {_i(_dct(r).get("id")): _dct(r) for r in _lst(_dct(raw.get("availableFilters")).get("rounds"))}
    rnd = rounds.get(rid, {})
    for g in _lst(raw["matches"]):
        g = _dct(g)
        yield (
            g,
            {
                "competition_id": _i(sel.get("competition")),
                **_ed_ctx(_i(sel.get("season"))),
                "round_id": rid,
                "round_number": _i(rnd.get("roundNumber")),
                "subphase_id": _i(rnd.get("subphaseId")),
                "subphase_number": _i(rnd.get("subphaseNumber")),
                "week_id": _i(sel.get("week")),
                **_lookup_ctx(g, teams),
                "broadcasters": _broadcasters(g),
            },
        )


def _x_calendar(raw: Any) -> Pairs:
    if _body(raw, "rounds") is None:
        return
    teams = _team_index(raw)
    sel = _dct(raw.get("selectedFilters"))
    for r in _lst(raw["rounds"]):
        r = _dct(r)
        for g in _lst(r.get("matches")):
            g = _dct(g)
            yield (
                g,
                {
                    "competition_id": _i(sel.get("competition")),
                    **_ed_ctx(_i(sel.get("season"))),
                    "round_id": _i(r.get("id")),
                    "round_number": _i(r.get("roundNumber")),
                    "subphase_id": _i(r.get("subphaseId")),
                    "subphase_number": _i(r.get("subphaseNumber")),
                    "week_id": None,
                    **_lookup_ctx(g, teams),
                    "broadcasters": _broadcasters(g),
                },
            )


def _x_standings(raw: Any) -> Pairs:
    if _body(raw, "standings") is None:
        return
    teams = _team_index(raw)
    sel = _dct(raw.get("selectedFilters"))
    for s in _lst(raw["standings"]):
        s = _dct(s)
        t = teams.get(_i(s.get("teamId")) or -1, {})
        yield (
            s,
            {
                "competition_id": _i(sel.get("competition")),
                **_ed_ctx(_i(sel.get("season"))),
                "round_id": _i(sel.get("round")),
                "total_rounds": _i(raw.get("totalRounds")),
                "club_id": _i(t.get("clubId")),
                "team": _s(t.get("fullName")),
                "team_abbr": _s(t.get("abbreviatedName")),
            },
        )


def _x_standings_rounds(raw: Any) -> Pairs:
    teams = _team_index(raw)
    sel = _dct(_dct(raw).get("selectedFilters"))
    for s in _lst(_dct(raw).get("standings")):
        s = _dct(s)
        tid = _i(s.get("teamId"))
        t = teams.get(tid or -1, {})
        for r in _lst(s.get("rounds")):
            yield (
                _dct(r),
                {
                    "competition_id": _i(sel.get("competition")),
                    **_ed_ctx(_i(sel.get("season"))),
                    "team_id": tid,
                    "club_id": _i(t.get("clubId")),
                    "team": _s(t.get("fullName")),
                    "team_abbr": _s(t.get("abbreviatedName")),
                },
            )


def _x_league_zones(raw: Any) -> Pairs:
    if _body(raw, "zones") is None:
        return
    for z in _lst(raw["zones"]):
        yield (
            _dct(z),
            {
                "competition_id": _i(raw.get("competitionId")),
                **_ed_ctx(_i(raw.get("editionId"))),
                "phase_id": _i(raw.get("phaseId")),
            },
        )


def _x_teams(raw: Any) -> Pairs:
    for t in _lst(_dct(raw).get("teams")):
        yield _dct(t), {"season": _season(_i(_dig(t, "selected.editionId")))}


def _x_club_data(raw: Any) -> Pairs:
    cd = _dct(_dct(raw).get("clubData"))
    if cd:
        yield cd, {"season": _season(_i(_dig(cd, "team.editionId")))}


def _x_current_roster(raw: Any) -> Pairs:
    r = _dct(raw)
    club = _i(_req(raw).get("club_id"))
    for role, key, who in (("player", "players", "player"), ("staff", "staff", "coach")):
        for m in _lst(r.get(key)):
            m = _dct(m)
            ed = _i(_dig(m, f"{who}.editionId"))
            nat = _s(m.get("nationalityCountry"))
            yield (
                {**m, "person": _dct(m.get(who))},
                {
                    "club_id": club,
                    "role": role,
                    "nationality_iso": COUNTRIES.get(nat, (None, None))[1] if nat else None,
                    "season": _season(ed),
                    "temporary_roster": _b(r.get("temporaryRoster")),
                },
            )


def _x_team_leaders(raw: Any) -> Pairs:
    req = _req(raw)
    sel = _dct(_dct(raw).get("selectedFilters"))
    ed = _i(sel.get("season"))
    for scope, key in (("all_time", "leaders"), ("edition", "seasonLeaders")):
        for metric, rows in _dct(_dct(raw).get(key)).items():
            for rank, r in enumerate(_lst(rows), start=1):
                yield (
                    _dct(r),
                    {
                        "club_id": _i(req.get("club_id")),
                        "competition_id": _i(sel.get("competition")),
                        **_ed_ctx(ed),
                        "scope": scope,
                        "metric": underscore(metric),
                        "rank": rank,
                    },
                )


def _x_team_stats(raw: Any) -> Pairs:
    team = _dct(_dct(_dct(raw).get("team")).get("selected"))
    for p in _lst(_dct(raw).get("players")):
        yield (
            _dct(p),
            {"club_id": _i(team.get("clubId")), "team_id": _i(team.get("id")), **_ed_ctx(_i(team.get("editionId")))},
        )


def _x_team_totals(raw: Any) -> Pairs:
    team = _dct(_dct(_dct(raw).get("team")).get("selected"))
    tt = _dct(_dct(raw).get("teamTotals"))
    if tt:
        yield tt, {"club_id": _i(team.get("clubId")), **_ed_ctx(_i(team.get("editionId")))}


def _x_team_records(raw: Any) -> Pairs:
    club = _i(_req(raw).get("club_id"))
    for stat, rec in _dct(raw).items():
        if stat != _REQUEST and isinstance(rec, dict):
            yield rec, {"club_id": club, "stat": underscore(stat)}


def _x_player_data(raw: Any) -> Pairs:
    pd_ = _dct(_dct(raw).get("playerData"))
    if not pd_:
        return
    bc, nat = _s(pd_.get("birthCountry")), _s(pd_.get("nationality"))
    yield (
        pd_,
        {
            "birth_country_iso": COUNTRIES.get(bc, (None, None))[1] if bc else None,
            "nationality_iso": COUNTRIES.get(nat, (None, None))[1] if nat else None,
            "season": _season(_i(_dig(pd_, "player.editionId"))),
        },
    )


def _x_player_stats(raw: Any) -> Pairs:
    pid = _i(_req(raw).get("player_id"))
    for t in _lst(_dct(raw).get("statsByTeam")):
        t = _dct(t)
        yield t, {"player_id": pid, "season": _season(_i(t.get("editionId")))}


def _x_player_totals(raw: Any) -> Pairs:
    pt = _dct(_dct(raw).get("playerTotals"))
    if pt:
        yield pt, {}


def _x_career_stints(raw: Any) -> Pairs:
    pid = _i(_req(raw).get("player_id"))
    for s in _lst(_dct(raw).get("stints")):
        s = _dct(s)
        yield s, {"player_id": pid, "n_editions": len(_lst(s.get("editions")))}


def _x_career_trophies(raw: Any) -> Pairs:
    pid = _i(_req(raw).get("player_id"))
    for t in _lst(_dct(raw).get("trophies")):
        yield _dct(t), {"player_id": pid}


def _x_player_matches(raw: Any) -> Pairs:
    pid = _i(_req(raw).get("player_id"))
    for m in _lst(_dct(raw).get("matches")):
        m = _dct(m)
        yield m, {"player_id": pid, "season": _season(_i(_dig(m, "homeTeam.editionId")))}


def _x_search(raw: Any) -> Pairs:
    for kind, key in (("player", "players"), ("team", "teams"), ("coach", "coaches")):
        for r in _lst(_dct(raw).get(key)):
            r = _dct(r)
            name = _s(r.get("fullName")) if kind == "team" else _s(r.get("nickname"))
            yield r, {"kind": kind, "name": name, "season": _season(_i(r.get("editionId")))}


_EXTRACT: Dict[str, Callable[[Any], Pairs]] = {
    "match_header": _x_match_header,
    "period_scores": _x_period_scores,
    "player_box": _x_player_box,
    "team_box": _x_team_box,
    "box_info": _x_box_info,
    "pbp": _x_pbp,
    "shots": _x_shots,
    "lead_tracker": _x_lead_tracker,
    "lead_stats": _x_lead_stats,
    "lineup": _x_lineup,
    "match_leaders": _x_leaders,
    "season_leaders": _x_leaders,
    "team_comparison": _x_team_comparison,
    "serie": _x_serie,
    "quickfacts": _x_quickfacts,
    "match_advanced_stats": _x_match_advanced_stats,
    "player_advanced_stats": _x_player_advanced_stats,
    "matchlist": _x_matchlist,
    "next_matches": _x_matches_plain,
    "team_matches": _x_matches_plain,
    "competition_editions": _x_competition_editions,
    "competition_rounds": _x_competition_sub("rounds"),
    "competition_weeks": _x_competition_sub("weeks"),
    "competition_current": _x_competition_current,
    "round_matches": _x_round_matches,
    "calendar": _x_calendar,
    "standings": _x_standings,
    "standings_rounds": _x_standings_rounds,
    "bracket": _x_bracket,
    "bracket_games": _x_bracket_games,
    "league_zones": _x_league_zones,
    "teams": _x_teams,
    "club_data": _x_club_data,
    "current_roster": _x_current_roster,
    "team_leaders": _x_team_leaders,
    "team_stats": _x_team_stats,
    "team_totals": _x_team_totals,
    "team_records": _x_team_records,
    "player_data": _x_player_data,
    "player_stats": _x_player_stats,
    "player_totals": _x_player_totals,
    "career_stints": _x_career_stints,
    "career_trophies": _x_career_trophies,
    "player_matches": _x_player_matches,
    "search": _x_search,
}


def _parse(short: str, raw: Any, columns: Optional[Sequence[str]], as_pandas: bool) -> Frame:
    try:
        pairs = list(_EXTRACT[short](raw))
    except (TypeError, AttributeError, KeyError, ValueError, IndexError):  # an odd body is an empty frame
        pairs = []
    return _emit(short, pairs, columns, as_pandas)


def parse_acb_match_header(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Header of one ACB game: teams, status, score, bonus, current period and the content the site offers.

    Args:
        raw: the body ``acb_match_header(..., return_parsed=False)`` returns (the route's JSON with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when the parser lacks them).

    Returns:
        One row. ``status`` is the API's ``matchStatus`` (``FINALIZED`` once over); ``home_score`` /
        ``away_score`` are the live or final score; ``time_left`` is the period clock as ``'mm:ss'``;
        ``start`` is UTC; the ``has_*`` flags say which panels (overview, play-by-play, box score,
        advanced stats) the site offers. ``team_id`` is the season-team entry, ``club_id`` the stable
        club.
        A body that is empty or not this route's gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: the parser never raises on data; the wrapper raises NoDataError / AssetFetchError before parsing.

    Example:
        Quick start::

            from sportsdataverse.basketball.acb import acb_match_header

            df = acb_match_header(match_id=105401)
    """
    return _parse("match_header", raw, columns, return_as_pandas)


def parse_acb_period_scores(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Points each team scored in each period of one ACB game (``quarterScores[]``).

    Args:
        raw: the body ``acb_period_scores(..., return_parsed=False)`` returns (the route's JSON with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when the parser lacks them).

    Returns:
        One row per period: ``quarter`` (1-4, 5+ overtime), ``home_score`` and ``away_score`` are the
        points scored IN that period, not cumulative. Old games list fewer periods.
        A body that is empty or not this route's gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: the parser never raises on data; the wrapper raises NoDataError / AssetFetchError before parsing.

    Example:
        Quick start::

            from sportsdataverse.basketball.acb import acb_period_scores

            df = acb_period_scores(match_id=105401)
    """
    return _parse("period_scores", raw, columns, return_as_pandas)


def parse_acb_player_box(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Per-player box score of one ACB game, per period (``teamBoxscores[].statsByPeriods[].stats.players[]``).

    Args:
        raw: the body ``acb_player_box(..., return_parsed=False)`` returns (the route's JSON with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when the parser lacks them).

    Returns:
        One row per team, period and player. ``quarter`` 0 is the whole game, 1-4 the periods, 5+
        overtime (games before about 2010 carry only quarter 0). ``team_side`` is home or away (the feed
        lists the home team first). ``play_time`` is ``'mm:ss'`` and ``seconds_played`` the same in
        seconds; counts are ``Int64``. ``player_id`` is the ACB licence number; ``game_role`` is the
        Spanish position.
        A body that is empty or not this route's gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: the parser never raises on data; the wrapper raises NoDataError / AssetFetchError before parsing.

    Example:
        Quick start::

            from sportsdataverse.basketball.acb import acb_player_box

            df = acb_player_box(match_id=105401)
    """
    return _parse("player_box", raw, columns, return_as_pandas)


def parse_acb_team_box(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Team totals of one ACB game, per period (``stats.total`` and ``stats.team`` of each period).

    Args:
        raw: the body ``acb_team_box(..., return_parsed=False)`` returns (the route's JSON with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when the parser lacks them).

    Returns:
        Two rows per team and period: ``scope`` ``total`` is everything the team did (its players plus
        the team's own events), ``team`` is the part credited to the team itself rather than a player
        (team rebounds, team turnovers). ``quarter`` 0 is the whole game.
        A body that is empty or not this route's gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: the parser never raises on data; the wrapper raises NoDataError / AssetFetchError before parsing.

    Example:
        Quick start::

            from sportsdataverse.basketball.acb import acb_team_box

            df = acb_team_box(match_id=105401)
    """
    return _parse("team_box", raw, columns, return_as_pandas)


def parse_acb_box_info(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Coaches and game facts of one ACB game, one row per team (head coach, assistants, arena, attendance, referees).

    Args:
        raw: the body ``acb_box_info(..., return_parsed=False)`` returns (the route's JSON with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when the parser lacks them).

    Returns:
        One row per team. ``arena``, ``attendance`` and ``referees`` (joined with ``'; '``) describe the
        game and repeat on both rows; ``assistant_coaches`` is joined with ``'; '``.
        A body that is empty or not this route's gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: the parser never raises on data; the wrapper raises NoDataError / AssetFetchError before parsing.

    Example:
        Quick start::

            from sportsdataverse.basketball.acb import acb_box_info

            df = acb_box_info(match_id=105401)
    """
    return _parse("box_info", raw, columns, return_as_pandas)


def parse_acb_pbp(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Play-by-play of one ACB game (2016-17 onward), one row per event in feed order.

    Args:
        raw: the body ``acb_pbp(..., return_parsed=False)`` returns (the route's JSON with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when the parser lacks them).

    Returns:
        One row per event. ``play_type`` is the API's event code and ``play_name`` its bundle name
        (``acb_codes.PLAY_TYPES``; a code outside the table, such as the minute marker 600, has no
        name); ``turnover_type`` decodes ``play_tag``. The clock counts DOWN: ``minute`` / ``second``
        are the time left in the period and ``clock_seconds`` the same in seconds. ``order`` is not
        unique (the starting five share one); ``play_index`` is the 1-based position in the feed.
        ``is_home`` is true for the home team's events (the feed's ``local``). ``run_*`` columns are the
        acting player's running box score AFTER the event. Games before 2016-17 answer no events (the
        wrapper raises NoDataError).
        A body that is empty or not this route's gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: the parser never raises on data; the wrapper raises NoDataError / AssetFetchError before parsing.

    Example:
        Quick start::

            from sportsdataverse.basketball.acb import acb_pbp

            df = acb_pbp(match_id=105401)
    """
    return _parse("pbp", raw, columns, return_as_pandas)


def parse_acb_shots(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Every field-goal and free-throw attempt of one ACB game (2016-17 onward) with its court position.

    Args:
        raw: the body ``acb_shots(..., return_parsed=False)`` returns (the route's JSON with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when the parser lacks them).

    Returns:
        One row per attempt. Two coordinate systems share ``pos_x`` / ``pos_y``, told apart by
        ``coord_system``: ``mm`` (match ids from 2020-21) is millimetres from the centre of the attacked
        basket, every shot mirrored to one basket, ``pos_x`` towards midcourt and ``pos_y`` across the
        court (negative = the shooter's left); ``x_basket_m`` / ``y_basket_m`` are those in metres.
        ``grid`` (up to 2019-20) is a 0-100 percent grid over the full court, NOT mirrored (teams switch
        ends at half time), and has no basket-relative metres (null). Free throws are (0, 0) with a null
        ``zone``; legacy zones are unreliable. The inference of units and origin is the refs' (arc and
        sideline fit), not documented by ACB. ``player_name`` comes from the game's shooter list.
        A body that is empty or not this route's gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: the parser never raises on data; the wrapper raises NoDataError / AssetFetchError before parsing.

    Example:
        Quick start::

            from sportsdataverse.basketball.acb import acb_shots

            df = acb_shots(match_id=105401)
    """
    return _parse("shots", raw, columns, return_as_pandas)


def parse_acb_lead_tracker(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Score through one ACB game, one row per scoring moment (``leadTrackerInfo[]``).

    Args:
        raw: the body ``acb_lead_tracker(..., return_parsed=False)`` returns (the route's JSON with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when the parser lacks them).

    Returns:
        One row per point on the lead chart: the period, the clock left, both scores and ``margin`` =
        ``home_score - away_score``.
        A body that is empty or not this route's gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: the parser never raises on data; the wrapper raises NoDataError / AssetFetchError before parsing.

    Example:
        Quick start::

            from sportsdataverse.basketball.acb import acb_lead_tracker

            df = acb_lead_tracker(match_id=105401)
    """
    return _parse("lead_tracker", raw, columns, return_as_pandas)


def parse_acb_lead_stats(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Lead statistics of one ACB game (best run, maximum lead, time ahead, lead changes) per side.

    Args:
        raw: the body ``acb_lead_stats(..., return_parsed=False)`` returns (the route's JSON with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when the parser lacks them).

    Returns:
        One row per statistic and side (``stat`` is ``best_score_run``, ``max_difference``,
        ``time_ahead`` or ``lead_changes``; ``side`` home or away). ``value`` is the display value as
        sent (a count, or ``'mm:ss'`` for time ahead); ``bar_progress`` is the length of the site's
        comparison bar.
        A body that is empty or not this route's gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: the parser never raises on data; the wrapper raises NoDataError / AssetFetchError before parsing.

    Example:
        Quick start::

            from sportsdataverse.basketball.acb import acb_lead_stats

            df = acb_lead_stats(match_id=105401)
    """
    return _parse("lead_stats", raw, columns, return_as_pandas)


def parse_acb_lineup(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Players of one ACB game with their game line, starters first (``teamLineUps[].players[]``).

    Args:
        raw: the body ``acb_lineup(..., return_parsed=False)`` returns (the route's JSON with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when the parser lacks them).

    Returns:
        One row per team and player in feed order; ``lineup_order`` is the 1-based position (the feed
        lists the starters first, then the bench). ``is_playing`` is true while the player is on court
        (false once the game is over). ``rebounds`` is total rebounds, ``fouls`` personal fouls
        committed, ``differential`` the plus/minus.
        A body that is empty or not this route's gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: the parser never raises on data; the wrapper raises NoDataError / AssetFetchError before parsing.

    Example:
        Quick start::

            from sportsdataverse.basketball.acb import acb_lineup

            df = acb_lineup(match_id=105401)
    """
    return _parse("lineup", raw, columns, return_as_pandas)


def parse_acb_match_leaders(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Each team's leaders in one ACB game by category (``teamLeaders[].leaders``).

    Args:
        raw: the body ``acb_match_leaders(..., return_parsed=False)`` returns (the route's JSON with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when the parser lacks them).

    Returns:
        One row per team, metric and rank (``metric`` is the API's category, e.g. ``rating``,
        ``points``, ``rebounds``, ``assists``; ``rank`` 1 is the leader). ``value`` is the player's
        count.
        A body that is empty or not this route's gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: the parser never raises on data; the wrapper raises NoDataError / AssetFetchError before parsing.

    Example:
        Quick start::

            from sportsdataverse.basketball.acb import acb_match_leaders

            df = acb_match_leaders(match_id=105401)
    """
    return _parse("match_leaders", raw, columns, return_as_pandas)


def parse_acb_team_comparison(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Team-versus-team comparison bars of one ACB game, per period.

    Args:
        raw: the body ``acb_team_comparison(..., return_parsed=False)`` returns (the route's JSON with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when the parser lacks them).

    Returns:
        One row per period and statistic. ``quarter`` 0 is the whole game. ``home_value`` /
        ``away_value`` parse the bar's display value (Spanish decimal comma, ``'79,17%'`` becomes 79.17;
        a count stays a count); for the shooting statistics ``*_made`` / ``*_attempted`` parse the
        ``'19/24'`` subtitle. ``*_is_highlighted`` marks the side the site highlights.
        A body that is empty or not this route's gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: the parser never raises on data; the wrapper raises NoDataError / AssetFetchError before parsing.

    Example:
        Quick start::

            from sportsdataverse.basketball.acb import acb_team_comparison

            df = acb_team_comparison(match_id=105401)
    """
    return _parse("team_comparison", raw, columns, return_as_pandas)


def parse_acb_serie(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """The playoff series that an ACB game belongs to, one row per game of the series.

    Args:
        raw: the body ``acb_serie(..., return_parsed=False)`` returns (the route's JSON with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when the parser lacks them).

    Returns:
        One row per game of the series: ``match_id`` is that game and ``requested_match_id`` the game asked
        for (every row repeats it). ``home_team_id`` / ``away_team_id`` are the team entries of THAT game,
        ``series_home_*`` the team with home court. ``series_home_wins`` / ``series_away_wins`` are the
        series wins, ``best_of`` the series length as sent (0 in the 2016-17 final sample: read it as unknown). A game outside a
        series answers HTTP 204 (NoDataError).
        A body that is empty or not this route's gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: the parser never raises on data; the wrapper raises NoDataError / AssetFetchError before parsing.

    Example:
        Quick start::

            from sportsdataverse.basketball.acb import acb_serie

            df = acb_serie(match_id=17718)
    """
    return _parse("serie", raw, columns, return_as_pandas)


def parse_acb_quickfacts(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Pre-match facts per team of one ACB game: standing, record, recent results.

    Args:
        raw: the body ``acb_quickfacts(..., return_parsed=False)`` returns (the route's JSON with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when the parser lacks them).

    Returns:
        One row per team (``team_side`` home then away): ``standing`` is the league position, ``wins`` /
        ``losses`` the record before the game, ``last_results`` the recent results joined with ``'; '``
        (``WIN`` / ``LOSS``).
        A body that is empty or not this route's gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: the parser never raises on data; the wrapper raises NoDataError / AssetFetchError before parsing.

    Example:
        Quick start::

            from sportsdataverse.basketball.acb import acb_quickfacts

            df = acb_quickfacts(match_id=105401)
    """
    return _parse("quickfacts", raw, columns, return_as_pandas)


def parse_acb_season_leaders(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Each team's season leaders before one ACB game, by category.

    Args:
        raw: the body ``acb_season_leaders(..., return_parsed=False)`` returns (the route's JSON with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when the parser lacks them).

    Returns:
        One row per team, metric and rank, like ``acb_match_leaders`` but over the season so far;
        ``value`` is a total or an average depending on the metric.
        A body that is empty or not this route's gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: the parser never raises on data; the wrapper raises NoDataError / AssetFetchError before parsing.

    Example:
        Quick start::

            from sportsdataverse.basketball.acb import acb_season_leaders

            df = acb_season_leaders(match_id=105401)
    """
    return _parse("season_leaders", raw, columns, return_as_pandas)


def parse_acb_match_advanced_stats(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Team advanced statistics of one ACB game against the team's season and win/loss splits.

    Args:
        raw: the body ``acb_match_advanced_stats(..., return_parsed=False)`` returns (the route's JSON with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when the parser lacks them).

    Returns:
        One row per team (``team_side``), statistic group and statistic: ``match_value`` is this game,
        ``season_value`` the team's season value, ``win_value`` / ``loss_value`` its values in wins /
        losses, ``*_diff`` the game minus that split. Groups: four_factors, game_rhythm, ball_handling,
        shooting, points, ratings, rebounds. All are ``Float64`` (rates are percentages 0-100 unless the
        stat is a ratio, such as ``pointsPerPossession``).
        A body that is empty or not this route's gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: the parser never raises on data; the wrapper raises NoDataError / AssetFetchError before parsing.

    Example:
        Quick start::

            from sportsdataverse.basketball.acb import acb_match_advanced_stats

            df = acb_match_advanced_stats(match_id=105401)
    """
    return _parse("match_advanced_stats", raw, columns, return_as_pandas)


def parse_acb_player_advanced_stats(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """One player's advanced statistics in one ACB game against his season and win/loss splits.

    Args:
        raw: the body ``acb_player_advanced_stats(..., return_parsed=False)`` returns (the route's JSON with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when the parser lacks them).

    Returns:
        One row per statistic group and statistic, with the same value columns as
        ``acb_match_advanced_stats``. ``player_id`` is the requested licence (the body names no player).
        A body that is empty or not this route's gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: the parser never raises on data; the wrapper raises NoDataError / AssetFetchError before parsing.

    Example:
        Quick start::

            from sportsdataverse.basketball.acb import acb_player_advanced_stats

            df = acb_player_advanced_stats(match_id=105401, player_id=20212243)
    """
    return _parse("player_advanced_stats", raw, columns, return_as_pandas)


def parse_acb_matchlist(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """The games of the round (or week) around one ACB game.

    Args:
        raw: the body ``acb_matchlist(..., return_parsed=False)`` returns (the route's JSON with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when the parser lacks them).

    Returns:
        One row per game. ``home_wins`` / ``home_losses`` / ``away_wins`` / ``away_losses`` are the
        teams' series record in a playoff series (null otherwise). ``match_grouping`` is ``round`` or
        ``week``. ``team_id`` columns are season-team entries; ``club_id`` is stable.
        A body that is empty or not this route's gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: the parser never raises on data; the wrapper raises NoDataError / AssetFetchError before parsing.

    Example:
        Quick start::

            from sportsdataverse.basketball.acb import acb_matchlist

            df = acb_matchlist(match_id=105401)
    """
    return _parse("matchlist", raw, columns, return_as_pandas)


def parse_acb_competition_editions(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Every competition edition (season) the ACB API knows, one row per edition.

    Args:
        raw: the body ``acb_competition_editions(..., return_parsed=False)`` returns (the route's JSON with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when the parser lacks them).

    Returns:
        One row per competition and edition: ``edition_id`` (START year - 1935), ``season_start_year``
        and ``season`` (END year), plus how many rounds and weeks the edition lists.
        ``competition_name`` is the API's name for the competition (the Liga is ``Liga Nacional`` here).
        A body that is empty or not this route's gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: the parser never raises on data; the wrapper raises NoDataError / AssetFetchError before parsing.

    Example:
        Quick start::

            from sportsdataverse.basketball.acb import acb_competition_editions

            df = acb_competition_editions()
    """
    return _parse("competition_editions", raw, columns, return_as_pandas)


def parse_acb_competition_rounds(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """The rounds of every ACB competition edition (``editions[].rounds[]``), the round ids to crawl.

    Args:
        raw: the body ``acb_competition_rounds(..., return_parsed=False)`` returns (the route's JSON with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when the parser lacks them).

    Returns:
        One row per edition and round: the global ``round_id`` (the key of the round routes),
        ``round_number`` within the phase and the sub-phase.
        A body that is empty or not this route's gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: the parser never raises on data; the wrapper raises NoDataError / AssetFetchError before parsing.

    Example:
        Quick start::

            from sportsdataverse.basketball.acb import acb_competition_rounds

            df = acb_competition_rounds()
    """
    return _parse("competition_rounds", raw, columns, return_as_pandas)


def parse_acb_competition_weeks(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """The weeks of every ACB competition edition (``editions[].weeks[]``), the week ids to crawl.

    Args:
        raw: the body ``acb_competition_weeks(..., return_parsed=False)`` returns (the route's JSON with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when the parser lacks them).

    Returns:
        One row per edition and week: the global ``week_id``, the Spanish ``description`` ('Semana 1')
        and the Monday ``start_date``.
        A body that is empty or not this route's gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: the parser never raises on data; the wrapper raises NoDataError / AssetFetchError before parsing.

    Example:
        Quick start::

            from sportsdataverse.basketball.acb import acb_competition_weeks

            df = acb_competition_weeks()
    """
    return _parse("competition_weeks", raw, columns, return_as_pandas)


def parse_acb_competition_current(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """The ACB API's current pointers: competition, edition, week, round and match.

    Args:
        raw: the body ``acb_competition_current(..., return_parsed=False)`` returns (the route's JSON with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when the parser lacks them).

    Returns:
        One row: the ids the site opens by default, and the current match with its teams.
        A body that is empty or not this route's gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: the parser never raises on data; the wrapper raises NoDataError / AssetFetchError before parsing.

    Example:
        Quick start::

            from sportsdataverse.basketball.acb import acb_competition_current

            df = acb_competition_current()
    """
    return _parse("competition_current", raw, columns, return_as_pandas)


def parse_acb_round_matches(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """The games of one ACB round (or week) with scores, status and broadcasters.

    Args:
        raw: the body ``acb_round_matches(..., return_parsed=False)`` returns (the route's JSON with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when the parser lacks them).

    Returns:
        One row per game. A round route returns the whole round, postponed games included.
        ``broadcasters`` joins the channel names with ``'; '``. The team columns are looked up in the
        body's team list; ``week_id`` is the selected week.
        A body that is empty or not this route's gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: the parser never raises on data; the wrapper raises NoDataError / AssetFetchError before parsing.

    Example:
        Quick start::

            from sportsdataverse.basketball.acb import acb_round_matches

            df = acb_round_matches(season=2027, round_id=6017)
    """
    return _parse("round_matches", raw, columns, return_as_pandas)


def parse_acb_standings(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """League table of an ACB competition after a round (default: the current round).

    Args:
        raw: the body ``acb_standings(..., return_parsed=False)`` returns (the route's JSON with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when the parser lacks them).

    Returns:
        One row per team: position, record, points for and against, home and away records. ``round_id``
        is the round the table is as of; ``total_rounds`` the rounds in the phase.
        A body that is empty or not this route's gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: the parser never raises on data; the wrapper raises NoDataError / AssetFetchError before parsing.

    Example:
        Quick start::

            from sportsdataverse.basketball.acb import acb_standings

            df = acb_standings(season=2027)
    """
    return _parse("standings", raw, columns, return_as_pandas)


def parse_acb_standings_rounds(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """The standings history of an ACB competition: each team's position after each round.

    Args:
        raw: the body ``acb_standings_rounds(..., return_parsed=False)`` returns (the route's JSON with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when the parser lacks them).

    Returns:
        One row per team and round: ``position`` after that round and the team's game of the round
        (``match_id``).
        A body that is empty or not this route's gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: the parser never raises on data; the wrapper raises NoDataError / AssetFetchError before parsing.

    Example:
        Quick start::

            from sportsdataverse.basketball.acb import acb_standings_rounds

            df = acb_standings_rounds(season=2027)
    """
    return _parse("standings_rounds", raw, columns, return_as_pandas)


def parse_acb_bracket(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Playoff bracket of an ACB competition, one row per series.

    Args:
        raw: the body ``acb_bracket(..., return_parsed=False)`` returns (the route's JSON with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when the parser lacks them).

    Returns:
        One row per series; ``stage`` is quarter_finals, semifinals or final. ``home_wins`` /
        ``away_wins`` are the series score, ``best_of`` the series length, ``n_games`` the games listed.
        An edition with no playoffs yet raises NoDataError.
        A body that is empty or not this route's gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: the parser never raises on data; the wrapper raises NoDataError / AssetFetchError before parsing.

    Example:
        Quick start::

            from sportsdataverse.basketball.acb import acb_bracket

            df = acb_bracket(season=2026)
    """
    return _parse("bracket", raw, columns, return_as_pandas)


def parse_acb_bracket_games(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """The games of every playoff series in an ACB bracket, one row per game.

    Args:
        raw: the body ``acb_bracket_games(..., return_parsed=False)`` returns (the route's JSON with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when the parser lacks them).

    Returns:
        One row per game; ``series_*`` columns describe its series and ``stage`` the round of the
        bracket.
        A body that is empty or not this route's gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: the parser never raises on data; the wrapper raises NoDataError / AssetFetchError before parsing.

    Example:
        Quick start::

            from sportsdataverse.basketball.acb import acb_bracket_games

            df = acb_bracket_games(season=2026)
    """
    return _parse("bracket_games", raw, columns, return_as_pandas)


def parse_acb_calendar(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Every round of an ACB season with its games, one row per game.

    Args:
        raw: the body ``acb_calendar(..., return_parsed=False)`` returns (the route's JSON with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when the parser lacks them).

    Returns:
        One row per game with its round (``round_id``, ``round_number``) and the teams looked up in the
        body's team list. The cheapest way to crawl a season: one call lists every game id.
        A body that is empty or not this route's gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: the parser never raises on data; the wrapper raises NoDataError / AssetFetchError before parsing.

    Example:
        Quick start::

            from sportsdataverse.basketball.acb import acb_calendar

            df = acb_calendar(season=2027)
    """
    return _parse("calendar", raw, columns, return_as_pandas)


def parse_acb_league_zones(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """League-wide shooting by court zone for an ACB season.

    Args:
        raw: the body ``acb_league_zones(..., return_parsed=False)`` returns (the route's JSON with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when the parser lacks them).

    Returns:
        One row per zone: ``attempts``, ``made`` and ``points_scored``. Zone names: restricted_area,
        paint, mid_range_left / center / right, corner_three_left / right, wing_three_left / right,
        top_three.
        A body that is empty or not this route's gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: the parser never raises on data; the wrapper raises NoDataError / AssetFetchError before parsing.

    Example:
        Quick start::

            from sportsdataverse.basketball.acb import acb_league_zones

            df = acb_league_zones(season=2027)
    """
    return _parse("league_zones", raw, columns, return_as_pandas)


def parse_acb_teams(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Teams entered in an ACB competition edition.

    Args:
        raw: the body ``acb_teams(..., return_parsed=False)`` returns (the route's JSON with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when the parser lacks them).

    Returns:
        One row per team: the entry of the selected edition (``team_id``, new every season) and the
        club's latest entry (``latest_team_id``). ``club_id`` is the stable key.
        A body that is empty or not this route's gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: the parser never raises on data; the wrapper raises NoDataError / AssetFetchError before parsing.

    Example:
        Quick start::

            from sportsdataverse.basketball.acb import acb_teams

            df = acb_teams(season=2027)
    """
    return _parse("teams", raw, columns, return_as_pandas)


def parse_acb_team_matches(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """A club's games in an ACB competition edition.

    Args:
        raw: the body ``acb_team_matches(..., return_parsed=False)`` returns (the route's JSON with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when the parser lacks them).

    Returns:
        One row per game, with ``is_selected_local`` true when the club played at home.
        A body that is empty or not this route's gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: the parser never raises on data; the wrapper raises NoDataError / AssetFetchError before parsing.

    Example:
        Quick start::

            from sportsdataverse.basketball.acb import acb_team_matches

            df = acb_team_matches(club_id=2, season=2027)
    """
    return _parse("team_matches", raw, columns, return_as_pandas)


def parse_acb_next_matches(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """A club's most recent and upcoming ACB games.

    Args:
        raw: the body ``acb_next_matches(..., return_parsed=False)`` returns (the route's JSON with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when the parser lacks them).

    Returns:
        One row per game (recent and upcoming around today); same columns as ``acb_matchlist``.
        A body that is empty or not this route's gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: the parser never raises on data; the wrapper raises NoDataError / AssetFetchError before parsing.

    Example:
        Quick start::

            from sportsdataverse.basketball.acb import acb_next_matches

            df = acb_next_matches(club_id=2)
    """
    return _parse("next_matches", raw, columns, return_as_pandas)


def parse_acb_club_data(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Club profile: arena, contacts and links of an ACB club.

    Args:
        raw: the body ``acb_club_data(..., return_parsed=False)`` returns (the route's JSON with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when the parser lacks them).

    Returns:
        One row: the club's current team entry, the arena (name, address, capacity), foundation year,
        contact details and social links. Several fields are null for many clubs.
        A body that is empty or not this route's gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: the parser never raises on data; the wrapper raises NoDataError / AssetFetchError before parsing.

    Example:
        Quick start::

            from sportsdataverse.basketball.acb import acb_club_data

            df = acb_club_data(club_id=2)
    """
    return _parse("club_data", raw, columns, return_as_pandas)


def parse_acb_current_roster(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """A club's players and staff for an ACB edition.

    Args:
        raw: the body ``acb_current_roster(..., return_parsed=False)`` returns (the route's JSON with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when the parser lacks them).

    Returns:
        One row per person; ``role`` is player or staff (head coach and assistants). ``person_id`` is
        the ACB licence number (a player's ``player_id``). ``height_cm`` is the player's height in
        centimetres (null for staff); ``age`` is the age now; ``nationality`` is Spanish and
        ``nationality_iso`` its ISO alpha-2 code. ``temporary_roster`` is true while the roster is
        provisional. Absence lists are not parsed.
        A body that is empty or not this route's gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: the parser never raises on data; the wrapper raises NoDataError / AssetFetchError before parsing.

    Example:
        Quick start::

            from sportsdataverse.basketball.acb import acb_current_roster

            df = acb_current_roster(club_id=2, season=2027)
    """
    return _parse("current_roster", raw, columns, return_as_pandas)


def parse_acb_team_leaders(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """A club's statistical leaders for an ACB edition and all-time.

    Args:
        raw: the body ``acb_team_leaders(..., return_parsed=False)`` returns (the route's JSON with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when the parser lacks them).

    Returns:
        One row per scope, metric and rank. ``scope`` is ``all_time`` (club record holders, the API's
        ``leaders``) or ``edition`` (the selected season, ``seasonLeaders``); ``metric`` is the API's
        category (``points``, ``avg_points``, ``played_matches``...); ``value`` is a total or an average
        depending on the metric (Float64). The scope assignment is inferred from the values.
        A body that is empty or not this route's gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: the parser never raises on data; the wrapper raises NoDataError / AssetFetchError before parsing.

    Example:
        Quick start::

            from sportsdataverse.basketball.acb import acb_team_leaders

            df = acb_team_leaders(club_id=2, number_of_players=3, season=2027)
    """
    return _parse("team_leaders", raw, columns, return_as_pandas)


def parse_acb_team_stats(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """A club's player season statistics for an ACB edition: totals, averages and per-40 side by side.

    Args:
        raw: the body ``acb_team_stats(..., return_parsed=False)`` returns (the route's JSON with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when the parser lacks them).

    Returns:
        One row per player. ``total_*`` are season counts (``Int64``; ``total_time_played`` is seconds,
        ``*_percentage`` are Float64), ``avg_*`` per-game averages and ``per40_*`` per-40-minute rates
        (Float64). ``num_games`` is games played and ``starter`` games started.
        A body that is empty or not this route's gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: the parser never raises on data; the wrapper raises NoDataError / AssetFetchError before parsing.

    Example:
        Quick start::

            from sportsdataverse.basketball.acb import acb_team_stats

            df = acb_team_stats(club_id=2, season=2027)
    """
    return _parse("team_stats", raw, columns, return_as_pandas)


def parse_acb_team_totals(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """A club's team season totals, averages and per-40 values for an ACB edition.

    Args:
        raw: the body ``acb_team_totals(..., return_parsed=False)`` returns (the route's JSON with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when the parser lacks them).

    Returns:
        One row: the same ``total_*`` / ``avg_*`` / ``per40_*`` columns as ``acb_team_stats``, for the
        team as a whole (``team_id`` is the season-team entry).
        A body that is empty or not this route's gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: the parser never raises on data; the wrapper raises NoDataError / AssetFetchError before parsing.

    Example:
        Quick start::

            from sportsdataverse.basketball.acb import acb_team_totals

            df = acb_team_totals(club_id=2, season=2027)
    """
    return _parse("team_totals", raw, columns, return_as_pandas)


def parse_acb_team_records(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """A club's single-game records (best performance per statistic).

    Args:
        raw: the body ``acb_team_records(..., return_parsed=False)`` returns (the route's JSON with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when the parser lacks them).

    Returns:
        One row per statistic (``stat``): ``value`` is the record, with the game (``game_id``,
        ``game_date``), the player and both teams of that game. Records go back to 1983-84; ``game_id``
        is a match id of the legacy sequence.
        A body that is empty or not this route's gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: the parser never raises on data; the wrapper raises NoDataError / AssetFetchError before parsing.

    Example:
        Quick start::

            from sportsdataverse.basketball.acb import acb_team_records

            df = acb_team_records(club_id=2)
    """
    return _parse("team_records", raw, columns, return_as_pandas)


def parse_acb_player_data(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Biography of one ACB player: birth, nationality, height, current team and social links.

    Args:
        raw: the body ``acb_player_data(..., return_parsed=False)`` returns (the route's JSON with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when the parser lacks them).

    Returns:
        One row. ``birth_date`` is a date (the API sends DD-MM-YYYY); ``height_m`` is metres (the API
        sends ``'2,20'``, a decimal comma); ``birth_country`` / ``nationality`` are Spanish with ISO
        alpha-2 in ``*_iso``; ``licensing`` is the licence code as sent (for example ``EUR`` or
        ``JFL``; meanings unverified). The current team columns are the
        player's team entry for the edition.
        A body that is empty or not this route's gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: the parser never raises on data; the wrapper raises NoDataError / AssetFetchError before parsing.

    Example:
        Quick start::

            from sportsdataverse.basketball.acb import acb_player_data

            df = acb_player_data(player_id=20209407)
    """
    return _parse("player_data", raw, columns, return_as_pandas)


def parse_acb_player_stats(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """A player's ACB season statistics, one row per team and edition.

    Args:
        raw: the body ``acb_player_stats(..., return_parsed=False)`` returns (the route's JSON with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when the parser lacks them).

    Returns:
        One row per team-edition the player played: ``total_*`` counts, ``avg_*`` averages and
        ``per40_*`` per-40 rates as in ``acb_team_stats``. ``edition_year`` is the label
        (``'2026-2027'``).
        A body that is empty or not this route's gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: the parser never raises on data; the wrapper raises NoDataError / AssetFetchError before parsing.

    Example:
        Quick start::

            from sportsdataverse.basketball.acb import acb_player_stats

            df = acb_player_stats(player_id=20209407)
    """
    return _parse("player_stats", raw, columns, return_as_pandas)


def parse_acb_player_totals(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """A player's career totals, averages and per-40 values in the ACB (all editions of the query).

    Args:
        raw: the body ``acb_player_totals(..., return_parsed=False)`` returns (the route's JSON with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when the parser lacks them).

    Returns:
        One row: the same ``total_*`` / ``avg_*`` / ``per40_*`` columns as ``acb_player_stats`` over the
        player's whole ACB career.
        A body that is empty or not this route's gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: the parser never raises on data; the wrapper raises NoDataError / AssetFetchError before parsing.

    Example:
        Quick start::

            from sportsdataverse.basketball.acb import acb_player_totals

            df = acb_player_totals(player_id=20209407)
    """
    return _parse("player_totals", raw, columns, return_as_pandas)


def parse_acb_career_stints(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """A player's club stints in the ACB, one row per stint.

    Args:
        raw: the body ``acb_career_stints(..., return_parsed=False)`` returns (the route's JSON with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when the parser lacks them).

    Returns:
        One row per stint: first and last year, the club's team entry for the stint and how many
        editions it spans (``n_editions``).
        A body that is empty or not this route's gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: the parser never raises on data; the wrapper raises NoDataError / AssetFetchError before parsing.

    Example:
        Quick start::

            from sportsdataverse.basketball.acb import acb_career_stints

            df = acb_career_stints(player_id=20209407)
    """
    return _parse("career_stints", raw, columns, return_as_pandas)


def parse_acb_career_trophies(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """A player's ACB trophies, one row per competition.

    Args:
        raw: the body ``acb_career_trophies(..., return_parsed=False)`` returns (the route's JSON with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when the parser lacks them).

    Returns:
        One row per competition: times champion and times runner-up.
        A body that is empty or not this route's gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: the parser never raises on data; the wrapper raises NoDataError / AssetFetchError before parsing.

    Example:
        Quick start::

            from sportsdataverse.basketball.acb import acb_career_trophies

            df = acb_career_trophies(player_id=20209407)
    """
    return _parse("career_trophies", raw, columns, return_as_pandas)


def parse_acb_player_matches(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """A player's game log for an ACB edition (default: the current one).

    Args:
        raw: the body ``acb_player_matches(..., return_parsed=False)`` returns (the route's JSON with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when the parser lacks them).

    Returns:
        One row per game. ``has_won`` is true when the player's team won; ``is_selected_local`` is true
        when it played at home. ``play_time`` is ``'mm:ss'`` (``seconds_played`` in seconds).
        Percentages are Float64, the rest counts. Names follow the player-route vocabulary
        (``rebounds``, ``shots_rejected``, ``fouls_received``).
        A body that is empty or not this route's gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: the parser never raises on data; the wrapper raises NoDataError / AssetFetchError before parsing.

    Example:
        Quick start::

            from sportsdataverse.basketball.acb import acb_player_matches

            df = acb_player_matches(player_id=20209407, season=2027)
    """
    return _parse("player_matches", raw, columns, return_as_pandas)


def parse_acb_search(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Search ACB players, teams and coaches by name.

    Args:
        raw: the body ``acb_search(..., return_parsed=False)`` returns (the route's JSON with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when the parser lacks them).

    Returns:
        One row per hit; ``kind`` is player, team or coach. The sample holds players only (``teams`` and
        ``coaches`` came back empty), so the team and coach row shapes are unverified. ``person_id`` is
        the licence number for players and coaches (a team hit carries its team entry id).
        A body that is empty or not this route's gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: the parser never raises on data; the wrapper raises NoDataError / AssetFetchError before parsing.

    Example:
        Quick start::

            from sportsdataverse.basketball.acb import acb_search

            df = acb_search(search_term='tavares')
    """
    return _parse("search", raw, columns, return_as_pandas)
