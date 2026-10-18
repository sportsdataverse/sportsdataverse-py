"""Parsers for the generated ``tbf`` wrappers (Turkish Basketball Federation app API, BSL / TBL / KBSL).

Every public ``parse_tbf_*`` takes the dict a wrapper returns with ``return_parsed=False`` -- the API envelope
``{data, isSuccess, message, problemDetails}`` plus the request stamped under ``_request`` by ``tbf_runtime`` -- and
follows the package parser contract: a polars frame by default, pandas with ``return_as_pandas=True``, snake_case
columns, and a zero-row frame with the documented columns and dtypes, never an exception, for an empty or unrecognised
input.

**Dtypes** are pinned by column name (:data:`SPECS`), never sampled. The API sends ids as ``22214`` or ``22214.0``
depending on route and season, ids and counts as strings on some routes (leaders, scores, the team statistics tables),
percentages as integers on the box score and as decimal strings on the team tables, and clock times as ``mm:ss`` or
``hh:mm:ss``. The parsers cast at the boundary: an integral float or digit string becomes ``Int64`` (never through
``str``), a decimal string ``Float64``. Shirt numbers stay strings.

**Id key spaces** (the refs README, "Ids"):

* ``match_id``: TBF games, ONE global sequence across leagues and seasons. The route echoes it as a string or null;
  the runtime stamps the integer asked for.
* ``genius_id``: Genius Sports LiveStats match id (TBF spells it ``genuisId``); joins the ``genius`` family. Null
  when TBF stored none (every game before 2018-19 and many 2021-25 games).
* ``team_process_id``: a team REGISTRATION, new for every club in every league-season (Tofaş = 265995 in BSL 2026-27);
  the ``team_process_id`` argument of the team wrappers. ``club_id`` is the stable club (Tofaş = 13).
* ``activity_id``: one league in one season; ``season_id`` the internal TBF season id. Neither is an sdv key: the
  public ``season`` is the END year (``"2026-2027"`` -> 2027), mapped by ``tbf_runtime``.
* ``person_id`` / ``player_id``: TBF persons, stable across seasons and clubs. The shot chart's player key is a
  different key space (``shot_player_id``): join shots to box rows on name + shirt number + side. Play-by-play carries
  no player id at all.

Personal data (birth dates, contact fields) is dropped; photos, logos and colours are dropped too.
"""

from __future__ import annotations

import math
import re
from datetime import date, datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

import pandas as pd
import polars as pl

__all__ = [
    "parse_tbf_seasons",
    "parse_tbf_leagues",
    "parse_tbf_league_seasons",
    "parse_tbf_weeks",
    "parse_tbf_standings",
    "parse_tbf_schedule",
    "parse_tbf_daily_matches",
    "parse_tbf_game_header",
    "parse_tbf_game_players",
    "parse_tbf_player_box",
    "parse_tbf_team_box",
    "parse_tbf_pbp",
    "parse_tbf_shots",
    "parse_tbf_period_scores",
    "parse_tbf_highlights",
    "parse_tbf_game_leaders",
    "parse_tbf_players",
    "parse_tbf_player_career",
    "parse_tbf_leaders",
    "parse_tbf_teams",
    "parse_tbf_team_player_stats",
    "parse_tbf_team_games",
    "parse_tbf_team_roster",
    "parse_tbf_team_staff",
]

Frame = Union[pl.DataFrame, pd.DataFrame]
Spec = Dict[str, pl.DataType]

#: Where the runtime stamps the request (``tbf_runtime.REQUEST_KEY``).
_REQUEST = "_request"
INT, F, S, B, D, DT = pl.Int64, pl.Float64, pl.Utf8, pl.Boolean, pl.Date, pl.Datetime("us")


#: Turkish play-by-play action -> English gloss (the 77 values seen in four 2025-26 / 2026-27 games; refs tbf-returns.md).
ACTIONS: Dict[str, str] = {
    "2 Sayı Aniden Durarak Sıçrayarak Atış Başarılı": "2PT pull-up jump shot made",
    "2 Sayı Aniden Durarak Sıçrayarak Atış Başarısız": "2PT pull-up jump shot missed",
    "2 Sayı Atış Başarılı": "2PT shot made",
    "2 Sayı Dönerek Sıçrayarak Atış Başarılı": "2PT turnaround jump shot made",
    "2 Sayı Dönerek Sıçrayarak Atış Başarısız": "2PT turnaround jump shot missed",
    "2 Sayı Geriye Adım Atarak Sıçrayarak Atış Başarılı": "2PT step-back jump shot made",
    "2 Sayı Geriye Adım Atarak Sıçrayarak Atış Başarısız": "2PT step-back jump shot missed",
    "2 Sayı Geriye Doğru Sıçrayarak Atış Başarılı": "2PT fade-away jump shot made",
    "2 Sayı Geriye Doğru Sıçrayarak Atış Başarısız": "2PT fade-away jump shot missed",
    "2 Sayı Havada Kayarak Sıçrayarak Atış Başarılı": "2PT floater made",
    "2 Sayı Havada Kayarak Sıçrayarak Atış Başarısız": "2PT floater missed",
    "2 Sayı Smaç Başarılı": "2PT dunk made",
    "2 Sayı Sıçrayarak Atış Başarılı": "2PT jump shot made",
    "2 Sayı Sıçrayarak Atış Başarısız": "2PT jump shot missed",
    "2 Sayı Ters Turnike Başarılı": "2PT reverse layup made",
    "2 Sayı Ters Turnike Başarısız": "2PT reverse layup missed",
    "2 Sayı Tip-In Başarılı": "2PT tip-in made",
    "2 Sayı Tip-In Başarısız": "2PT tip-in missed",
    "2 Sayı Top Sürerek Turnike Başarılı": "2PT driving layup made",
    "2 Sayı Top Sürerek Turnike Başarısız": "2PT driving layup missed",
    "2 Sayı Turnike Başarılı": "2PT layup made",
    "2 Sayı Turnike Başarısız": "2PT layup missed",
    "2 Sayı Çengel Atış Başarılı": "2PT hook shot made",
    "2 Sayı Çengel Atış Başarısız": "2PT hook shot missed",
    "3 Sayı Aniden Durarak Sıçrayarak Atış Başarılı": "3PT pull-up jump shot made",
    "3 Sayı Aniden Durarak Sıçrayarak Atış Başarısız": "3PT pull-up jump shot missed",
    "3 Sayı Geriye Adım Atarak Sıçrayarak Atış Başarılı": "3PT step-back jump shot made",
    "3 Sayı Geriye Adım Atarak Sıçrayarak Atış Başarısız": "3PT step-back jump shot missed",
    "3 Sayı Sıçrayarak Atış Başarılı": "3PT jump shot made",
    "3 Sayı Sıçrayarak Atış Başarısız": "3PT jump shot missed",
    "Asist": "assist",
    "Blok": "block",
    "Değişiklik Giren": "substitution - player in",
    "Değişiklik Çıkan": "substitution - player out",
    "Faul": "foul",
    "Faul Yapılan": "foul drawn",
    "Hava Atışı - Kaybetti": "jump ball - lost",
    "Hava Atışı - Kazandı": "jump ball - won",
    "Head Coach Challenge": "head coach's challenge",
    "Hücum Faul": "offensive foul",
    "Hücum Ribaundu": "offensive rebound",
    "Jump Ball - Tutulmuş Top": "jump ball - held ball",
    "Kişisel Faul": "personal foul",
    "Koç Teknik Faul": "technical foul (coach)",
    "Maç Başladı": "game started",
    "Maç Sonu": "game end",
    "Mola": "timeout",
    "Ribaund": "rebound",
    "Savunma Ribaundu": "defensive rebound",
    "Serbest Atış 1/1 Başarılı": "free throw 1 of 1 made",
    "Serbest Atış 1/1 Başarısız": "free throw 1 of 1 missed",
    "Serbest Atış 1/2 Başarılı": "free throw 1 of 2 made",
    "Serbest Atış 1/2 Başarısız": "free throw 1 of 2 missed",
    "Serbest Atış 1/3 Başarılı": "free throw 1 of 3 made",
    "Serbest Atış 1/3 Başarısız": "free throw 1 of 3 missed",
    "Serbest Atış 2/2 Başarılı": "free throw 2 of 2 made",
    "Serbest Atış 2/2 Başarısız": "free throw 2 of 2 missed",
    "Serbest Atış 2/3 Başarılı": "free throw 2 of 3 made",
    "Serbest Atış 3/3 Başarılı": "free throw 3 of 3 made",
    "Serbest Atış 3/3 Başarısız": "free throw 3 of 3 missed",
    "Sıkışmış Top": "held ball",
    "Takım Hücum Ribaundu": "team offensive rebound",
    "Takım Savunma Ribaundu": "team defensive rebound",
    "Teknik Faul": "technical foul (player)",
    "Top Kaybı - 24 Saniye": "turnover - 24-second (shot-clock) violation",
    "Top Kaybı - 3 Saniye": "turnover - 3-second violation",
    "Top Kaybı - 5 Saniye": "turnover - 5-second violation",
    "Top Kaybı - 8 Saniye": "turnover - 8-second violation",
    "Top Kaybı - Hatalı Yürüme": "turnover - travelling",
    "Top Kaybı - Hücum Faul": "turnover - offensive foul",
    "Top Kaybı - Kötü Pas": "turnover - bad pass",
    "Top Kaybı - Oyun Sahası Dışı": "turnover - out of bounds",
    "Top Kaybı - Top Hakimiyeti": "turnover - ball handling",
    "Top Kaybı - Yarı Saha İhlali": "turnover - backcourt violation",
    "Top Çalma": "steal",
    "Çeyrek Başladı": "period started",
    "Çeyrek Sonu": "period end",
}


# ------------------------------------------------------------------------------------------ casts
def _i(v: Any) -> Optional[int]:
    """An id / count from an integer, an integral float or a digit string, else ``None`` (never via ``str(float)``)."""
    if isinstance(v, bool) or v is None:
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v) if v == v and v not in (float("inf"), float("-inf")) and v == int(v) else None
    if isinstance(v, str) and re.fullmatch(r"\s*[+-]?\d+\s*", v):
        return int(v)
    return None


def _f(v: Any) -> Optional[float]:
    """A number from a number or a decimal string (``"47.62"``, ``"71.4%"``); ``""`` and ``"-"`` are ``None``."""
    if isinstance(v, bool) or v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str) and re.fullmatch(r"\s*[+-]?\d+(?:[.,]\d+)?%?\s*", v):
        return float(v.strip().rstrip("%").replace(",", "."))
    return None


def _s(v: Any) -> Optional[str]:
    """A stripped non-empty string, else ``None`` (numbers are stringified, integral floats without ``.0``)."""
    if v is None or isinstance(v, (dict, list, bool)):
        return None
    if isinstance(v, float):  # NaN / inf are not values; 123.0 -> "123"
        if not math.isfinite(v):
            return None
        v = int(v) if v.is_integer() else v
    t = str(v).strip()
    return t or None


def _b(v: Any) -> Optional[bool]:
    return v if isinstance(v, bool) else None


def _date(v: Any) -> Optional[date]:
    """``"2026-09-25"`` or ``"2026-09-25T19:00:00"`` -> a date."""
    try:
        return date.fromisoformat(v[:10]) if isinstance(v, str) and v else None
    except ValueError:
        return None


def _dt(v: Any) -> Optional[datetime]:
    """An ISO 8601 timestamp without zone (Turkey local time, UTC+3) -> a naive datetime."""
    try:
        return datetime.fromisoformat(v) if isinstance(v, str) and v else None
    except ValueError:
        return None


def _clock(v: Any) -> Optional[int]:
    """``"25:19"`` -> 1519 s; ``"01:25:20"`` -> 5120 s (a team row sums its players: ``"200:00"`` -> 12000)."""
    m = re.fullmatch(r"\s*(?:(\d+):)?(\d+):(\d{2})\s*", v) if isinstance(v, str) else None
    return (int(m.group(1) or 0) * 3600 + int(m.group(2)) * 60 + int(m.group(3))) if m else None


def _end_year(v: Any) -> Optional[int]:
    """Season label ``"2026-2027"`` -> its END year 2027; a bare ``"2027"`` is itself."""
    m = re.search(r"(\d{4})\s*-\s*(\d{4})\s*$", v) if isinstance(v, str) else None
    return int(m.group(2)) if m else None


def _side(v: Any) -> Optional[str]:
    return "home" if v is True else "away" if v is False else None


def _score(idx: int) -> Callable[[Any], Optional[int]]:
    """The home (0) or away (1) half of a running score ``"34-30"``."""

    def pick(v: Any) -> Optional[int]:
        m = re.fullmatch(r"\s*(\d+)\s*-\s*(\d+)\s*", v) if isinstance(v, str) else None
        return int(m.group(idx + 1)) if m else None

    return pick


def _dict(v: Any) -> Dict[str, Any]:
    return v if isinstance(v, dict) else {}


def _list(v: Any) -> List[Dict[str, Any]]:
    return [x for x in v if isinstance(x, dict)] if isinstance(v, list) else []


_CAST: Dict[Any, Callable[[Any], Any]] = {INT: _i, F: _f, S: _s, B: _b, D: _date, DT: _dt}


def _get(row: Dict[str, Any], path: str) -> Any:
    """First non-null value among the ``|``-separated dotted ``path`` alternatives of ``row``."""
    for alt in path.split("|"):
        cur: Any = row
        for k in alt.split("."):
            cur = cur.get(k) if isinstance(cur, dict) else None
        if cur is not None:
            return cur
    return None


# ------------------------------------------------------------------------------------------ tables
# short -> [(column, source path in the row, dtype, optional converter of the extracted value)]. A path starting
# with ``_`` reads the request context the runtime stamps under ``_request`` (``_match_id``, ``_season`` ...).
Col = Tuple[str, str, Any, Optional[Callable[[Any], Any]]]


def c(col: str, path: str, dtype: Any = S, fn: Optional[Callable[[Any], Any]] = None) -> Col:
    """One column spec of a row table.

    Args:
        col: the output column name.
        path: the dotted wire path the value is read from.
        dtype: the polars dtype (default String).
        fn: an optional coercion applied to the raw value.

    Returns:
        The ``(col, path, dtype, fn)`` tuple the table builders read.
    """
    return (col, path, dtype, fn)


_SEASON_CTX = [c("season", "_season", INT), c("competition", "_competition", S)]
_GAME_CTX = [c("match_id", "_match_id", INT)]
_BOX_STATS = [
    c("seconds_played", "minutesPlayed", INT, _clock),
    c("points", "points", INT),
    c("fg_made", "successfulShots", INT),
    c("fg_attempted", "shotAttempts", INT),
    c("fg_pct", "shotPercentage", INT),
    c("fg2_made", "twoPointShots", INT),
    c("fg2_attempted", "twoPointShotAttempts", INT),
    c("fg2_pct", "twoPointPercentage", INT),
    c("fg3_made", "threePointShots", INT),
    c("fg3_attempted", "threePointShotAttempts", INT),
    c("fg3_pct", "threePointPercentage", INT),
    c("ft_made", "freeThrows", INT),
    c("ft_attempted", "freeThrowAttempts", INT),
    c("ft_pct", "freeThrowPercentage", INT),
    c("off_rebounds", "offensiveRebounds", INT),
    c("def_rebounds", "defensiveRebounds", INT),
    c("rebounds", "totalRebounds", INT),
    c("assists", "assists", INT),
    c("steals", "steals", INT),
    c("blocks", "blocks", INT),
    c("turnovers", "turnovers", INT),
    c("fouls", "fouls", INT),
    c("fouls_drawn", "rivalFoul", INT),
    c("blocks_received", "blocksReceived", INT),
]
_MATCH = [
    c("match_id", "matchId", INT),
    c("match_code", "matchCode"),
    c("genius_id", "genuisId", INT),
    c("activity_id", "activityId", INT),
    c("activity_name", "activityName"),
    c("competition", "activityCode"),
    c("season_id", "seasonId", INT),
    c("season_label", "season"),
    c("season", "season", INT, _end_year),
    c("tipoff", "matchDate", DT),
    c("match_date", "matchDateOnly", D),
    c("week", "week"),
    c("phase", "halfValue", INT),
    c("status_id", "matchStatusId", INT),
    c("home_team_process_id", "homeTeam.id", INT),
    c("home_team", "homeTeam.name"),
    c("home_score", "homeTeam.score", INT),
    c("away_team_process_id", "awayTeam.id", INT),
    c("away_team", "awayTeam.name"),
    c("away_score", "awayTeam.score", INT),
    c("venue", "salonAdi|venueName"),
    c("province", "il"),
    c("gender", "cinsiyet"),
    c("broadcast_channel", "broadcastChannel"),
    c("has_overtime", "hasUzatma", B),
]
_LEADER = [
    c("season", "_season", INT),
    c("competition", "_competition"),
    c("category", "_category"),
    c("kind", "_kind"),
    c("rank", "_rank", INT),
    c("person_id", "oyuncuKodu", INT),
    c("player_name", "oyuncuAdi"),
    c("team_process_id", "takimKodu", INT),
    c("team_name", "takimAdi"),
    c("games", "macSayisi", INT),
    c("value", "grup", F),
]
_CAREER_STATS = [
    c("games", "matchCount", INT),
    c("seconds_played", "formattedTotalSeconds", INT, _clock),
    c("points", "totalPoints", INT),
    c("assists", "assists", INT),
    c("fg_made", "fieldGoalsMade", INT),
    c("fg_attempted", "fieldGoalsAttempted", INT),
    c("fg3_made", "threePointsMade", INT),
    c("fg3_attempted", "threePointsAttempted", INT),
    c("ft_made", "freeThowsMade", INT),
    c("ft_attempted", "freeThowsAttempted", INT),
    c("off_rebounds", "offensiveRebounds", INT),
    c("def_rebounds", "defensiveRebounds", INT),
    c("rebounds", "rebounds", INT),
    c("steals", "steals", INT),
    c("turnovers", "tournovers", INT),
    c("blocks", "blocks", INT),
    c("fouls", "fauls", INT),
    c("efficiency", "playerEfficiency", INT),
    c("fouls_drawn", "yapilanFaul", INT),
    c("blocks_received", "yenilenBlok", INT),
]

TABLES: Dict[str, List[Col]] = {
    "seasons": [
        c("season_id", "seasonId", INT),
        c("season_label", "code"),
        c("season", "code", INT, _end_year),
        c("start_date", "startDate", D),
        c("end_date", "endDate", D),
        c("international", "international", B),
    ],
    "leagues": [
        c("activity_id", "activityId", INT),
        c("name", "name"),
        c("display_name", "displayName"),
        c("competition", "prefixUrl"),
        c("gender", "gender"),
        c("season_id", "seasonId", INT),
        c("season_label", "season"),
        c("season", "season", INT, _end_year),
        c("sort", "sort", INT),
    ],
    "league_seasons": [
        c("activity_id", "faaliyet_ID", INT),
        c("activity_name", "faaliyet"),
        c("url", "url"),
        c("season_id", "sezon_ID", INT),
        c("season_label", "sezon"),
        c("season", "sezon", INT, _end_year),
        c("competition", "_competition"),
    ],
    "weeks": [
        c("activity_id", "_activity_id", INT),
        c("season", "_season", INT),
        c("competition", "_competition"),
        c("week", "sezon_Hafta"),
        c("phase", "devre_ID", INT),
        c("phase_name", "devre_Deger"),
        c("label", "display_Text"),
        c("is_current", "isCurrentWeek", B),
    ],
    "standings": [
        c("season", "_season", INT),
        c("competition", "_competition"),
        c("group", "grup"),
        c("activity_id", "faaliyetAltAlanId", INT),
        c("team_process_id", "takimIslemId", INT),
        c("team_name", "takimAdi"),
        c("week", "hafta", INT),
        c("rank", "sira", INT),
        c("played", "o", INT),
        c("won", "g", INT),
        c("lost", "m", INT),
        c("points_for", "a", INT),
        c("points_against", "y", INT),
        c("points", "puan", INT),
        c("head_to_head_points", "icP", INT),
        c("head_to_head_diff", "icA", INT),
        c("total_diff", "toA", INT),
        c("excluded", "ihrac", INT),
    ],
    "schedule": _MATCH,
    "daily_matches": [c("group_name", "_group"), *_MATCH, c("status", "matchStatus")],
    "game_header": [
        *_GAME_CTX,
        c("match_code", "matchCode"),
        c("genius_id", "genuisId", INT),
        c("activity_id", "faaliyetId", INT),
        c("activity_name", "faaliyetAdi"),
        c("season_id", "seasonId", INT),
        c("season_label", "seasonName"),
        c("season", "seasonName", INT, _end_year),
        c("tipoff", "matchDateTime", DT),
        c("status_id", "matchStatusId", INT),
        c("status", "matchStatus"),
        c("venue", "location"),
        c("attendance", "matchSpectatorCount", INT),
        c("has_overtime", "hasUzatma", B),
        c("broadcast_channel", "matchBroadcastChannel"),
        c("home_team_process_id", "homeTeamProcessId", INT),
        c("home_team", "homeTeam.teamName"),
        c("home_coach", "homeTeam.coachName"),
        c("home_score", "homeTeamScore", INT),
        c("away_team_process_id", "awayTeamProcessId", INT),
        c("away_team", "awayTeam.teamName"),
        c("away_coach", "awayTeam.coachName"),
        c("away_score", "awayTeamScore", INT),
        c("referee", "mainReferee"),
        c("first_assistant_referee", "firstAssistantReferee"),
        c("second_assistant_referee", "secondAssistantReferee"),
        c("commissioner", "temsilciAdi"),
    ],
    "game_players": [
        *_GAME_CTX,
        c("side", "isHome", S, _side),
        c("team", "teamName"),
        c("player_name", "playerName"),
        c("jersey_number", "jerseyNumber"),
        c("is_starter", "isStarter", B),
        c("is_active", "isActive", B),
    ],
    "player_box": [
        *_GAME_CTX,
        c("side", "_side"),
        c("team_process_id", "_team_process_id", INT),
        c("player_id", "playerId", INT),
        c("player_name", "playerName"),
        c("jersey_number", "jerseyNumber"),
        c("is_starter", "_starter", B),
        *_BOX_STATS,
        c("plus_minus", "plusMinus", INT),
        c("efficiency", "efficiency", INT),
    ],
    "team_box": [
        *_GAME_CTX,
        c("side", "_side"),
        c("team_process_id", "_team_process_id", INT),
        *_BOX_STATS,
        c("efficiency", "efficiency", INT),  # the team line sends 92.0: the players' integer ratings summed
        c("team_off_rebounds", "_stats.offensiveRebounds", INT),
        c("team_def_rebounds", "_stats.defensiveRebounds", INT),
        c("team_rebounds", "_stats.totalRebounds", INT),
        c("team_turnovers", "_stats.turnovers", INT),
        c("team_fouls", "_stats.fouls", INT),
    ],
    "pbp": [
        *_GAME_CTX,
        c("action_number", "actionNumber", INT),
        c("period", "quarterNumber", INT),
        c("period_type", "periodType", INT),
        c("clock", "time"),
        c("seconds_remaining", "time", INT, _clock),
        c("home_score", "score", INT, _score(0)),
        c("away_score", "score", INT, _score(1)),
        c("side", "isHome", S, _side),
        c("player_name", "playerName"),
        c("jersey_number", "jerseyNumber"),
        c("action", "action"),
        c("action_en", "action", S, lambda v: ACTIONS.get(v) if isinstance(v, str) else None),
    ],
    "shots": [
        *_GAME_CTX,
        c("period", "period", INT),
        c("period_type", "periodType", INT),
        c("side", "isHome", S, _side),
        c("shot_player_id", "playerId", INT),
        c("player_name", "playerName"),
        c("jersey_number", "jerseyNumber"),
        c("x", "x", INT),
        c("y", "y", INT),
        c("made", "isSucceed", B),
    ],
    "period_scores": [
        *_GAME_CTX,
        c("side", "_side"),
        c("period", "quarter", INT),
        c("period_type", "periodType", INT),
        c("points", "score", INT),
    ],
    "highlights": [
        *_GAME_CTX,
        c("title", "title"),
        c("category", "leaderCategory"),
        c("home_value", "homeValue"),
        c("away_value", "awayValue"),
        c("home_number", "homeValue", F),
        c("away_number", "awayValue", F),
    ],
    "game_leaders": [
        *_GAME_CTX,
        c("category", "_category"),
        c("category_label", "_category_label"),
        c("rank", "_rank", INT),
        c("side", "isHome", S, _side),
        c("player_id", "playerId", INT),
        c("player_name", "playerName"),
        c("jersey_number", "jerseyNumber"),
        c("value", "value", F),
    ],
    "players": [
        *_SEASON_CTX,
        c("person_id", "kisiID", INT),
        c("player_name", "adiSoyadi"),
        c("nationality", "tabiyet"),
        c("nationality_code", "tabiyetKısa"),
        c("club_id", "takimID", INT),
        c("team_name", "takimAdi"),
        c("jersey_number", "formaNo"),
        c("prefix", "prefix"),
        c("season_id", "seasonId", INT),
    ],
    "player_career": [
        c("person_id", "_player_id", INT),
        c("player_name", "_name"),
        c("nationality", "_country"),
        c("height_cm", "_height", INT),
        c("season_label", "seasonCode"),
        c("season", "seasonCode", INT, _end_year),
        c("team_name", "teamName"),
        c("activity_name", "activityName"),
        c("activity_type_id", "faaliyetTuruId", INT),
        c("activity_id", "faaliyetAltAlaniId", INT),
        *_CAREER_STATS,
    ],
    "leaders": _LEADER,
    "teams": [
        *_SEASON_CTX,
        c("team_process_id", "teamProcessId", INT),
        c("club_id", "teamId", INT),
        c("team_name", "teamName"),
        c("city", "teamCityName"),
        c("code", "teamCode"),
        c("season_id", "seasonId", INT),
        c("season_label", "season"),
    ],
    "team_player_stats": [
        *_SEASON_CTX,
        c("team_process_id", "_team_process_id", INT),
        c("person_id", "code", INT),
        c("jersey_number", "oyuncuNo"),
        c("first_name", "firstName"),
        c("last_name", "lastName"),
        c("games", "macSayisi", INT),
        c("seconds_played", "sureToplam", INT, _clock),
        c("points", "sayiToplam", INT),
        c("fg_made", "agBasarili", INT),
        c("fg_attempted", "agDeneme", INT),
        c("fg2_made", "puan20Toplam", INT),
        c("fg2_attempted", "puan21Toplam", INT),
        c("fg3_made", "puan30Toplam", INT),
        c("fg3_attempted", "puan31Toplam", INT),
        c("ft_made", "sA0Toplam", INT),
        c("ft_attempted", "sA1Toplam", INT),
        c("def_rebounds", "drToplam", INT),
        c("off_rebounds", "hrToplam", INT),
        c("rebounds", "trToplam", INT),
        c("assists", "asistToplam", INT),
        c("blocks", "blokToplam", INT),
        c("steals", "topCalmaToplam", INT),
        c("turnovers", "topKaybiToplam", INT),
        c("fouls", "faulToplam", INT),
        c("plus_minus", "oyunaKatki", F),
        c("efficiency", "vpToplam", INT),
        c("fg_pct", "agYuzdesi", F),
        c("fg2_pct", "ikilikYuzdesi", F),
        c("fg3_pct", "uclukYuzdesi", F),
        c("ft_pct", "serbestAtisYuzdesi", F),
        c("seconds_per_game", "avg.sureOrtalama", INT, _clock),
        c("points_per_game", "avg.sayiOrtalama", F),
        c("rebounds_per_game", "avg.trOrtalama", F),
        c("assists_per_game", "avg.asistOrtalama", F),
        c("steals_per_game", "avg.topCalmaOrtalama", F),
        c("blocks_per_game", "avg.blokOrtalama", F),
        c("turnovers_per_game", "avg.topKaybiOrtalama", F),
        c("efficiency_per_game", "avg.vpOrtalama", F),
    ],
    "team_games": [
        *_SEASON_CTX,
        c("team_process_id", "_team_process_id", INT),
        c("match_id", "matchId", INT),
        c("match_code", "matchCode"),
        c("tipoff", "tarih", DT),
        c("week", "formattedWeek"),
        c("city", "sehir"),
        c("venue", "salon"),
        c("home_team_process_id", "takimAId", INT),
        c("home_team", "takimA"),
        c("home_score", "skorA", INT),
        c("away_team_process_id", "takimBId", INT),
        c("away_team", "takimB"),
        c("away_score", "skorB", INT),
        c("team_is_home", "isHome", B),
        c("team_won", "isWin", B),
        c("is_played", "isPlayed", B),
        c("status", "durum"),
        c("status_id", "durumId", INT),
        c("has_overtime", "hasUzatma", B),
    ],
    "team_roster": [
        *_SEASON_CTX,
        c("team_process_id", "_team_process_id", INT),
        c("person_id", "personId", INT),
        c("first_name", "firstName"),
        c("last_name", "lastName"),
        c("nationality", "country"),
        c("height_cm", "height", INT),
        c("jersey_number", "jerseyNo"),
        c("games", "macSayisi", INT),
        c("seconds_played", "sure", INT, _clock),
        c("points_per_game", "sayi", F),
        c("efficiency_per_game", "verimlilikPuani", F),
    ],
    "team_staff": [
        *_SEASON_CTX,
        c("team_process_id", "_team_process_id", INT),
        c("person_id", "code", INT),
        c("first_name", "firstName"),
        c("last_name", "lastName"),
        c("role", "job"),
        c("role_code", "subRolesCode", INT),
    ],
}
#: The nine season-leader routes share one table.
LEADER_CATEGORIES = (
    "points",
    "assists",
    "blocks",
    "rebounds",
    "steals",
    "minutes",
    "threes",
    "turnovers",
    "efficiency",
)
SPECS: Dict[str, Spec] = {k: {col: dt for col, _p, dt, _f in v} for k, v in TABLES.items()}


def _frame(short: str, rows: List[Dict[str, Any]], columns: Optional[Sequence[str]], as_pandas: bool) -> Frame:
    """``rows`` projected through ``TABLES[short]`` into a frame with exactly the table's columns and dtypes."""
    table = TABLES[short]
    data: Dict[str, List[Any]] = {}
    for col, path, dtype, fn in table:
        vals = []
        for r in rows:
            v = _get(r, path)
            vals.append(fn(v) if fn is not None and v is not None else _CAST[dtype](v) if fn is None else None)
        data[col] = vals
    schema: Dict[str, Any] = {col: dt for col, _p, dt, _f in table}
    for extra in columns or ():
        if extra not in schema:
            schema[extra] = S
            data[extra] = [None] * len(rows)
    df = pl.DataFrame(data, schema=schema)
    return df.to_pandas() if as_pandas else df


def _req(raw: Any) -> Dict[str, Any]:
    r = raw.get(_REQUEST) if isinstance(raw, dict) else None
    return {"_" + k: v for k, v in r.items()} if isinstance(r, dict) else {}


def _data(raw: Any) -> Any:
    return raw.get("data") if isinstance(raw, dict) else None


def _with(ctx: Dict[str, Any], row: Dict[str, Any], **extra: Any) -> Dict[str, Any]:
    return {**row, **ctx, **extra}


# ------------------------------------------------------------------------------------------ row builders
def _rows_list(raw: Any) -> List[Dict[str, Any]]:
    ctx = _req(raw)
    return [_with(ctx, r) for r in _list(_data(raw))]


def _rows_daily(raw: Any) -> List[Dict[str, Any]]:
    ctx = _req(raw)
    return [_with(ctx, m, _group=g.get("groupName")) for g in _list(_data(raw)) for m in _list(g.get("matches"))]


def _rows_standings(raw: Any) -> List[Dict[str, Any]]:
    ctx = _req(raw)
    return [_with(ctx, s) for g in _list(_data(raw)) for s in _list(g.get("standings"))]


def _rows_one(raw: Any) -> List[Dict[str, Any]]:
    d = _data(raw)
    return [_with(_req(raw), d)] if isinstance(d, dict) and d else []


def _rows_game_players(raw: Any) -> List[Dict[str, Any]]:
    ctx = _req(raw)
    return [_with(ctx, p) for p in _list(_dict(_data(raw)).get("players"))]


def _rows_player_box(raw: Any) -> List[Dict[str, Any]]:
    ctx, d = _req(raw), _dict(_data(raw))
    rows = []
    for side in ("home", "away"):
        tp = ctx.get(f"_{side}_team_process_id")
        for block, starter in ((f"{side}TopFive", True), (f"{side}BenchPlayers", False)):
            rows += [_with(ctx, p, _side=side, _team_process_id=tp, _starter=starter) for p in _list(d.get(block))]
    return rows


def _rows_team_box(raw: Any) -> List[Dict[str, Any]]:
    ctx, d = _req(raw), _dict(_data(raw))
    rows = []
    for side in ("home", "away"):
        total = d.get(f"{side}TeamTotal")
        if isinstance(total, dict) and total:
            rows.append(
                _with(
                    ctx,
                    total,
                    _side=side,
                    _team_process_id=ctx.get(f"_{side}_team_process_id"),
                    _stats=_dict(d.get(f"{side}TeamStats")),
                )
            )
    return rows


def _rows_events(key: str) -> Callable[[Any], List[Dict[str, Any]]]:
    def rows(raw: Any) -> List[Dict[str, Any]]:
        ctx = _req(raw)
        return [_with(ctx, e) for e in _list(_dict(_data(raw)).get(key))]

    return rows


def _rows_period_scores(raw: Any) -> List[Dict[str, Any]]:
    ctx, d = _req(raw), _dict(_data(raw))
    return [_with(ctx, p, _side=side) for side in ("home", "away") for p in _list(d.get(f"{side}TeamScore"))]


def _rows_game_leaders(raw: Any) -> List[Dict[str, Any]]:
    ctx, rows = _req(raw), []
    for cat in _list(_dict(_data(raw)).get("categories")):
        for n, p in enumerate(_list(cat.get("leaders")), start=1):
            rows.append(
                _with(
                    ctx,
                    p,
                    _category=_s(cat.get("leaderCategory")),
                    _category_label=_s(cat.get("leaderCategoryDescription")),
                    _rank=n,
                )
            )
    return rows


def _rows_leaders(raw: Any) -> List[Dict[str, Any]]:
    ctx, d, rows = _req(raw), _dict(_data(raw)), []
    for key, kind in (("sum", "total"), ("avg", "average")):
        rows += [_with(ctx, r, _kind=kind, _rank=n) for n, r in enumerate(_list(d.get(key)), start=1)]
    return rows


def _rows_career(raw: Any) -> List[Dict[str, Any]]:
    ctx, d = _req(raw), _dict(_data(raw))
    prof = _dict(d.get("playerProfile"))
    extra = {"_name": prof.get("fullName"), "_country": prof.get("country"), "_height": prof.get("height")}
    return [_with(ctx, r, **extra) for r in _list(d.get("careerInfo"))]


def _rows_team_stats(raw: Any) -> List[Dict[str, Any]]:
    ctx, d = _req(raw), _dict(_data(raw))
    avg = {_i(r.get("code")): r for r in _list(d.get("playersAverage")) if _i(r.get("code")) is not None}
    return [_with(ctx, r, avg=avg.get(_i(r.get("code")), {})) for r in _list(d.get("playersTotal"))]


def _rows_team_games(raw: Any) -> List[Dict[str, Any]]:
    ctx = _req(raw)
    return [_with(ctx, r) for r in _list(_dict(_data(raw)).get("maclar"))]


ROWS: Dict[str, Callable[[Any], List[Dict[str, Any]]]] = {
    "seasons": _rows_list,
    "leagues": _rows_list,
    "league_seasons": _rows_list,
    "weeks": _rows_list,
    "standings": _rows_standings,
    "schedule": _rows_list,
    "daily_matches": _rows_daily,
    "game_header": _rows_one,
    "game_players": _rows_game_players,
    "player_box": _rows_player_box,
    "team_box": _rows_team_box,
    "pbp": _rows_events("events"),
    "shots": _rows_events("shotInfos"),
    "period_scores": _rows_period_scores,
    "highlights": _rows_events("matchHighlightStatistics"),
    "game_leaders": _rows_game_leaders,
    "players": _rows_list,
    "player_career": _rows_career,
    "leaders": _rows_leaders,
    "teams": _rows_list,
    "team_player_stats": _rows_team_stats,
    "team_games": _rows_team_games,
    "team_roster": _rows_list,
    "team_staff": _rows_list,
}
if set(ROWS) != set(TABLES):  # an explicit check (an ``assert`` vanishes under ``python -O``)
    raise RuntimeError(f"tbf_parsers: ROWS and TABLES disagree on {sorted(set(ROWS) ^ set(TABLES))}")


def parse(short: str, raw: Any, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """The ``short`` frame of an enveloped body; a typed zero-row frame for anything unrecognised.

    Args:
        short: the table name (a key of :data:`ROWS`).
        raw: the enveloped body a wrapper returns with ``return_parsed=False``.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        The frame, with the documented dtypes.

    Raises:
        KeyError: ``short`` is not a known table.
    """
    try:
        rows = ROWS[short](raw)
    except Exception:  # noqa: BLE001 -- a malformed body is a typed empty frame, never an exception
        rows = []
    return _frame(short, rows, columns, return_as_pandas)


# ------------------------------------------------------------------------------------------ public parsers
def parse_tbf_seasons(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """TBF seasons: the 20 most recent seasons, domestic and international (a server cap).

    Args:
        raw: the body ``tbf_seasons(..., return_parsed=False)`` returns (the API envelope plus ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per season, newest first. ``season`` is the END year of the label (2026-2027 -> 2027); ``international`` marks the odd ``UA-`` seasons. An empty or unrecognised body gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.tbf import tbf_seasons

            df = tbf_seasons()
    """
    return parse("seasons", raw, return_as_pandas, columns)


def parse_tbf_leagues(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """The featured leagues (league-season activities) of one TBF season.

    Args:
        raw: the body ``tbf_leagues(..., return_parsed=False)`` returns (the API envelope plus ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per league: BSL, TBL, KBSL, TKBL, TB2L and the youth leagues with their ``activity_id``. An empty or unrecognised body gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.tbf import tbf_leagues

            df = tbf_leagues(season=2027)
    """
    return parse("leagues", raw, return_as_pandas, columns)


def parse_tbf_league_seasons(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Every season of one TBF league (``bsl`` back to 1966-67), with its activity and season ids.

    Args:
        raw: the body ``tbf_league_seasons(..., return_parsed=False)`` returns (the API envelope plus ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per season of the league, newest first. An empty or unrecognised body gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.tbf import tbf_league_seasons

            df = tbf_league_seasons(competition='bsl')
    """
    return parse("league_seasons", raw, return_as_pandas, columns)


def parse_tbf_weeks(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """The weeks (rounds) of one league-season, playoff rounds included.

    Args:
        raw: the body ``tbf_weeks(..., return_parsed=False)`` returns (the API envelope plus ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per week in play order; ``week`` is the value the schedule and standings take. An empty or unrecognised body gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.tbf import tbf_weeks

            df = tbf_weeks(season=2027, competition='bsl')
    """
    return parse("weeks", raw, return_as_pandas, columns)


def parse_tbf_standings(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """League table of one league-season after a week (default the latest).

    Args:
        raw: the body ``tbf_standings(..., return_parsed=False)`` returns (the API envelope plus ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per team and group. Tie-break detail rows (``tiedRows``) are not included. An empty or unrecognised body gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.tbf import tbf_standings

            df = tbf_standings(season=2027, competition='bsl')
    """
    return parse("standings", raw, return_as_pandas, columns)


def parse_tbf_schedule(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Schedule and results of one league-season: every game of one week, or of every week.

    Args:
        raw: the body ``tbf_schedule(..., return_parsed=False)`` returns (the API envelope plus ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per game. Scores are null before the game; ``status_id`` 1 = played, 4 = not played, 19 = no date yet. An empty or unrecognised body gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.tbf import tbf_schedule

            df = tbf_schedule(season=2027, competition='bsl', week=1)
    """
    return parse("schedule", raw, return_as_pandas, columns)


def parse_tbf_daily_matches(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Every TBF game played on one date, across competitions or for one league-season.

    Args:
        raw: the body ``tbf_daily_matches(..., return_parsed=False)`` returns (the API envelope plus ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per game with its ``group_name`` (the competition group the app shows). An empty or unrecognised body gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.tbf import tbf_daily_matches

            df = tbf_daily_matches(match_date='2026-10-11')
    """
    return parse("daily_matches", raw, return_as_pandas, columns)


def parse_tbf_game_header(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Game header of a finished game: teams, coaches, score, officials, venue and the Genius id.

    Args:
        raw: the body ``tbf_game_header(..., return_parsed=False)`` returns (the API envelope plus ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row. An empty or unrecognised body gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.tbf import tbf_game_header

            df = tbf_game_header(match_id=346279)
    """
    return parse("game_header", raw, return_as_pandas, columns)


def parse_tbf_game_players(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """The game roster from the header: every listed player of both sides.

    Args:
        raw: the body ``tbf_game_players(..., return_parsed=False)`` returns (the API envelope plus ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per rostered player. The header carries no usable player id; join on name + shirt number + side. An empty or unrecognised body gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.tbf import tbf_game_players

            df = tbf_game_players(match_id=346279)
    """
    return parse("game_players", raw, return_as_pandas, columns)


def parse_tbf_player_box(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Player box score of a finished game (starters and bench, both sides) from the full statistics route.

    Args:
        raw: the body ``tbf_player_box(..., return_parsed=False)`` returns (the API envelope plus ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per player who is on the sheet; ``is_starter`` marks the starting five (none flagged in 2006-07). An empty or unrecognised body gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.tbf import tbf_player_box

            df = tbf_player_box(match_id=346279)
    """
    return parse("player_box", raw, return_as_pandas, columns)


def parse_tbf_team_box(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Team totals of a finished game, with the team-credited rebounds, turnovers and fouls.

    Args:
        raw: the body ``tbf_team_box(..., return_parsed=False)`` returns (the API envelope plus ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        Two rows: home, then away. Minutes are the players' summed seconds. An empty or unrecognised body gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.tbf import tbf_team_box

            df = tbf_team_box(match_id=346279)
    """
    return parse("team_box", raw, return_as_pandas, columns)


def parse_tbf_pbp(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Play-by-play of a finished game (2025-26 onward), in game order, with the running score.

    Args:
        raw: the body ``tbf_pbp(..., return_parsed=False)`` returns (the API envelope plus ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per event. Games before 2025-26 raise NoDataError. The feed has no player id. An empty or unrecognised body gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.tbf import tbf_pbp

            df = tbf_pbp(match_id=346279)
    """
    return parse("pbp", raw, return_as_pandas, columns)


def parse_tbf_shots(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Every field-goal attempt of a finished game (2025-26 onward) with its court position.

    Args:
        raw: the body ``tbf_shots(..., return_parsed=False)`` returns (the API envelope plus ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per attempt. ``x`` and ``y`` are percentages of a horizontal full court. An empty or unrecognised body gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.tbf import tbf_shots

            df = tbf_shots(match_id=346279)
    """
    return parse("shots", raw, return_as_pandas, columns)


def parse_tbf_period_scores(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Points each side scored in each period of a finished game.

    Args:
        raw: the body ``tbf_period_scores(..., return_parsed=False)`` returns (the API envelope plus ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per side and period (home first). An empty or unrecognised body gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.tbf import tbf_period_scores

            df = tbf_period_scores(match_id=346279)
    """
    return parse("period_scores", raw, return_as_pandas, columns)


def parse_tbf_highlights(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Team comparison of a finished game: one row per highlighted statistic.

    Args:
        raw: the body ``tbf_highlights(..., return_parsed=False)`` returns (the API envelope plus ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per statistic with the home and away value. An empty or unrecognised body gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.tbf import tbf_highlights

            df = tbf_highlights(match_id=346279)
    """
    return parse("highlights", raw, return_as_pandas, columns)


def parse_tbf_game_leaders(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Game leaders of a finished game per statistic category (top five each).

    Args:
        raw: the body ``tbf_game_leaders(..., return_parsed=False)`` returns (the API envelope plus ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per leader, best first within a category. An empty or unrecognised body gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.tbf import tbf_game_leaders

            df = tbf_game_leaders(match_id=346279)
    """
    return parse("game_leaders", raw, return_as_pandas, columns)


def parse_tbf_players(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Every player registered in one league-season.

    Args:
        raw: the body ``tbf_players(..., return_parsed=False)`` returns (the API envelope plus ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per registered player. Birth dates are not returned. An empty or unrecognised body gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.tbf import tbf_players

            df = tbf_players(season=2027, competition='bsl')
    """
    return parse("players", raw, return_as_pandas, columns)


def parse_tbf_player_career(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """One player's career: a row per season and competition.

    Args:
        raw: the body ``tbf_player_career(..., return_parsed=False)`` returns (the API envelope plus ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per career season and competition. Totals and averages rows are not included. An empty or unrecognised body gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.tbf import tbf_player_career

            df = tbf_player_career(player_id=196413)
    """
    return parse("player_career", raw, return_as_pandas, columns)


def parse_tbf_leaders(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Season leaders of one statistic: the server's top 20 by season total and by per-game average.

    Args:
        raw: the body ``tbf_leaders_points(..., return_parsed=False)`` returns (the API envelope plus ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        Up to 20 rows per ``kind`` (``total`` and ``average``), best first. An empty or unrecognised body gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.tbf import tbf_leaders_points

            df = tbf_leaders_points(season=2027)
    """
    return parse("leaders", raw, return_as_pandas, columns)


def parse_tbf_teams(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Every team registered in one league-season.

    Args:
        raw: the body ``tbf_teams(..., return_parsed=False)`` returns (the API envelope plus ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per team registration. An empty or unrecognised body gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.tbf import tbf_teams

            df = tbf_teams(season=2027, competition='bsl')
    """
    return parse("teams", raw, return_as_pandas, columns)


def parse_tbf_team_player_stats(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Season player statistics of one team-season: totals and per-game averages.

    Args:
        raw: the body ``tbf_team_player_stats(..., return_parsed=False)`` returns (the API envelope plus ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per player. An empty or unrecognised body gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.tbf import tbf_team_player_stats

            df = tbf_team_player_stats(season=2027, team_process_id=265995)
    """
    return parse("team_player_stats", raw, return_as_pandas, columns)


def parse_tbf_team_games(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Games and results of one team-season.

    Args:
        raw: the body ``tbf_team_games(..., return_parsed=False)`` returns (the API envelope plus ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per game of the team (scheduled games included). An empty or unrecognised body gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.tbf import tbf_team_games

            df = tbf_team_games(season=2027, team_process_id=265995)
    """
    return parse("team_games", raw, return_as_pandas, columns)


def parse_tbf_team_roster(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Roster of one team-season.

    Args:
        raw: the body ``tbf_team_roster(..., return_parsed=False)`` returns (the API envelope plus ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per rostered person. Birth dates are not returned. An empty or unrecognised body gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.tbf import tbf_team_roster

            df = tbf_team_roster(season=2027, team_process_id=265995)
    """
    return parse("team_roster", raw, return_as_pandas, columns)


def parse_tbf_team_staff(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Coaching and club staff of one team-season.

    Args:
        raw: the body ``tbf_team_staff(..., return_parsed=False)`` returns (the API envelope plus ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per staff member. An empty or unrecognised body gives a zero-row frame with the documented columns and dtypes.

    Raises:
        None: bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.tbf import tbf_team_staff

            df = tbf_team_staff(season=2027, team_process_id=265995)
    """
    return parse("team_staff", raw, return_as_pandas, columns)
