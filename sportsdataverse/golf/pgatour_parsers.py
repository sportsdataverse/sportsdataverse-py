"""Tidy parsers for the PGA Tour GraphQL API (``pgatour_*`` wrappers).

Each parser takes the operation's ``data`` value as the wrapper's getter returns it, with a
compressed ``payload`` already decoded, and returns a polars frame. Pass
``return_as_pandas=True`` for pandas instead. An empty or malformed answer returns a zero-row
frame that still carries the parser's documented schema. Tournament, player and stat ids are
strings (``"R2026014"``, ``"28237"``, ``"02675"``).
"""

from __future__ import annotations

import math
import re
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import polars as pl

__all__ = [
    "parse_pgatour_course_stats",
    "parse_pgatour_field",
    "parse_pgatour_hole_by_hole",
    "parse_pgatour_hole_details",
    "parse_pgatour_leaderboard",
    "parse_pgatour_leaderboard_stats",
    "parse_pgatour_past_champions",
    "parse_pgatour_past_results",
    "parse_pgatour_player",
    "parse_pgatour_player_directory",
    "parse_pgatour_player_results",
    "parse_pgatour_schedule",
    "parse_pgatour_schedule_years",
    "parse_pgatour_scorecard",
    "parse_pgatour_scorecard_stats",
    "parse_pgatour_shot_details",
    "parse_pgatour_stat_catalog",
    "parse_pgatour_stat_details",
    "parse_pgatour_tee_times",
    "parse_pgatour_tour_cup",
    "parse_pgatour_tournaments",
]

S, INT, F, B = pl.String, pl.Int64, pl.Float64, pl.Boolean
UTC_DT = pl.Datetime("ms", "UTC")

SCHEDULE_SCHEMA = {
    "season": INT, "schedule_status": S, "tournament_id": S, "tournament_name": S, "date": S, "start_date": UTC_DT,
    "sequence_number": INT, "tournament_status": S, "course_name": S, "city": S, "state": S, "state_code": S,
    "country": S, "country_code": S, "purse": S, "champion": S, "champion_id": S, "champion_earnings": S,
    "standing_heading": S, "standing_value": S,
}  # fmt: skip
LEADERBOARD_SCHEMA = {
    "tournament_id": S, "tournament_status": S, "sort_order": INT, "player_id": S, "first_name": S,
    "last_name": S, "display_name": S, "short_name": S, "country": S, "amateur": B, "position": S,
    "total": S, "to_par": INT, "total_strokes": INT, "thru": S, "today": S, "current_round": INT, "player_state": S,
    **{f"round_{i}": INT for i in range(1, 5)},
    "fedexcup_official": S, "fedexcup_projected": S,
}  # fmt: skip
SCORECARD_SCHEMA = {
    "tournament_id": S, "player_id": S, "round_number": INT, "course_id": S, "course_name": S, "hole_number": INT,
    "par": INT, "yardage": INT, "score": INT, "score_status": S, "round_to_par": S,
}  # fmt: skip
TEE_TIMES_SCHEMA = {
    "tournament_id": S, "round_number": INT, "round_status": S, "group_number": INT, "tee_time": UTC_DT,
    "start_tee": INT, "back_nine": B, "course_id": S, "course_name": S, "player_id": S, "display_name": S,
    "country": S, "amateur": B,
}  # fmt: skip
PLAYER_SCHEMA = {
    "player_id": S, "first_name": S, "last_name": S, "display_name": S, "country": S, "country_code": S,
    "owgr": S, "height": S, "height_meters": S, "weight": S, "weight_kilograms": S, "age": S, "born": S,
    "birthplace_city": S, "birthplace_state": S, "birthplace_country": S, "turned_pro": S, "school": S,
    "residence_city": S, "residence_state": S, "residence_country": S, "family": S, "bio_link": S,
}  # fmt: skip
PLAYER_DIRECTORY_SCHEMA = {
    "tour_code": S, "player_id": S, "is_active": B, "first_name": S, "last_name": S, "short_name": S,
    "display_name": S, "country": S, "country_code": S, "age": S, "education": S, "turned_pro": S,
}  # fmt: skip
STAT_CATALOG_SCHEMA = {
    "tour_code": S, "year": INT, "category": S, "category_name": S, "sub_category": S, "stat_id": S, "stat_title": S,
}  # fmt: skip
STAT_DETAILS_SCHEMA = {
    "tour_code": S, "year": INT, "stat_id": S, "stat_title": S, "stat_type": S, "tour_average": S, "player_id": S,
    "player_name": S, "country": S, "country_code": S, "rank": INT, "rank_change": S, "stat_name": S,
    "stat_value": S,
}  # fmt: skip
TOUR_CUP_SCHEMA = {
    "cup_id": S, "title": S, "season": INT, "player_id": S, "first_name": S, "last_name": S, "display_name": S,
    "country": S, "country_code": S, "rank_official": S, "rank_projected": S, "rank_movement": S,
    "points_official": S, "points_projected": S, "points_event": S,
}  # fmt: skip
SHOT_DETAILS_SCHEMA = {
    "tournament_id": S, "player_id": S, "round_number": INT, "hole_number": INT, "par": INT, "yardage": INT,
    "hole_score": INT, "hole_status": S, "stroke_number": INT, "stroke_type": S, "play_by_play": S, "distance": S,
    "distance_remaining": S, "from_location_code": S, "from_location": S, "to_location_code": S,
    "to_location": S, "final_stroke": B, "from_x": F, "from_y": F, "from_z": F, "to_x": F, "to_y": F, "to_z": F,
}  # fmt: skip
SCHEDULE_YEARS_SCHEMA = {"season": INT, "display_season": S, "is_default": B}  # fmt: skip
TOURNAMENTS_SCHEMA = {
    "tournament_id": S, "tournament_name": S, "season": INT, "display_date": S, "tournament_status": S,
    "round_status": S, "round_display": S, "current_round": INT, "format_type": S, "scored_level": S, "city": S,
    "state": S, "country": S, "timezone": S, "location": S,
}  # fmt: skip
PAST_CHAMPIONS_SCHEMA = {
    "format_type": S, "season": INT, "display_season": S, "player_id": S, "display_name": S, "country_code": S,
    "total": INT, "to_par": S,
}  # fmt: skip
HOLE_BY_HOLE_SCHEMA = {
    "tournament_id": S, "player_id": S, "course_id": S, "course_code": S, "hole_number": INT, "sequence_number": INT,
    "par": INT, "yardage": INT, "score": INT, "score_status": S, "round_to_par": S, "front_nine": INT,
    "back_nine": INT, "round_total": INT, "round_total_to_par": S,
}  # fmt: skip
LEADERBOARD_STATS_SCHEMA = {
    "tournament_id": S, "stats_type": S, "player_id": S, "stat_id": S, "stat_title": S, "stat_value": S, "rank": S,
    "sort_value": F,
}  # fmt: skip
FIELD_SCHEMA = {
    "tournament_id": S, "player_id": S, "first_name": S, "last_name": S, "display_name": S, "short_name": S,
    "country": S, "country_code": S, "amateur": B, "alternate": B, "withdrawn": B, "status": S, "owgr": S,
    "qualifier": S, "ranking_points": S,
}  # fmt: skip
PAST_RESULTS_SCHEMA = {
    "tournament_id": S, "season": INT, "position": S, "player_id": S, "first_name": S, "last_name": S,
    "display_name": S, "short_name": S, "country": S, "country_code": S, "amateur": B, "total": INT, "to_par": S,
    **{f"round_{i}": INT for i in range(1, 5)},
}  # fmt: skip
COURSE_STATS_SCHEMA = {
    "tournament_id": S, "course_id": S, "course_name": S, "course_code": S, "course_par": INT, "course_yardage": INT,
    "host_course": B, "round_number": INT, "round_header": S, "hole_number": INT, "par": INT, "yards": INT,
    "scoring_average": F, "scoring_average_diff": F, "difficulty_rank": INT, "eagles": INT, "birdies": INT,
    "pars": INT, "bogeys": INT, "double_bogeys": INT,
}  # fmt: skip
HOLE_DETAILS_SCHEMA = {
    "tournament_id": S, "course_id": S, "hole_number": INT, "par": INT, "yards": INT, "difficulty_rank": INT,
    "rounds_played": INT, "scoring_average_diff": F, "eagles": INT, "eagles_pct": S, "birdies": INT,
    "birdies_pct": S, "pars": INT, "pars_pct": S, "bogeys": INT, "bogeys_pct": S, "double_bogeys": INT,
    "double_bogeys_pct": S,
}  # fmt: skip
SCORECARD_STATS_SCHEMA = {
    "tournament_id": S, "round": S, "round_name": S, "round_status": S, "stat_group": S, "stat_id": S,
    "stat_name": S, "stat_value": S, "rank": S, "year_to_date": S,
}  # fmt: skip
PLAYER_RESULTS_SCHEMA = {
    "player_id": S, "tournament_num": S, "tournament_id": S, "tournament_name": S, "season": INT, "start_date": S,
    "course_name": S, "position": S, "to_par": S, "total": INT, "points": S, "points_rank": S, "winnings": S,
    **{f"round_{i}": INT for i in range(1, 5)},
}  # fmt: skip


def _d(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _l(value: Any) -> list[Any]:
    return [v for v in value if isinstance(v, dict)] if isinstance(value, list) else []


def _payload(raw: Any) -> dict[str, Any]:
    """The decoded body of a compressed answer (``{"id", "payload"}``), else ``raw`` itself."""
    raw = _d(raw)
    return _d(raw["payload"]) if "payload" in raw else raw


_INT64_MIN, _INT64_MAX = -(2**63), 2**63 - 1


def _year(value: Any) -> int | None:
    """The first four-digit year in ``value`` (``"2025 Standings"`` -> 2025, ``"R-02671-2025"`` -> 2025)."""
    found = re.search(r"(?<!\d)(\d{4})(?!\d)", value) if isinstance(value, str) else None
    return int(found.group(1)) if found else _int(value)


def _int(value: Any) -> int | None:
    """``"276"`` / ``276`` / ``276.0`` -> 276; ``"-"``, ``"E"``, ``""``, ``2.5``, None, or outside Int64 -> None."""
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, float):
        value = int(value) if math.isfinite(value) and value.is_integer() else None
    elif not isinstance(value, int):
        try:
            value = int(str(value).replace(",", "").strip())
        except ValueError:
            return None
    return value if value is not None and _INT64_MIN <= value <= _INT64_MAX else None


def _str(value: Any) -> str | None:
    return None if value is None or isinstance(value, (dict, list)) else str(value)


def _epoch_ms(value: Any) -> datetime | None:
    ms = _int(value)
    if ms is None:
        return None
    try:
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    except (OverflowError, OSError, ValueError):  # out of the platform's range (Windows rejects negatives)
        return None


def _cell(value: Any, dtype: Any) -> Any:
    """``value`` coerced to ``dtype``'s Python type, or None: a malformed field never raises."""
    if dtype == pl.String:
        return _str(value)
    if dtype == pl.Int64:
        return _int(value)
    if dtype == pl.Boolean:
        return value if isinstance(value, bool) else None
    if dtype == pl.Float64:
        ok = isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)
        return float(value) if ok else None
    return value if isinstance(value, datetime) else None


def _frame(rows: list[dict[str, Any]], schema: dict[str, Any], return_as_pandas: bool) -> pl.DataFrame | pd.DataFrame:
    clean = [{k: _cell(r.get(k), t) for k, t in schema.items()} for r in rows]
    df = pl.DataFrame(clean, schema=schema) if clean else pl.DataFrame(schema=schema)
    return df.to_pandas() if return_as_pandas else df


def parse_pgatour_schedule(raw: Any, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """One row per tournament on a tour's season schedule, completed events first.

    Args:
        raw: The ``schedule`` answer (``pgatour_schedule(..., return_parsed=False)``).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A frame with :data:`SCHEDULE_SCHEMA` columns.

    Example:
        Parse a raw answer::

            from sportsdataverse.golf import parse_pgatour_schedule, pgatour_schedule
            parse_pgatour_schedule(pgatour_schedule(tour_code="R", year=2026, return_parsed=False))
    """
    raw = _d(raw)
    rows = []
    for status in ("completed", "upcoming"):
        for month in _l(raw.get(status)):
            rows.extend(
                {
                    "season": _int(raw.get("seasonYear")), "schedule_status": status,
                    "tournament_id": _str(t.get("id")), "tournament_name": t.get("tournamentName"),
                    "date": t.get("date"), "start_date": _epoch_ms(t.get("startDate")),
                    "sequence_number": _int(t.get("sequenceNumber")), "tournament_status": t.get("tournamentStatus"),
                    "course_name": t.get("courseName"), "city": t.get("city"), "state": t.get("state"),
                    "state_code": t.get("stateCode"), "country": t.get("country"), "country_code": t.get("countryCode"),
                    "purse": t.get("purse"), "champion": t.get("champion"), "champion_id": _str(t.get("championId")),
                    "champion_earnings": t.get("championEarnings"), "standing_heading": t.get("tourStandingHeading"),
                    "standing_value": t.get("tourStandingValue"),
                }
                for t in _l(month.get("tournaments"))
            )  # fmt: skip
    return _frame(rows, SCHEDULE_SCHEMA, return_as_pandas)


def _leaderboard_row(body: dict[str, Any], row: dict[str, Any]) -> dict[str, Any]:
    player, scoring = _d(row.get("player")), _d(row.get("scoringData"))
    rounds = scoring.get("rounds") if isinstance(scoring.get("rounds"), list) else []
    return {
        "tournament_id": _str(body.get("tournamentId") or body.get("id")),
        "tournament_status": body.get("tournamentStatus"), "sort_order": _int(row.get("leaderboardSortOrder")),
        "player_id": _str(player.get("id") or row.get("id")), "first_name": player.get("firstName"),
        "last_name": player.get("lastName"), "display_name": player.get("displayName"),
        "short_name": player.get("shortName"), "country": player.get("country"), "amateur": player.get("amateur"),
        "position": scoring.get("position"), "total": scoring.get("total"),
        "to_par": _int(scoring.get("totalSort")) if scoring.get("total") not in (None, "-") else None,
        "total_strokes": _int(scoring.get("totalStrokes")), "thru": scoring.get("thru"),
        "today": scoring.get("score"), "current_round": _int(scoring.get("currentRound")),
        "player_state": scoring.get("playerState"),
        **{f"round_{i}": _int(rounds[i - 1]) if i <= len(rounds) else None for i in range(1, 5)},
        "fedexcup_official": _str(scoring.get("official")), "fedexcup_projected": _str(scoring.get("projected")),
    }  # fmt: skip


def parse_pgatour_leaderboard(raw: Any, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """One row per player on a tournament leaderboard, in leaderboard order.

    Information rows (the cut-line banner) are dropped. ``round_<n>`` is null for a round
    not played; ``to_par`` is the integer total (``"E"`` is 0).

    Args:
        raw: The ``leaderboardCompressedV3`` answer, payload decoded.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A frame with :data:`LEADERBOARD_SCHEMA` columns.

    Example:
        Parse a raw answer::

            from sportsdataverse.golf import parse_pgatour_leaderboard, pgatour_leaderboard
            parse_pgatour_leaderboard(pgatour_leaderboard(tournament_id="R2026014", return_parsed=False))
    """
    body = _payload(raw)
    rows = [
        _leaderboard_row(body, r)
        for r in _l(body.get("players"))
        if r.get("__typename", "PlayerRowV3") == "PlayerRowV3"
    ]
    return _frame(rows, LEADERBOARD_SCHEMA, return_as_pandas)


def parse_pgatour_scorecard(raw: Any, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """One row per hole played: a player's scorecard for every round of one tournament.

    Args:
        raw: The ``scorecardCompressedV3`` answer, payload decoded.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A frame with :data:`SCORECARD_SCHEMA` columns; ``score`` is null on a hole not yet played.

    Example:
        Parse a raw answer::

            from sportsdataverse.golf import parse_pgatour_scorecard, pgatour_scorecard
            parse_pgatour_scorecard(pgatour_scorecard(tournament_id="R2026014", player_id="28237", return_parsed=False))
    """
    body = _payload(raw)
    player_id = _str(_d(body.get("player")).get("id"))
    # the answer's id is "<tournament id>-<player id>" (R2026014-28237); keep the tournament part
    tournament_id = _str(body.get("id"))
    if tournament_id and player_id and tournament_id.endswith(f"-{player_id}"):
        tournament_id = tournament_id[: -len(player_id) - 1]
    rows = []
    for rnd in _l(body.get("roundScores")):
        for nine in ("firstNine", "secondNine"):
            rows.extend(
                {
                    "tournament_id": tournament_id, "player_id": player_id, "round_number": _int(rnd.get("roundNumber")),
                    "course_id": _str(rnd.get("courseId")), "course_name": rnd.get("courseName"),
                    "hole_number": _int(hole.get("holeNumber")), "par": _int(hole.get("par")),
                    "yardage": _int(hole.get("yardage")), "score": _int(hole.get("score")),
                    "score_status": hole.get("status"), "round_to_par": _str(hole.get("roundScore")),
                }
                for hole in _l(_d(rnd.get(nine)).get("holes"))
            )  # fmt: skip
    return _frame(rows, SCORECARD_SCHEMA, return_as_pandas)


def parse_pgatour_tee_times(raw: Any, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """One row per player per tee-time group, for every round of a tournament.

    Args:
        raw: The ``teeTimesCompressedV2`` answer, payload decoded.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A frame with :data:`TEE_TIMES_SCHEMA` columns; ``tee_time`` is UTC.

    Example:
        Parse a raw answer::

            from sportsdataverse.golf import parse_pgatour_tee_times, pgatour_tee_times
            parse_pgatour_tee_times(pgatour_tee_times(tournament_id="R2026014", return_parsed=False))
    """
    body = _payload(raw)
    rows = []
    for rnd in _l(body.get("rounds")):
        for group in _l(rnd.get("groups")):
            rows.extend(
                {
                    "tournament_id": _str(body.get("id")), "round_number": _int(rnd.get("roundInt")),
                    "round_status": rnd.get("roundStatus"), "group_number": _int(group.get("groupNumber")),
                    "tee_time": _epoch_ms(group.get("teeTime")), "start_tee": _int(group.get("startTee")),
                    "back_nine": group.get("backNine"), "course_id": _str(group.get("courseId")),
                    "course_name": group.get("courseName"), "player_id": _str(p.get("id")),
                    "display_name": p.get("displayName"), "country": p.get("country"), "amateur": p.get("amateur"),
                }
                for p in _l(group.get("players"))
            )  # fmt: skip
    return _frame(rows, TEE_TIMES_SCHEMA, return_as_pandas)


def parse_pgatour_player(raw: Any, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """One row: a player's biography (height, weight, birth, turned pro, school, residence).

    Args:
        raw: The ``player`` answer.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A frame with :data:`PLAYER_SCHEMA` columns; zero rows when ``raw`` carries no player id.

    Example:
        Parse a raw answer::

            from sportsdataverse.golf import parse_pgatour_player, pgatour_player
            parse_pgatour_player(pgatour_player(player_id="28237", return_parsed=False))
    """
    p = _d(raw)
    bio, born, home = (
        _d(p.get("playerBio")),
        _d(_d(p.get("playerBio")).get("birthplace")),
        _d(_d(p.get("playerBio")).get("residence")),
    )
    rows = [] if p.get("id") is None else [{
        "player_id": _str(p.get("id")), "first_name": p.get("firstName"), "last_name": p.get("lastName"),
        "display_name": p.get("displayName"), "country": p.get("country"), "country_code": p.get("countryFlag"),
        "owgr": _str(p.get("owgr")), "height": bio.get("heightImperial"), "height_meters": _str(bio.get("heightMeters")),
        "weight": _str(bio.get("weightImperial")), "weight_kilograms": _str(bio.get("weightKilograms")),
        "age": _str(bio.get("age")), "born": bio.get("born"), "birthplace_city": born.get("city"),
        "birthplace_state": born.get("state"), "birthplace_country": born.get("country"),
        "turned_pro": _str(bio.get("turnedPro")), "school": bio.get("school"), "residence_city": home.get("city"),
        "residence_state": home.get("state"), "residence_country": home.get("country"), "family": bio.get("family"),
        "bio_link": p.get("bioLink"),
    }]  # fmt: skip
    return _frame(rows, PLAYER_SCHEMA, return_as_pandas)


def parse_pgatour_player_directory(raw: Any, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """One row per player in a tour's player directory.

    Args:
        raw: The ``playerDirectory`` answer.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A frame with :data:`PLAYER_DIRECTORY_SCHEMA` columns.

    Example:
        Parse a raw answer::

            from sportsdataverse.golf import parse_pgatour_player_directory, pgatour_player_directory
            parse_pgatour_player_directory(pgatour_player_directory(tour_code="R", return_parsed=False))
    """
    raw = _d(raw)
    rows = [
        {
            "tour_code": raw.get("tourCode"), "player_id": _str(p.get("id")), "is_active": p.get("isActive"),
            "first_name": p.get("firstName"), "last_name": p.get("lastName"), "short_name": p.get("shortName"),
            "display_name": p.get("displayName"), "country": p.get("country"), "country_code": p.get("countryFlag"),
            "age": _str(_d(p.get("playerBio")).get("age")), "education": _d(p.get("playerBio")).get("education"),
            "turned_pro": _str(_d(p.get("playerBio")).get("turnedPro")),
        }
        for p in _l(raw.get("players"))
    ]  # fmt: skip
    return _frame(rows, PLAYER_DIRECTORY_SCHEMA, return_as_pandas)


def parse_pgatour_stat_catalog(raw: Any, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """One row per stat a tour publishes: category, sub-category, stat id and title.

    The ``stat_id`` values are what ``pgatour_stat_details(stat_id=...)`` takes.

    Args:
        raw: The ``statOverview`` answer.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A frame with :data:`STAT_CATALOG_SCHEMA` columns.

    Example:
        Parse a raw answer::

            from sportsdataverse.golf import parse_pgatour_stat_catalog, pgatour_stat_catalog
            parse_pgatour_stat_catalog(pgatour_stat_catalog(tour_code="R", year=2026, return_parsed=False))
    """
    raw = _d(raw)
    rows = [
        {
            "tour_code": raw.get("tourCode"), "year": _int(raw.get("year")), "category": cat.get("category"),
            "category_name": cat.get("displayName"), "sub_category": sub.get("displayName"),
            "stat_id": _str(stat.get("statId")), "stat_title": stat.get("statTitle"),
        }
        for cat in _l(raw.get("categories"))
        for sub in _l(cat.get("subCategories"))
        for stat in _l(sub.get("stats"))
    ]  # fmt: skip
    return _frame(rows, STAT_CATALOG_SCHEMA, return_as_pandas)


def parse_pgatour_stat_details(raw: Any, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """One row per player per stat column of one stat's season table (long form).

    Each stat has its own columns (scoring average: ``Avg``, ``Total Strokes``, ...), so the
    table is returned long: ``stat_name`` is the column, ``stat_value`` its value as displayed.
    The tour-average row is dropped (it is ``tour_average``).

    Args:
        raw: The ``statDetails`` answer.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A frame with :data:`STAT_DETAILS_SCHEMA` columns.

    Example:
        Parse a raw answer::

            from sportsdataverse.golf import parse_pgatour_stat_details, pgatour_stat_details
            parse_pgatour_stat_details(pgatour_stat_details(stat_id="120", year=2026, return_parsed=False))
    """
    raw = _d(raw)
    head = {
        "tour_code": raw.get("tourCode"), "year": _int(raw.get("year")), "stat_id": _str(raw.get("statId")),
        "stat_title": raw.get("statTitle"), "stat_type": raw.get("statType"), "tour_average": _str(raw.get("tourAvg")),
    }  # fmt: skip
    rows = [
        {
            **head, "player_id": _str(r.get("playerId")), "player_name": r.get("playerName"),
            "country": r.get("country"), "country_code": r.get("countryFlag"), "rank": _int(r.get("rank")),
            "rank_change": _str(r.get("rankDiff")), "stat_name": s.get("statName"), "stat_value": _str(s.get("statValue")),
        }
        for r in _l(raw.get("rows"))
        if r.get("playerId") is not None
        for s in _l(r.get("stats"))
    ]  # fmt: skip
    return _frame(rows, STAT_DETAILS_SCHEMA, return_as_pandas)


def parse_pgatour_tour_cup(raw: Any, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """One row per player in a season-long points race (FedExCup by default).

    Rank and points are as displayed (``"1"``, ``"T5"``, ``"4,806.123"``); ``-`` means none.

    Args:
        raw: The ``tourCupCombined`` answer.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A frame with :data:`TOUR_CUP_SCHEMA` columns.

    Example:
        Parse a raw answer::

            from sportsdataverse.golf import parse_pgatour_tour_cup, pgatour_tour_cup
            parse_pgatour_tour_cup(pgatour_tour_cup(year=2025, return_parsed=False))
    """
    raw = _d(raw)
    rows = []
    for p in _l(raw.get("players")):
        if p.get("__typename", "TourCupCombinedPlayer") != "TourCupCombinedPlayer":
            continue
        rank, points = _d(p.get("rankingData")), _d(p.get("pointData"))
        rows.append({
            "cup_id": _str(raw.get("id")), "title": raw.get("title"), "season": _year(raw.get("season")) or _year(raw.get("id")),
            "player_id": _str(p.get("id")), "first_name": p.get("firstName"), "last_name": p.get("lastName"),
            "display_name": p.get("displayName"), "country": p.get("country"), "country_code": p.get("countryFlag"),
            "rank_official": _str(rank.get("official")), "rank_projected": _str(rank.get("projected")),
            "rank_movement": rank.get("movement"), "points_official": _str(points.get("official")),
            "points_projected": _str(points.get("projected")), "points_event": _str(points.get("event")),
        })  # fmt: skip
    return _frame(rows, TOUR_CUP_SCHEMA, return_as_pandas)


def _coords(stroke: dict[str, Any]) -> dict[str, float | None]:
    """ShotLink ``tourcastX/Y/Z`` coordinates of a stroke's start and end."""
    lr = _d(_d(stroke.get("overview")).get("leftToRightCoords"))
    out: dict[str, float | None] = {}
    for end, key in (("from", "fromCoords"), ("to", "toCoords")):
        c = _d(lr.get(key))
        for axis in ("x", "y", "z"):
            v = c.get(f"tourcast{axis.upper()}")
            out[f"{end}_{axis}"] = float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else None
    return out


def parse_pgatour_shot_details(raw: Any, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """One row per stroke of one player's round (ShotLink), with start and end coordinates.

    An event without ShotLink tracking (the Masters, say) returns its holes with no strokes,
    which is a zero-row frame. Coordinates are ShotLink's course-relative ``tourcast`` values.

    Args:
        raw: The ``shotDetailsV4Compressed`` answer, payload decoded.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A frame with :data:`SHOT_DETAILS_SCHEMA` columns.

    Example:
        Parse a raw answer::

            from sportsdataverse.golf import parse_pgatour_shot_details, pgatour_shot_details
            parse_pgatour_shot_details(pgatour_shot_details(tournament_id="R2026006", player_id="59095", round=4, return_parsed=False))
    """
    body = _payload(raw)
    head = {
        "tournament_id": _str(body.get("tournamentId")),
        "player_id": _str(body.get("playerId")),
        "round_number": _int(body.get("round")),
    }
    rows = []
    for hole in _l(body.get("holes")):
        rows.extend(
            {
                **head, "hole_number": _int(hole.get("holeNumber")), "par": _int(hole.get("par")),
                "yardage": _int(hole.get("yardage")), "hole_score": _int(hole.get("score")),
                "hole_status": hole.get("status"), "stroke_number": _int(st.get("strokeNumber")),
                "stroke_type": st.get("strokeType"), "play_by_play": st.get("playByPlay"),
                "distance": st.get("distance"), "distance_remaining": st.get("distanceRemaining"),
                "from_location_code": st.get("fromLocationCode"), "from_location": st.get("fromLocation"),
                "to_location_code": st.get("toLocationCode"), "to_location": st.get("toLocation"),
                "final_stroke": st.get("finalStroke"), **_coords(st),
            }
            for st in _l(hole.get("strokes"))
        )  # fmt: skip
    return _frame(rows, SHOT_DETAILS_SCHEMA, return_as_pandas)


def _num(value: Any) -> float | None:
    """``"4.036"`` / ``"+0.152"`` / ``0.2`` -> float; ``"-"``, ``""``, None -> None."""
    if isinstance(value, bool) or value is None:
        return None
    try:
        out = float(str(value).replace(",", "").strip())
    except ValueError:
        return None
    return out if math.isfinite(out) else None


def _tournament(value: Any) -> str | None:
    """The tournament id at the head of an answer id: ``"R2026014-probability"`` / ``"R2026014-28237"`` -> ``"R2026014"``."""
    text = _str(value)
    return text.split("-", 1)[0] if text else None


def _rounds(scores: Any, key: str, number: str | None = None) -> dict[str, int | None]:
    """``round_1`` .. ``round_4`` from a list of per-round dicts (strokes under ``key``).

    With ``number``, each dict says its own round (``roundNum``); otherwise the list order is the round order.
    """
    scores = _l(scores)
    if number is not None:
        by = {_int(r.get(number)): r for r in scores}
        return {f"round_{i}": _int(by[i].get(key)) if i in by else None for i in range(1, 5)}
    return {f"round_{i}": _int(scores[i - 1].get(key)) if i <= len(scores) else None for i in range(1, 5)}


def parse_pgatour_schedule_years(raw: Any, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """One row per season with a published schedule for a tour, newest first.

    Args:
        raw: The ``scheduleYears`` answer (``pgatour_schedule_years(..., return_parsed=False)``).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A frame with :data:`SCHEDULE_YEARS_SCHEMA` columns; ``season`` is the year ``pgatour_schedule``
        takes (``2023`` for the split ``"2022-2023"`` season), ``is_default`` marks the current one.

    Example:
        Parse a raw answer::

            from sportsdataverse.golf import parse_pgatour_schedule_years, pgatour_schedule_years
            parse_pgatour_schedule_years(pgatour_schedule_years(tour_code="R", return_parsed=False))
    """
    rows = [
        {"season": _int(y.get("queryValue")), "display_season": _str(y.get("displayValue")), "is_default": y.get("default")}
        for y in _l(_d(raw).get("years"))
    ]  # fmt: skip
    return _frame(rows, SCHEDULE_YEARS_SCHEMA, return_as_pandas)


def parse_pgatour_tournaments(raw: Any, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """One row per requested tournament: dates, status, current round, format and location.

    Args:
        raw: The ``tournaments`` answer (a list, one entry per id asked for).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A frame with :data:`TOURNAMENTS_SCHEMA` columns.

    Example:
        Parse a raw answer::

            from sportsdataverse.golf import parse_pgatour_tournaments, pgatour_tournaments
            parse_pgatour_tournaments(pgatour_tournaments(tournament_ids="R2026014", return_parsed=False))
    """
    rows = [
        {
            "tournament_id": _str(t.get("id")), "tournament_name": t.get("tournamentName"),
            "season": _int(t.get("seasonYear")), "display_date": t.get("displayDate"),
            "tournament_status": t.get("tournamentStatus"), "round_status": t.get("roundStatus"),
            "round_display": t.get("roundDisplay"), "current_round": _int(t.get("currentRound")),
            "format_type": t.get("formatType"), "scored_level": t.get("scoredLevel"), "city": t.get("city"),
            "state": t.get("state"), "country": t.get("country"), "timezone": t.get("timezone"),
            "location": t.get("tournamentLocation"),
        }
        for t in _l(raw)
    ]  # fmt: skip
    return _frame(rows, TOURNAMENTS_SCHEMA, return_as_pandas)


def parse_pgatour_past_champions(raw: Any, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """One row per past champion of a tournament, newest edition first.

    The answer does not repeat the tournament id; it is the one the wrapper was given.

    Args:
        raw: The ``tournamentOverview`` answer.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A frame with :data:`PAST_CHAMPIONS_SCHEMA` columns; ``to_par`` is as displayed (``"-11"``).

    Example:
        Parse a raw answer::

            from sportsdataverse.golf import parse_pgatour_past_champions, pgatour_past_champions
            parse_pgatour_past_champions(pgatour_past_champions(tournament_id="R2026014", return_parsed=False))
    """
    raw = _d(raw)
    rows = [
        {
            "format_type": raw.get("formatType"), "season": _int(c.get("year")),
            "display_season": _str(c.get("displaySeason")), "player_id": _str(c.get("playerId")),
            "display_name": c.get("displayName"), "country_code": c.get("countryCode"), "total": _int(c.get("total")),
            "to_par": _str(c.get("score")),
        }
        for c in _l(raw.get("pastChampions"))
    ]  # fmt: skip
    return _frame(rows, PAST_CHAMPIONS_SCHEMA, return_as_pandas)


def parse_pgatour_hole_by_hole(raw: Any, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """One row per player per hole of one round of a tournament.

    The answer does not say which round it holds: it is the round the wrapper asked for, or
    the tournament's current round when none was given.

    Args:
        raw: The ``leaderboardHoleByHole`` answer.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A frame with :data:`HOLE_BY_HOLE_SCHEMA` columns; ``score`` is null on a hole not yet played.

    Example:
        Parse a raw answer::

            from sportsdataverse.golf import parse_pgatour_hole_by_hole, pgatour_hole_by_hole
            parse_pgatour_hole_by_hole(pgatour_hole_by_hole(tournament_id="R2026014", round=4, return_parsed=False))
    """
    raw = _d(raw)
    rows = []
    for p in _l(raw.get("playerData")):
        head = {
            "tournament_id": _str(raw.get("tournamentId")), "player_id": _str(p.get("playerId")),
            "course_id": _str(p.get("courseId")), "course_code": p.get("courseCode"), "front_nine": _int(p.get("out")),
            "back_nine": _int(p.get("in")), "round_total": _int(p.get("total")),
            "round_total_to_par": _str(p.get("totalToPar")),
        }  # fmt: skip
        rows.extend(
            {
                **head, "hole_number": _int(h.get("holeNumber")), "sequence_number": _int(h.get("sequenceNumber")),
                "par": _int(h.get("par")), "yardage": _int(h.get("yardage")), "score": _int(h.get("score")),
                "score_status": h.get("status"), "round_to_par": _str(h.get("roundScore")),
            }
            for h in _l(p.get("scores"))
        )  # fmt: skip
    return _frame(rows, HOLE_BY_HOLE_SCHEMA, return_as_pandas)


def parse_pgatour_leaderboard_stats(raw: Any, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """One row per player per stat of a tournament's leaderboard stat table (long form).

    ``stat_title`` is the column heading the site shows for ``stat_id`` (``win`` -> ``Win %``).
    An event without that table (no strokes-gained data, say) is a zero-row frame.

    Args:
        raw: The ``leaderboardStats`` answer.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A frame with :data:`LEADERBOARD_STATS_SCHEMA` columns; ``stat_value`` and ``rank`` are as displayed.

    Example:
        Parse a raw answer::

            from sportsdataverse.golf import parse_pgatour_leaderboard_stats, pgatour_leaderboard_stats
            parse_pgatour_leaderboard_stats(pgatour_leaderboard_stats(tournament_id="R2026014", return_parsed=False))
    """
    raw = _d(raw)
    ids, titles = raw.get("statIds"), raw.get("titles")
    title = {_str(i): t for i, t in zip(ids, titles)} if isinstance(ids, list) and isinstance(titles, list) else {}
    rows = [
        {
            "tournament_id": _tournament(raw.get("id")), "stats_type": raw.get("type"), "player_id": _str(p.get("playerId")),
            "stat_id": _str(s.get("statId")), "stat_title": _str(title.get(_str(s.get("statId")))),
            "stat_value": _str(s.get("value")), "rank": _str(s.get("rank")), "sort_value": _num(s.get("sortValue")),
        }
        for p in _l(raw.get("players"))
        for s in _l(p.get("stats"))
    ]  # fmt: skip
    return _frame(rows, LEADERBOARD_STATS_SCHEMA, return_as_pandas)


def parse_pgatour_field(raw: Any, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """One row per player in a tournament's field, entered players first, then the alternates.

    Args:
        raw: The ``field`` answer.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A frame with :data:`FIELD_SCHEMA` columns.

    Example:
        Parse a raw answer::

            from sportsdataverse.golf import parse_pgatour_field, pgatour_field
            parse_pgatour_field(pgatour_field(tournament_id="R2026014", return_parsed=False))
    """
    raw = _d(raw)
    rows = [
        {
            "tournament_id": _str(raw.get("id")), "player_id": _str(p.get("id")), "first_name": p.get("firstName"),
            "last_name": p.get("lastName"), "display_name": p.get("displayName"), "short_name": p.get("shortName"),
            "country": p.get("country"), "country_code": p.get("countryFlag"), "amateur": p.get("amateur"),
            "alternate": p.get("alternate"), "withdrawn": p.get("withdrawn"), "status": p.get("status"),
            "owgr": _str(p.get("owgr")), "qualifier": p.get("qualifier"), "ranking_points": _str(p.get("rankingPoints")),
        }
        for p in _l(raw.get("players")) + _l(raw.get("alternates"))
    ]  # fmt: skip
    return _frame(rows, FIELD_SCHEMA, return_as_pandas)


def parse_pgatour_past_results(raw: Any, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """One row per player of one edition's final leaderboard.

    ``tournament_id`` is the edition the API answered with (``R2025014`` for the 2025 Masters),
    and ``season`` is read from it.

    Args:
        raw: The ``tournamentPastResults`` answer.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A frame with :data:`PAST_RESULTS_SCHEMA` columns; ``round_<n>`` is null for a round not played.

    Example:
        Parse a raw answer::

            from sportsdataverse.golf import parse_pgatour_past_results, pgatour_past_results
            parse_pgatour_past_results(pgatour_past_results(tournament_id="R2026014", year=2025, return_parsed=False))
    """
    raw = _d(raw)
    tid = _str(raw.get("id"))
    rows = []
    for r in _l(raw.get("players")):
        p = _d(r.get("player"))
        rows.append({
            "tournament_id": tid, "season": _int(tid[1:5]) if tid else None, "position": _str(r.get("position")),
            "player_id": _str(p.get("id")), "first_name": p.get("firstName"), "last_name": p.get("lastName"),
            "display_name": p.get("displayName"), "short_name": p.get("shortName"), "country": p.get("country"),
            "country_code": p.get("countryFlag"), "amateur": p.get("amateur"), "total": _int(r.get("total")),
            "to_par": _str(r.get("parRelativeScore")), **_rounds(r.get("rounds"), "score"),
        })  # fmt: skip
    return _frame(rows, PAST_RESULTS_SCHEMA, return_as_pandas)


def parse_pgatour_course_stats(raw: Any, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """One row per hole per round per course of a tournament: scoring average and score counts.

    ``round_number`` is null on the all-rounds rows (``round_header`` ``"All Rounds"``);
    ``difficulty_rank`` 1 is the hardest hole. The summary rows the site shows are dropped.

    Args:
        raw: The ``courseStats`` answer.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A frame with :data:`COURSE_STATS_SCHEMA` columns.

    Example:
        Parse a raw answer::

            from sportsdataverse.golf import parse_pgatour_course_stats, pgatour_course_stats
            parse_pgatour_course_stats(pgatour_course_stats(tournament_id="R2026014", return_parsed=False))
    """
    raw = _d(raw)
    rows = []
    for c in _l(raw.get("courses")):
        course = {
            "tournament_id": _str(raw.get("tournamentId")), "course_id": _str(c.get("courseId")),
            "course_name": c.get("courseName"), "course_code": c.get("courseCode"), "course_par": _int(c.get("par")),
            "course_yardage": _int(c.get("yardage")), "host_course": c.get("hostCourse"),
        }  # fmt: skip
        for rnd in _l(c.get("roundHoleStats")):
            rows.extend(
                {
                    **course, "round_number": _int(rnd.get("roundNum")), "round_header": rnd.get("roundHeader"),
                    "hole_number": _int(h.get("courseHoleNum")), "par": _int(h.get("parValue")),
                    "yards": _int(h.get("yards")), "scoring_average": _num(h.get("scoringAverage")),
                    "scoring_average_diff": _num(h.get("scoringAverageDiff")), "difficulty_rank": _int(h.get("rank")),
                    "eagles": _int(h.get("eagles")), "birdies": _int(h.get("birdies")), "pars": _int(h.get("pars")),
                    "bogeys": _int(h.get("bogeys")), "double_bogeys": _int(h.get("doubleBogey")),
                }
                for h in _l(rnd.get("holeStats"))
                if h.get("__typename", "CourseHoleStats") == "CourseHoleStats"
            )  # fmt: skip
    return _frame(rows, COURSE_STATS_SCHEMA, return_as_pandas)


def parse_pgatour_hole_details(raw: Any, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """One row: one hole of one course over a whole tournament (par, yardage, score counts).

    Args:
        raw: The ``holeDetails`` answer.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A frame with :data:`HOLE_DETAILS_SCHEMA` columns; zero rows when ``raw`` names no hole.

    Example:
        Parse a raw answer::

            from sportsdataverse.golf import parse_pgatour_hole_details, pgatour_hole_details
            parse_pgatour_hole_details(pgatour_hole_details(tournament_id="R2026014", course_id="014", hole=12, return_parsed=False))
    """
    raw = _d(raw)
    info, summary = _d(raw.get("holeInfo")), _d(raw.get("statsSummary"))
    rows = [] if raw.get("holeNum") is None else [{
        "tournament_id": _str(raw.get("tournamentId")), "course_id": _str(raw.get("courseId")),
        "hole_number": _int(raw.get("holeNum")), "par": _int(info.get("par")), "yards": _int(info.get("yards")),
        "difficulty_rank": _int(info.get("rank")), "rounds_played": _int(info.get("rounds")),
        "scoring_average_diff": _num(info.get("scoringAverageDiff")),
        **{
            f"{k}{suffix}": (_int if suffix == "" else _str)(summary.get(f"{camel}{'Percent' if suffix else ''}"))
            for k, camel in (("eagles", "eagles"), ("birdies", "birdies"), ("pars", "pars"), ("bogeys", "bogeys"),
                             ("double_bogeys", "doubleBogeys"))
            for suffix in ("", "_pct")
        },
    }]  # fmt: skip
    return _frame(rows, HOLE_DETAILS_SCHEMA, return_as_pandas)


_SCORECARD_STAT_GROUPS = (("performance", "performance"), ("scoring", "scoring"), ("strokesGained", "strokes_gained"))


def parse_pgatour_scorecard_stats(raw: Any, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """One row per stat per round of one player's tournament (performance, scoring, strokes gained).

    ``round`` is the round number as a string (``"1"`` .. ``"4"``; ``"-1"``, named ``All``, is the
    whole tournament); ``year_to_date`` is the player's season value of the same stat. An event
    without ShotLink (the Masters) carries only the scoring group.

    Args:
        raw: The ``scorecardStatsV3Compressed`` answer, payload decoded.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A frame with :data:`SCORECARD_STATS_SCHEMA` columns; values are as displayed.

    Example:
        Parse a raw answer::

            from sportsdataverse.golf import parse_pgatour_scorecard_stats, pgatour_scorecard_stats
            parse_pgatour_scorecard_stats(pgatour_scorecard_stats(tournament_id="R2026014", player_id="28237", return_parsed=False))
    """
    body = _payload(raw)
    rows = [
        {
            "tournament_id": _tournament(body.get("id")), "round": _str(rnd.get("round")), "round_name": rnd.get("displayName"),
            "round_status": rnd.get("roundStatus"), "stat_group": group, "stat_id": _str(s.get("statId")),
            "stat_name": s.get("label"), "stat_value": _str(s.get("total")), "rank": _str(s.get("rank")),
            "year_to_date": _str(s.get("yearToDate")),
        }
        for rnd in _l(body.get("rounds"))
        for key, group in _SCORECARD_STAT_GROUPS
        for s in _l(rnd.get(key))
    ]  # fmt: skip
    return _frame(rows, SCORECARD_STATS_SCHEMA, return_as_pandas)


def parse_pgatour_player_results(raw: Any, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """One row per tournament edition a player has played on a tour (career results).

    Args:
        raw: The ``playerProfileTournamentResults`` answer.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A frame with :data:`PLAYER_RESULTS_SCHEMA` columns; ``start_date``, ``winnings`` and ``points`` are as
        displayed (``"3.19.2015"``, ``"$151,200.00"``, ``"67.500"``). ``total`` is null for a match-play event
        (the API sends ``"0"``), ``round_<n>`` for a round not played.

    Example:
        Parse a raw answer::

            from sportsdataverse.golf import parse_pgatour_player_results, pgatour_player_results
            parse_pgatour_player_results(pgatour_player_results(player_id="28237", return_parsed=False))
    """
    raw = _d(raw)
    rows = [
        {
            "player_id": _str(raw.get("playerId")), "tournament_num": _str(t.get("tournamentNum")),
            "tournament_id": _str(e.get("tournamentId")), "tournament_name": e.get("tournamentName"),
            "season": _int(e.get("year")), "start_date": _str(e.get("startDate")), "course_name": e.get("courseName"),
            "position": _str(e.get("position")), "to_par": _str(e.get("toPar")), "total": _int(e.get("total")) or None,
            "points": _str(e.get("points")), "points_rank": _str(e.get("pointsRank")),
            "winnings": _str(e.get("winnings")), **_rounds(e.get("roundScores"), "roundScore", "roundNum"),
        }
        for t in _l(raw.get("tournaments"))
        for e in _l(t.get("tournaments"))
    ]  # fmt: skip
    return _frame(rows, PLAYER_RESULTS_SCHEMA, return_as_pandas)
