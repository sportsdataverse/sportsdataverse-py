"""Tidy parsers for the PGA Tour GraphQL API (``pgatour_*`` wrappers).

Each parser takes the operation's ``data`` value as the wrapper's getter returns it, with a
compressed ``payload`` already decoded, and returns a polars frame. Pass
``return_as_pandas=True`` for pandas instead. An empty or malformed answer returns a zero-row
frame that still carries the parser's documented schema. Tournament, player and stat ids are
strings (``"R2026014"``, ``"28237"``, ``"02675"``).
"""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
import polars as pl

__all__ = [
    "parse_pgatour_schedule",
    "parse_pgatour_leaderboard",
    "parse_pgatour_scorecard",
    "parse_pgatour_tee_times",
    "parse_pgatour_player",
    "parse_pgatour_player_directory",
    "parse_pgatour_stat_catalog",
    "parse_pgatour_stat_details",
    "parse_pgatour_tour_cup",
    "parse_pgatour_shot_details",
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


def _d(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _l(value: Any) -> List[Any]:
    return [v for v in value if isinstance(v, dict)] if isinstance(value, list) else []


def _payload(raw: Any) -> Dict[str, Any]:
    """The decoded body of a compressed answer (``{"id", "payload"}``), else ``raw`` itself."""
    raw = _d(raw)
    return _d(raw["payload"]) if "payload" in raw else raw


def _int(value: Any) -> Optional[int]:
    """``"276"`` / ``276`` / ``276.0`` -> 276; ``"-"``, ``"E"``, ``""``, None -> None."""
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value) if math.isfinite(value) else None
    try:
        return int(str(value).replace(",", "").strip())
    except ValueError:
        return None


def _str(value: Any) -> Optional[str]:
    return None if value is None or isinstance(value, (dict, list)) else str(value)


def _epoch_ms(value: Any) -> Optional[datetime]:
    ms = _int(value)
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc) if ms is not None else None


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


def _frame(rows: List[Dict[str, Any]], schema: Dict[str, Any], return_as_pandas: bool) -> pl.DataFrame | pd.DataFrame:
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


def _leaderboard_row(body: Dict[str, Any], row: Dict[str, Any]) -> Dict[str, Any]:
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
    """
    body = _payload(raw)
    player_id = _str(_d(body.get("player")).get("id"))
    tournament_id = _str(body.get("id"))
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
    """
    raw = _d(raw)
    rows = []
    for p in _l(raw.get("players")):
        if p.get("__typename", "TourCupCombinedPlayer") != "TourCupCombinedPlayer":
            continue
        rank, points = _d(p.get("rankingData")), _d(p.get("pointData"))
        rows.append({
            "cup_id": _str(raw.get("id")), "title": raw.get("title"), "season": _int(raw.get("season")),
            "player_id": _str(p.get("id")), "first_name": p.get("firstName"), "last_name": p.get("lastName"),
            "display_name": p.get("displayName"), "country": p.get("country"), "country_code": p.get("countryFlag"),
            "rank_official": _str(rank.get("official")), "rank_projected": _str(rank.get("projected")),
            "rank_movement": rank.get("movement"), "points_official": _str(points.get("official")),
            "points_projected": _str(points.get("projected")), "points_event": _str(points.get("event")),
        })  # fmt: skip
    return _frame(rows, TOUR_CUP_SCHEMA, return_as_pandas)


def _coords(stroke: Dict[str, Any]) -> Dict[str, Optional[float]]:
    """ShotLink ``tourcastX/Y/Z`` coordinates of a stroke's start and end."""
    lr = _d(_d(stroke.get("overview")).get("leftToRightCoords"))
    out: Dict[str, Optional[float]] = {}
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
