"""Tidy parsers for the Premier Lacrosse League stats API (``pll_*`` wrappers).

Each parser takes the ``data`` object the wrapper's getter returns (request stamped under
``_request``) and returns a polars frame; pass ``return_as_pandas=True`` for pandas. An empty or
malformed answer returns a zero-row frame that still carries the parser's documented schema.
Ids are strings: game slugs (``"2026-ev-4"``), team codes (``"WAT"``), six-digit player ids
(``"003009"``) and the numeric game id (``"468"``). Stat columns are the API's stat names in
snake case (``twoPointGoals`` -> ``two_point_goals``, ``GAA`` -> ``gaa``); counts are Int64, rates,
percentages and per-game values Float64.
"""

from __future__ import annotations

import math
import re
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import underscore

__all__ = [
    "parse_pll_schedule",
    "parse_pll_game",
    "parse_pll_play_by_play",
    "parse_pll_player_box",
    "parse_pll_team_box",
    "parse_pll_game_insights",
    "parse_pll_standings",
    "parse_pll_team_season_stats",
    "parse_pll_team_stats",
    "parse_pll_team_roster",
    "parse_pll_player_season_stats",
    "parse_pll_player_advanced_stats",
    "parse_pll_season_leaders",
    "parse_pll_career_leaders",
    "parse_pll_player",
    "parse_pll_player_seasons",
]

S, INT, F, B = pl.String, pl.Int64, pl.Float64, pl.Boolean
UTC_DT = pl.Datetime("ms", "UTC")
INT_LIST = pl.List(pl.Int64)

# A stat is a rate (Float64) when its name says so; every other stat is a count (Int64).
_RATE = re.compile(r"(Pct|Rate|PG|Gaa|GAA)$|^(saa|pim|pimValue|avgPerSeason|statValue)$")

#: The stat names of a player stat line (season stats, a player's seasons, rosters' players).
PLAYER_STAT_KEYS = (
    "points", "scoringPoints", "goals", "onePointGoals", "twoPointGoals", "assists", "shots", "shotPct",
    "shotsOnGoal", "shotsOnGoalPct", "twoPointShots", "twoPointShotPct", "twoPointShotsOnGoal",
    "twoPointShotsOnGoalPct", "groundBalls", "turnovers", "causedTurnovers", "faceoffsWon", "faceoffsLost",
    "faceoffs", "faceoffPct", "saves", "savePct", "cleanSaves", "messySaves", "cleanSavePct", "scoresAgainst",
    "goalsAgainst", "twoPointGoalsAgainst", "saa", "GAA", "twoPtGaa", "goalieWins", "goalieLosses", "goalieTies",
    "numPenalties", "pim", "pimValue", "powerPlayGoals", "powerPlayShots", "shortHandedGoals", "shortHandedShots",
    "shortHandedGoalsAgainst", "powerPlayGoalsAgainst", "tof", "plusMinus", "shotTurnovers", "touches",
    "totalPasses", "unassistedGoals", "assistedGoals", "passRate", "shotRate", "goalRate", "assistRate",
    "turnoverRate", "gamesPlayed", "gamesStarted",
)  # fmt: skip
#: The stat names of a team stat line (team season stats, one team's stats).
TEAM_STAT_KEYS = (
    "gamesPlayed", "scores", "scoresAgainst", "scoresPG", "goals", "onePointGoals", "twoPointGoals", "assists",
    "shots", "shotsPG", "shotPct", "shotsOnGoal", "shotsOnGoalPct", "twoPointShots", "twoPointShotPct",
    "twoPointShotsOnGoal", "twoPointShotsOnGoalPct", "goalsAgainst", "twoPointGoalsAgainst", "saa", "saves",
    "savePct", "cleanSaves", "messySaves", "cleanSavePct", "groundBalls", "turnovers", "causedTurnovers",
    "faceoffsWon", "faceoffsLost", "faceoffs", "faceoffPct", "clears", "clearAttempts", "clearPct", "rides",
    "rideAttempts", "ridesPct", "numPenalties", "pim", "offsides", "shotClockExpirations", "timesManUp",
    "powerPlayGoals", "powerPlayShots", "powerPlayPct", "timesShortHanded", "shortHandedGoals", "shortHandedShots",
    "shortHandedPct", "powerPlayGoalsAgainst", "powerPlayShotsAgainst", "powerPlayGoalsAgainstPct",
    "shortHandedGoalsAgainst", "shortHandedShotsAgainst", "shortHandedGoalsAgainstPct", "manDownPct",
    "totalPasses", "touches",
)  # fmt: skip
#: The stat names of a player's game line (box score).
BOX_PLAYER_STAT_KEYS = (
    "points", "goals", "onePointGoals", "twoPointGoals", "assists", "shots", "shotPct", "shotsOnGoal",
    "twoPointShots", "groundBalls", "turnovers", "causedTurnovers", "faceoffsWon", "faceoffsLost", "faceoffs",
    "faceoffPct", "saves", "savePct", "cleanSaves", "messySaves", "cleanSavePct", "scoresAgainst", "goalsAgainst",
    "twoPointGoalsAgainst", "saa", "GAA", "twoPtGaa", "numPenalties", "pim", "assistOpportunities", "totalPasses",
    "touches", "gamesPlayed",
)  # fmt: skip
#: The stat names of a team's game line (box score).
BOX_TEAM_STAT_KEYS = (
    "scores", "scoresAgainst", "goals", "onePointGoals", "twoPointGoals", "assists", "shots", "shotPct",
    "shotsOnGoal", "shotsOnGoalPct", "twoPointShots", "twoPointShotPct", "twoPointShotsOnGoal",
    "twoPointShotsOnGoalPct", "goalsAgainst", "twoPointGoalsAgainst", "saves", "savePct", "cleanSaves",
    "messySaves", "cleanSavePct", "groundBalls", "turnovers", "causedTurnovers", "faceoffsWon", "faceoffsLost",
    "faceoffs", "faceoffPct", "clears", "clearAttempts", "clearPct", "rideAttempts", "numPenalties", "pim",
    "shotClockExpirations", "timesManUp", "powerPlayGoals", "powerPlayShots", "powerPlayPct", "timesShortHanded",
    "powerPlayGoalsAgainst", "powerPlayGoalsAgainstPct", "manDownPct", "totalPasses", "touches", "timeInPossesion",
    "timeInPossesionPct", "totalPossessions",
)  # fmt: skip
#: The stat names of an advanced stat line.
ADVANCED_STAT_KEYS = (
    "goals", "unassistedGoals", "assistedGoals", "settledGoals", "fastbreakGoals", "substitutionGoals",
    "doorstepGoals", "powerPlayGoals", "assistOpportunities", "settledAssists", "powerPlayAssists",
    "fastbreakAssists", "dodgeAssists", "pnrAssists", "unassistedShots", "assistedShots", "unassistedShotPct",
    "assistedShotPct", "pipeShots", "lhShots", "lhGoals", "lhShotPct", "rhShots", "rhGoals", "rhShotPct",
)  # fmt: skip


def _stat_schema(keys: Iterable[str]) -> Dict[str, Any]:
    return {underscore(k): (F if _RATE.search(k) else INT) for k in keys}


def _stats(src: Any, keys: Iterable[str]) -> Dict[str, Any]:
    src = _d(src)
    return {underscore(k): src.get(k) for k in keys}


GAME_SCHEMA = {
    "game_id": S, "event_slug": S, "event_id": S, "external_id": S, "league": S, "season": INT,
    "season_segment": S, "week": INT, "game_number": INT, "start_time": UTC_DT, "event_status": INT, "period": INT,
    "home_team_id": S, "home_team_name": S, "home_team_location": S, "home_team_location_code": S,
    "home_team_conference": S, "away_team_id": S, "away_team_name": S, "away_team_location": S,
    "away_team_location_code": S, "away_team_conference": S, "home_score": INT, "away_score": INT, "venue": S,
    "location": S, "broadcaster": S, "description": S, "url_streaming": S,
}  # fmt: skip
SCHEDULE_SCHEMA = GAME_SCHEMA
GAME_DETAIL_SCHEMA = {
    **GAME_SCHEMA,
    "home_period_scores": INT_LIST, "away_period_scores": INT_LIST, "home_team_wins": INT, "home_team_losses": INT,
    "home_team_ties": INT, "away_team_wins": INT, "away_team_losses": INT, "away_team_ties": INT,
    "pregame_home_win_probability": F, "pregame_away_win_probability": F,
}  # fmt: skip
PLAY_BY_PLAY_SCHEMA = {
    "event_slug": S, "play_index": INT, "marker_id": S, "event_type": S, "period": INT, "minutes": INT,
    "seconds": INT, "seconds_passed": INT, "team_id": S, "description": S, "shot_type": S, "home_score": INT,
    "away_score": INT, "home_win_probability": F, "away_win_probability": F, "penalty_length": INT,
    "penalty_description": S, "faceoff_winner_id": S, "faceoff_loser_id": S, "ground_ball_player_id": S,
    "committed_turnover_id": S, "caused_turnover_id": S, "shooter_id": S, "goalie_id": S, "offense_goalie_id": S,
    "committed_penalty_id": S, "shot_assist_id": S, "assist_opportunity_player_id": S, "closest_defender_id": S,
    "shot_on_goal": B, "shot_saved": B, "save_type": S,
}  # fmt: skip
PLAYER_BOX_SCHEMA = {
    "event_slug": S, "player_id": S, "player_slug": S, "first_name": S, "last_name": S, "team_id": S,
    "position": S, "position_name": S, "jersey_number": S, **_stat_schema(BOX_PLAYER_STAT_KEYS),
}  # fmt: skip
TEAM_BOX_SCHEMA = {"event_slug": S, "team_id": S, **_stat_schema(BOX_TEAM_STAT_KEYS)}
GAME_INSIGHTS_SCHEMA = {
    "event_slug": S, "insight_index": INT, "title": S, "insight_type": S, "subject": S, "detail": S, "team_id": S,
    "player_id": S, "position": S, "jersey_number": S, "period": INT, "minutes": INT, "seconds": INT,
    "primary_image_url": S,
}  # fmt: skip
STANDINGS_SCHEMA = {
    "league": S, "season": INT, "champ_series": B, "team_id": S, "full_name": S, "location": S, "location_code": S,
    "conference": S, "seed": INT, "conference_seed": INT, "wins": INT, "losses": INT, "ties": INT, "scores": INT,
    "scores_against": INT, "score_diff": INT, "conference_wins": INT, "conference_losses": INT,
    "conference_ties": INT, "conference_scores": INT, "conference_scores_against": INT,
}  # fmt: skip
TEAM_SEASON_STATS_SCHEMA = {
    "season": INT, "season_segment": S, "team_id": S, "team_slug": S, "full_name": S, "location": S,
    "location_code": S, "conference": S, **_stat_schema(TEAM_STAT_KEYS),
}  # fmt: skip
TEAM_STATS_SCHEMA = {
    "team_id": S, "team_slug": S, "full_name": S, "location": S, "location_code": S, "conference": S, "wins": INT,
    "losses": INT, "ties": INT, "seed": INT, "season": INT, "season_segment": S, **_stat_schema(TEAM_STAT_KEYS),
}  # fmt: skip
TEAM_ROSTER_SCHEMA = {
    "team_slug": S, "season": INT, "season_segment": S, "player_id": S, "player_slug": S, "first_name": S,
    "last_name": S, "last_name_suffix": S, "nickname": S, "jersey_number": S, "position": S, "position_name": S,
    "dob": S, "age": INT, "height": S, "weight": INT, "handedness": S, "country": S, "country_code": S,
    "hometown": S, "high_school": S, "college": S, "college_year": INT, "club_team": S, "experience": INT,
    "is_captain": B, "scratch": B, "injury_status": S, "injury_description": S, "status": S, "contract_status": S,
}  # fmt: skip
PLAYER_SEASON_STATS_SCHEMA = {
    "season": INT, "season_segment": S, "player_id": S, "player_slug": S, "first_name": S, "last_name": S,
    "jersey_number": S, "position": S, "position_name": S, "experience": INT, "team_id": S, "team_name": S,
    **_stat_schema(PLAYER_STAT_KEYS),
}  # fmt: skip
PLAYER_ADVANCED_STATS_SCHEMA = {
    "season": INT, "season_segment": S, "player_id": S, "player_slug": S, "first_name": S, "last_name": S,
    "jersey_number": S, "position": S, "position_name": S, "experience": INT, "team_id": S, "team_name": S,
    **_stat_schema(ADVANCED_STAT_KEYS),
}  # fmt: skip
SEASON_LEADERS_SCHEMA = {
    "season": INT, "season_segment": S, "stat": S, "rank": INT, "player_id": S, "first_name": S, "last_name": S,
    "jersey_number": S, "position": S, "team_id": S, "team_name": S, "stat_value": F,
}  # fmt: skip
CAREER_LEADERS_SCHEMA = {
    "stat": S, "rank": INT, "player_id": S, "player_slug": S, "first_name": S, "last_name": S, "status": S,
    "team_id": S, "team_name": S, "stat_value": F, "games_played": INT, "seasons_played": INT, "avg_per_season": F,
}  # fmt: skip
PLAYER_SCHEMA = {
    "player_id": S, "player_slug": S, "first_name": S, "last_name": S, "last_name_suffix": S, "nickname": S,
    "team_id": S, "team_name": S, "position": S, "position_name": S, "jersey_number": S, "dob": S, "age": INT,
    "height": S, "weight": INT, "handedness": S, "country": S, "country_code": S, "hometown": S, "high_school": S,
    "college": S, "college_year": INT, "club_team": S, "exp_from_year": INT, "is_captain": B, "scratch": B,
    "injury_status": S, "injury_description": S, "status": S, "contract_status": S, "all_years": INT_LIST,
    "bio": S,
}  # fmt: skip
PLAYER_SEASONS_SCHEMA = {
    "player_slug": S, "season": INT, "season_segment": S, "team_id": S, "team_name": S,
    **_stat_schema(PLAYER_STAT_KEYS),
}  # fmt: skip


def _d(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _l(value: Any) -> List[Any]:
    return [v for v in value if isinstance(v, dict)] if isinstance(value, list) else []


def _req(raw: Any) -> Dict[str, Any]:
    return _d(_d(raw).get("_request"))


def _int(value: Any) -> Optional[int]:
    """``468`` / ``"468"`` / ``468.0`` -> 468; ``2.5``, ``""``, ``True``, None -> None."""
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, float):
        return int(value) if math.isfinite(value) and value.is_integer() else None
    if isinstance(value, int):
        return value
    text = str(value).strip()
    return int(text) if re.fullmatch(r"-?\d+", text) else None


def _str(value: Any) -> Optional[str]:
    """Text of a scalar; a whole float id (``468.0``) loses its ``.0``; dicts, lists and None are null."""
    if value is None or isinstance(value, (dict, list, bool)):
        return None
    if isinstance(value, float):
        return str(int(value)) if math.isfinite(value) and value.is_integer() else str(value)
    text = str(value)
    return text if text != "" else None


def _epoch_s(value: Any) -> Optional[datetime]:
    sec = _int(value)
    return datetime.fromtimestamp(sec, tz=timezone.utc) if sec is not None else None


def _cell(value: Any, dtype: Any) -> Any:
    """``value`` coerced to ``dtype``'s Python type, or None: a malformed field never raises."""
    if dtype == S:
        return _str(value)
    if dtype == INT:
        return _int(value)
    if dtype == B:
        return value if isinstance(value, bool) else None
    if dtype == F:
        ok = isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)
        return float(value) if ok else None
    if dtype == INT_LIST:
        return [i for i in (_int(v) for v in value) if i is not None] if isinstance(value, list) else None
    return value if isinstance(value, datetime) else None


def _frame(rows: List[Dict[str, Any]], schema: Dict[str, Any], return_as_pandas: bool) -> pl.DataFrame | pd.DataFrame:
    clean = [{k: _cell(r.get(k), t) for k, t in schema.items()} for r in rows]
    df = pl.DataFrame(clean, schema=schema) if clean else pl.DataFrame(schema=schema)
    return df.to_pandas() if return_as_pandas else df


def _items(raw: Any) -> List[Dict[str, Any]]:
    return _l(_d(raw).get("items"))


def _team_side(team: Any, side: str) -> Dict[str, Any]:
    t = _d(team)
    return {
        f"{side}_team_id": t.get("officialId"), f"{side}_team_name": t.get("fullName"),
        f"{side}_team_location": t.get("location"), f"{side}_team_location_code": t.get("locationCode"),
        f"{side}_team_conference": t.get("conference"),
    }  # fmt: skip


def _game_row(g: Dict[str, Any]) -> Dict[str, Any]:
    casts = g.get("broadcaster")
    return {
        "game_id": g.get("id"), "event_slug": g.get("slugname"), "event_id": g.get("eventId"),
        "external_id": g.get("externalId"), "league": g.get("league"), "season": g.get("year"),
        "season_segment": g.get("seasonSegment"), "week": g.get("week"), "game_number": g.get("gameNumber"),
        "start_time": _epoch_s(g.get("startTime")), "event_status": g.get("eventStatus"), "period": g.get("period"),
        **_team_side(g.get("homeTeam"), "home"), **_team_side(g.get("awayTeam"), "away"),
        "home_score": g.get("homeScore"), "away_score": g.get("visitorScore"), "venue": g.get("venue"),
        "location": g.get("location"),
        "broadcaster": ", ".join(str(c) for c in casts if c) if isinstance(casts, list) else casts,
        "description": g.get("description"), "url_streaming": g.get("urlStreaming"),
    }  # fmt: skip


def parse_pll_schedule(raw: Any, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """One row per game of a season: teams, score, start time (UTC), segment, week, venue, slug.

    Also parses the Global Lacrosse Games and WLL schedules, which share the shape.
    ``away_score`` is the API's ``visitorScore``; ``event_slug`` is the id every per-game
    wrapper takes.

    Args:
        raw: The ``events`` answer (``pll_schedule(..., return_parsed=False)``).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A frame with :data:`SCHEDULE_SCHEMA` columns.
    """
    return _frame([_game_row(g) for g in _items(raw)], SCHEDULE_SCHEMA, return_as_pandas)


def parse_pll_game(raw: Any, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """One row: a game's header, period scores, both teams' records and the pregame win probability.

    Args:
        raw: The ``events/{event_slug}`` answer.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A frame with :data:`GAME_DETAIL_SCHEMA` columns (one row, or none for an empty answer).
    """
    g = _d(raw)
    if not g.get("slugname"):
        return _frame([], GAME_DETAIL_SCHEMA, return_as_pandas)
    home, away, pre = _d(g.get("homeTeam")), _d(g.get("awayTeam")), _d(g.get("pregameWinProbability"))
    row = {
        **_game_row(g),
        "home_period_scores": g.get("homePeriodScores"), "away_period_scores": g.get("visitorPeriodScores"),
        "home_team_wins": home.get("wins"), "home_team_losses": home.get("losses"), "home_team_ties": home.get("ties"),
        "away_team_wins": away.get("wins"), "away_team_losses": away.get("losses"), "away_team_ties": away.get("ties"),
        "pregame_home_win_probability": pre.get("homeWinProbability"),
        "pregame_away_win_probability": pre.get("awayWinProbability"),
    }  # fmt: skip
    return _frame([row], GAME_DETAIL_SCHEMA, return_as_pandas)


def parse_pll_play_by_play(raw: Any, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """One row per logged play, in game order, with the players involved and the win probability after it.

    ``play_index`` is the row's 0-based position in the log. ``minutes`` / ``seconds`` are the
    clock remaining in the period, ``seconds_passed`` the game time elapsed (cumulative across
    periods: 2165 is 5 s into the fourth 12-minute quarter). The ``*_id`` columns are six-digit
    player ids (``player_id`` in :func:`parse_pll_player_box`).

    ``home_score`` / ``away_score`` are the score after the play on shot and goal rows. On the
    other rows the 2026 logs carry the game's final score and the 2021 logs the running score, so
    take the running score from the shot and goal rows (forward-filled), not from every row.
    ``shot_on_goal`` / ``shot_saved`` / ``save_type`` are null wherever the log's ``details`` object is
    empty (every play of the 2021 game checked; the 2026 logs fill it on shots).

    Args:
        raw: The ``events/{event_slug}/playlogs`` answer.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A frame with :data:`PLAY_BY_PLAY_SCHEMA` columns.
    """
    slug = _req(raw).get("event_slug")
    rows = []
    for i, p in enumerate(_items(raw)):
        det = _d(p.get("details"))
        rows.append({
            "event_slug": slug, "play_index": i, "marker_id": p.get("markerId"), "event_type": p.get("eventType"),
            "period": p.get("period"), "minutes": p.get("minutes"), "seconds": p.get("seconds"),
            "seconds_passed": p.get("secondsPassed"), "team_id": p.get("teamId"), "description": p.get("description"),
            "shot_type": p.get("shotType"), "home_score": p.get("homeScore"), "away_score": p.get("visitorScore"),
            "home_win_probability": p.get("homeTeamWinProbability"),
            "away_win_probability": p.get("awayTeamWinProbability"), "penalty_length": p.get("penaltyLength"),
            "penalty_description": p.get("penaltyDescription"), "faceoff_winner_id": p.get("faceoffWinnerId"),
            "faceoff_loser_id": p.get("faceoffLoserId"), "ground_ball_player_id": p.get("gbPlayerId"),
            "committed_turnover_id": p.get("commitedTurnoverId"), "caused_turnover_id": p.get("causedTurnoverId"),
            "shooter_id": p.get("shooterId"), "goalie_id": p.get("goalieId"),
            "offense_goalie_id": p.get("offenseGoalieId"), "committed_penalty_id": p.get("commitedPenaltyId"),
            "shot_assist_id": p.get("shotAssistId"), "assist_opportunity_player_id": p.get("assistOpportunityPlayerId"),
            "closest_defender_id": p.get("closestDefenderId"), "shot_on_goal": det.get("shotOnGoal"),
            "shot_saved": det.get("shotSaved"), "save_type": det.get("saveType"),
        })  # fmt: skip
    return _frame(rows, PLAY_BY_PLAY_SCHEMA, return_as_pandas)


def parse_pll_player_box(raw: Any, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """One row per player in a game's box score.

    Args:
        raw: The ``events/{event_slug}/players/stats`` answer.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A frame with :data:`PLAYER_BOX_SCHEMA` columns.
    """
    slug = _req(raw).get("event_slug")
    rows = [
        {
            "event_slug": slug, "player_id": p.get("officialId"), "player_slug": p.get("slug"),
            "first_name": p.get("firstName"), "last_name": p.get("lastName"), "team_id": p.get("teamId"),
            "position": p.get("position"), "position_name": p.get("positionName"), "jersey_number": p.get("jerseyNum"),
            **_stats(p, BOX_PLAYER_STAT_KEYS),
        }
        for p in _items(raw)
    ]  # fmt: skip
    return _frame(rows, PLAYER_BOX_SCHEMA, return_as_pandas)


def parse_pll_team_box(raw: Any, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """One row per team in a game's box score (possession time in seconds and as a share).

    Args:
        raw: The ``events/{event_slug}/teams/stats`` answer.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A frame with :data:`TEAM_BOX_SCHEMA` columns.
    """
    slug = _req(raw).get("event_slug")
    rows = [{"event_slug": slug, "team_id": t.get("officialId"), **_stats(t, BOX_TEAM_STAT_KEYS)} for t in _items(raw)]
    return _frame(rows, TEAM_BOX_SCHEMA, return_as_pandas)


def parse_pll_game_insights(raw: Any, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """One row per written insight on a game (streaks, milestones), in the league's order.

    Args:
        raw: The ``events/{event_slug}/insights`` answer.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A frame with :data:`GAME_INSIGHTS_SCHEMA` columns.
    """
    slug = _req(raw).get("event_slug")
    rows = [
        {
            "event_slug": slug, "insight_index": i, "title": x.get("title"), "insight_type": x.get("insightType"),
            "subject": x.get("subject"), "detail": x.get("detail"), "team_id": x.get("teamId"),
            "player_id": x.get("playerId"), "position": x.get("position"), "jersey_number": x.get("jerseyNumber"),
            "period": x.get("period"), "minutes": x.get("minutes"), "seconds": x.get("seconds"),
            "primary_image_url": x.get("primaryImageUrl"),
        }
        for i, x in enumerate(_items(raw))
    ]  # fmt: skip
    return _frame(rows, GAME_INSIGHTS_SCHEMA, return_as_pandas)


def parse_pll_standings(raw: Any, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """One row per team in the standings (also the Global Lacrosse Games standings).

    ``league`` is ``PLL``, or the Global Lacrosse Games code (``intlm`` / ``intlw``) for those;
    ``season`` and ``champ_series`` repeat the request.

    Args:
        raw: The ``standings`` answer.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A frame with :data:`STANDINGS_SCHEMA` columns.
    """
    req = _req(raw)
    champ = req.get("champSeries")
    base = {
        "league": str(req.get("league") or "PLL").upper(), "season": req.get("year"),
        "champ_series": champ == "true" if isinstance(champ, str) else bool(champ),
    }  # fmt: skip
    rows = [
        {
            **base, "team_id": t.get("teamId"), "full_name": t.get("fullName"), "location": t.get("location"),
            "location_code": t.get("locationCode"), "conference": t.get("conference"), "seed": t.get("seed"),
            "conference_seed": t.get("conferenceSeed"), "wins": t.get("wins"), "losses": t.get("losses"),
            "ties": t.get("ties"), "scores": t.get("scores"), "scores_against": t.get("scoresAgainst"),
            "score_diff": t.get("scoreDiff"), "conference_wins": t.get("conferenceWins"),
            "conference_losses": t.get("conferenceLosses"), "conference_ties": t.get("conferenceTies"),
            "conference_scores": t.get("conferenceScores"), "conference_scores_against": t.get("conferenceScoresAgainst"),
        }
        for t in _items(raw)
    ]  # fmt: skip
    return _frame(rows, STANDINGS_SCHEMA, return_as_pandas)


def _team_head(t: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "team_id": t.get("officialId"), "team_slug": t.get("slug"), "full_name": t.get("fullName"),
        "location": t.get("location"), "location_code": t.get("locationCode"), "conference": t.get("conference"),
    }  # fmt: skip


def parse_pll_team_season_stats(raw: Any, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """One row per team: season totals and rates (offense, defense, faceoffs, clears and rides, special teams).

    Args:
        raw: The ``teams/season-stats`` answer.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A frame with :data:`TEAM_SEASON_STATS_SCHEMA` columns.
    """
    req = _req(raw)
    rows = [
        {"season": req.get("year"), "season_segment": req.get("seasonSegment"), **_team_head(t),
         **_stats(t.get("stats"), TEAM_STAT_KEYS)}
        for t in _items(raw)
    ]  # fmt: skip
    return _frame(rows, TEAM_SEASON_STATS_SCHEMA, return_as_pandas)


def parse_pll_team_stats(raw: Any, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """One team's stat lines: one row per season segment it played (regular season, playoffs).

    Args:
        raw: The ``teams/{team_slug}/stats`` answer.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A frame with :data:`TEAM_STATS_SCHEMA` columns.
    """
    t = _d(raw)
    rec = _d(t.get("record"))
    head = {**_team_head(t), "wins": rec.get("wins"), "losses": rec.get("losses"), "ties": rec.get("ties"),
            "seed": t.get("seed")}  # fmt: skip
    rows = [
        {
            **head,
            "season": s.get("year"),
            "season_segment": s.get("seasonSegment"),
            **_stats(s.get("stats"), TEAM_STAT_KEYS),
        }
        for s in _l(t.get("stats"))
    ]
    return _frame(rows, TEAM_STATS_SCHEMA, return_as_pandas)


def parse_pll_team_roster(raw: Any, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """One row per player on a team's roster: bio, jersey, position, injury and contract status.

    Args:
        raw: The ``teams/{team_slug}/roster`` answer.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A frame with :data:`TEAM_ROSTER_SCHEMA` columns.
    """
    req = _req(raw)
    base = {"team_slug": req.get("team_slug"), "season": req.get("year"), "season_segment": req.get("seasonSegment")}
    rows = [{**base, **_bio(p)} for p in _items(raw)]
    return _frame(rows, TEAM_ROSTER_SCHEMA, return_as_pandas)


def _bio(p: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "player_id": p.get("officialId"), "player_slug": p.get("slug"), "first_name": p.get("firstName"),
        "last_name": p.get("lastName"), "last_name_suffix": p.get("lastNameSuffix"), "nickname": p.get("nickname"),
        "jersey_number": p.get("jerseyNum"), "position": p.get("position"), "position_name": p.get("positionName"),
        "dob": p.get("dob"), "age": p.get("age"), "height": p.get("height"), "weight": p.get("weight"),
        "handedness": p.get("handedness"), "country": p.get("country"), "country_code": p.get("countryCode"),
        "hometown": p.get("hometown"), "high_school": p.get("highSchool"), "college": p.get("college"),
        "college_year": p.get("collegeYear"), "club_team": p.get("clubTeam"), "experience": p.get("experience"),
        "exp_from_year": p.get("expFromYear"), "is_captain": p.get("isCaptain"), "scratch": p.get("scratch"),
        "injury_status": p.get("injuryStatus"), "injury_description": p.get("injuryDescription"),
        "status": p.get("status"), "contract_status": p.get("contractStatus"),
    }  # fmt: skip


def _player_head(req: Dict[str, Any], p: Dict[str, Any]) -> Dict[str, Any]:
    team = _d(p.get("team"))
    return {
        "season": req.get("year"), "season_segment": req.get("seasonSegment"), "player_id": p.get("officialId"),
        "player_slug": p.get("slug"), "first_name": p.get("firstName"), "last_name": p.get("lastName"),
        "jersey_number": p.get("jerseyNum", team.get("jerseyNum")), "position": p.get("position", team.get("position")),
        "position_name": p.get("positionName", team.get("positionName")), "experience": p.get("experience"),
        "team_id": team.get("officialId"), "team_name": team.get("fullName"),
    }  # fmt: skip


def parse_pll_player_season_stats(raw: Any, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """One row per player: a season segment's stat line (scoring, shooting, faceoffs, goalie, passing).

    Args:
        raw: The ``players/season-stats`` answer.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A frame with :data:`PLAYER_SEASON_STATS_SCHEMA` columns.
    """
    req = _req(raw)
    rows = [{**_player_head(req, p), **_stats(p.get("stats"), PLAYER_STAT_KEYS)} for p in _items(raw)]
    return _frame(rows, PLAYER_SEASON_STATS_SCHEMA, return_as_pandas)


def parse_pll_player_advanced_stats(raw: Any, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """One row per player: goal, assist and shot splits (settled / fast break, assisted, left / right hand).

    Args:
        raw: The ``players/stats/advanced`` answer.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A frame with :data:`PLAYER_ADVANCED_STATS_SCHEMA` columns.
    """
    req = _req(raw)
    rows = [{**_player_head(req, p), **_stats(p.get("stats"), ADVANCED_STAT_KEYS)} for p in _items(raw)]
    return _frame(rows, PLAYER_ADVANCED_STATS_SCHEMA, return_as_pandas)


def _leaderboards(raw: Any) -> List[Tuple[str, int, Dict[str, Any]]]:
    """(stat, 1-based rank, row) for every ranked player of every stat in a leaders answer."""
    out = []
    for stat, rows in _d(raw).items():
        if stat != "_request":
            out.extend((stat, i, r) for i, r in enumerate(_l(rows), start=1))
    return out


def parse_pll_season_leaders(raw: Any, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """Long format: one row per (stat, ranked player) of a season's leaderboards.

    ``rank`` is the 1-based position in the league's list (ties are not merged).

    Args:
        raw: The ``players/stats/season-leaders`` answer.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A frame with :data:`SEASON_LEADERS_SCHEMA` columns.
    """
    req = _req(raw)
    rows = []
    for stat, rank, r in _leaderboards(raw):
        team = _d(r.get("team"))
        rows.append({
            "season": req.get("year"), "season_segment": req.get("seasonSegment"), "stat": underscore(stat),
            "rank": rank, "player_id": r.get("officialId"), "first_name": r.get("firstName"),
            "last_name": r.get("lastName"), "jersey_number": r.get("jerseyNum"), "position": r.get("position"),
            "team_id": team.get("officialId"), "team_name": team.get("fullName"), "stat_value": r.get("statValue"),
        })  # fmt: skip
    return _frame(rows, SEASON_LEADERS_SCHEMA, return_as_pandas)


def parse_pll_career_leaders(raw: Any, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """Long format: one row per (stat, ranked player) of the all-time leaderboards.

    Args:
        raw: The ``players/stats/career-leaders`` answer.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A frame with :data:`CAREER_LEADERS_SCHEMA` columns (``team_id`` is the player's last team).
    """
    rows = []
    for stat, rank, r in _leaderboards(raw):
        team = _d(r.get("team"))
        rows.append({
            "stat": underscore(stat), "rank": rank, "player_id": r.get("officialId"), "player_slug": r.get("slug"),
            "first_name": r.get("firstName"), "last_name": r.get("lastName"), "status": r.get("status"),
            "team_id": team.get("officialId"), "team_name": team.get("fullName"), "stat_value": r.get("statValue"),
            "games_played": r.get("gamesPlayed"), "seasons_played": r.get("seasonsPlayed"),
            "avg_per_season": r.get("avgPerSeason"),
        })  # fmt: skip
    return _frame(rows, CAREER_LEADERS_SCHEMA, return_as_pandas)


def parse_pll_player(raw: Any, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """One row: a player's profile (bio, team, position, college, hometown, seasons played).

    Args:
        raw: The ``players/{player_slug}`` answer.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A frame with :data:`PLAYER_SCHEMA` columns (one row, or none for an empty answer).
    """
    p = _d(raw)
    if not p.get("officialId"):
        return _frame([], PLAYER_SCHEMA, return_as_pandas)
    team = _d(p.get("team"))
    row = {
        **_bio(p), "team_id": team.get("officialId"), "team_name": team.get("fullName"),
        "position": team.get("position"), "position_name": team.get("positionName"),
        "jersey_number": team.get("jerseyNum"), "all_years": p.get("allYears"), "bio": p.get("bio"),
    }  # fmt: skip
    return _frame([row], PLAYER_SCHEMA, return_as_pandas)


def parse_pll_player_seasons(raw: Any, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """One row per season and segment a player played, with that stat line.

    Args:
        raw: The ``players/{player_slug}/stats/seasons`` answer.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A frame with :data:`PLAYER_SEASONS_SCHEMA` columns.
    """
    slug = _req(raw).get("player_slug")
    rows = [
        {"player_slug": slug, "season": s.get("year"), "season_segment": s.get("seasonSegment"),
         "team_id": s.get("teamId"), "team_name": s.get("teamName"), **_stats(s.get("stats"), PLAYER_STAT_KEYS)}
        for s in _items(raw)
    ]  # fmt: skip
    return _frame(rows, PLAYER_SEASONS_SCHEMA, return_as_pandas)
