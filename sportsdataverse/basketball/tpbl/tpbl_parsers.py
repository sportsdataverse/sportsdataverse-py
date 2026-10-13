"""Parsers for the generated ``tpbl`` wrappers (TPBL, Taiwan's Taipei Professional Basketball League).

The source is the Laravel JSON API behind tpbl.basketball (``https://api.tpbl.basketball/api``): 11 keyless GET
routes, no play-by-play. Each public ``parse_tpbl_*`` takes what the wrapper returns with ``return_parsed=False``
(the JSON body, a list wrapped as ``{"data": [...]}``, with the request stamped under ``_request`` by
``tpbl_runtime``) or a bare body, and follows the package parser contract: a polars frame by default, pandas with
``return_as_pandas=True``, snake_case columns, and a zero-row frame with the documented columns and dtypes, never an
exception, for an empty or unrecognised input.

**Dtypes** are pinned by column name (:data:`SPECS`), never sampled. The API sends percentages as one-decimal
strings (``"57.1"``) and attendance as a string; the parsers cast at the boundary. Every id is ``Int64``; shirt
numbers stay strings. The ``'made/attempted'`` text columns (``"12/21"``) duplicate the made / attempted counts and
are dropped. Birth dates and contact fields never reach a frame (the runtime also redacts them from the raw body);
referees appear by name only.

**Id key spaces.** TPBL ids and GoHoops ids are different systems; the API echoes both.

* ``season_id``: TPBL seasons (1 = 2024-25, 2 = 2025-26, 3 = 2026-27); the ``event_id`` of ``/events/{id}/*`` is the
  same number. ``season`` is the END year (2026 = 2025-26). ``gohoops_season_id`` / ``gohoops_event_id``: GoHoops
  seasons (101, 213, 275) and events (319, 490, 613), one each per TPBL season.
* ``division_id``: TPBL divisions (one competition phase of one season: preseason, regular season, play-in,
  playoffs, finals). ``gohoops_division_id`` is GoHoops' own.
* ``team_id``: TPBL teams = franchises (2-8), stable across seasons; ``gohoops_team_id`` = ``team_id + 938``.
  ``gohoops_squad_id`` is a team in ONE season (it changes every season).
* ``game_id``: TPBL games, the ``{game_id}`` of ``/games/{game_id}`` and ``.../stats``. Not sequential by date.
  The box score echoes no game id, so the runtime stamps it and ``game_id`` is null on a bare box-score body.
* ``player_id``: TPBL players, one per person across seasons and teams. ``gohoops_person_id`` is the GoHoops person.
  ``gohoops_roster_id`` is a person registered with one squad in one season: it keys the box score players, and it
  changes when the player changes season or team. Join a box score to a roster on ``gohoops_roster_id``, never on
  ``player_id`` alone and never on names.
* ``referee_id``: TPBL referees (``gohoops_referee_id`` GoHoops').

**Sides.** ``side`` is ``home`` / ``away`` in the box-score frames.
"""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import pandas as pd
import polars as pl

__all__ = [
    "parse_tpbl_box_player_periods",
    "parse_tpbl_box_players",
    "parse_tpbl_box_team_periods",
    "parse_tpbl_box_teams",
    "parse_tpbl_divisions",
    "parse_tpbl_game",
    "parse_tpbl_games",
    "parse_tpbl_player_games",
    "parse_tpbl_player_shots",
    "parse_tpbl_referees",
    "parse_tpbl_roster",
    "parse_tpbl_seasons",
    "parse_tpbl_standings",
    "parse_tpbl_standings_against",
    "parse_tpbl_team_shots",
    "parse_tpbl_teams",
]

Frame = Union[pl.DataFrame, pd.DataFrame]
Spec = Dict[str, Any]

_REQUEST = "_request"
N, S, F, B = pl.Int64, pl.String, pl.Float64, pl.Boolean
D, DT = pl.Date, pl.Datetime("us")
_LABEL = re.compile(r"^\s*(\d{4})\s*-\s*(\d{4})")

# Stat columns, in the API's own order. Names are the API's; the text duplicates ('12/21') and the
# placeholders that are always null on a player line are left out.
_PLAYER_STATS = (
    "time_on_court",
    "score",
    "plus_minus",
    "field_goals_made",
    "field_goals_missed",
    "field_goals_attempted",
    "field_goals_percentage",
    "two_pointers_made",
    "two_pointers_missed",
    "two_pointers_attempted",
    "two_pointers_percentage",
    "three_pointers_made",
    "three_pointers_missed",
    "three_pointers_attempted",
    "three_pointers_percentage",
    "free_throws_made",
    "free_throws_missed",
    "free_throws_attempted",
    "free_throws_percentage",
    "rebounds",
    "defensive_rebounds",
    "offensive_rebounds",
    "defensive_fouls",
    "offensive_fouls",
    "personal_fouls",
    "fouls",
    "assists",
    "steals",
    "blocks",
    "turnovers",
    "points_in_paint",
    "second_chance_points",
    "fast_break_points",
    "efficiency",
    "efg",
    "tsp",
    "pir",
    "field_goals_made_in_the_paint",
    "field_goals_missed_in_the_paint",
    "field_goals_attempted_in_the_paint",
    "field_goals_made_on_fast_break",
    "field_goals_missed_on_fast_break",
    "field_goals_attempted_on_fast_break",
    "field_goals_made_on_second_chance",
    "field_goals_missed_on_second_chance",
    "field_goals_attempted_on_second_chance",
    "technical_fouls",
    "unsportsmanlike_fouls",
    "disqualifying_fouls",
    "bad_pass_turnovers",
    "ball_handling_turnovers",
    "out_of_bounds_turnovers",
    "travel_turnovers",
    "three_seconds_turnovers",
    "five_seconds_turnovers",
    "back_court_turnovers",
    "offensive_goal_tending_turnovers",
    "double_dribble_turnovers",
    "carry_turnovers",
    "offensive_foul_turnovers",
    "other_turnovers",
    "points_off_turnovers",
    "two_pointers_jump_shot_made",
    "two_pointers_jump_shot_missed",
    "two_pointers_jump_shot_attempted",
    "three_pointers_jump_shot_made",
    "three_pointers_jump_shot_missed",
    "three_pointers_jump_shot_attempted",
    "two_pointers_layup_made",
    "two_pointers_layup_missed",
    "two_pointers_layup_attempted",
    "two_pointers_driving_layup_made",
    "two_pointers_driving_layup_missed",
    "two_pointers_driving_layup_attempted",
    "two_pointers_dunk_made",
    "two_pointers_dunk_missed",
    "two_pointers_dunk_attempted",
    "two_pointers_putback_dunk_made",
    "two_pointers_putback_dunk_missed",
    "two_pointers_putback_dunk_attempted",
    "two_pointers_putback_tip_in_made",
    "two_pointers_putback_tip_in_missed",
    "two_pointers_putback_tip_in_attempted",
    "two_pointers_alley_oop_made",
    "two_pointers_alley_oop_missed",
    "two_pointers_alley_oop_attempted",
    "two_pointers_hook_shot_made",
    "two_pointers_hook_shot_missed",
    "two_pointers_hook_shot_attempted",
    "three_pointers_hook_shot_made",
    "three_pointers_hook_shot_missed",
    "three_pointers_hook_shot_attempted",
    "two_pointers_floating_jump_shot_made",
    "two_pointers_floating_jump_shot_missed",
    "two_pointers_floating_jump_shot_attempted",
    "three_pointers_floating_jump_shot_made",
    "three_pointers_floating_jump_shot_missed",
    "three_pointers_floating_jump_shot_attempted",
    "two_pointers_fadeaway_jump_shot_made",
    "two_pointers_fadeaway_jump_shot_missed",
    "two_pointers_fadeaway_jump_shot_attempted",
    "three_pointers_fadeaway_jump_shot_made",
    "three_pointers_fadeaway_jump_shot_missed",
    "three_pointers_fadeaway_jump_shot_attempted",
    "two_pointers_turnaround_jump_shot_made",
    "two_pointers_turnaround_jump_shot_missed",
    "two_pointers_turnaround_jump_shot_attempted",
    "three_pointers_turnaround_jump_shot_made",
    "three_pointers_turnaround_jump_shot_missed",
    "three_pointers_turnaround_jump_shot_attempted",
    "two_pointers_step_back_jump_shot_made",
    "two_pointers_step_back_jump_shot_missed",
    "two_pointers_step_back_jump_shot_attempted",
    "three_pointers_step_back_jump_shot_made",
    "three_pointers_step_back_jump_shot_missed",
    "three_pointers_step_back_jump_shot_attempted",
    "two_pointers_pull_up_jump_shot_made",
    "two_pointers_pull_up_jump_shot_missed",
    "two_pointers_pull_up_jump_shot_attempted",
    "three_pointers_pull_up_jump_shot_made",
    "three_pointers_pull_up_jump_shot_missed",
    "three_pointers_pull_up_jump_shot_attempted",
)
_TEAM_TOTAL_STATS = (
    "field_goals_made",
    "field_goals_missed",
    "field_goals_attempted",
    "field_goals_percentage",
    "two_pointers_made",
    "two_pointers_missed",
    "two_pointers_attempted",
    "two_pointers_percentage",
    "three_pointers_made",
    "three_pointers_missed",
    "three_pointers_attempted",
    "three_pointers_percentage",
    "free_throws_made",
    "free_throws_missed",
    "free_throws_attempted",
    "free_throws_percentage",
    "won_score",
    "total_won_score",
    "lost_score",
    "total_lost_score",
    "rebounds",
    "defensive_rebounds",
    "offensive_rebounds",
    "defensive_fouls",
    "offensive_fouls",
    "personal_fouls",
    "fouls",
    "assists",
    "steals",
    "blocks",
    "turnovers",
    "points_in_paint",
    "second_chance_points",
    "fast_break_points",
)
_TEAM_PERIOD_STATS = (
    "won_score",
    "time_on_court",
    "lost_score",
    "total_won_score",
    "total_lost_score",
    "field_goals_made",
    "field_goals_missed",
    "field_goals_attempted",
    "field_goals_percentage",
    "two_pointers_made",
    "two_pointers_missed",
    "two_pointers_attempted",
    "two_pointers_percentage",
    "three_pointers_made",
    "three_pointers_missed",
    "three_pointers_attempted",
    "three_pointers_percentage",
    "free_throws_made",
    "free_throws_missed",
    "free_throws_attempted",
    "free_throws_percentage",
    "rebounds",
    "defensive_rebounds",
    "offensive_rebounds",
    "defensive_fouls",
    "offensive_fouls",
    "personal_fouls",
    "fouls",
    "assists",
    "steals",
    "blocks",
    "turnovers",
    "points_in_paint",
    "second_chance_points",
    "fast_break_points",
)
_LOG_STATS = (
    "score",
    "rebounds",
    "assists",
    "steals",
    "blocks",
    "turnovers",
    "fouls",
    "roster_fouls",
    "bench_fouls",
    "time_on_court",
    "two_pointers_made",
    "two_pointers_missed",
    "two_pointers_attempted",
    "three_pointers_made",
    "three_pointers_missed",
    "three_pointers_attempted",
    "free_throws_made",
    "free_throws_missed",
    "free_throws_attempted",
    "field_goals_made",
    "field_goals_missed",
    "field_goals_attempted",
    "field_goals_made_in_the_paint",
    "field_goals_missed_in_the_paint",
    "field_goals_attempted_in_the_paint",
    "field_goals_made_on_fast_break",
    "field_goals_missed_on_fast_break",
    "field_goals_attempted_on_fast_break",
    "field_goals_made_on_second_chance",
    "field_goals_missed_on_second_chance",
    "field_goals_attempted_on_second_chance",
    "defensive_rebounds",
    "offensive_rebounds",
    "offensive_fouls",
    "defensive_fouls",
    "personal_fouls",
    "technical_fouls",
    "unsportsmanlike_fouls",
    "disqualifying_fouls",
    "bad_pass_turnovers",
    "ball_handling_turnovers",
    "out_of_bounds_turnovers",
    "travel_turnovers",
    "three_seconds_turnovers",
    "five_seconds_turnovers",
    "back_court_turnovers",
    "offensive_goal_tending_turnovers",
    "double_dribble_turnovers",
    "carry_turnovers",
    "offensive_foul_turnovers",
    "other_turnovers",
    "plus_minus",
    "fast_break_points",
    "second_chance_points",
    "points_in_paint",
    "points_off_turnovers",
    "two_pointers_jump_shot_made",
    "two_pointers_jump_shot_missed",
    "two_pointers_jump_shot_attempted",
    "three_pointers_jump_shot_made",
    "three_pointers_jump_shot_missed",
    "three_pointers_jump_shot_attempted",
    "two_pointers_layup_made",
    "two_pointers_layup_missed",
    "two_pointers_layup_attempted",
    "two_pointers_driving_layup_made",
    "two_pointers_driving_layup_missed",
    "two_pointers_driving_layup_attempted",
    "two_pointers_dunk_made",
    "two_pointers_dunk_missed",
    "two_pointers_dunk_attempted",
    "two_pointers_putback_dunk_made",
    "two_pointers_putback_dunk_missed",
    "two_pointers_putback_dunk_attempted",
    "two_pointers_putback_tip_in_made",
    "two_pointers_putback_tip_in_missed",
    "two_pointers_putback_tip_in_attempted",
    "two_pointers_alley_oop_made",
    "two_pointers_alley_oop_missed",
    "two_pointers_alley_oop_attempted",
    "two_pointers_hook_shot_made",
    "two_pointers_hook_shot_missed",
    "two_pointers_hook_shot_attempted",
    "three_pointers_hook_shot_made",
    "three_pointers_hook_shot_missed",
    "three_pointers_hook_shot_attempted",
    "two_pointers_floating_jump_shot_made",
    "two_pointers_floating_jump_shot_missed",
    "two_pointers_floating_jump_shot_attempted",
    "three_pointers_floating_jump_shot_made",
    "three_pointers_floating_jump_shot_missed",
    "three_pointers_floating_jump_shot_attempted",
    "two_pointers_fadeaway_jump_shot_made",
    "two_pointers_fadeaway_jump_shot_missed",
    "two_pointers_fadeaway_jump_shot_attempted",
    "three_pointers_fadeaway_jump_shot_made",
    "three_pointers_fadeaway_jump_shot_missed",
    "three_pointers_fadeaway_jump_shot_attempted",
    "two_pointers_turnaround_jump_shot_made",
    "two_pointers_turnaround_jump_shot_missed",
    "two_pointers_turnaround_jump_shot_attempted",
    "three_pointers_turnaround_jump_shot_made",
    "three_pointers_turnaround_jump_shot_missed",
    "three_pointers_turnaround_jump_shot_attempted",
    "two_pointers_step_back_jump_shot_made",
    "two_pointers_step_back_jump_shot_missed",
    "two_pointers_step_back_jump_shot_attempted",
    "three_pointers_step_back_jump_shot_made",
    "three_pointers_step_back_jump_shot_missed",
    "three_pointers_step_back_jump_shot_attempted",
    "two_pointers_pull_up_jump_shot_made",
    "two_pointers_pull_up_jump_shot_missed",
    "two_pointers_pull_up_jump_shot_attempted",
    "three_pointers_pull_up_jump_shot_made",
    "three_pointers_pull_up_jump_shot_missed",
    "three_pointers_pull_up_jump_shot_attempted",
    "efficiency",
    "pir",
)
_LOG_PERCENTAGES = (
    "two_pointers_percentage",
    "three_pointers_percentage",
    "free_throws_percentage",
    "field_goals_percentage",
    "turnovers_percentage",
    "efg",
    "tsp",
)


def _stat_dtype(name: str) -> Any:
    """Counts are ``Int64``; the percentage columns (one-decimal strings on the wire) are ``Float64``."""
    return F if name.endswith("_percentage") or name in ("efg", "tsp") else N


def _stats_spec(names: Sequence[str]) -> Spec:
    return {n: _stat_dtype(n) for n in names}


_SEASONS_SPEC: Spec = {
    "season": N,
    "season_id": N,
    "gohoops_season_id": N,
    "name": S,
    "status": S,
    "started_at": S,
    "ended_at": S,
}
_DIVISIONS_SPEC: Spec = {
    "season": N,
    "season_id": N,
    "division_id": N,
    "gohoops_division_id": N,
    "gohoops_event_id": N,
    "name": S,
    "status": S,
}
_TEAMS_SPEC: Spec = {
    "season": N,
    "season_id": N,
    "team_id": N,
    "gohoops_team_id": N,
    "name": S,
    "is_real_team": B,
    "status": S,
}
_GAMES_SPEC: Spec = {
    "season": N,
    "season_id": N,
    "game_id": N,
    "gohoops_game_id": N,
    "division_id": N,
    "gohoops_division_id": N,
    "gohoops_season_id": N,
    "gohoops_event_id": N,
    "code": S,
    "status": S,
    "is_live": B,
    "periods": N,
    "game_date": D,
    "game_day_of_week": S,
    "game_time": S,
    "tipoff": DT,
    "venue": S,
    "attendance": N,
    "home_team_id": N,
    "home_gohoops_team_id": N,
    "home_gohoops_squad_id": N,
    "home_team": S,
    "home_alt_name": S,
    "home_score": N,
    "away_team_id": N,
    "away_gohoops_team_id": N,
    "away_gohoops_squad_id": N,
    "away_team": S,
    "away_alt_name": S,
    "away_score": N,
    "recap_url": S,
    "live_stream_url": S,
    "judgement_report_url": S,
    "ticket_url": S,
}
_REFEREES_SPEC: Spec = {
    "game_id": N,
    "sequence": N,
    "referee_id": N,
    "gohoops_referee_id": N,
    "name": S,
    "role": S,
}
_STANDINGS_SPEC: Spec = {
    "division_id": N,
    "rank": N,
    "team_id": N,
    "gohoops_team_id": N,
    "gohoops_squad_id": N,
    "team": S,
    "alt_name": S,
    "won": N,
    "lost": N,
    "drawn": N,
    "games_behind": F,
    "streak": S,
}
_AGAINST_SPEC: Spec = {
    "division_id": N,
    "team_id": N,
    "opponent_team_id": N,
    "opponent_gohoops_team_id": N,
    "opponent": S,
    "won": N,
    "lost": N,
    "drawn": N,
}
_BOX_PLAYER_ID: Spec = {
    "game_id": N,
    "side": S,
    "team_id": N,
    "team": S,
    "gohoops_squad_id": N,
}
_BOX_PLAYER_WHO: Spec = {
    "player_id": N,
    "gohoops_person_id": N,
    "gohoops_roster_id": N,
    "player": S,
    "number": S,
    "position": S,
    "status": S,
    "roster_status": S,
    "is_starting": B,
    "entered": B,
}
_BOX_PLAYERS_SPEC: Spec = {**_BOX_PLAYER_ID, **_BOX_PLAYER_WHO, **_stats_spec(_PLAYER_STATS)}
# A quarter line carries no whole-game ``efficiency`` (it has pir, efg and tsp), so the column is left out.
_PERIOD_PLAYER_STATS = tuple(n for n in _PLAYER_STATS if n != "efficiency")
_BOX_PLAYER_PERIODS_SPEC: Spec = {
    **_BOX_PLAYER_ID,
    "period": N,
    **_BOX_PLAYER_WHO,
    **_stats_spec(_PERIOD_PLAYER_STATS),
}
_BOX_TEAMS_SPEC: Spec = {**_BOX_PLAYER_ID, "opponent_team_id": N, **_stats_spec(_TEAM_TOTAL_STATS)}
_BOX_TEAM_PERIODS_SPEC: Spec = {**_BOX_PLAYER_ID, "period": N, **_stats_spec(_TEAM_PERIOD_STATS)}
_ROSTER_SPEC: Spec = {
    "division_id": N,
    "team_id": N,
    "team": S,
    "player_id": N,
    "gohoops_person_id": N,
    "gohoops_roster_id": N,
    "gohoops_squad_id": N,
    "gohoops_event_id": N,
    "gohoops_season_id": N,
    "player": S,
    "alt_name": S,
    "number": S,
    "position": S,
    "height_cm": N,
    "weight_kg": F,
    "nationality": S,
    "national_identity": S,
    "national_identity_detail": S,
    "registered": B,
    "is_listing": B,
}
_SHOT: Spec = {"rounds": S, "is_made": B, "point_x": N, "point_y": N, "offense_basket": N}
_TEAM_SHOTS_SPEC: Spec = {"division_id": N, "team_id": N, **_SHOT}
_PLAYER_SHOTS_SPEC: Spec = {"division_id": N, "player_id": N, **{k: v for k, v in _SHOT.items() if k != "rounds"}}
_PLAYER_GAMES_SPEC: Spec = {
    "player_id": N,
    "gohoops_person_id": N,
    "gohoops_roster_id": N,
    "player": S,
    "division_id": N,
    "gohoops_division_id": N,
    "team_id": N,
    "team": S,
    "game_id": N,
    "gohoops_game_id": N,
    "game_title": S,
    "home_team_id": N,
    "away_team_id": N,
    "home_score": N,
    "away_score": N,
    "venue": S,
    "tipoff": DT,
    "periods_played": N,
    **_stats_spec(_LOG_STATS),
    **_stats_spec(_LOG_PERCENTAGES),
}

#: wrapper short -> {column: dtype}: the documented schema of each parser.
SPECS: Dict[str, Spec] = {
    "seasons": _SEASONS_SPEC,
    "divisions": _DIVISIONS_SPEC,
    "teams": _TEAMS_SPEC,
    "games": _GAMES_SPEC,
    "game": _GAMES_SPEC,
    "referees": _REFEREES_SPEC,
    "standings": _STANDINGS_SPEC,
    "standings_against": _AGAINST_SPEC,
    "box_players": _BOX_PLAYERS_SPEC,
    "box_player_periods": _BOX_PLAYER_PERIODS_SPEC,
    "box_teams": _BOX_TEAMS_SPEC,
    "box_team_periods": _BOX_TEAM_PERIODS_SPEC,
    "roster": _ROSTER_SPEC,
    "team_shots": _TEAM_SHOTS_SPEC,
    "player_shots": _PLAYER_SHOTS_SPEC,
    "player_games": _PLAYER_GAMES_SPEC,
}


# ----------------------------------------------------------------------------- coercion helpers
def _i(v: Any) -> Optional[int]:
    """An int from an int, an integral float or a digit string; ``None`` otherwise (a bool is not an id)."""
    if isinstance(v, bool) or v is None:
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v) if v.is_integer() else None
    if isinstance(v, str) and re.fullmatch(r"-?\d+", v.strip()):
        return int(v.strip())
    return None


def _f(v: Any) -> Optional[float]:
    if isinstance(v, bool) or v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        try:
            return float(v.strip().rstrip("%"))
        except ValueError:
            return None
    return None


def _s(v: Any) -> Optional[str]:
    return v if isinstance(v, str) else None


def _b(v: Any) -> Optional[bool]:
    return v if isinstance(v, bool) else None


def _date(v: Any) -> Optional[date]:
    try:
        return datetime.strptime(v, "%Y-%m-%d").date() if isinstance(v, str) else None
    except ValueError:
        return None


def _datetime(v: Any) -> Optional[datetime]:
    try:
        return datetime.strptime(v, "%Y-%m-%d %H:%M:%S") if isinstance(v, str) else None
    except ValueError:
        return None


def _at(d: Any, *path: str) -> Any:
    for k in path:
        if not isinstance(d, dict):
            return None
        d = d.get(k)
    return d


def _dict(v: Any) -> Dict[str, Any]:
    return v if isinstance(v, dict) else {}


def _frame(rows: List[Dict[str, Any]], spec: Spec, columns: Optional[Sequence[str]], as_pandas: bool) -> Frame:
    """``rows`` as a frame with exactly ``spec``'s columns and dtypes (a zero-row frame when there are none).

    ``columns`` is the wrapper's documented column list; a name the spec lacks is added as a null string column.
    """
    full: Dict[str, Any] = dict(spec)
    for c in columns or ():
        full.setdefault(c, S)
    df = pl.DataFrame({c: [r.get(c) for r in rows] for c in full}, schema=full)
    return df.to_pandas() if as_pandas else df


def _body(raw: Any) -> Tuple[Any, Dict[str, Any]]:
    """``(body, request)``: a wrapped list or dict body with its ``_request`` stamp, or a bare body with ``{}``."""
    if isinstance(raw, dict):
        req = raw.get(_REQUEST)
        req = req if isinstance(req, dict) else {}
        if isinstance(raw.get("data"), list) and set(raw) <= {"data", _REQUEST}:
            return raw["data"], req
        return raw, req
    return raw, {}


def _list(raw: Any) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    body, req = _body(raw)
    return ([r for r in body if isinstance(r, dict)] if isinstance(body, list) else []), req


def _season_ctx(req: Dict[str, Any]) -> Dict[str, Any]:
    return {"season": _i(req.get("season")), "season_id": _i(req.get("season_id"))}


# ----------------------------------------------------------------------------- season routes
def parse_tpbl_seasons(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Every TPBL season (``/seasons``).

    Args:
        raw: the body ``tpbl_seasons(..., return_parsed=False)`` returns.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per season. ``season_id`` is the TPBL id the other routes take (the ``event_id`` is the same
        number), ``gohoops_season_id`` GoHoops' own, ``season`` the END year read from the label
        ``2024-2025 賽季`` (2025). ``started_at`` / ``ended_at`` stay ``YYYY-MM-DD HH:MM:SS`` text. A body
        without rows gives a zero-row frame.

    Raises:
        None: bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.tpbl import tpbl_seasons

            seasons = tpbl_seasons()
    """
    rows, _ = _list(raw)
    out = []
    for r in rows:
        m = _LABEL.match(r["name"]) if isinstance(r.get("name"), str) else None
        out.append(
            {
                "season": int(m.group(2)) if m else None,
                "season_id": _i(r.get("id")),
                "gohoops_season_id": _i(r.get("gohoops_id")),
                "name": _s(r.get("name")),
                "status": _s(r.get("status")),
                "started_at": _s(r.get("started_at")),
                "ended_at": _s(r.get("ended_at")),
            }
        )
    return _frame(out, _SEASONS_SPEC, columns, return_as_pandas)


def parse_tpbl_divisions(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """The divisions (competition phases) of one season (``/events/{season_id}/divisions``).

    Args:
        raw: the body ``tpbl_divisions(..., return_parsed=False)`` returns (with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per division: ``division_id`` (the id every division route takes), ``gohoops_division_id``,
        ``gohoops_event_id`` and the Traditional Chinese ``name`` (熱身賽 preseason, 例行賽 regular season,
        季後挑戰賽 postseason challenge / play-in, 季後賽 playoffs, 冠軍賽 finals). ``season`` and ``season_id``
        come from the stamp (null on a bare body). A body without rows gives a zero-row frame.

    Raises:
        None: bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.tpbl import tpbl_divisions

            divisions = tpbl_divisions(season=2026)
    """
    rows, req = _list(raw)
    ctx = _season_ctx(req)
    out = [
        {
            **ctx,
            "division_id": _i(r.get("id")),
            "gohoops_division_id": _i(r.get("gohoops_id")),
            "gohoops_event_id": _i(r.get("gohoops_event_id")),
            "name": _s(r.get("name")),
            "status": _s(r.get("status")),
        }
        for r in rows
    ]
    return _frame(out, _DIVISIONS_SPEC, columns, return_as_pandas)


def parse_tpbl_teams(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """The teams of one season (``/events/{season_id}/teams``).

    Args:
        raw: the body ``tpbl_teams(..., return_parsed=False)`` returns (with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per team. ``is_real_team`` is false for a bracket placeholder (id 67 '高順位隊伍' = higher-seed
        team, 68 '低順位隊伍' = lower-seed team in 2025-26). ``team_id`` is the franchise id, stable across seasons.
        A body without rows gives a zero-row frame.

    Raises:
        None: bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.tpbl import tpbl_teams

            teams = tpbl_teams(season=2026)
    """
    rows, req = _list(raw)
    ctx = _season_ctx(req)
    out = [
        {
            **ctx,
            "team_id": _i(r.get("id")),
            "gohoops_team_id": _i(r.get("gohoops_id")),
            "name": _s(r.get("name")),
            "is_real_team": _b(r.get("is_real_team")),
            "status": _s(r.get("status")),
        }
        for r in rows
    ]
    return _frame(out, _TEAMS_SPEC, columns, return_as_pandas)


def _game_row(g: Dict[str, Any], ctx: Dict[str, Any]) -> Dict[str, Any]:
    meta = _dict(g.get("meta"))
    home, away = _dict(g.get("home_team")), _dict(g.get("away_team"))
    visitors = meta.get("visitors")
    return {
        **ctx,
        "game_id": _i(g.get("id")),
        "gohoops_game_id": _i(g.get("gohoops_id")),
        "division_id": _i(g.get("division_id")),
        "gohoops_division_id": _i(g.get("gohoops_division_id")),
        "gohoops_season_id": _i(home.get("gohoops_season_id")),
        "gohoops_event_id": _i(home.get("gohoops_event_id")),
        "code": _s(g.get("code")),
        "status": _s(g.get("status")),
        "is_live": _b(g.get("is_live")),
        "periods": _i(g.get("round")),
        "game_date": _date(g.get("game_date")),
        "game_day_of_week": _s(g.get("game_day_of_week")),
        "game_time": _s(g.get("game_time")),
        "tipoff": _datetime(g.get("gamed_at")),
        "venue": _s(g.get("venue")),
        "attendance": _i(visitors),
        "home_team_id": _i(home.get("id")),
        "home_gohoops_team_id": _i(home.get("gohoops_id")),
        "home_gohoops_squad_id": _i(home.get("gohoops_squad_id")),
        "home_team": _s(home.get("name")),
        "home_alt_name": _s(_at(home, "meta", "alt_name")),
        "home_score": _i(home.get("won_score")),
        "away_team_id": _i(away.get("id")),
        "away_gohoops_team_id": _i(away.get("gohoops_id")),
        "away_gohoops_squad_id": _i(away.get("gohoops_squad_id")),
        "away_team": _s(away.get("name")),
        "away_alt_name": _s(_at(away, "meta", "alt_name")),
        "away_score": _i(away.get("won_score")),
        "recap_url": _s(meta.get("recap")),
        "live_stream_url": _s(meta.get("live_stream_url")),
        "judgement_report_url": _s(meta.get("judgement_report")),
        "ticket_url": _s(meta.get("ticket_url")),
    }


def parse_tpbl_games(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Every game of one season, all divisions (``/seasons/{season_id}/games``).

    Args:
        raw: the body ``tpbl_games(..., return_parsed=False)`` returns (with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per game, played and scheduled mixed: ``status`` is COMPLETED or NOT_STARTED and the scores are null
        before tip-off. ``game_id`` is the id of ``tpbl_game`` / ``tpbl_box_*``, ``division_id`` the id of the
        division routes, ``periods`` the number of quarters played (4 regulation, 5 or 6 with overtime, 0 before
        tip-off). ``tipoff`` is a naive timestamp in Taiwan local time (UTC+8 by every tip-off seen; the API sends no
        zone); ``attendance`` is the API's ``visitors`` string as an integer. Team names are Traditional Chinese with
        the sponsor; ``*_alt_name`` is the lowercase English slug. A body without rows gives a zero-row frame.

    Raises:
        None: bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.tpbl import tpbl_games

            games = tpbl_games(season=2026)
    """
    rows, req = _list(raw)
    ctx = _season_ctx(req)
    return _frame([_game_row(g, ctx) for g in rows], _GAMES_SPEC, columns, return_as_pandas)


def parse_tpbl_game(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """One finished game's header (``/games/{game_id}``).

    Args:
        raw: the body ``tpbl_game(..., return_parsed=False)`` returns (with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row with the columns of ``tpbl_games`` (so the two stack). ``season`` and ``season_id`` are null here: a
        game detail does not name its season (``gohoops_season_id`` does). The recap, live-stream and
        judgement-report links are filled on a detail and may be missing on a list row. The referees are in
        ``tpbl_referees``. A body without a game gives a zero-row frame.

    Raises:
        None: bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.tpbl import tpbl_game

            game = tpbl_game(game_id=6)
    """
    body, _ = _body(raw)
    if not isinstance(body, dict) or not isinstance(body.get("id"), int):
        return _frame([], _GAMES_SPEC, columns, return_as_pandas)
    return _frame([_game_row(body, {})], _GAMES_SPEC, columns, return_as_pandas)


def parse_tpbl_referees(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """The officiating crew of one finished game (``/games/{game_id}``).

    Args:
        raw: the body ``tpbl_referees(..., return_parsed=False)`` returns (with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per referee, chief first (``sequence`` is the API's order). ``role`` is the API enum:
        BASKETBALL_CHIEF_REFEREE (crew chief), BASKETBALL_FIRST_ASSISTANT_REFEREE (umpire 1),
        BASKETBALL_SECOND_ASSISTANT_REFEREE (umpire 2). Names only: no contact or birth data is published. A game
        without referees gives a zero-row frame.

    Raises:
        None: bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.tpbl import tpbl_referees

            crew = tpbl_referees(game_id=6)
    """
    body, req = _body(raw)
    refs = body.get("referees") if isinstance(body, dict) else None
    gid = _i(req.get("game_id")) if req.get("game_id") is not None else _i(_at(body, "id"))
    out = [
        {
            "game_id": gid,
            "sequence": n,
            "referee_id": _i(r.get("id")),
            "gohoops_referee_id": _i(r.get("gohoops_id")),
            "name": _s(r.get("name")),
            "role": _s(r.get("type")),
        }
        for n, r in enumerate((r for r in (refs or []) if isinstance(r, dict)), start=1)
    ]
    return _frame(out, _REFEREES_SPEC, columns, return_as_pandas)


def parse_tpbl_standings(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """The standings of one division (``/divisions/{division_id}/games/standings``).

    Args:
        raw: the body ``tpbl_standings(..., return_parsed=False)`` returns (with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per ranked team: ``won`` / ``lost`` / ``drawn`` are the division's games so far (``drawn`` is always
        0 in basketball), ``games_behind`` is the gap to the leader (a whole number in every captured division; read
        as ``Float64`` so half games cannot break the dtype) and ``streak`` is 'W' or 'L' plus the run length
        (W2 = two straight wins). The head-to-head records are in ``tpbl_standings_against``. A body without rows
        gives a zero-row frame.

    Raises:
        None: bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.tpbl import tpbl_standings

            table = tpbl_standings(division_id=2)
    """
    rows, req = _list(raw)
    div = _i(req.get("division_id"))
    out = [
        {
            "division_id": div,
            "rank": _i(r.get("rank")),
            "team_id": _i(_at(r, "team", "id")),
            "gohoops_team_id": _i(_at(r, "team", "gohoops_id")),
            "gohoops_squad_id": _i(_at(r, "team", "gohoops_squad_id")),
            "team": _s(_at(r, "team", "name")),
            "alt_name": _s(_at(r, "team", "meta", "alt_name")),
            "won": _i(r.get("score_won_matches")),
            "lost": _i(r.get("score_lost_matches")),
            "drawn": _i(r.get("score_draw_matches")),
            "games_behind": _f(r.get("games_behind")),
            "streak": _s(r.get("streaks")),
        }
        for r in rows
    ]
    return _frame(out, _STANDINGS_SPEC, columns, return_as_pandas)


def parse_tpbl_standings_against(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """The head-to-head records of one division's standings (``/divisions/{division_id}/games/standings``).

    Args:
        raw: the body ``tpbl_standings_against(..., return_parsed=False)`` returns (with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per (team, opponent): ``won`` / ``lost`` / ``drawn`` are the games ``team_id`` won, lost and drew
        against ``opponent_team_id`` in the division. A body without records gives a zero-row frame.

    Raises:
        None: bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.tpbl import tpbl_standings_against

            h2h = tpbl_standings_against(division_id=2)
    """
    rows, req = _list(raw)
    div = _i(req.get("division_id"))
    out = [
        {
            "division_id": div,
            "team_id": _i(_at(r, "team", "id")),
            "opponent_team_id": _i(_at(a, "team", "id")),
            "opponent_gohoops_team_id": _i(_at(a, "team", "gohoops_id")),
            "opponent": _s(_at(a, "team", "name")),
            "won": _i(a.get("score_won_matches")),
            "lost": _i(a.get("score_lost_matches")),
            "drawn": _i(a.get("score_draw_matches")),
        }
        for r in rows
        for a in (r.get("against_result") or [])
        if isinstance(a, dict)
    ]
    return _frame(out, _AGAINST_SPEC, columns, return_as_pandas)


# ----------------------------------------------------------------------------- box score
_SIDES = ("home", "away")


def _team_head(raw: Any) -> List[Tuple[str, Dict[str, Any], Dict[str, Any], Dict[str, Any]]]:
    """``[(side, team body, identity columns, request)]`` for the two sides of a box score."""
    body, req = _body(raw)
    if not isinstance(body, dict):
        return []
    out = []
    for side in _SIDES:
        t = body.get(f"{side}_team")
        if not isinstance(t, dict):
            continue
        ident = {
            "game_id": _i(req.get("game_id")),
            "side": side,
            "team_id": _i(t.get("id")),
            "team": _s(t.get("name")),
            "gohoops_squad_id": _i(t.get("gohoops_squad_id")),
        }
        out.append((side, t, ident, req))
    return out


def _player_row(ident: Dict[str, Any], rid: Any, line: Dict[str, Any]) -> Dict[str, Any]:
    row: Dict[str, Any] = {
        **ident,
        "player_id": _i(line.get("id")),
        "gohoops_person_id": _i(line.get("gohoops_id")),
        "gohoops_roster_id": _i(line.get("gohoops_roster_id"))
        if line.get("gohoops_roster_id") is not None
        else _i(rid),
        "player": _s(line.get("name")),
        "number": _s(line.get("number")),
        "position": _s(line.get("positions")),
        "status": _s(line.get("status")),
        "roster_status": _s(line.get("roster_status")),
        "is_starting": _b(line.get("is_starting")),
        "entered": "time_on_court" in line,
    }
    for n in _PLAYER_STATS:
        v = line.get(n)
        row[n] = _f(v) if _stat_dtype(n) is F else _i(v)
    return row


def _stats_row(line: Dict[str, Any], names: Sequence[str]) -> Dict[str, Any]:
    return {n: (_f(line.get(n)) if _stat_dtype(n) is F else _i(line.get(n))) for n in names}


def parse_tpbl_box_players(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Whole-game player lines of one finished game (``/games/{game_id}/stats``).

    Args:
        raw: the body ``tpbl_box_players(..., return_parsed=False)`` returns (with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per player on either team sheet, home first. ``gohoops_roster_id`` is the key the box score uses
        (join it to ``tpbl_roster.gohoops_roster_id``); ``player_id`` is the TPBL player. A player who did not
        enter has ``entered`` false and null stats (the line then carries identity only); ``time_on_court`` is
        seconds. Percentages are 0-100 floats; the 'made/attempted' text columns are dropped. ``status`` is the
        player's registration as of the request, not at game time ('withdrawn' = the player left the squad later
        and may well have played). A body without players gives a zero-row frame.

    Raises:
        None: bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.tpbl import tpbl_box_players

            box = tpbl_box_players(game_id=6)
    """
    rows = []
    for _side, t, ident, _req in _team_head(raw):
        players = _dict(_at(t, "players", "total"))
        rows += [_player_row(ident, rid, line) for rid, line in players.items() if isinstance(line, dict)]
    return _frame(rows, _BOX_PLAYERS_SPEC, columns, return_as_pandas)


def parse_tpbl_box_player_periods(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Per-quarter player lines of one finished game (``/games/{game_id}/stats``).

    Args:
        raw: the body ``tpbl_box_player_periods(..., return_parsed=False)`` returns (with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per player and quarter in which he was on court (a quarter line without stats is skipped), with the
        columns of ``tpbl_box_players`` plus ``period``: 1-4 regulation, 5 the first overtime, 6 the second. The
        periods of a player add up to his whole-game line. A body without lines gives a zero-row frame.

    Raises:
        None: bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.tpbl import tpbl_box_player_periods

            quarters = tpbl_box_player_periods(game_id=6)
    """
    rows = []
    for _side, t, ident, _req in _team_head(raw):
        for q, lines in _dict(_at(t, "players", "rounds")).items():
            for rid, line in _dict(lines).items():
                if isinstance(line, dict) and "time_on_court" in line:
                    rows.append({**_player_row(ident, rid, line), "period": _i(q)})
    return _frame(rows, _BOX_PLAYER_PERIODS_SPEC, columns, return_as_pandas)


def parse_tpbl_box_teams(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Whole-game team lines of one finished game (``/games/{game_id}/stats``).

    Args:
        raw: the body ``tpbl_box_teams(..., return_parsed=False)`` returns (with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        Two rows, home then away. ``won_score`` is the final score, ``lost_score`` the opponent's. The team line has
        fewer stat columns than a player line (no turnover types or shot types). Percentages are 0-100 floats. A body
        without teams gives a zero-row frame.

    Raises:
        None: bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.tpbl import tpbl_box_teams

            totals = tpbl_box_teams(game_id=6)
    """
    heads = _team_head(raw)
    opp = {h[0]: h[2]["team_id"] for h in heads}
    rows = []
    for side, t, ident, _req in heads:
        other = opp.get("away" if side == "home" else "home")
        total = _dict(_at(t, "teams", "total"))
        rows.append({**ident, "opponent_team_id": other, **_stats_row(total, _TEAM_TOTAL_STATS)})
    return _frame(rows, _BOX_TEAMS_SPEC, columns, return_as_pandas)


def parse_tpbl_box_team_periods(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Per-quarter team lines of one finished game (``/games/{game_id}/stats``).

    Args:
        raw: the body ``tpbl_box_team_periods(..., return_parsed=False)`` returns (with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per team and quarter (``period`` 1-4, 5 and 6 for overtimes). ``won_score`` / ``lost_score`` are
        the points scored and conceded IN the quarter; ``total_won_score`` / ``total_lost_score`` are running
        totals (the last quarter's ``total_won_score`` is the final score); ``time_on_court`` is the summed seconds
        of the five players on court. A body without lines gives a zero-row frame.

    Raises:
        None: bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.tpbl import tpbl_box_team_periods

            quarters = tpbl_box_team_periods(game_id=6)
    """
    rows = []
    for _side, t, ident, _req in _team_head(raw):
        for q, line in _dict(_at(t, "teams", "rounds")).items():
            if isinstance(line, dict):
                rows.append({**ident, "period": _i(q), **_stats_row(line, _TEAM_PERIOD_STATS)})
    return _frame(rows, _BOX_TEAM_PERIODS_SPEC, columns, return_as_pandas)


# ----------------------------------------------------------------------------- division and player routes
def parse_tpbl_roster(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """The roster of one team in one division (``/divisions/{division_id}/teams/{team_id}/players``).

    Args:
        raw: the body ``tpbl_roster(..., return_parsed=False)`` returns (with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per registered player. Birth dates are dropped (personal data). ``gohoops_roster_id`` is the key of
        ``tpbl_box_players``. A roster is not everyone who played: players who left the squad are missing here and
        appear in the box scores with status 'withdrawn' (division 2 lists 13 registered players for team 4 while 22
        appeared in its box scores). ``position`` is the API's PascalCase enum (PointGuardAndShootingGuard, ...),
        '' when unset. A body without rows gives a zero-row frame.

    Raises:
        None: bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.tpbl import tpbl_roster

            roster = tpbl_roster(division_id=2, team_id=4)
    """
    rows, req = _list(raw)
    ctx = {"division_id": _i(req.get("division_id")), "team_id_req": _i(req.get("team_id"))}
    out = []
    for r in rows:
        meta = _dict(r.get("meta"))
        out.append(
            {
                "division_id": ctx["division_id"],
                "team_id": _i(_at(r, "team", "id")) if _at(r, "team", "id") is not None else ctx["team_id_req"],
                "team": _s(_at(r, "team", "name")),
                "player_id": _i(r.get("id")),
                "gohoops_person_id": _i(r.get("gohoops_id")),
                "gohoops_roster_id": _i(r.get("gohoops_roster_id")),
                "gohoops_squad_id": _i(r.get("gohoops_squad_id")),
                "gohoops_event_id": _i(r.get("gohoops_event_id")),
                "gohoops_season_id": _i(r.get("gohoops_season_id")),
                "player": _s(r.get("name")),
                "alt_name": _s(meta.get("alt_name")),
                "number": _s(r.get("number")),
                "position": _s(meta.get("position")),
                "height_cm": _i(meta.get("height")),
                "weight_kg": _f(meta.get("weight")),
                "nationality": _s(meta.get("nationality")),
                "national_identity": _s(meta.get("national_identity")),
                "national_identity_detail": _s(meta.get("national_identity_extra_detail")),
                "registered": _b(r.get("registered")),
                "is_listing": _b(r.get("is_listing")),
            }
        )
    return _frame(out, _ROSTER_SPEC, columns, return_as_pandas)


def _shots(raw: Any, spec: Spec, keys: Dict[str, Any], columns: Optional[Sequence[str]], as_pandas: bool) -> Frame:
    rows, req = _list(raw)
    rounds = req.get("rounds")
    ctx: Dict[str, Any] = {k: _i(req.get(v)) for k, v in keys.items()}
    if "rounds" in spec:
        ctx["rounds"] = ",".join(str(q) for q in rounds) if isinstance(rounds, list) and rounds else None
    out = [
        {
            **ctx,
            "is_made": _b(r.get("is_made")),
            "point_x": _i(r.get("point_x")),
            "point_y": _i(r.get("point_y")),
            "offense_basket": _i(r.get("offense_basket")),
        }
        for r in rows
    ]
    return _frame(out, spec, columns, as_pandas)


def parse_tpbl_team_shots(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Every shot location of one team in one division (``/divisions/{division_id}/teams/{team_id}/shooting-spots``).

    Args:
        raw: the body ``tpbl_team_shots(..., return_parsed=False)`` returns (with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per field-goal attempt of the team (its offense only). ``point_x`` is millimetres along the court
        length (0-28000), ``point_y`` millimetres across (0-15000); ``offense_basket`` 0 attacks the hoop at
        x = 1575, 1 the hoop at x = 26425. Which baseline is x = 0 and which sideline is y = 0 are unverified. There
        is NO shooter, game, quarter or time field, and the body names no team or division, so ``division_id`` and
        ``team_id`` come from the request stamp (null on a bare body). ``rounds`` is the quarter filter asked for
        ('1,2'), null for all quarters. A body without shots gives a zero-row frame.

    Raises:
        None: bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.tpbl import tpbl_team_shots

            shots = tpbl_team_shots(division_id=2, team_id=4, rounds=[1])
    """
    return _shots(
        raw, _TEAM_SHOTS_SPEC, {"division_id": "division_id", "team_id": "team_id"}, columns, return_as_pandas
    )


def parse_tpbl_player_shots(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Every shot location of one player in one division (``/divisions/{division_id}/players/{player_id}/shooting-spots``).

    Args:
        raw: the body ``tpbl_player_shots(..., return_parsed=False)`` returns (with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per field-goal attempt of the player, in the shape of ``tpbl_team_shots`` without ``rounds``. The
        body names no player or division, so ``division_id`` and ``player_id`` come from the request stamp (null on
        a bare body). A body without shots gives a zero-row frame.

    Raises:
        None: bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.tpbl import tpbl_player_shots

            shots = tpbl_player_shots(division_id=2, player_id=75)
    """
    return _shots(
        raw, _PLAYER_SHOTS_SPEC, {"division_id": "division_id", "player_id": "player_id"}, columns, return_as_pandas
    )


def parse_tpbl_player_games(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """A player's game log in one division, one row per game (``/players/{player_id}/games/stats``).

    Args:
        raw: the body ``tpbl_player_games(..., return_parsed=False)`` returns (with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per game in which the player was on court, with that game's whole line (the API's
        ``accumulated_stats`` and ``percentage_stats``; its ``average_stats`` equal them on a one-game row and are
        dropped). ``team_id`` is the team the player played for in that game; ``gohoops_roster_id`` is his roster in
        the division. The API's ``performance_index_rating``, ``effective_field_goals_percentage`` and
        ``true_shooting_percentage`` are named ``pir``, ``efg`` and ``tsp`` as in ``tpbl_box_players``. Birth dates
        and the career note are not kept. A body without rows gives a zero-row frame.

    Raises:
        None: bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.tpbl import tpbl_player_games

            log = tpbl_player_games(player_id=75, division_id=2)
    """
    rows, req = _list(raw)
    div = _i(req.get("division_id"))
    renames = {
        "performance_index_rating": "pir",
        "effective_field_goals_percentage": "efg",
        "true_shooting_percentage": "tsp",
    }
    out = []
    for r in rows:
        game = _dict(r.get("game"))
        player = _dict(r.get("player"))
        stats = {
            renames.get(k, k): v
            for k, v in {**_dict(r.get("accumulated_stats")), **_dict(r.get("percentage_stats"))}.items()
        }
        row: Dict[str, Any] = {
            "player_id": _i(player.get("id")),
            "gohoops_person_id": _i(player.get("gohoops_id")),
            "gohoops_roster_id": _i(player.get("gohoops_roster_id")),
            "player": _s(player.get("name")),
            "division_id": div,
            "gohoops_division_id": _i(game.get("gohoops_division_id")),
            "team_id": _i(_at(r, "team", "id")),
            "team": _s(_at(r, "team", "name")),
            "game_id": _i(game.get("id")),
            "gohoops_game_id": _i(game.get("gohoops_id")),
            "game_title": _s(game.get("title")),
            "home_team_id": _i(_at(game, "home_team", "id")),
            "away_team_id": _i(_at(game, "away_team", "id")),
            "home_score": _i(_at(game, "home_team", "won_score")),
            "away_score": _i(_at(game, "away_team", "won_score")),
            "venue": _s(game.get("location")),
            "tipoff": _datetime(game.get("gamed_at")),
            "periods_played": _i(r.get("round_count")),
        }
        row.update(_stats_row(stats, [*_LOG_STATS, *_LOG_PERCENTAGES]))
        out.append(row)
    return _frame(out, _PLAYER_GAMES_SPEC, columns, return_as_pandas)
