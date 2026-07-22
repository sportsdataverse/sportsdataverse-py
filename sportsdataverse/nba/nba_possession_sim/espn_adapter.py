"""ESPN pbp adapter — Site v2 summary plays → possession events (WS4).

stats.nba.com ``playbyplayv3`` only covers NBA/G-League; college and WNBA
pbp ships through the ESPN summary ``plays[]`` shape instead. This adapter
classifies those plays into the SAME possession-event frame the shelf
builder consumes, so every basketball league runs the one engine
(sport-parameterization, again). Rim/mid separation uses the shot subtype
text (Layup/Dunk/Tip = rim) since ESPN carries no shot distance.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Sequence

import polars as pl

_RIM_RE = re.compile(r"(?i)layup|dunk|tip")
_FT_OF_RE = re.compile(r"(\d+) of (\d+)")


def _clock_seconds(display: str) -> float:
    display = (display or "").strip()
    if ":" in display:
        minutes, _, seconds = display.partition(":")
        try:
            return float(minutes) * 60.0 + float(seconds)
        except ValueError:
            return 0.0
    try:
        return float(display)
    except ValueError:
        return 0.0


def espn_summary_to_events(
    summary: Dict[str, Any],
    *,
    game_id: Optional[str] = None,
) -> pl.DataFrame:
    """Classify an ESPN basketball summary's plays into possession events.

    Args:
        summary: A Site v2 ``summary`` payload with top-level ``plays`` and
            a ``header`` (used to resolve the home team id).
        game_id: Override the game id (defaults to the header's event id).

    Returns:
        The same event frame :func:`~sportsdataverse.nba.nba_possession_sim.shelf.possessions_from_pbp`
        emits — ``kind`` outcome/rebound rows with gamestate — so
        :func:`~sportsdataverse.nba.nba_possession_sim.shelf.build_shelf`
        consumes it unchanged.

    Raises:
        ValueError: When the payload has no plays or the home team can't be
            resolved.

    Example:
        One engine, any league::

            import json
            from sportsdataverse.nba.nba_possession_sim import build_shelf
            from sportsdataverse.nba.nba_possession_sim.espn_adapter import (
                espn_summary_to_events,
            )
            summary = json.load(open("summary_wnba.json"))
            shelf = build_shelf(espn_summary_to_events(summary))
    """
    plays: Sequence[Dict[str, Any]] = summary.get("plays") or []
    if not plays:
        raise ValueError("summary has no plays[]")
    competitions = summary.get("header", {}).get("competitions") or [{}]
    competitors = competitions[0].get("competitors") or []
    home_team = next(
        (str(c.get("team", {}).get("id")) for c in competitors if c.get("homeAway") == "home"),
        None,
    )
    if home_team is None:
        raise ValueError("could not resolve the home team from summary header")
    if game_id is None:
        game_id = str(competitions[0].get("id") or summary.get("header", {}).get("id") or "espn")

    rows: List[Dict[str, Any]] = []
    prev_home = prev_away = 0
    pending_trips: Dict[str, Dict[str, Any]] = {}

    def _emit(kind: str, outcome: str, points: int, *, state: Dict[str, Any]) -> None:
        rows.append(
            {
                "game_id": game_id,
                "period": state["period"],
                "clock_seconds": state["clock_seconds"],
                "score_diff": state["diff"],
                "kind": kind,
                "outcome": outcome,
                "team_id": state["team_id"],
                "points": points,
            }
        )

    def _flush(shooter: str) -> None:
        trip = pending_trips.pop(shooter)
        n = min(int(trip["total"]), 3)
        _emit("outcome", f"ft_trip_{n}", int(trip["made"]), state=trip["state"])

    ordered = sorted(plays, key=lambda x: float(x.get("sequenceNumber") or 0))
    for play in ordered:
        type_text = str(play.get("type", {}).get("text") or "")
        text = str(play.get("text") or "")
        team_id = str(play.get("team", {}).get("id") or "")
        period = int(play.get("period", {}).get("number") or 0)
        clock = _clock_seconds(str(play.get("clock", {}).get("displayValue") or ""))
        offense_is_home = team_id == home_team
        diff = float((prev_home - prev_away) if offense_is_home else (prev_away - prev_home))
        state = {
            "period": period,
            "clock_seconds": clock,
            "diff": diff,
            "team_id": int(team_id) if team_id.isdigit() else 0,
        }
        scoring = bool(play.get("scoringPlay"))
        score_value = int(play.get("scoreValue") or 0)

        if "Free Throw" in type_text:
            shooter = str((play.get("participants") or [{}])[0].get("athlete", {}).get("id") or team_id)
            match = _FT_OF_RE.search(type_text)
            idx, total = (int(match.group(1)), int(match.group(2))) if match else (1, 1)
            if idx == 1 or shooter not in pending_trips:
                if shooter in pending_trips:
                    _flush(shooter)
                pending_trips[shooter] = {"total": total, "made": 0, "seen": 0, "state": state}
            trip = pending_trips[shooter]
            trip["made"] += int(scoring)
            trip["seen"] += 1
            if trip["seen"] >= trip["total"]:
                _flush(shooter)
        elif play.get("shootingPlay"):
            attempted = int(play.get("pointsAttempted") or (3 if score_value == 3 else 2))
            if attempted >= 3:
                outcome = "three_make" if scoring else "three_miss"
            elif _RIM_RE.search(type_text):
                outcome = "rim_make" if scoring else "rim_miss"
            else:
                outcome = "mid_make" if scoring else "mid_miss"
            _emit("outcome", outcome, score_value if scoring else 0, state=state)
        elif "Turnover" in type_text or "Turnover" in text:
            _emit("outcome", "tov", 0, state=state)
        elif type_text in ("Offensive Rebound", "Defensive Rebound"):
            _emit(
                "rebound",
                "oreb" if type_text.startswith("Offensive") else "dreb",
                0,
                state=state,
            )

        prev_home = int(play.get("homeScore") or prev_home)
        prev_away = int(play.get("awayScore") or prev_away)
    for shooter in list(pending_trips):
        _flush(shooter)

    schema = {
        "game_id": pl.Utf8,
        "period": pl.Int64,
        "clock_seconds": pl.Float64,
        "score_diff": pl.Float64,
        "kind": pl.Utf8,
        "outcome": pl.Utf8,
        "team_id": pl.Int64,
        "points": pl.Int64,
    }
    return pl.DataFrame(rows, schema=schema) if rows else pl.DataFrame(schema=schema)


def espn_final_total(summary: Dict[str, Any]) -> int:
    """The real final combined score from a summary's play stream.

    Cumulative scores never decrease, so the max total is the final —
    the conservation oracle's right-hand side.

    Args:
        summary: A Site v2 ``summary`` payload with ``plays``.

    Returns:
        Home + away final points.
    """
    best = 0
    for play in summary.get("plays") or []:
        total = int(play.get("homeScore") or 0) + int(play.get("awayScore") or 0)
        best = max(best, total)
    return best


def player_game_logs_from_espn(
    summary: Dict[str, Any],
    *,
    game_id: Optional[str] = None,
) -> pl.DataFrame:
    """Per-player game logs from an ESPN summary's play participants.

    ESPN plays carry athlete ids directly — the scorer is ``participants[0]``,
    the assister ``participants[1]`` on assisted makes, the rebounder /
    turnover committer ``participants[0]`` on their rows — so this builder
    needs no name resolution (cleaner than the v3 description parse).

    Args:
        summary: Site v2 ``summary`` payload with ``plays``.
        game_id: Override the game id (defaults to the header's event id).

    Returns:
        The same schema as
        :func:`~sportsdataverse.nba.nba_possession_sim.shelf.player_game_logs_from_pbp`:
        one row per (game_id, player_id) with ``team_id``, ``fga``, ``fg3a``,
        ``fta``, ``pts``, ``tov``, ``reb``, ``ast``.

    Example:
        WNBA prop surface::

            from sportsdataverse.nba.nba_possession_sim import PlayerAttribution
            logs = player_game_logs_from_espn(summary)
            att = PlayerAttribution.from_logs(logs, home_team_id=..., away_team_id=...)
    """
    plays: Sequence[Dict[str, Any]] = summary.get("plays") or []
    if not plays:
        raise ValueError("summary has no plays[]")
    competitions = summary.get("header", {}).get("competitions") or [{}]
    if game_id is None:
        game_id = str(competitions[0].get("id") or "espn")

    stats: Dict[int, Dict[str, int]] = {}
    teams: Dict[int, int] = {}

    def _bump(athlete: Optional[Dict[str, Any]], team_id: str, stat: str, amount: int = 1) -> None:
        if not athlete:
            return
        raw = str(athlete.get("athlete", {}).get("id") or "")
        if not raw.isdigit():
            return
        pid = int(raw)
        stats.setdefault(pid, {"fga": 0, "fg3a": 0, "fta": 0, "ftm": 0, "pts": 0, "tov": 0, "reb": 0, "ast": 0})
        stats[pid][stat] += amount
        if team_id.isdigit() and pid not in teams:
            teams[pid] = int(team_id)

    for play in plays:
        type_text = str(play.get("type", {}).get("text") or "")
        text = str(play.get("text") or "")
        team_id = str(play.get("team", {}).get("id") or "")
        participants = play.get("participants") or []
        first = participants[0] if participants else None
        scoring = bool(play.get("scoringPlay"))
        score_value = int(play.get("scoreValue") or 0)

        if "Free Throw" in type_text:
            _bump(first, team_id, "fta")
            if scoring:
                _bump(first, team_id, "ftm")
                _bump(first, team_id, "pts", score_value)
        elif play.get("shootingPlay"):
            _bump(first, team_id, "fga")
            if int(play.get("pointsAttempted") or 2) >= 3:
                _bump(first, team_id, "fg3a")
            if scoring:
                _bump(first, team_id, "pts", score_value)
                if len(participants) > 1 and "assist" in text.lower():
                    _bump(participants[1], team_id, "ast")
        elif "Turnover" in type_text or "Turnover" in text:
            _bump(first, team_id, "tov")
        elif "Rebound" in type_text and "Deadball" not in type_text:
            _bump(first, team_id, "reb")

    rows = [{"game_id": game_id, "player_id": pid, "team_id": teams.get(pid), **vals} for pid, vals in stats.items()]
    schema = {
        "game_id": pl.Utf8,
        "player_id": pl.Int64,
        "team_id": pl.Int64,
        "fga": pl.Int64,
        "fg3a": pl.Int64,
        "fta": pl.Int64,
        "ftm": pl.Int64,
        "pts": pl.Int64,
        "tov": pl.Int64,
        "reb": pl.Int64,
        "ast": pl.Int64,
    }
    return pl.DataFrame(rows, schema=schema).sort("player_id") if rows else pl.DataFrame(schema=schema)
