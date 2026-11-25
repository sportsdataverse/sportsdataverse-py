"""Tidy parsers for ESPN golf payloads (``espn_golf_*``).

A golf event is a field of players, not a home/away pair, so the generic ESPN
scoreboard parser does not fit. :func:`parse_golf_leaderboard` returns one row per
player per event from either golf payload that carries the field:

* the Site v2 ``scoreboard`` (``/golf/{league}/scoreboard``): order, to-par and
  round strokes only;
* the ``leaderboard`` (``/golf/leaderboard?league=``): adds the displayed position
  (``T5``), tie flag, player status (``STATUS_CUT``, ``STATUS_WD``), holes completed in
  the current round, total strokes, earnings and cup points.

Columns the payload does not carry are null. Event and player ids are strings.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
import polars as pl

#: Rounds kept as columns. A 54-hole event (LIV) leaves ``round_4`` null. A playoff is not a
#: round: a line ESPN flags ``isPlayoff`` is dropped, whatever its period.
GOLF_ROUNDS = 4

GOLF_LEADERBOARD_SCHEMA: dict[str, Any] = {
    "event_id": pl.String,
    "event_name": pl.String,
    "event_date": pl.String,
    "event_status": pl.String,
    "player_id": pl.String,
    "player_name": pl.String,
    "player_short_name": pl.String,
    "country": pl.String,
    "amateur": pl.Boolean,
    "sort_order": pl.Int64,
    "position": pl.String,
    "position_is_tie": pl.Boolean,
    "player_status": pl.String,
    "thru": pl.Int64,
    "to_par": pl.Int64,
    "to_par_display": pl.String,
    "total_strokes": pl.Int64,
    **{f"round_{i}": pl.Int64 for i in range(1, GOLF_ROUNDS + 1)},
    **{f"round_{i}_to_par": pl.String for i in range(1, GOLF_ROUNDS + 1)},
    "earnings": pl.Float64,
    "cup_points": pl.Float64,
}


def _to_par(display: Any) -> int | None:
    """``"-12"`` -> -12, ``"E"`` -> 0, ``"+3"`` -> 3; anything else (``"-"``, ``"--"``, None) -> None."""
    if not isinstance(display, str):
        return None
    text = display.strip()
    if text.upper() == "E":
        return 0
    try:
        return int(text.lstrip("+"))
    except ValueError:
        return None


def _rounds(linescores: Any) -> dict[str, Any]:
    """``round_<n>`` strokes and ``round_<n>_to_par`` from a competitor's ``linescores``.

    ESPN fills an unplayed round (a missed cut, a round not yet started) with ``value``
    0.0 and to-par ``"-"``; both become null. A round in progress carries the strokes so far
    (``thru`` says how far). Playoff lines (``isPlayoff``) are skipped."""
    out: dict[str, Any] = {}
    for line in linescores if isinstance(linescores, list) else []:
        if not isinstance(line, dict) or line.get("isPlayoff") is True:
            continue
        period = line.get("period")
        if not isinstance(period, int) or not 1 <= period <= GOLF_ROUNDS:
            continue
        value = line.get("value")
        played = isinstance(value, (int, float)) and value > 0
        out[f"round_{period}"] = int(value) if played else None
        out[f"round_{period}_to_par"] = line.get("displayValue") if played else None
    return out


def _stat(statistics: Any, name: str) -> Any:
    for s in statistics if isinstance(statistics, list) else []:
        if isinstance(s, dict) and s.get("name") == name:
            return s.get("value")
    return None


def _dict(value: Any) -> dict:
    """``value`` when it is a dict, else ``{}`` -- a malformed node never raises."""
    return value if isinstance(value, dict) else {}


def _player_row(event: dict, comp: dict) -> dict[str, Any]:
    athlete = _dict(comp.get("athlete"))
    status = _dict(comp.get("status"))
    position = _dict(status.get("position"))
    # A player off the board (missed cut, withdrawn) is listed as "-": no position, no tie.
    shown = position.get("displayName")
    on_board = isinstance(shown, str) and shown not in ("", "-")
    score = comp.get("score")
    # scoreboard ships ``score`` as the to-par string; leaderboard as {value: strokes, displayValue: to-par}
    to_par_display = score.get("displayValue") if isinstance(score, dict) else score
    rounds = _rounds(comp.get("linescores"))
    strokes = score.get("value") if isinstance(score, dict) else None
    if strokes is None:
        played = [v for k, v in rounds.items() if not k.endswith("_to_par") and v is not None]
        strokes = sum(played) if played else None
    player_id = comp.get("id") or athlete.get("id")
    return {
        "event_id": str(event["id"]) if event.get("id") is not None else None,
        "event_name": event.get("name"),
        "event_date": event.get("date"),
        "event_status": _dict(_dict(event.get("status")).get("type")).get("name"),
        "player_id": str(player_id) if player_id is not None else None,
        "player_name": athlete.get("displayName") or athlete.get("fullName"),
        "player_short_name": athlete.get("shortName"),
        "country": _dict(athlete.get("flag")).get("alt"),
        "amateur": comp.get("amateur", athlete.get("amateur")),
        "sort_order": comp.get("sortOrder", comp.get("order")),
        "position": shown if on_board else None,
        "position_is_tie": position.get("isTie") if on_board else None,
        "player_status": _dict(status.get("type")).get("name"),
        "thru": status.get("hole") if status else None,
        "to_par": _to_par(to_par_display),
        "to_par_display": to_par_display if isinstance(to_par_display, str) else None,
        "total_strokes": int(strokes) if isinstance(strokes, (int, float)) and strokes > 0 else None,
        **rounds,
        "earnings": comp.get("earnings"),
        "cup_points": _stat(comp.get("statistics"), "cupPoints"),
    }


def parse_golf_leaderboard(
    payload: dict[str, Any] | None,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """One row per player per event from an ESPN golf ``scoreboard`` or ``leaderboard`` payload.

    Args:
        payload: The raw JSON ``Dict`` (``{"events": [...]}``). ``None``, ``{}`` or an event
            with no field yields a zero-row frame carrying :data:`GOLF_LEADERBOARD_SCHEMA`.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A frame with :data:`GOLF_LEADERBOARD_SCHEMA` columns, sorted by event then ``sort_order``.

    Example:
        Parse a raw leaderboard::

            raw = espn_golf_leaderboard(league="pga", event_id="401811941", return_parsed=False)
            parse_golf_leaderboard(raw)
    """
    rows: list[dict[str, Any]] = []
    events = payload.get("events") if isinstance(payload, dict) else None
    for event in events if isinstance(events, list) else []:
        if not isinstance(event, dict):
            continue
        competitions = event.get("competitions")
        for competition in competitions if isinstance(competitions, list) else []:
            competitors = _dict(competition).get("competitors")
            players = competitors if isinstance(competitors, list) else []
            rows.extend(_player_row(event, comp) for comp in players if isinstance(comp, dict))
    df = (
        pl.DataFrame(rows, schema=GOLF_LEADERBOARD_SCHEMA, strict=False)
        if rows
        else pl.DataFrame(schema=GOLF_LEADERBOARD_SCHEMA)
    )
    df = df.sort(["event_id", "sort_order"], nulls_last=True, maintain_order=True)
    return df.to_pandas() if return_as_pandas else df


def parse_golf_scoreboard(
    payload: dict[str, Any] | None,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """The ``espn_golf_scoreboard`` parser: :func:`parse_golf_leaderboard` over the scoreboard's field.

    The scoreboard carries no displayed position, player status, holes completed, total
    strokes, earnings or cup points: those columns are null (``total_strokes`` is summed
    from the played rounds). Use ``espn_golf_leaderboard`` for them.

    Args:
        payload: The raw scoreboard JSON ``Dict``.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A frame with :data:`GOLF_LEADERBOARD_SCHEMA` columns, one row per player per event.
    """
    return parse_golf_leaderboard(payload, return_as_pandas=return_as_pandas)
