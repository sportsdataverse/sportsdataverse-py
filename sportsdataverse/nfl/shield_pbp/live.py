"""Live-game concerns: phase, provisional rows, the current-situation row, game context.

The archive path (``nfl/raw`` finals) never needs anything here — the module exists
because the SAME parser also serves a game that is still being played, where three
things are true that are never true of a final payload:

1. the newest plays are **provisional** — Shield keeps revising a play's
   ``playDescription`` / ``stats`` / yardage for a while after the snap (measured on
   the 2026 wk2 DET@BUF capture: revisions reach ~11 plays back, but the feed's own
   "not settled" marker is a null ``playEndTime``);
2. there is a **next snap** that has no play row yet — the down/distance/clock/
   timeouts in ``summary`` (what a scoreboard shows) — and every live EP/WP consumer
   wants exactly that row;
3. ``roof`` / ``spread_line`` / ``total_line`` cannot come from a nightly schedules
   join that may not have run yet, so they are **injectable**.

The live columns (``live_phase``, ``is_play``, ``provisional``) are added by
:func:`add_live_columns`, which :func:`sportsdataverse.nfl.shield_pbp.build.shield_nfl_pbp`
calls — never by ``build_pbp``, so the published ``nfl_model_pbp`` schema is unchanged.
"""

from __future__ import annotations

import warnings
from typing import Any, Dict, Mapping, Optional, Tuple

import polars as pl

from sportsdataverse.nfl.shield_pbp.parse import (
    _clock_to_seconds,
    _game_half,
    _seconds_remaining,
    _yardline_100,
)

#: Shield ``summary.phase`` values that mean the game is over. ``phase`` is the only
#: status signal the feed gives — top-level ``status`` is always ``"SCHEDULED"``.
FINAL_PHASES = ("FINAL", "FINAL_OVERTIME")

#: The one phase whose ``summary`` situation describes a real next snap. Observed
#: phases on the 2026 wk2 capture: ``PREGAME``, ``INGAME``, ``HALFTIME``, ``FINAL``
#: (plus ``FINAL_OVERTIME`` in the archive) — note ``HALFTIME``, which the endpoint
#: notes did not list. ``PREGAME`` carries a placeholder situation (1st-and-10 at a
#: side-less ``"35"``, the kickoff spot) and ``HALFTIME`` carries no quarter, so
#: neither yields a scoreable row.
LIVE_PHASE = "INGAME"

#: Fallback game context when neither the caller nor the nflverse schedule supplies
#: one. ``spread_line`` / ``total_line`` reuse the repo-wide processor default
#: (``NFLPlayProcess``: 2.5 / 55.5) so a Shield-sourced game and an ESPN-sourced one
#: degrade identically; ``roof`` defaults to the modal NFL value.
DEFAULT_CONTEXT: Dict[str, Any] = {"roof": "outdoors", "spread_line": 2.5, "total_line": 55.5}

#: Marker ``playType`` values that are not plays (no snap): the synthetic game/quarter
#: markers the feed emits. TIMEOUT rows are dropped by ``build_pbp`` before this runs.
_MARKER_PLAY_TYPES = ("GAME_START", "END_QUARTER", "END_GAME", "TIMEOUT")

#: ``shield_play_type`` stamped on the synthetic current-situation row.
CURRENT_SITUATION = "CURRENT_SITUATION"

#: Game-level columns the current-situation row inherits from the built frame; every
#: other column is null on it unless explicitly set from ``summary``.
_GAME_LEVEL_COLUMNS = (
    "game_id",
    "season",
    "week",
    "season_type",
    "home_team",
    "away_team",
    "roof",
    "spread_line",
    "total_line",
    "home_score",
    "away_score",
    "result",
    "live_phase",
)


def game_phase(game: Mapping[str, Any]) -> Optional[str]:
    """``summary.phase`` of a Shield game payload (``PREGAME``/``INGAME``/``FINAL*``)."""
    return ((game.get("summary") or {}) or {}).get("phase")


def is_final(game: Mapping[str, Any]) -> bool:
    """True when the payload's ``summary.phase`` says the game is over.

    Args:
        game: A Shield game payload (``experience/v2/gamedetails`` body).

    Returns:
        ``True`` for phase ``FINAL`` / ``FINAL_OVERTIME``, else ``False``. A payload
        with no ``summary`` is treated as not final (the conservative direction: the
        game-outcome columns stay null rather than being stamped with a live score).
    """
    return str(game_phase(game) or "") in FINAL_PHASES


def resolve_context(
    game: Mapping[str, Any],
    context: Optional[Mapping[str, Any]] = None,
    *,
    game_id: Optional[str] = None,
) -> Tuple[Optional[str], Optional[float], Optional[float]]:
    """Resolve ``(roof, spread_line, total_line)`` for one game.

    Cascade, first hit wins per field: the caller's ``context`` -> the nflverse
    schedule row for this ``game_id`` -> :data:`DEFAULT_CONTEXT`. The schedule step is
    skipped (no network) when ``context`` already supplies all three, and degrades to
    the defaults with a ``RuntimeWarning`` when the schedule cannot be loaded.

    Every field that reaches :data:`DEFAULT_CONTEXT` raises a ``RuntimeWarning`` naming
    it: ``spread_line`` / ``total_line`` are the only inputs to ``vegas_wp``, and a
    defaulted line is indistinguishable from a real one once it is in the frame.

    Args:
        game: The Shield game payload (for ``season``).
        context: Optional ``{"roof": ..., "spread_line": ..., "total_line": ...}``.
            Extra keys are ignored; a key present with value ``None`` falls through
            to the next step.
        game_id: nflverse game_id used for the schedule lookup; no lookup without it.

    Returns:
        A ``(roof, spread_line, total_line)`` tuple, never ``(None, None, None)`` —
        unresolved fields fall back to :data:`DEFAULT_CONTEXT`.

    Warns:
        RuntimeWarning: Once per call, naming every field that fell back to
            :data:`DEFAULT_CONTEXT`; separately when the schedule lookup itself failed.
    """
    out = {k: (context or {}).get(k) for k in DEFAULT_CONTEXT}
    if any(v is None for v in out.values()) and game_id is not None and game.get("season") is not None:
        for key, value in _schedule_context(int(game["season"]), game_id).items():
            if out.get(key) is None:
                out[key] = value
    fell_back = sorted(k for k in DEFAULT_CONTEXT if out.get(k) is None)
    if fell_back:
        # These feed the EP/WP models -- ``spread_line`` / ``total_line`` are the only
        # inputs to ``vegas_wp`` -- and the frame cannot tell a default apart from a
        # real line, so the fallback has to announce itself. A live consumer that must
        # not show a Vegas-informed number off a made-up line passes its own context.
        warnings.warn(
            f"shield_pbp: no {', '.join(fell_back)} for "
            f"{game_id or 'this game'} from the caller or the nflverse schedule — "
            f"using {', '.join(f'{k}={DEFAULT_CONTEXT[k]!r}' for k in fell_back)}; "
            f"vegas_wp / vegas_wpa on this game are not market-informed.",
            RuntimeWarning,
            stacklevel=2,
        )
        for key in fell_back:
            out[key] = DEFAULT_CONTEXT[key]
    return out["roof"], out["spread_line"], out["total_line"]


def _schedule_context(season: int, game_id: str) -> Dict[str, Any]:
    """``{roof, spread_line, total_line}`` for one game from the nflverse schedule.

    Returns ``{}`` (with a ``RuntimeWarning``) when the schedule cannot be loaded —
    live processing must never fail because a release asset is unreachable.
    """
    try:
        from sportsdataverse.nfl import load_nfl_schedule

        sched = load_nfl_schedule([season])
        if not isinstance(sched, pl.DataFrame):
            sched = pl.from_pandas(sched)
        keep = [c for c in ("roof", "spread_line", "total_line") if c in sched.columns]
        row = sched.filter(pl.col("game_id") == game_id).select(keep)
        if row.height:
            return {k: v for k, v in row.row(0, named=True).items() if v is not None}
    except Exception as exc:  # noqa: BLE001 — offline / missing asset must degrade, not raise
        warnings.warn(
            f"shield_pbp: nflverse schedule context unavailable for {game_id} — "
            f"{type(exc).__name__}: {exc}; using defaults.",
            RuntimeWarning,
            stacklevel=3,
        )
    return {}


def add_live_columns(df: pl.DataFrame, game: Mapping[str, Any]) -> pl.DataFrame:
    """Add the three live-status columns to a built play frame.

    Args:
        df: A frame from :func:`sportsdataverse.nfl.shield_pbp.build.build_pbp`.
        game: The same Shield payload it was built from (for ``phase`` and each
            play's ``playEndTime``, which the play frame does not carry).

    Returns:
        The frame with, per row:

        | col_name | type | description |
        |----------|------|-------------|
        | `live_phase` | `str` | The payload's `summary.phase` (`PREGAME`, `INGAME`, `HALFTIME`, `FINAL`, `FINAL_OVERTIME`), broadcast to every row; `None` when the payload carries no `summary`. |
        | `is_play` | `int` | `1` for a real play, `0` for a non-play row: the feed's `GAME_START` / `END_QUARTER` / `END_GAME` markers and the synthetic current-situation row. |
        | `provisional` | `int` | `1` when the row's outcome may still change: a play the feed has not closed (`playEndTime` null) in the trailing run of such plays of a non-final game. Always `0` once `phase` is `FINAL*`. |
    """
    if df.height == 0:
        return df
    phase = game_phase(game)
    open_ids = _provisional_play_ids(game) if not is_final(game) else set()
    return df.with_columns(
        live_phase=pl.lit(phase, dtype=pl.Utf8),
        is_play=(~pl.col("shield_play_type").fill_null("").is_in(_MARKER_PLAY_TYPES)).cast(pl.Int64),
        provisional=(
            pl.col("play_id").is_in(list(open_ids)).fill_null(False).cast(pl.Int64)
            if open_ids
            else pl.lit(0, dtype=pl.Int64)
        ),
    )


def _provisional_play_ids(game: Mapping[str, Any]) -> set:
    """playIds of the trailing run of plays the feed has not closed (``playEndTime`` null).

    ``playEndTime`` is null on plenty of settled rows too (every XP_KICK, every
    TIMEOUT, some scrimmage plays — 46/200 on the measured final payload), so the
    null alone does not mean provisional; only the *trailing* run of them can still
    be revised in the ordinary case. Deliberately a heuristic: on the DET@BUF capture
    Shield revised text/stats up to 11 plays back, so this flags the common case (the
    play in progress and its immediate predecessors), not every possible revision.
    """
    plays = [p for p in ((game.get("driveChart") or {}).get("plays") or []) if not p.get("playDeleted")]
    plays.sort(key=lambda p: p.get("playSequenceNumber") or 0)
    out = set()
    for play in reversed(plays):
        if play.get("playEndTime") is not None:
            break
        # A play with no playId cannot be matched against the frame's play_id, and a
        # set holding only ``None`` is still truthy -- which would take the is_in
        # branch in add_live_columns with a needle that matches nothing.
        if play.get("playId") is not None:
            out.add(play["playId"])
    return out


def current_situation_row(df: pl.DataFrame, game: Mapping[str, Any]) -> pl.DataFrame:
    """Append the next-snap row built from ``summary`` (down/dist/yardline/clock/timeouts).

    The live feed describes the situation the game is *in* (what a scoreboard shows)
    in ``summary``, for which no play row exists yet — and that is precisely the row a
    live win-probability display needs. It is appended as one extra row flagged
    ``is_play = 0`` / ``shield_play_type = "CURRENT_SITUATION"``, carrying only game
    identity plus the situation; every play-outcome column is null, so no consumer can
    mistake it for a snap that happened. Scores and timeouts come from ``summary``
    (authoritative) rather than from the play-derived running totals.

    No-ops unless ``summary.phase`` is ``INGAME`` (:data:`LIVE_PHASE`) — a PREGAME
    payload's situation is a placeholder (a side-less ``"35"``) and HALFTIME has no
    quarter, so neither is a real snap — and on an empty frame or a ``summary``
    with no yard line (nothing to place the ball on).

    Args:
        df: The built frame, already carrying the :func:`add_live_columns` columns.
        game: The Shield payload.

    Returns:
        ``df`` with at most one row appended (the frame's own schema, unchanged).
    """
    if df.height == 0 or game_phase(game) != LIVE_PHASE:
        return df
    summary = game.get("summary") or {}
    if not summary.get("yardLine"):
        return df

    home_team, away_team = df.item(0, "home_team"), df.item(0, "away_team")
    home = (summary.get("homeTeam") or {}) or {}
    away = (summary.get("awayTeam") or {}) or {}
    posteam = home_team if home.get("hasPossession") else (away_team if away.get("hasPossession") else None)
    if posteam is None:
        return df
    pos, dfn = (home, away) if posteam == home_team else (away, home)
    defteam = away_team if posteam == home_team else home_team

    quarter = _quarter_number(summary.get("quarter"))
    qsr = _clock_to_seconds(summary.get("clock"))
    half_sr, game_sr = _seconds_remaining(quarter, qsr)
    pos_score = ((pos.get("score") or {}) or {}).get("total")
    def_score = ((dfn.get("score") or {}) or {}).get("total")

    last = df.tail(1).to_dicts()[0]
    row: Dict[str, Any] = {c: (last.get(c) if c in _GAME_LEVEL_COLUMNS else None) for c in df.columns}
    row.update(
        play_id=_next_id(df, "play_id"),
        play_seq=_next_id(df, "play_seq"),
        shield_play_type=CURRENT_SITUATION,
        is_play=0,
        provisional=0,
        posteam=posteam,
        defteam=defteam,
        home=1 if posteam == home_team else 0,
        qtr=quarter,
        game_half=_game_half(quarter),
        down=summary.get("down") or None,
        ydstogo=summary.get("distance"),
        yardline_100=_yardline_100(summary.get("yardLine"), posteam),
        goal_to_go=1 if summary.get("isGoalToGo") else 0,
        quarter_seconds_remaining=qsr,
        half_seconds_remaining=half_sr,
        game_seconds_remaining=game_sr,
        posteam_score=pos_score,
        defteam_score=def_score,
        score_differential=(pos_score - def_score) if (pos_score is not None and def_score is not None) else None,
        posteam_timeouts_remaining=((pos.get("timeouts") or {}) or {}).get("remaining"),
        defteam_timeouts_remaining=((dfn.get("timeouts") or {}) or {}).get("remaining"),
        # Same open drive as the last play when possession has not changed, else the next one.
        fixed_drive=(last.get("fixed_drive") if last.get("posteam") == posteam else _bump(last.get("fixed_drive"))),
        sp=0,
    )
    # A column that is all-null in the built frame carries polars' ``Null`` dtype
    # (e.g. ``posteam`` on a 1-row PREGAME payload), which cannot hold a value — let
    # those infer and let ``diagonal_relaxed`` resolve the supertype on concat.
    schema = {c: (None if dt == pl.Null else dt) for c, dt in df.schema.items()}
    extra = pl.DataFrame([row], schema=schema)
    return pl.concat([df, extra], how="diagonal_relaxed")


def _bump(value: Optional[int]) -> Optional[int]:
    return None if value is None else value + 1


def _next_id(df: pl.DataFrame, column: str) -> Optional[float]:
    """``max(column) + 1`` in the column's own dtype (``None`` on an all-null column)."""
    top = df[column].max()
    if top is None:
        return None
    return type(top)(top + 1)


def _quarter_number(quarter: Optional[str]) -> Optional[int]:
    """``"Q3"`` -> 3, ``"OT"`` -> 5, ``"HALFTIME"``/``"END_OF_GAME"``/unknown -> None."""
    if not quarter:
        return None
    text = str(quarter).upper()
    if text.startswith("Q") and text[1:].isdigit():
        return int(text[1:])
    if text.isdigit():
        return int(text)
    if "OT" in text or "OVERTIME" in text:
        return 5
    return None
