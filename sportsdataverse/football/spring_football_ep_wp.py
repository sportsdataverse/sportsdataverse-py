"""Spring-football EP/WP port: ESPN summary -> nflverse-shape pbp -> NFL scorers.

``build_spring_football_pbp`` unrolls an ``espn_{ufl,xfl}_summary(return_parsed=False)``
payload's ``drives.previous[].plays[]`` (via the shared
``sportsdataverse._common_espn_parsers.parse_summary_drive_plays``) into the
minimal ``NFLVERSE_FRAME_CONTRACT`` column set. ``enrich_spring_football_pbp``
then scores that frame with ``sportsdataverse.nfl.ep_wp.enrich_nfl_pbp`` --
the already-shipped, parity-validated NFL EP/WP/CP/xYAC pipeline -- unchanged.
Neither function re-implements any model or derivation logic (see
``spring_football_constants`` for the documented rule-delta downscope).

**Capture finding (Task 0.1 / 1.2):** ESPN publishes no play-by-play for UFL
games as of this port -- verified empty (``drives.previous`` absent/empty
AND the Core v2 ``.../events/{id}/competitions/{id}/plays`` endpoint returns
zero items) across every completed 2024 + 2025 UFL game probed. XFL DOES
carry full drives/plays. ``build_spring_football_pbp(..., league="ufl")``
therefore correctly returns a zero-row contract frame on today's data --
this is real data, not a bug -- and will pick up real rows automatically
once/if ESPN backfills UFL play-by-play. See
``tests/fixtures/league_ports/FEASIBILITY.md``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Union

import polars as pl

from sportsdataverse._common_espn_parsers import parse_summary_drive_plays
from sportsdataverse.football.spring_football_constants import get_sf_constants
from sportsdataverse.nfl.ep_wp import enrich_nfl_pbp
from sportsdataverse.nfl.model_vars import NFLVERSE_FRAME_CONTRACT

if TYPE_CHECKING:
    import pandas as pd

#: dtype for every ``NFLVERSE_FRAME_CONTRACT`` column produced by this module.
_SF_CONTRACT_SCHEMA: dict[str, pl.PolarsDataType] = {
    "game_id": pl.Utf8,
    "play_id": pl.Utf8,
    "season": pl.Int64,
    "game_half": pl.Utf8,
    "posteam": pl.Utf8,
    "defteam": pl.Utf8,
    "home_team": pl.Utf8,
    "half_seconds_remaining": pl.Float64,
    "yardline_100": pl.Int64,
    "ydstogo": pl.Int64,
    "down": pl.Int64,
    "posteam_timeouts_remaining": pl.Int64,
    "defteam_timeouts_remaining": pl.Int64,
    "home": pl.Int8,
    "retractable": pl.Int8,
    "dome": pl.Int8,
    "outdoors": pl.Int8,
    "score_differential": pl.Int64,
    "game_seconds_remaining": pl.Float64,
    "spread_line": pl.Float64,
    "receive_2h_ko": pl.Int8,
    "posteam_score": pl.Int64,
    "defteam_score": pl.Int64,
    "roof": pl.Utf8,
}

assert set(_SF_CONTRACT_SCHEMA) == set(NFLVERSE_FRAME_CONTRACT)


def _empty_sf_frame() -> pl.DataFrame:
    return pl.DataFrame(schema=_SF_CONTRACT_SCHEMA)


def _play_team_roles(payload: dict) -> dict[str, tuple[Union[str, None], Union[str, None]]]:
    """``{play_id: (offense_team_id, defense_team_id)}`` from the raw
    ``drives.previous[].plays[].teamParticipants``.

    ``parse_summary_drive_plays`` stringifies list-valued cells (including
    ``teamParticipants``) so its output frame can't carry this; walked here
    directly off the raw JSON instead.
    """
    out: dict[str, tuple[Union[str, None], Union[str, None]]] = {}
    drives = ((payload or {}).get("drives") or {}).get("previous") or []
    for drive in drives:
        if not isinstance(drive, dict):
            continue
        for play in drive.get("plays") or []:
            if not isinstance(play, dict):
                continue
            pid = play.get("id")
            off_id: Union[str, None] = None
            def_id: Union[str, None] = None
            for tp in play.get("teamParticipants") or []:
                if not isinstance(tp, dict):
                    continue
                tid = tp.get("id")
                if tp.get("type") == "offense":
                    off_id = str(tid) if tid is not None else None
                elif tp.get("type") == "defense":
                    def_id = str(tid) if tid is not None else None
            out[str(pid)] = (off_id, def_id)
    return out


def _half2_receiver(payload: dict, home_id: Union[str, None], away_id: Union[str, None]) -> Union[str, None]:
    """Team id receiving the 2nd-half opening kickoff (the OTHER team from
    whoever received the game-opening kickoff), or ``None`` if undetermined.
    """
    drives = ((payload or {}).get("drives") or {}).get("previous") or []
    for drive in drives:
        if not isinstance(drive, dict):
            continue
        for play in drive.get("plays") or []:
            if not isinstance(play, dict):
                continue
            if play.get("type", {}).get("text") != "Kickoff":
                continue
            if play.get("period", {}).get("number") != 1:
                continue
            off = next(
                (
                    tp.get("id")
                    for tp in play.get("teamParticipants") or []
                    if isinstance(tp, dict) and tp.get("type") == "offense"
                ),
                None,
            )
            off = str(off) if off is not None else None
            if off == home_id:
                return away_id
            if off == away_id:
                return home_id
            return None
    return None


def build_spring_football_pbp(summary: dict, *, league: str) -> pl.DataFrame:
    """Unroll an ESPN spring-football summary into an nflverse-shape pbp frame.

    Reuses ``parse_summary_drive_plays`` for the drive/play unroll, then maps
    ESPN's Site v2 field names (``start.down``, ``clock.displayValue``, ...)
    onto the ``NFLVERSE_FRAME_CONTRACT`` minimum ``sportsdataverse.nfl.ep_wp``
    needs. Timeouts remaining are not tracked by ESPN's spring-football
    summary and default to 3/3 (full) for every play; ``roof`` defaults to
    null (the contract's documented "unknown -> retractable" default).

    Args:
        summary: Raw dict from ``espn_{league}_summary(return_parsed=False)``.
        league: ``"ufl"``, ``"xfl"``, or ``"cfl"`` -- validated against
            :func:`~sportsdataverse.football.spring_football_constants.get_sf_constants`.

    Returns:
        One row per play with every ``NFLVERSE_FRAME_CONTRACT`` column
        (ids ``Utf8``). Zero rows (same schema) when the payload carries no
        drives -- true today for every captured UFL game (see module
        docstring).

    Raises:
        ValueError: ``league`` is not a recognized spring-football league.

    Example:
        Quick start::

            from sportsdataverse.football.xfl import espn_xfl_summary
            from sportsdataverse.football.spring_football_ep_wp import build_spring_football_pbp

            summary = espn_xfl_summary(game_id, return_parsed=False)
            pbp = build_spring_football_pbp(summary, league="xfl")
            print(pbp.select("down", "ydstogo", "yardline_100").head())
    """
    get_sf_constants(league)  # validates league; raises ValueError on unknown

    raw = parse_summary_drive_plays(summary)
    if raw.height == 0:
        return _empty_sf_frame()

    comp = ((summary.get("header") or {}).get("competitions") or [{}])[0]
    game_id = str(comp.get("id") or "")
    season = ((summary.get("header") or {}).get("season") or {}).get("year")

    home_id: Union[str, None] = None
    away_id: Union[str, None] = None
    for c in comp.get("competitors") or []:
        if c.get("homeAway") == "home":
            home_id = str(c.get("id")) if c.get("id") is not None else None
        elif c.get("homeAway") == "away":
            away_id = str(c.get("id")) if c.get("id") is not None else None

    roles = _play_team_roles(summary)
    half2_receiver = _half2_receiver(summary, home_id, away_id)

    ids = raw["id"].cast(pl.Utf8).to_list()
    off_ids = [roles.get(i, (None, None))[0] for i in ids]
    def_ids = [roles.get(i, (None, None))[1] for i in ids]

    df = raw.with_columns(
        pl.Series("posteam", off_ids, dtype=pl.Utf8),
        pl.Series("defteam", def_ids, dtype=pl.Utf8),
    )

    sort_cols = ["drive_sequence"]
    if "sequence_number" in df.columns:
        df = df.with_columns(pl.col("sequence_number").cast(pl.Int64, strict=False).alias("_seq"))
        sort_cols.append("_seq")
    df = df.sort(sort_cols)

    def _col_or(name: str, dtype: pl.PolarsDataType) -> pl.Expr:
        return pl.col(name) if name in df.columns else pl.lit(None, dtype=dtype)

    # ESPN marks kickoffs / non-play markers with start.down == 0 -- map to
    # null so `_apply_feature_substitution` / `_is_real_play_expr` in
    # `enrich_nfl_pbp` (both keyed on `down.is_null()`) fire correctly.
    down_raw = _col_or("start_down", pl.Int64).cast(pl.Int64, strict=False)
    period = _col_or("period_number", pl.Int64).cast(pl.Int64, strict=False).fill_null(1)
    clock_parts = _col_or("clock_display_value", pl.Utf8).cast(pl.Utf8).str.split(":")
    clock_secs = clock_parts.list.get(0).cast(pl.Int64, strict=False).fill_null(0) * 60 + clock_parts.list.get(1).cast(
        pl.Int64, strict=False
    ).fill_null(0)
    quarters_left = (pl.lit(4) - period).clip(0, 3)
    home_score_after = _col_or("home_score", pl.Int64).cast(pl.Int64, strict=False).fill_null(0)
    away_score_after = _col_or("away_score", pl.Int64).cast(pl.Int64, strict=False).fill_null(0)
    type_text = _col_or("type_text", pl.Utf8).cast(pl.Utf8)
    # nflverse `play_type` vocabulary, mapped from ESPN's `type.text` label.
    # Unmapped labels (markers like "Official Timeout" / "End of Game") stay
    # null, matching nflverse's own convention for non-play rows.
    play_type = (
        pl.when(type_text.is_in(["Rush", "Rushing Touchdown"]))
        .then(pl.lit("run"))
        .when(type_text.is_in(["Pass Reception", "Pass Incompletion", "Passing Touchdown", "Interception", "Sack"]))
        .then(pl.lit("pass"))
        .when(type_text.is_in(["Punt", "Punt Return"]))
        .then(pl.lit("punt"))
        .when(type_text.is_in(["Field Goal Good", "Field Goal Missed"]))
        .then(pl.lit("field_goal"))
        .when(type_text.is_in(["Kickoff", "Kickoff Return Touchdown"]))
        .then(pl.lit("kickoff"))
        .when(type_text == "Penalty")
        .then(pl.lit("no_play"))
        .otherwise(None)
    )

    df = df.with_columns(
        pl.lit(game_id, dtype=pl.Utf8).alias("game_id"),
        pl.col("id").cast(pl.Utf8).alias("play_id"),
        pl.lit(season, dtype=pl.Int64).alias("season"),
        pl.lit(home_id, dtype=pl.Utf8).alias("home_team"),
        pl.when(down_raw == 0).then(None).otherwise(down_raw).alias("down"),
        play_type.alias("play_type"),
        period.alias("qtr"),
        _col_or("start_distance", pl.Int64).cast(pl.Int64, strict=False).alias("ydstogo"),
        _col_or("start_yards_to_endzone", pl.Int64).cast(pl.Int64, strict=False).alias("yardline_100"),
        pl.when(period <= 4)
        .then(clock_secs + quarters_left * 900)
        .otherwise(0)
        .cast(pl.Float64)
        .alias("game_seconds_remaining"),
        pl.when(period <= 4)
        .then(clock_secs + pl.when(period.is_in([1, 3])).then(900).otherwise(0))
        .otherwise(0)
        .cast(pl.Float64)
        .alias("half_seconds_remaining"),
        pl.when(period <= 2)
        .then(pl.lit("Half1"))
        .when(period <= 4)
        .then(pl.lit("Half2"))
        .otherwise(pl.lit("OT"))
        .alias("game_half"),
        pl.lit(3, dtype=pl.Int64).alias("posteam_timeouts_remaining"),
        pl.lit(3, dtype=pl.Int64).alias("defteam_timeouts_remaining"),
        pl.lit(None, dtype=pl.Float64).alias("spread_line"),
        pl.lit(None, dtype=pl.Utf8).alias("roof"),
        pl.lit(1, dtype=pl.Int8).alias("retractable"),
        pl.lit(0, dtype=pl.Int8).alias("dome"),
        pl.lit(0, dtype=pl.Int8).alias("outdoors"),
        # ESPN's spring-football drive/play object carries no air-yards
        # annotation -- `calculate_completion_probability` requires the
        # column to exist (non-optional) but every row being null degrades
        # gracefully to a null `cp`/`cpoe` for every play (documented
        # downscope; see module docstring).
        pl.lit(None, dtype=pl.Float64).alias("air_yards"),
        (type_text == "Kickoff").fill_null(False).cast(pl.Int8).alias("kickoff_attempt"),
        home_score_after.shift(1).fill_null(0).alias("_home_score_before"),
        away_score_after.shift(1).fill_null(0).alias("_away_score_before"),
    )

    df = df.with_columns(
        pl.when(pl.col("posteam") == pl.col("home_team"))
        .then(pl.col("_home_score_before"))
        .otherwise(pl.col("_away_score_before"))
        .cast(pl.Int64)
        .alias("posteam_score"),
        pl.when(pl.col("posteam") == pl.col("home_team"))
        .then(pl.col("_away_score_before"))
        .otherwise(pl.col("_home_score_before"))
        .cast(pl.Int64)
        .alias("defteam_score"),
        (pl.col("posteam") == pl.col("home_team")).fill_null(False).cast(pl.Int8).alias("home"),
        (pl.col("posteam") == pl.lit(half2_receiver, dtype=pl.Utf8))
        .fill_null(False)
        .cast(pl.Int8)
        .alias("receive_2h_ko"),
    )
    df = df.with_columns((pl.col("posteam_score") - pl.col("defteam_score")).alias("score_differential"))

    # `air_yards` / `kickoff_attempt` / `play_type` / `qtr` aren't part of
    # NFLVERSE_FRAME_CONTRACT (the contract is the documented *minimum*) but
    # `enrich_nfl_pbp`'s CP / xpass steps unconditionally require them --
    # kept alongside the contract columns.
    extra_cols = ["air_yards", "kickoff_attempt", "play_type", "qtr"]
    return df.select(sorted(NFLVERSE_FRAME_CONTRACT) + extra_cols)


def enrich_spring_football_pbp(
    pbp: pl.DataFrame,
    *,
    league: str,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Score a spring-football pbp frame with the NFL EP/WP/CP/xYAC pipeline.

    Calls ``sportsdataverse.nfl.ep_wp.enrich_nfl_pbp`` UNCHANGED -- this is
    the "by reference, do not re-implement" port: the same parity-validated
    NFL scorers, run on genuine league-native down/distance/yardline/clock/
    score state built by :func:`build_spring_football_pbp`. See
    ``spring_football_constants`` for the documented rule-delta downscope
    (kickoff-touchback spot and EP point-value collapse are not yet
    league-specific).

    Args:
        pbp: Output of :func:`build_spring_football_pbp`.
        league: ``"ufl"``, ``"xfl"``, ``"cfl"``, or ``"nfl_parity"`` --
            validated via
            :func:`~sportsdataverse.football.spring_football_constants.get_sf_constants`
            (only used to validate the league; the constants aren't yet
            threaded into scoring -- see the downscope note above).
        return_as_pandas: When ``True``, return a ``pandas.DataFrame``.

    Returns:
        ``pbp`` with ``ep``/``epa``/``wp``/``wpa``/``cp``/``cpoe``/xYAC and
        the other ``enrich_nfl_pbp`` output columns added. Unchanged
        (zero-row) passthrough when ``pbp`` is empty.

    Raises:
        ValueError: ``league`` is not a recognized spring-football league.

    Example:
        Quick start::

            from sportsdataverse.football.spring_football_ep_wp import (
                build_spring_football_pbp, enrich_spring_football_pbp,
            )

            pbp = build_spring_football_pbp(summary, league="xfl")
            scored = enrich_spring_football_pbp(pbp, league="xfl")
            print(scored.select("play_id", "epa", "wp").head())
    """
    get_sf_constants(league)  # validates league; raises ValueError on unknown

    if pbp.height == 0:
        return pbp.to_pandas() if return_as_pandas else pbp

    return enrich_nfl_pbp(pbp, return_as_pandas=return_as_pandas)
