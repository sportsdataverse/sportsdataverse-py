"""(1) Synergy play-type-adjusted offense/defense (season-aggregate, opponent-adjusted).

Synergy publishes season-aggregate per-team PPP by play type (11 canonical
types) for both the offensive and defensive grouping, but not per-game splits.
This module runs a KenPom/RAPM-style iterative opponent adjustment at the
season-aggregate level, using each team's actual schedule (from the league
game log) as the opponent-strength weight -- see
:mod:`sportsdataverse.nba.nba_playtype_constants` for the shared metrics and
the design spec (``2026-07-07-nba-playtype-impact-design.md``) for the
methodology rationale.
"""

from __future__ import annotations

from typing import Optional, Union

import numpy as np
import pandas as pd
import polars as pl

from sportsdataverse.nba.nba_playtype_constants import SYNERGY_PLAY_TYPES

_RAW_SCHEMA: dict[str, type[pl.DataType]] = {
    "team_id": pl.Int64,
    "play_type": pl.Utf8,
    "off_poss": pl.Float64,
    "off_pts": pl.Float64,
    "off_ppp": pl.Float64,
    "off_freq": pl.Float64,
    "def_poss": pl.Float64,
    "def_pts": pl.Float64,
    "def_ppp": pl.Float64,
    "def_freq": pl.Float64,
}

_RATINGS_BASE_SCHEMA: dict[str, type[pl.DataType]] = {
    "team_id": pl.Int64,
    "adj_off": pl.Float64,
    "adj_def": pl.Float64,
    "adj_net": pl.Float64,
}


def _empty(schema: dict[str, type[pl.DataType]]) -> pl.DataFrame:
    return pl.DataFrame({c: pl.Series([], dtype=d) for c, d in schema.items()})


def raw_playtype_efficiency(off_team: pl.DataFrame, def_team: pl.DataFrame) -> pl.DataFrame:
    """Compute raw per-(team, play-type) offensive + defensive efficiency.

    Args:
        off_team: Parsed Synergy offensive team-grouping frame with (at least)
            ``team_id``, ``play_type``, ``poss``, ``pts`` columns.
        def_team: Parsed Synergy defensive team-grouping frame, same shape.

    Returns:
        One row per ``(team_id, play_type)``: ``team_id`` (Int64),
        ``play_type`` (Utf8), ``off_poss``/``off_pts``/``off_ppp``/``off_freq``,
        ``def_poss``/``def_pts``/``def_ppp``/``def_freq`` (all Float64).
        ``off_freq`` is the team's ``off_poss`` share across its own play
        types (analogously ``def_freq`` on the defensive side). Returns a
        zero-row frame with this schema when either input is empty.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_playtype import raw_playtype_efficiency
            raw = raw_playtype_efficiency(off_team_df, def_team_df)
            print(raw.filter(pl.col("play_type") == "Isolation"))
    """
    if off_team.is_empty() or def_team.is_empty():
        return _empty(_RAW_SCHEMA)

    off = (
        off_team.select(
            pl.col("team_id").cast(pl.Int64),
            pl.col("play_type"),
            pl.col("poss").cast(pl.Float64).alias("off_poss"),
            pl.col("pts").cast(pl.Float64).alias("off_pts"),
        )
        .with_columns((pl.col("off_pts") / pl.col("off_poss")).alias("off_ppp"))
        .with_columns((pl.col("off_poss") / pl.col("off_poss").sum().over("team_id")).alias("off_freq"))
    )
    deff = (
        def_team.select(
            pl.col("team_id").cast(pl.Int64),
            pl.col("play_type"),
            pl.col("poss").cast(pl.Float64).alias("def_poss"),
            pl.col("pts").cast(pl.Float64).alias("def_pts"),
        )
        .with_columns((pl.col("def_pts") / pl.col("def_poss")).alias("def_ppp"))
        .with_columns((pl.col("def_poss") / pl.col("def_poss").sum().over("team_id")).alias("def_freq"))
    )
    assert off.schema["team_id"] == deff.schema["team_id"]  # join-key dtype guard
    return (
        off.join(deff, on=["team_id", "play_type"], how="full", coalesce=True)
        .select(list(_RAW_SCHEMA.keys()))
        .sort("team_id", "play_type")
    )


def adjust_playtype_efficiency(
    raw: pl.DataFrame,
    schedule: pl.DataFrame,
    *,
    max_iter: int = 50,
    tol: float = 1e-5,
) -> pl.DataFrame:
    """Iteratively opponent-adjust per-play-type efficiency using the actual schedule.

    Fixed point (see module docstring), solved independently per play type::

        adj_off_ppp[team] = raw_off_ppp[team] + (lg_off_ppp - mean_opp adj_def_ppp[opp])
        adj_def_ppp[team] = raw_def_ppp[team] + (lg_def_ppp - mean_opp adj_off_ppp[opp])

    where ``lg_*_ppp`` is the poss-weighted league mean for that type. Each
    iteration re-centers the result so the poss-weighted league mean is exactly
    preserved (mean-preserving invariant holds to float precision, not just
    approximately).

    Args:
        raw: Output of :func:`raw_playtype_efficiency`.
        schedule: Long frame, one row per team-game: ``team_id`` (Int64),
            ``opp_team_id`` (Int64).
        max_iter: Maximum fixed-point iterations.
        tol: Convergence tolerance on the max absolute per-iteration change.

    Returns:
        *raw* with two added Float64 columns: ``adj_off_ppp``, ``adj_def_ppp``.
        Returns *raw* unchanged (no adjustment columns) if *raw* or *schedule*
        is empty -- callers should check ``raw.is_empty()`` first.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_playtype import adjust_playtype_efficiency
            adj = adjust_playtype_efficiency(raw, schedule_df)
            print(adj.select("team_id", "play_type", "adj_off_ppp", "adj_def_ppp"))
    """
    if raw.is_empty() or schedule.is_empty():
        return raw.with_columns(
            pl.lit(None, dtype=pl.Float64).alias("adj_off_ppp"),
            pl.lit(None, dtype=pl.Float64).alias("adj_def_ppp"),
        )

    assert raw.schema["team_id"] == schedule.schema["team_id"]  # join-key dtype guard

    team_ids = sorted(raw["team_id"].unique().to_list())
    idx = {t: i for i, t in enumerate(team_ids)}
    T = len(team_ids)

    sched = schedule.filter(pl.col("team_id").is_in(team_ids) & pl.col("opp_team_id").is_in(team_ids))
    team_idx_arr = np.array([idx[t] for t in sched["team_id"].to_list()], dtype=np.int64)
    opp_idx_arr = np.array([idx[t] for t in sched["opp_team_id"].to_list()], dtype=np.int64)
    counts = np.bincount(team_idx_arr, minlength=T).astype(np.float64)
    counts[counts == 0] = np.nan  # avoid div-by-zero; result is NaN for unscheduled teams (edge case)

    out_frames: list[pl.DataFrame] = []
    for pt in raw["play_type"].unique().to_list():
        block = raw.filter(pl.col("play_type") == pt).sort("team_id")
        # reindex onto the full team_ids axis (a type may be missing for some teams)
        block_by_team = {row["team_id"]: row for row in block.iter_rows(named=True)}
        off_poss = np.array([block_by_team.get(t, {}).get("off_poss") or 0.0 for t in team_ids])
        off_pts = np.array([block_by_team.get(t, {}).get("off_pts") or 0.0 for t in team_ids])
        def_poss = np.array([block_by_team.get(t, {}).get("def_poss") or 0.0 for t in team_ids])
        def_pts = np.array([block_by_team.get(t, {}).get("def_pts") or 0.0 for t in team_ids])

        with np.errstate(invalid="ignore", divide="ignore"):
            raw_off_ppp = np.where(off_poss > 0, off_pts / off_poss, 0.0)
            raw_def_ppp = np.where(def_poss > 0, def_pts / def_poss, 0.0)

        lg_off_ppp = float(off_pts.sum() / off_poss.sum()) if off_poss.sum() > 0 else 0.0
        lg_def_ppp = float(def_pts.sum() / def_poss.sum()) if def_poss.sum() > 0 else 0.0

        adj_off = raw_off_ppp.copy()
        adj_def = raw_def_ppp.copy()

        def _recenter(vals: np.ndarray, weights: np.ndarray, target: float) -> np.ndarray:
            wsum = weights.sum()
            if wsum <= 0:
                return vals
            cur = float((vals * weights).sum() / wsum)
            return vals + (target - cur)

        adj_off = _recenter(adj_off, off_poss, lg_off_ppp)
        adj_def = _recenter(adj_def, def_poss, lg_def_ppp)

        for _ in range(max_iter):
            mean_opp_def = np.bincount(team_idx_arr, weights=adj_def[opp_idx_arr], minlength=T) / counts
            mean_opp_off = np.bincount(team_idx_arr, weights=adj_off[opp_idx_arr], minlength=T) / counts
            mean_opp_def = np.nan_to_num(mean_opp_def, nan=lg_def_ppp)
            mean_opp_off = np.nan_to_num(mean_opp_off, nan=lg_off_ppp)

            new_off = raw_off_ppp + (lg_off_ppp - mean_opp_def)
            new_def = raw_def_ppp + (lg_def_ppp - mean_opp_off)
            new_off = _recenter(new_off, off_poss, lg_off_ppp)
            new_def = _recenter(new_def, def_poss, lg_def_ppp)

            delta = max(np.max(np.abs(new_off - adj_off)), np.max(np.abs(new_def - adj_def)))
            adj_off, adj_def = new_off, new_def
            if delta < tol:
                break

        block_out = block.with_columns(
            pl.Series("adj_off_ppp", [adj_off[idx[t]] for t in block["team_id"].to_list()], dtype=pl.Float64),
            pl.Series("adj_def_ppp", [adj_def[idx[t]] for t in block["team_id"].to_list()], dtype=pl.Float64),
        )
        out_frames.append(block_out)

    return pl.concat(out_frames, how="vertical_relaxed").sort("team_id", "play_type")


def _fetch_synergy(league_id: str, season: str, grouping: str, entity: str = "T") -> pl.DataFrame:
    from sportsdataverse.nba.nba_stats import nba_stats_synergyplaytypes

    frames = []
    for pt in SYNERGY_PLAY_TYPES:
        df = nba_stats_synergyplaytypes(
            league_id=league_id,
            season=season,
            play_type_nullable=pt,
            player_or_team_abbreviation=entity,
            type_grouping_nullable=grouping,
        )
        if isinstance(df, pl.DataFrame) and not df.is_empty():
            frames.append(df)
    if not frames:
        return pl.DataFrame()
    return pl.concat(frames, how="diagonal_relaxed").rename(
        {"poss_pct": "freq", "tov_poss_pct": "turnover_freq", "ft_poss_pct": "ft_freq"}, strict=False
    )


def _fetch_synergy_team(league_id: str, season: str, grouping: str) -> pl.DataFrame:
    return _fetch_synergy(league_id, season, grouping, entity="T")


def _fetch_synergy_player(league_id: str, season: str, grouping: str = "Offensive") -> pl.DataFrame:
    return _fetch_synergy(league_id, season, grouping, entity="P")


def _fetch_schedule(league_id: str, season: str) -> pl.DataFrame:
    from sportsdataverse.nba.nba_stats import nba_stats_leaguegamelog

    log = nba_stats_leaguegamelog(league_id=league_id, season=season, player_or_team_abbreviation="T")
    if log.is_empty():
        return pl.DataFrame()
    g = log.select(pl.col("game_id").cast(pl.Utf8), pl.col("team_id").cast(pl.Int64))
    pairs = g.join(g, on="game_id", suffix="_opp").filter(pl.col("team_id") != pl.col("team_id_opp"))
    return pairs.select(pl.col("team_id"), pl.col("team_id_opp").alias("opp_team_id"))


def nba_playtype_ratings(
    season: str,
    *,
    league_id: str = "00",
    off_team: Optional[pl.DataFrame] = None,
    def_team: Optional[pl.DataFrame] = None,
    schedule: Optional[pl.DataFrame] = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Season play-type-adjusted offensive/defensive team ratings.

    Fetches (or uses injected) Synergy offensive/defensive team frames plus the
    league schedule, computes raw per-type efficiency
    (:func:`raw_playtype_efficiency`), opponent-adjusts it
    (:func:`adjust_playtype_efficiency`), then rolls up to one row per team.

    Args:
        season: Season string, e.g. ``"2023-24"``.
        league_id: ``"00"`` NBA (default), ``"10"`` WNBA, ``"20"`` G-League.
        off_team: Injected Synergy offensive team frame (bypasses the live
            fetch -- used for tests / oracle fixtures).
        def_team: Injected Synergy defensive team frame.
        schedule: Injected ``team_id``/``opp_team_id`` schedule frame.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per team: ``team_id`` (Int64), ``adj_off``/``adj_def``/``adj_net``
        (Float64) roll-ups, plus per-type wide columns
        ``adj_off_ppp_<playtype>``/``adj_def_ppp_<playtype>``/``off_freq_<playtype>``
        (Float64) for each play type present in the data.
        ``adj_off = Σ_t off_freq_t · adj_off_ppp_t · 100`` (symmetric for
        ``adj_def`` off ``def_freq_t``); ``adj_net = adj_off - adj_def``.
        Returns a zero-row frame with the base roll-up schema when the
        upstream fetch is empty (sparse-coverage leagues never raise).

    Example:
        Quick start::

            from sportsdataverse.nba import nba_playtype_ratings
            r = nba_playtype_ratings("2023-24")
            print(r.sort("adj_off", descending=True).head(10))

        Injected offline (oracle / test) path::

            r = nba_playtype_ratings("2023-24", off_team=off_df, def_team=def_df, schedule=sched_df)

        Pipeline next step::

            r.filter(pl.col("adj_net") > 0).sort("adj_net", descending=True)

        See Also:
            * `nba_api`_ -- upstream Synergy/stats.nba.com source

        .. _nba_api: https://github.com/swar/nba_api
    """
    if off_team is None or def_team is None or schedule is None:
        off_team = off_team if off_team is not None else _fetch_synergy_team(league_id, season, "Offensive")
        def_team = def_team if def_team is not None else _fetch_synergy_team(league_id, season, "Defensive")
        schedule = schedule if schedule is not None else _fetch_schedule(league_id, season)

    raw = raw_playtype_efficiency(off_team, def_team)
    if raw.is_empty() or schedule.is_empty():
        return _empty(_RATINGS_BASE_SCHEMA) if return_as_pandas is False else _empty(_RATINGS_BASE_SCHEMA).to_pandas()

    adj = adjust_playtype_efficiency(raw, schedule)

    off_pivot = adj.pivot(on="play_type", index="team_id", values="adj_off_ppp").rename(
        {pt: f"adj_off_ppp_{pt}" for pt in adj["play_type"].unique().to_list()}
    )
    def_pivot = adj.pivot(on="play_type", index="team_id", values="adj_def_ppp").rename(
        {pt: f"adj_def_ppp_{pt}" for pt in adj["play_type"].unique().to_list()}
    )
    off_freq_pivot = adj.pivot(on="play_type", index="team_id", values="off_freq").rename(
        {pt: f"off_freq_{pt}" for pt in adj["play_type"].unique().to_list()}
    )
    def_freq_pivot = adj.pivot(on="play_type", index="team_id", values="def_freq").rename(
        {pt: f"def_freq_{pt}" for pt in adj["play_type"].unique().to_list()}
    )

    play_types = adj["play_type"].unique().to_list()
    wide = off_pivot.join(def_pivot, on="team_id").join(off_freq_pivot, on="team_id").join(def_freq_pivot, on="team_id")

    adj_off_expr = (
        sum((pl.col(f"off_freq_{pt}").fill_null(0.0) * pl.col(f"adj_off_ppp_{pt}").fill_null(0.0)) for pt in play_types)
        * 100.0
    )
    adj_def_expr = (
        sum((pl.col(f"def_freq_{pt}").fill_null(0.0) * pl.col(f"adj_def_ppp_{pt}").fill_null(0.0)) for pt in play_types)
        * 100.0
    )

    wide = wide.with_columns(adj_off_expr.alias("adj_off"), adj_def_expr.alias("adj_def")).with_columns(
        (pl.col("adj_off") - pl.col("adj_def")).alias("adj_net")
    )
    # drop the internal def_freq_* helper columns (not part of the documented wide schema)
    wide = wide.drop([f"def_freq_{pt}" for pt in play_types])

    ordered = ["team_id", "adj_off", "adj_def", "adj_net"] + [
        c for c in wide.columns if c not in ("team_id", "adj_off", "adj_def", "adj_net")
    ]
    wide = wide.select(ordered).sort("team_id").with_columns(pl.col("team_id").cast(pl.Int64))

    if return_as_pandas:
        return wide.to_pandas()
    return wide
