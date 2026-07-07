"""Adjusted-efficiency team-rating engine (model ①) for the prediction stack.

Computes opponent-adjusted offensive/defensive efficiency (KenPom / Torvik
style) from schedule results + per-team boxscores. The pieces:

* :func:`raw_game_efficiency` -- per-team, per-game possessions + raw
  offensive/defensive efficiency.
* :func:`adjust_efficiency` -- iterative opponent adjustment → AdjO / AdjD /
  AdjEM (Task 1.2).
* :func:`adjust_tempo` -- opponent-adjusted tempo (Task 1.3).
* :func:`mbb_team_ratings` -- public per-team-season entry point (Task 1.4).

Algorithms here are league-agnostic; league-specific constants (HFA, tempo /
efficiency baselines) come from
:func:`sportsdataverse.mbb.mbb_prediction_constants.get_constants`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
import polars as pl

from sportsdataverse.mbb.mbb_loaders import load_mbb_schedule, load_mbb_team_boxscore
from sportsdataverse.mbb.mbb_prediction_constants import get_constants

if TYPE_CHECKING:
    import pandas as pd


def raw_game_efficiency(schedule: pl.DataFrame, team_box: pl.DataFrame) -> pl.DataFrame:
    """Per-team, per-game possessions + raw offensive/defensive efficiency.

    Possessions use the standard box estimate averaged over both teams::

        team_poss = FGA - OREB + TO + 0.44 * FTA
        poss      = 0.5 * (team_poss + opp_poss)
        off_eff   = 100 * pts     / poss
        def_eff   = 100 * opp_pts / poss

    The opponent is derived by self-joining ``team_box`` on ``game_id`` (each game
    has exactly two team rows); home/neutral context comes from ``schedule``.

    Args:
        schedule: Frame with ``game_id, season, date, home_team_id, away_team_id,
            neutral_site`` (ids as strings or ints; cast to ``Utf8`` here).
        team_box: Per-team boxscore with ``game_id, team_id,
            field_goals_attempted, offensive_rebounds, turnovers,
            free_throws_attempted, team_score``.

    Returns:
        One row per (game_id, team_id): ``game_id, season, date, team_id,
        opp_team_id, is_home, neutral_site, poss, off_eff, def_eff``. Empty
        input returns that schema with zero rows.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_loaders import load_mbb_schedule, load_mbb_team_boxscore
            from sportsdataverse.mbb.mbb_team_ratings import raw_game_efficiency
            eff = raw_game_efficiency(load_mbb_schedule([2024]), load_mbb_team_boxscore([2024]))
    """
    box = team_box.select(
        pl.col("game_id").cast(pl.Utf8),
        pl.col("team_id").cast(pl.Utf8),
        pl.col("field_goals_attempted").cast(pl.Float64).alias("fga"),
        pl.col("offensive_rebounds").cast(pl.Float64).alias("orb"),
        pl.col("turnovers").cast(pl.Float64).alias("tov"),
        pl.col("free_throws_attempted").cast(pl.Float64).alias("fta"),
        pl.col("team_score").cast(pl.Float64).alias("pts"),
    ).with_columns((pl.col("fga") - pl.col("orb") + pl.col("tov") + 0.44 * pl.col("fta")).alias("team_poss"))

    opp = box.select(
        "game_id",
        pl.col("team_id").alias("opp_team_id"),
        pl.col("team_poss").alias("opp_poss"),
        pl.col("pts").alias("opp_pts"),
    )
    paired = (
        box.join(opp, on="game_id")
        .filter(pl.col("team_id") != pl.col("opp_team_id"))
        .with_columns((0.5 * (pl.col("team_poss") + pl.col("opp_poss"))).alias("poss"))
        .with_columns(
            (100.0 * pl.col("pts") / pl.col("poss")).alias("off_eff"),
            (100.0 * pl.col("opp_pts") / pl.col("poss")).alias("def_eff"),
        )
    )

    sched = schedule.select(
        pl.col("game_id").cast(pl.Utf8),
        pl.col("season").cast(pl.Int64),
        pl.col("date").cast(pl.Date),
        pl.col("home_team_id").cast(pl.Utf8),
        pl.col("away_team_id").cast(pl.Utf8),
        pl.col("neutral_site").cast(pl.Boolean),
    )

    return (
        paired.join(sched, on="game_id", how="inner")
        .with_columns((pl.col("team_id") == pl.col("home_team_id")).alias("is_home"))
        .select(
            "game_id",
            "season",
            "date",
            "team_id",
            "opp_team_id",
            "is_home",
            "neutral_site",
            "poss",
            "off_eff",
            "def_eff",
        )
        .sort("game_id", "team_id")
    )


_ADJ_SCHEMA = {
    "season": pl.Int64,
    "team_id": pl.Utf8,
    "adj_o": pl.Float64,
    "adj_d": pl.Float64,
    "adj_em": pl.Float64,
    "raw_o": pl.Float64,
    "raw_d": pl.Float64,
    "games": pl.Int64,
}


def _adjust_one_season(sub: pl.DataFrame, season: int, hfa: float, max_iter: int, tol: float) -> pl.DataFrame:
    """Fixed-point opponent adjustment for one season's game-efficiency rows."""
    teams = sub["team_id"].unique(maintain_order=True).to_list()
    index = {t: i for i, t in enumerate(teams)}
    n = len(teams)
    ti = np.array([index[t] for t in sub["team_id"].to_list()], dtype=np.int64)
    oi = np.array([index[t] for t in sub["opp_team_id"].to_list()], dtype=np.int64)
    off = sub["off_eff"].to_numpy().astype(float)
    dfn = sub["def_eff"].to_numpy().astype(float)
    is_home = sub["is_home"].to_numpy()
    neutral = sub["neutral_site"].to_numpy()

    half = hfa / 2.0
    loc_o = np.where(neutral, 0.0, np.where(is_home, half, -half))
    loc_d = np.where(neutral, 0.0, np.where(is_home, -half, half))
    avg = float(off.mean())

    counts: np.ndarray = np.bincount(ti, minlength=n).astype(float)
    raw_o = np.bincount(ti, weights=off, minlength=n) / counts
    raw_d = np.bincount(ti, weights=dfn, minlength=n) / counts

    adj_o, adj_d = raw_o.copy(), raw_d.copy()
    for _ in range(max_iter):
        contrib_o = off - (adj_d[oi] - avg) - loc_o
        contrib_d = dfn - (adj_o[oi] - avg) - loc_d
        new_o = np.bincount(ti, weights=contrib_o, minlength=n) / counts
        new_d = np.bincount(ti, weights=contrib_d, minlength=n) / counts
        delta = max(float(np.abs(new_o - adj_o).max()), float(np.abs(new_d - adj_d).max()))
        adj_o, adj_d = new_o, new_d
        if delta < tol:
            break

    return pl.DataFrame(
        {
            "season": [season] * n,
            "team_id": teams,
            "adj_o": adj_o,
            "adj_d": adj_d,
            "adj_em": adj_o - adj_d,
            "raw_o": raw_o,
            "raw_d": raw_d,
            "games": counts.astype(np.int64),
        },
        schema=_ADJ_SCHEMA,
    )


def adjust_efficiency(
    game_eff: pl.DataFrame, *, league: str = "mens", max_iter: int = 100, tol: float = 1e-4
) -> pl.DataFrame:
    """Iterative opponent-adjusted efficiency -> AdjO / AdjD / AdjEM per team-season.

    KenPom-style fixed point: initialise ``adj_o = raw_o`` / ``adj_d = raw_d``,
    then repeatedly recompute each team's rating from its games with the
    opponent's *current* adjusted rating and a home-court adjustment removed,
    until the largest change is below ``tol``. Ratings are computed independently
    per season (a team's opponent pool is within-season).

    The per-game offensive update is
    ``off_eff - (adj_d_opp - avg) - loc_o`` where ``loc_o`` is ``+hfa/2`` at
    home, ``-hfa/2`` away, ``0`` neutral (defense is symmetric with the opposite
    sign); ``avg`` is the league mean efficiency and ``hfa`` comes from
    :func:`~sportsdataverse.mbb.mbb_prediction_constants.get_constants`.

    Args:
        game_eff: Output of :func:`raw_game_efficiency`.
        league: ``"mens"`` / ``"womens"`` -- selects the HFA constant.
        max_iter: Maximum fixed-point iterations.
        tol: Convergence tolerance on the largest rating change.

    Returns:
        One row per (season, team_id): ``season, team_id, adj_o, adj_d, adj_em,
        raw_o, raw_d, games``. Empty input returns that schema with zero rows.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_team_ratings import adjust_efficiency, raw_game_efficiency
            ratings = adjust_efficiency(raw_game_efficiency(sched, box))
    """
    if game_eff.height == 0:
        return pl.DataFrame(schema=_ADJ_SCHEMA)
    hfa = float(get_constants(league).hfa)
    frames = [
        _adjust_one_season(sub, int(sub["season"][0]), hfa, max_iter, tol)
        for _key, sub in game_eff.group_by("season", maintain_order=True)
    ]
    return pl.concat(frames)


_TEMPO_SCHEMA = {"season": pl.Int64, "team_id": pl.Utf8, "adj_tempo": pl.Float64}


def _adjust_tempo_one_season(sub: pl.DataFrame, season: int, avg: float, max_iter: int, tol: float) -> pl.DataFrame:
    """Fixed-point opponent-adjusted tempo for one season's game rows."""
    teams = sub["team_id"].unique(maintain_order=True).to_list()
    index = {t: i for i, t in enumerate(teams)}
    n = len(teams)
    ti = np.array([index[t] for t in sub["team_id"].to_list()], dtype=np.int64)
    oi = np.array([index[t] for t in sub["opp_team_id"].to_list()], dtype=np.int64)
    poss = sub["poss"].to_numpy().astype(float)

    counts: np.ndarray = np.bincount(ti, minlength=n).astype(float)
    raw = np.bincount(ti, weights=poss, minlength=n) / counts

    adj = raw.copy()
    for _ in range(max_iter):
        contrib = poss - (adj[oi] - avg)
        new = np.bincount(ti, weights=contrib, minlength=n) / counts
        delta = float(np.abs(new - adj).max())
        adj = new
        if delta < tol:
            break

    return pl.DataFrame({"season": [season] * n, "team_id": teams, "adj_tempo": adj}, schema=_TEMPO_SCHEMA)


def adjust_tempo(
    game_eff: pl.DataFrame, *, league: str = "mens", max_iter: int = 100, tol: float = 1e-4
) -> pl.DataFrame:
    """Opponent-adjusted tempo (possessions/40) per team-season.

    Same fixed point as :func:`adjust_efficiency`, applied to game possessions
    under the additive model ``poss = tempo_i + tempo_j - avg``: a team's tempo
    is recovered by removing its opponents' current adjusted tempo. ``avg`` is
    the league baseline tempo from
    :func:`~sportsdataverse.mbb.mbb_prediction_constants.get_constants`.

    Args:
        game_eff: Output of :func:`raw_game_efficiency`.
        league: ``"mens"`` / ``"womens"`` -- selects the tempo baseline.
        max_iter: Maximum fixed-point iterations.
        tol: Convergence tolerance on the largest tempo change.

    Returns:
        One row per (season, team_id): ``season, team_id, adj_tempo``. Empty
        input returns that schema with zero rows.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_team_ratings import adjust_tempo, raw_game_efficiency
            tempo = adjust_tempo(raw_game_efficiency(sched, box))
    """
    if game_eff.height == 0:
        return pl.DataFrame(schema=_TEMPO_SCHEMA)
    avg = float(get_constants(league).avg_tempo)
    frames = [
        _adjust_tempo_one_season(sub, int(sub["season"][0]), avg, max_iter, tol)
        for _key, sub in game_eff.group_by("season", maintain_order=True)
    ]
    return pl.concat(frames)


_RATINGS_SCHEMA = {
    "season": pl.Int64,
    "team_id": pl.Utf8,
    "adj_o": pl.Float64,
    "adj_d": pl.Float64,
    "adj_em": pl.Float64,
    "adj_tempo": pl.Float64,
    "raw_o": pl.Float64,
    "raw_d": pl.Float64,
    "games": pl.Int64,
    "rank": pl.Int64,
    "adj_em_z": pl.Float64,
}


def _normalize_schedule(schedule: pl.DataFrame) -> pl.DataFrame:
    """Map ESPN ``load_mbb_schedule`` columns to the engine's canonical names.

    ESPN ships ``home_id`` / ``away_id`` and both a string ``date`` (with time)
    and a proper ``game_date``; the engine wants ``home_team_id`` /
    ``away_team_id`` / ``date`` (a ``Date``). Frames already using the canonical
    names (e.g. the committed fixtures) pass through unchanged.
    """
    out = schedule
    if "home_id" in out.columns and "home_team_id" not in out.columns:
        out = out.rename({"home_id": "home_team_id"})
    if "away_id" in out.columns and "away_team_id" not in out.columns:
        out = out.rename({"away_id": "away_team_id"})
    if "game_date" in out.columns:
        if "date" in out.columns:
            out = out.drop("date")
        out = out.rename({"game_date": "date"})
    return out


def mbb_team_ratings(
    seasons: int | list[int], *, league: str = "mens", return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """Opponent-adjusted team ratings (AdjO/AdjD/AdjEM/AdjTempo) per team-season.

    Loads schedule + team boxscore for ``seasons``, computes per-game efficiency,
    runs the opponent-adjustment fixed points, and adds a per-season dense
    ``rank`` (on ``adj_em`` descending) and ``adj_em_z`` (z-score of ``adj_em``).

    Args:
        seasons: A season (e.g. ``2024``) or list of seasons.
        league: ``"mens"`` / ``"womens"`` -- selects the constants.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per (season, team_id) with columns ``season, team_id, adj_o,
        adj_d, adj_em, adj_tempo, raw_o, raw_d, games, rank, adj_em_z``. Empty
        input returns that schema with zero rows.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_team_ratings import mbb_team_ratings
            ratings = mbb_team_ratings(2024)
            ratings.sort("rank").head()
    """
    seasons_list = [seasons] if isinstance(seasons, int) else list(seasons)
    schedule = _normalize_schedule(load_mbb_schedule(seasons_list))
    team_box = load_mbb_team_boxscore(seasons_list)
    eff = raw_game_efficiency(schedule, team_box)

    if eff.height == 0:
        out = pl.DataFrame(schema=_RATINGS_SCHEMA)
    else:
        ratings = adjust_efficiency(eff, league=league)
        tempo = adjust_tempo(eff, league=league)
        out = (
            ratings.join(tempo, on=["season", "team_id"], how="left")
            .with_columns(
                pl.col("adj_em").rank(method="dense", descending=True).over("season").cast(pl.Int64).alias("rank"),
                (
                    (pl.col("adj_em") - pl.col("adj_em").mean().over("season")) / pl.col("adj_em").std().over("season")
                ).alias("adj_em_z"),
            )
            .select(list(_RATINGS_SCHEMA))
            .sort(["season", "rank"])
        )

    return out.to_pandas() if return_as_pandas else out


__all__ = [
    "raw_game_efficiency",
    "adjust_efficiency",
    "adjust_tempo",
    "mbb_team_ratings",
]
