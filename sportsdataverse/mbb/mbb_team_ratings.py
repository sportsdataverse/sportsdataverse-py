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

from sportsdataverse._common.ratings import drop_unusable_possession_rows, iterative_opponent_adjust
from sportsdataverse.errors import InsufficientInputError
from sportsdataverse.mbb.mbb_loaders import load_mbb_schedule, load_mbb_team_boxscore
from sportsdataverse.mbb.mbb_prediction_constants import get_constants

if TYPE_CHECKING:
    import pandas as pd


_EFF_SCHEMA: dict[str, pl.DataType] = {
    "game_id": pl.Utf8,
    "season": pl.Int64,
    "date": pl.Date,
    "team_id": pl.Utf8,
    "opp_team_id": pl.Utf8,
    "is_home": pl.Boolean,
    "neutral_site": pl.Boolean,
    "poss": pl.Float64,
    "off_eff": pl.Float64,
    "def_eff": pl.Float64,
}

_BOX_REQUIRED = (
    "game_id",
    "team_id",
    "field_goals_attempted",
    "offensive_rebounds",
    "turnovers",
    "free_throws_attempted",
    "team_score",
)
_SCHED_REQUIRED = ("game_id", "season", "date", "home_team_id", "away_team_id", "neutral_site")


def _assert_possession_inputs(team_box: pl.DataFrame) -> None:
    """Refuse a boxscore whose ``turnovers`` are structurally absent (all zero).

    A real basketball game always has turnovers, so a frame in which *no* row
    carries a positive one is a schema gap wearing a zero -- ESPN's pre-2013
    women's team box is exactly that (``turnovers`` is ``0`` in 100% of rows
    for WBB 2003-2012, and populated from 2013). Zeros are not nulls, so no
    null check sees it, and the possession estimate silently loses the whole
    turnover term (~16 of ~71 possessions per team-game, ~23%). The damage is
    not a constant rescale that a level band could absorb: each team's poss is
    understated by *its own* turnover count, so the induced error is
    correlated with turnover rate and reorders teams.

    Raises:
        InsufficientInputError: When the frame has rows and no positive turnover.
    """
    if team_box.height == 0 or "turnovers" not in team_box.columns:
        return
    tov = team_box["turnovers"].cast(pl.Float64, strict=False)
    if tov.null_count() == tov.len():
        return
    if float(tov.max() or 0.0) <= 0.0:
        raise InsufficientInputError(
            f"team boxscore carries no turnovers (0 in all {team_box.height} rows): the possession "
            "estimate FGA - OREB + TO + 0.44*FTA is missing its turnover term (~23% of possessions), "
            "so efficiency and tempo would be distorted by each team's own turnover rate. This is the "
            "ESPN pre-2013 women's box schema -- the season is not ratable from these inputs."
        )


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
        input returns that schema with zero rows. Team-game rows whose
        possession estimate is non-positive (an all-zero ESPN boxscore shell)
        are dropped with a ``UserWarning`` -- their efficiency is undefined,
        and one of them poisons the whole season's fixed point.

    Raises:
        InsufficientInputError: When ``team_box`` has rows but no positive
            ``turnovers`` -- the possession estimate's turnover term is
            structurally absent (ESPN's pre-2013 women's box), so no rating
            built from it would be meaningful.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_loaders import load_mbb_schedule, load_mbb_team_boxscore
            from sportsdataverse.mbb.mbb_team_ratings import raw_game_efficiency
            eff = raw_game_efficiency(load_mbb_schedule([2024]), load_mbb_team_boxscore([2024]))
    """
    # A season with no released boxscore asset comes back from the loader as a
    # COLUMN-LESS empty frame (e.g. WBB 2003) -- selecting game_id off it
    # raises ColumnNotFoundError instead of honoring the documented
    # empty-in/empty-out contract. Guard on emptiness AND required columns.
    if (
        schedule.height == 0
        or team_box.height == 0
        or any(c not in team_box.columns for c in _BOX_REQUIRED)
        or any(c not in schedule.columns for c in _SCHED_REQUIRED)
    ):
        return pl.DataFrame(schema=_EFF_SCHEMA)
    _assert_possession_inputs(team_box)
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
    )
    paired = drop_unusable_possession_rows(paired)
    paired = paired.with_columns(
        (100.0 * pl.col("pts") / pl.col("poss")).alias("off_eff"),
        (100.0 * pl.col("opp_pts") / pl.col("poss")).alias("def_eff"),
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
    """Fixed-point opponent adjustment for one season's game-efficiency rows.

    Thin wrapper (T7.2): the fixed-point core moved verbatim to
    :func:`sportsdataverse._common.ratings.iterative_opponent_adjust`
    (``baseline=None`` reproduces this module's own data-mean average);
    only the column rename to this module's public names stays local.
    """
    core = iterative_opponent_adjust(
        sub,
        team_col="team_id",
        opp_col="opp_team_id",
        off_col="off_eff",
        def_col="def_eff",
        home_col="is_home",
        neutral_col="neutral_site",
        hfa=hfa,
        baseline=None,
        max_iter=max_iter,
        tol=tol,
    )
    return (
        core.rename(
            {"adj_off": "adj_o", "adj_def": "adj_d", "adj_net": "adj_em", "raw_off": "raw_o", "raw_def": "raw_d"}
        )
        .with_columns(pl.lit(season, dtype=pl.Int64).alias("season"))
        .select("season", "team_id", "adj_o", "adj_d", "adj_em", "raw_o", "raw_d", "games")
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


def _league_loaders(league: str):  # type: ignore[no-untyped-def]
    """(schedule, team_box) loaders for a league -- the mens cores serve wbb too."""
    if league == "womens":
        from sportsdataverse.wbb.wbb_loaders import load_wbb_schedule, load_wbb_team_boxscore  # noqa: PLC0415

        return load_wbb_schedule, load_wbb_team_boxscore
    return load_mbb_schedule, load_mbb_team_boxscore


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

    Raises:
        InsufficientInputError: When a requested season's team boxscore has no
            turnovers at all (ESPN's pre-2013 women's box schema), so its
            possession estimate -- and every rating derived from it -- would be
            wrong rather than merely missing.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_team_ratings import mbb_team_ratings
            ratings = mbb_team_ratings(2024)
            ratings.sort("rank").head()
    """
    seasons_list = [seasons] if isinstance(seasons, int) else list(seasons)
    load_schedule, load_team_box = _league_loaders(league)
    schedule = _normalize_schedule(load_schedule(seasons_list))
    team_box = load_team_box(seasons_list)
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
