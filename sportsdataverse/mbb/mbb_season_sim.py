"""Season and bracket Monte Carlo simulators.

Phase 6 of the MBB/WBB prediction & tournament stack. Samples game outcomes
from the Phase-2 closed forms (margin ``~ Normal(exp_margin, margin_sd)``)
with a caller-seeded ``numpy.random.default_rng`` so every simulation is
deterministic and reproducible.
"""

from __future__ import annotations

from typing import Literal, Union, overload

import numpy as np
import pandas as pd
import polars as pl
from scipy.stats import norm

from sportsdataverse.mbb.mbb_game_predict import predict_margin
from sportsdataverse.mbb.mbb_prediction_constants import get_constants

__all__ = [
    "mbb_bracket_sim",
    "mbb_season_sim",
    "simulate_game",
]


def simulate_game(
    home_em: float,
    away_em: float,
    neutral: bool,
    rng: np.random.Generator,
    *,
    league: str = "mens",
) -> bool:
    """Sample one game outcome: margin ``~ Normal(exp_margin, margin_sd)``.

    Args:
        home_em: Home team's adjusted efficiency margin.
        away_em: Away team's adjusted efficiency margin.
        neutral: True for a neutral-site game.
        rng: A seeded ``numpy.random.Generator`` (caller owns determinism).
        league: ``"mens"`` or ``"womens"``.

    Returns:
        True if the home team wins the sampled game.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.mbb.mbb_season_sim import simulate_game
            simulate_game(20.0, 5.0, False, np.random.default_rng(0))
    """
    exp = predict_margin(home_em, away_em, neutral, league=league)
    return bool(rng.normal(exp, get_constants(league).margin_sd) > 0.0)


def _win_matrix(
    p_home: np.ndarray,
    home_idx: np.ndarray,
    away_idx: np.ndarray,
    n_teams: int,
    n_sims: int,
    rng: np.random.Generator,
) -> np.ndarray:
    """(n_sims, n_teams) win counts from independent Bernoulli(p_home) draws."""
    home_wins = rng.random((n_sims, len(p_home))) < p_home  # margin>0 <=> U < Phi(exp/sd)
    wins: np.ndarray = np.zeros((n_sims, n_teams), dtype=np.int64)
    for g in range(len(p_home)):
        wins[:, home_idx[g]] += home_wins[:, g]
        wins[:, away_idx[g]] += ~home_wins[:, g]
    return wins


@overload
def mbb_season_sim(
    ratings: pl.DataFrame,
    remaining_schedule: pl.DataFrame,
    *,
    n_sims: int = 10000,
    seed: int = 0,
    league: str = "mens",
    return_as_pandas: Literal[False] = False,
) -> pl.DataFrame: ...


@overload
def mbb_season_sim(
    ratings: pl.DataFrame,
    remaining_schedule: pl.DataFrame,
    *,
    n_sims: int = 10000,
    seed: int = 0,
    league: str = "mens",
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...


def mbb_season_sim(
    ratings: pl.DataFrame,
    remaining_schedule: pl.DataFrame,
    *,
    n_sims: int = 10000,
    seed: int = 0,
    league: str = "mens",
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Monte Carlo the remaining schedule: expected wins + title odds.

    Args:
        ratings: One row per team: ``season, team_id, adj_em`` and optionally
            ``conference`` (enables ``conf_title_prob``) and ``current_wins``
            (added to the simulated remaining wins).
        remaining_schedule: Games to simulate: ``home_team_id, away_team_id,
            neutral_site``.
        n_sims: Number of simulated seasons.
        seed: Seed for ``numpy.random.default_rng`` (deterministic output).
        league: ``"mens"`` or ``"womens"``.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per team: ``season, team_id, exp_wins`` (mean simulated total
        wins), ``playoff_prob`` (share of sims finishing in the top 68 win
        totals -- a field-size proxy, ties broken by ``adj_em``) and
        ``conf_title_prob`` (share of sims with the most wins among conference
        members; ties count for every tied team; null without a
        ``conference`` column).

    Raises:
        KeyError: If a scheduled team is missing from ``ratings`` -- every
            ``home_team_id`` / ``away_team_id`` must have a ratings row.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_season_sim import mbb_season_sim
            odds = mbb_season_sim(ratings, remaining, n_sims=5000, seed=42)
    """
    c = get_constants(league)
    teams = ratings.get_column("team_id").to_list()
    index = {t: i for i, t in enumerate(teams)}
    em = ratings.get_column("adj_em").to_numpy()

    sched = remaining_schedule
    home_idx = np.array([index[t] for t in sched.get_column("home_team_id").to_list()])
    away_idx = np.array([index[t] for t in sched.get_column("away_team_id").to_list()])
    neutral = sched.get_column("neutral_site").cast(pl.Boolean).to_numpy()
    exp_margin = c.em_scale * (em[home_idx] - em[away_idx]) + c.hfa * (~neutral)
    p_home = norm.cdf(exp_margin / c.margin_sd)

    rng = np.random.default_rng(seed)
    wins = _win_matrix(p_home, home_idx, away_idx, len(teams), n_sims, rng)
    if "current_wins" in ratings.columns:
        wins = wins + ratings.get_column("current_wins").cast(pl.Int64).to_numpy()[None, :]

    # playoff proxy: top-68 win totals per sim, adj_em as the tiebreaker
    order_key = wins.astype(np.float64) + (em - em.min())[None, :] / (np.ptp(em) + 1.0) * 0.5
    if len(teams) <= 68:
        playoff = np.ones(len(teams))
    else:
        cut = np.partition(order_key, -68, axis=1)[:, -68]
        playoff = (order_key >= cut[:, None]).mean(axis=0)

    out = pl.DataFrame(
        {
            "season": ratings.get_column("season"),
            "team_id": ratings.get_column("team_id"),
            "exp_wins": wins.mean(axis=0),
            "playoff_prob": playoff,
        }
    )
    if "conference" in ratings.columns:
        conf = ratings.get_column("conference").to_list()
        conf_title = np.zeros(len(teams))
        for cname in set(conf):
            members = [i for i, x in enumerate(conf) if x == cname]
            best = wins[:, members].max(axis=1)
            for i in members:
                conf_title[i] = (wins[:, i] == best).mean()
        out = out.with_columns(pl.Series("conf_title_prob", conf_title, dtype=pl.Float64))
    else:
        out = out.with_columns(pl.lit(None, dtype=pl.Float64).alias("conf_title_prob"))
    result = out.select("season", "team_id", "exp_wins", "playoff_prob", "conf_title_prob")
    return result.to_pandas() if return_as_pandas else result


_ROUND_COLS = ["reach_r32", "reach_s16", "reach_e8", "reach_f4", "reach_final", "champion"]


@overload
def mbb_bracket_sim(
    seeded_field: pl.DataFrame,
    ratings: pl.DataFrame,
    *,
    n_sims: int = 10000,
    seed: int = 0,
    league: str = "mens",
    return_as_pandas: Literal[False] = False,
) -> pl.DataFrame: ...


@overload
def mbb_bracket_sim(
    seeded_field: pl.DataFrame,
    ratings: pl.DataFrame,
    *,
    n_sims: int = 10000,
    seed: int = 0,
    league: str = "mens",
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...


def mbb_bracket_sim(
    seeded_field: pl.DataFrame,
    ratings: pl.DataFrame,
    *,
    n_sims: int = 10000,
    seed: int = 0,
    league: str = "mens",
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Single-elimination Monte Carlo over a bracket-ordered field.

    Rows of ``seeded_field`` are bracket slots: adjacent rows meet in round 1
    and winners of adjacent games meet next round (the standard fold). All
    games are neutral-site. Round columns are named from the END of a 64-team
    bracket (``champion`` back to ``reach_r32``); with a smaller field the
    early columns are 1.0 for everyone (trivially reached).

    Args:
        seeded_field: Bracket-ordered rows with ``team_id`` (and typically
            ``seed`` for reference).
        ratings: One row per team: ``team_id, adj_em``.
        n_sims: Number of simulated brackets.
        seed: Seed for ``numpy.random.default_rng``.
        league: ``"mens"`` or ``"womens"``.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per field team: ``team_id, seed?, reach_r32, reach_s16,
        reach_e8, reach_f4, reach_final, champion`` (probabilities).

    Raises:
        ValueError: If the field size is not a power of two.
        KeyError: If a field team is missing from ``ratings``.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_season_sim import mbb_bracket_sim
            odds = mbb_bracket_sim(field_64, ratings, n_sims=20000, seed=42)
    """
    n = seeded_field.height
    if n & (n - 1) != 0 or n < 2:
        raise ValueError(f"bracket field size must be a power of two, got {n}")
    c = get_constants(league)
    em_map = dict(zip(ratings.get_column("team_id").to_list(), ratings.get_column("adj_em").to_list()))
    slots = seeded_field.get_column("team_id").to_list()
    em = np.array([em_map[t] for t in slots])

    n_rounds = int(np.log2(n))
    rng = np.random.default_rng(seed)
    alive = np.tile(np.arange(n), (n_sims, 1))  # slot indices of surviving teams
    reach = np.zeros((n_rounds, n), dtype=np.int64)  # [round, team] survivors AFTER each round

    for r in range(n_rounds):
        a, b = alive[:, 0::2], alive[:, 1::2]
        exp = c.em_scale * (em[a] - em[b])  # neutral: no HFA
        p_a = norm.cdf(exp / c.margin_sd)
        a_wins = rng.random(a.shape) < p_a
        alive = np.where(a_wins, a, b)
        np.add.at(reach[r], alive.ravel(), 1)

    probs = reach / n_sims  # row r = P(surviving round r)
    out = seeded_field.select([col for col in ("team_id", "seed") if col in seeded_field.columns])
    for i, col in enumerate(reversed(_ROUND_COLS)):  # champion, reach_final, ...
        r = n_rounds - 1 - i
        vals = probs[r] if r >= 0 else np.ones(n)
        out = out.with_columns(pl.Series(col, vals, dtype=pl.Float64))
    result = out.select(*[col for col in ("team_id", "seed") if col in out.columns], *_ROUND_COLS)
    return result.to_pandas() if return_as_pandas else result
