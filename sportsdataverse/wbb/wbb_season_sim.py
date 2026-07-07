"""Women's college basketball season / bracket Monte Carlo.

Thin shim over :mod:`sportsdataverse.mbb.mbb_season_sim` -- the samplers are
league-agnostic (margin sigma / HFA / em_scale come from
``LEAGUE_CONSTANTS["womens"]``).

Example:
    Quick start::

        from sportsdataverse.wbb.wbb_season_sim import wbb_season_sim
        odds = wbb_season_sim(ratings, remaining, n_sims=5000, seed=42)

See Also:
    * `wehoop <https://wehoop.sportsdataverse.org>`_ -- women's basketball (R)
"""

from __future__ import annotations

from typing import Union

import numpy as np
import pandas as pd
import polars as pl

from sportsdataverse.mbb.mbb_season_sim import mbb_bracket_sim, mbb_season_sim

__all__ = [
    "simulate_game",
    "wbb_bracket_sim",
    "wbb_season_sim",
]


def simulate_game(home_em: float, away_em: float, neutral: bool, rng: np.random.Generator) -> bool:
    """Sample one women's game outcome (women's sigma/HFA/em_scale).

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.wbb.wbb_season_sim import simulate_game
            simulate_game(20.0, 5.0, False, np.random.default_rng(0))
    """
    from sportsdataverse.mbb.mbb_season_sim import simulate_game as _core  # noqa: PLC0415

    return _core(home_em, away_em, neutral, rng, league="womens")


def wbb_season_sim(
    ratings: pl.DataFrame,
    remaining_schedule: pl.DataFrame,
    *,
    n_sims: int = 10000,
    seed: int = 0,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Women's remaining-schedule Monte Carlo.

    Delegates to :func:`sportsdataverse.mbb.mbb_season_sim.mbb_season_sim`
    with ``league="womens"`` -- see that function for the full contract.

    Example:
        Quick start::

            from sportsdataverse.wbb import wbb_season_sim
            odds = wbb_season_sim(ratings, remaining, n_sims=5000, seed=42)
    """
    if return_as_pandas:
        return mbb_season_sim(
            ratings, remaining_schedule, n_sims=n_sims, seed=seed, league="womens", return_as_pandas=True
        )
    return mbb_season_sim(ratings, remaining_schedule, n_sims=n_sims, seed=seed, league="womens")


def wbb_bracket_sim(
    seeded_field: pl.DataFrame,
    ratings: pl.DataFrame,
    *,
    n_sims: int = 10000,
    seed: int = 0,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Women's single-elimination bracket Monte Carlo.

    Delegates to :func:`sportsdataverse.mbb.mbb_season_sim.mbb_bracket_sim`
    with ``league="womens"`` -- see that function for the full contract.

    Example:
        Quick start::

            from sportsdataverse.wbb import wbb_bracket_sim
            odds = wbb_bracket_sim(field_64, ratings, n_sims=20000, seed=42)
    """
    if return_as_pandas:
        return mbb_bracket_sim(seeded_field, ratings, n_sims=n_sims, seed=seed, league="womens", return_as_pandas=True)
    return mbb_bracket_sim(seeded_field, ratings, n_sims=n_sims, seed=seed, league="womens")
