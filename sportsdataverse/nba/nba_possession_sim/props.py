"""Player prop distributions + fair pricing from the simulated boxscores.

The seam where the sim engine meets the odds math:
:func:`~sportsdataverse.nba.nba_possession_sim.attribution.simulate_player_boxscores`
already emits per-player sample vectors for pts/reb/ast with exact team
conservation; this module turns them into the prop surface — empirical PMFs
per (player, stat) and fair over/under prices at any line, with push mass
handled the market way (integer lines refund pushes, so fair prices come
from the push-excluded conditional probabilities).

Pricing composes :mod:`sportsdataverse.odds.odds_math` rather than
reimplementing it — ``prob_to_american`` for fair odds; feed the same
sample vectors to ``combine_legs``/``sample_using_copula`` for correlated
parlays across props.
"""

from __future__ import annotations

import dataclasses
from typing import Any, Dict, Sequence, Tuple

import numpy as np
import polars as pl

from sportsdataverse.odds.odds_math import prob_to_american

_DEFAULT_STATS: Tuple[str, ...] = ("pts", "reb", "ast")


def player_prop_distributions(
    box: Dict[str, Any],
    *,
    stats: Sequence[str] = _DEFAULT_STATS,
) -> pl.DataFrame:
    """Empirical per-(player, stat) PMFs from simulated boxscore vectors.

    Args:
        box: Output of ``simulate_player_boxscores`` (``{stat: {player_id:
            sample vector}}`` plus ``n_sim``).
        stats: Stats to tabulate (must exist in ``box``).

    Returns:
        Long-form frame: ``player_id``, ``stat``, ``value``, ``prob``
        (probabilities per (player, stat) sum to 1), ``n_sim``.

    Raises:
        ValueError: When a requested stat is absent from ``box``.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_possession_sim import (
                simulate_player_boxscores, player_prop_distributions,
            )
            box = simulate_player_boxscores(shelf, att, n_sim=500, seed=7)
            pmfs = player_prop_distributions(box)
            pmfs.filter(pl.col("stat") == "pts").head()
    """
    missing = [stat for stat in stats if stat not in box]
    if missing:
        raise ValueError(f"stats absent from the boxscore dict: {missing}")
    n_sim = int(box["n_sim"])
    rows = []
    for stat in stats:
        for player_id, samples in box[stat].items():
            values, counts = np.unique(np.asarray(samples, dtype=np.int64), return_counts=True)
            for value, count in zip(values.tolist(), counts.tolist()):
                rows.append(
                    {
                        "player_id": int(player_id),
                        "stat": stat,
                        "value": int(value),
                        "prob": count / n_sim,
                        "n_sim": n_sim,
                    }
                )
    schema = {"player_id": pl.Int64, "stat": pl.Utf8, "value": pl.Int64, "prob": pl.Float64, "n_sim": pl.Int64}
    return pl.DataFrame(rows, schema=schema).sort("player_id", "stat", "value")


@dataclasses.dataclass(frozen=True)
class PropPrice:
    """A fair over/under quote for one prop line.

    Attributes:
        line: The prop line.
        p_over: P(stat > line).
        p_under: P(stat < line).
        p_push: P(stat == line) (nonzero only at achievable integer lines).
        fair_over: Fair american odds for the over (push-excluded
            conditional probability, the refund convention).
        fair_under: Fair american odds for the under.
        mean: Sample mean of the stat.
        median: Sample median.
    """

    line: float
    p_over: float
    p_under: float
    p_push: float
    fair_over: int
    fair_under: int
    mean: float
    median: float


def price_prop(samples: np.ndarray, line: float) -> PropPrice:
    """Fair over/under pricing for one prop from a sample vector.

    Args:
        samples: Simulated stat values for the player.
        line: The prop line (half-point lines carry no push mass).

    Returns:
        The :class:`PropPrice`.

    Raises:
        ValueError: On an empty sample vector.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_possession_sim.props import price_prop
            quote = price_prop(box["pts"][player_id], 24.5)
            quote.p_over, quote.fair_over
    """
    values = np.asarray(samples, dtype=float)
    if values.size == 0:
        raise ValueError("cannot price a prop from an empty sample vector")
    p_over = float(np.mean(values > line))
    p_push = float(np.mean(values == line))
    p_under = float(np.mean(values < line))
    live = max(1e-12, p_over + p_under)  # push-excluded conditional mass
    return PropPrice(
        line=float(line),
        p_over=p_over,
        p_under=p_under,
        p_push=p_push,
        fair_over=prob_to_american(min(1.0 - 1e-9, max(1e-9, p_over / live))),
        fair_under=prob_to_american(min(1.0 - 1e-9, max(1e-9, p_under / live))),
        mean=float(values.mean()),
        median=float(np.median(values)),
    )


def price_board(box: Dict[str, Any], board: pl.DataFrame) -> pl.DataFrame:
    """Price a board of (player_id, stat, line) props from one simulation.

    Args:
        box: Output of ``simulate_player_boxscores``.
        board: One row per prop: ``player_id``, ``stat``, ``line``.

    Returns:
        The board with ``p_over``, ``p_under``, ``p_push``, ``fair_over``,
        ``fair_under``, ``mean``, ``median`` appended.

    Raises:
        ValueError: When a board row names a (player, stat) the simulation
            did not produce — pricing an unknown prop silently is worse
            than failing loudly.

    Example:
        An availability-scenario board::

            board = pl.DataFrame({"player_id": [pid], "stat": ["pts"], "line": [24.5]})
            priced = price_board(box, board)
    """
    rows = []
    for row in board.iter_rows(named=True):
        stat, player_id = str(row["stat"]), int(row["player_id"])
        if stat not in box or player_id not in box[stat]:
            raise ValueError(f"prop not in the simulation: player {player_id} stat {stat!r}")
        quote = price_prop(box[stat][player_id], float(row["line"]))
        rows.append({**row, **dataclasses.asdict(quote)})
    return pl.DataFrame(rows)
