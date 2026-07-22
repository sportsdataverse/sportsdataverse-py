"""Ensemble outputs as tidy datasets.

:func:`~sportsdataverse.nba.nba_possession_sim.engine.simulate_ensemble`
returns numpy sample vectors — ideal for pricing math, awkward to publish.
These converters give the ensemble a dataset shape with a stable,
documented schema: a per-simulation samples frame, a one-row market
summary, and the long player-points frame (the team-level counterparts of
:func:`~sportsdataverse.nba.nba_possession_sim.props.player_prop_distributions`).
Producers can gate them with
:func:`~sportsdataverse.modeling.integrity.contracts.derive_contract`
like any other published dataset.
"""

from __future__ import annotations

from typing import Any, Dict

import polars as pl

_SAMPLES_SCHEMA = {
    "sim_id": pl.Int64,
    "score_home": pl.Int64,
    "score_away": pl.Int64,
    "total": pl.Int64,
    "margin": pl.Int64,
    "home_win": pl.Boolean,
}
_PLAYER_POINTS_SCHEMA = {"sim_id": pl.Int64, "player_id": pl.Int64, "pts": pl.Int64}


def ensemble_samples(ensemble: Dict[str, Any]) -> pl.DataFrame:
    """One row per simulated game.

    Args:
        ensemble: Output of ``simulate_ensemble``.

    Returns:
        Frame with ``sim_id``, ``score_home``, ``score_away``, ``total``,
        ``margin`` (home perspective), ``home_win``.

    Raises:
        ValueError: When the dict is missing the sample vectors.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_possession_sim import (
                ensemble_samples, simulate_ensemble,
            )
            frame = ensemble_samples(simulate_ensemble(shelf, n_sim=500, seed=7))
            frame.filter(pl.col("margin") > 0).height
    """
    missing = [key for key in ("score_home", "score_away") if key not in ensemble]
    if missing:
        raise ValueError(f"ensemble dict is missing sample vectors: {missing}")
    home = ensemble["score_home"]
    away = ensemble["score_away"]
    return pl.DataFrame(
        {
            "sim_id": range(len(home)),
            "score_home": home,
            "score_away": away,
            "total": home + away,
            "margin": home - away,
            "home_win": home > away,
        },
        schema=_SAMPLES_SCHEMA,
    )


def ensemble_market_summary(ensemble: Dict[str, Any]) -> pl.DataFrame:
    """The game-market summary as a one-row dataset.

    Args:
        ensemble: Output of ``simulate_ensemble``.

    Returns:
        One row: ``n_sim``, ``win_prob_home``, mean/std and p10/p50/p90 of
        ``total`` and ``margin``.

    Example:
        Quick start::

            summary = ensemble_market_summary(ens)
            summary["total_p50"][0]
    """
    samples = ensemble_samples(ensemble)
    row: Dict[str, Any] = {
        "n_sim": samples.height,
        "win_prob_home": float(samples["home_win"].cast(pl.Float64).mean()),
    }
    for stat in ("total", "margin"):
        column = samples[stat].cast(pl.Float64)
        row[f"{stat}_mean"] = float(column.mean())
        row[f"{stat}_std"] = float(column.std(ddof=0) or 0.0)
        for quantile_label, quantile in (("p10", 0.1), ("p50", 0.5), ("p90", 0.9)):
            row[f"{stat}_{quantile_label}"] = float(column.quantile(quantile, interpolation="linear"))
    return pl.DataFrame([row])


def player_points_long(ensemble: Dict[str, Any]) -> pl.DataFrame:
    """Per-player point samples in long form (empty without attribution).

    Args:
        ensemble: Output of ``simulate_ensemble`` (``player_points`` is
            populated only when an attribution was supplied).

    Returns:
        Frame with ``sim_id``, ``player_id``, ``pts`` — one row per
        (simulation, player); zero rows (documented schema) when the
        ensemble carries no attribution.

    Example:
        Quick start::

            long = player_points_long(ens)
            long.group_by("player_id").agg(pl.col("pts").mean()).head()
    """
    player_points = ensemble.get("player_points")
    if not player_points:
        return pl.DataFrame(schema=_PLAYER_POINTS_SCHEMA)
    frames = [
        pl.DataFrame(
            {"sim_id": range(len(samples)), "player_id": [int(pid)] * len(samples), "pts": samples},
            schema=_PLAYER_POINTS_SCHEMA,
        )
        for pid, samples in sorted(player_points.items())
    ]
    return pl.concat(frames)
