"""Per-node models — the ``models2shelf`` half of the shelf (WS4 final form).

Empirical shelves (:func:`~sportsdataverse.nba.nba_possession_sim.shelf.build_shelf`)
are the reference ``priors2shelf`` path: raw frequency tables, sparse and noisy
where data is thin. This module is the ``models2shelf`` path that fully
actualizes the tree: each stochastic node gets a FITTED model —

* **OutcomeNode** — multinomial logistic regression of possession outcome
  on the gamestate features (score diff, period, clock);
* **ReboundNode** — logistic regression of P(offensive rebound) on the same
  features (per-state rebound rates, not one global scalar);
* **FreeThrowNode** — the empirical make rate IS its maximum-likelihood
  model at current data volume (a shooter-conditional model is the seam).

The models are then evaluated over the COMPLETE gamestate-key grid and the
predictions baked into an ordinary :class:`~sportsdataverse.nba.nba_possession_sim.shelf.Shelf`
— every reachable key exists (fallback rate is zero by construction),
sparse cells are smoothed by the model instead of memorized, and the engine
still does a dict lookup at sim time (train/serve decoupling preserved).
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import polars as pl
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline, make_pipeline
from sklearn.preprocessing import StandardScaler

from sportsdataverse.nba.nba_possession_sim.keygen import (
    SCORE_DIFF_CLIP,
    SCORE_DIFF_WIDTH,
    gamestate_key,
)
from sportsdataverse.nba.nba_possession_sim.shelf import OUTCOMES, Shelf, build_shelf

_FEATURES = ["score_diff", "period", "clock_seconds"]
#: Representative mid-bucket clock values, one per clock bin.
_CLOCK_MIDPOINTS = {"early": 600.0, "mid": 360.0, "late": 150.0, "clutch": 30.0}


def fit_outcome_node_model(events: pl.DataFrame) -> Pipeline:
    """Fit the OutcomeNode model: multiclass outcome | gamestate.

    Args:
        events: Classified possession events (real data; ``kind=="outcome"``
            rows are used).

    Returns:
        A fitted sklearn pipeline with ``predict_proba``.

    Raises:
        ValueError: When no outcome rows exist.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_possession_sim.node_models import (
                fit_outcome_node_model,
            )
            model = fit_outcome_node_model(events)
    """
    outcomes = events.filter(pl.col("kind") == "outcome")
    if outcomes.height == 0:
        raise ValueError("no outcome events to fit on")
    X = outcomes.select(_FEATURES).to_numpy().astype(float)
    y = outcomes.get_column("outcome").to_numpy()
    model = make_pipeline(StandardScaler(), LogisticRegression(max_iter=2000, C=1.0))
    model.fit(X, y)
    return model


def fit_rebound_node_model(events: pl.DataFrame) -> Optional[Pipeline]:
    """Fit the ReboundNode model: P(offensive rebound) | gamestate.

    Args:
        events: Classified possession events (``kind=="rebound"`` rows).

    Returns:
        A fitted pipeline, or None when the rebound stream is single-class
        (the empirical scalar then remains the node's model).
    """
    rebounds = events.filter(pl.col("kind") == "rebound")
    if rebounds.height == 0 or rebounds.get_column("outcome").n_unique() < 2:
        return None
    X = rebounds.select(_FEATURES).to_numpy().astype(float)
    y = (rebounds.get_column("outcome") == "oreb").to_numpy().astype(int)
    model = make_pipeline(StandardScaler(), LogisticRegression(max_iter=2000, C=1.0))
    model.fit(X, y)
    return model


def _grid_points() -> List[Tuple[str, np.ndarray]]:
    """(key, feature-vector) for every cell of the complete gamestate grid."""
    points: List[Tuple[str, np.ndarray]] = []
    n_bins = SCORE_DIFF_CLIP // SCORE_DIFF_WIDTH
    for diff_bin in range(-n_bins, n_bins + 1):
        diff = float(diff_bin * SCORE_DIFF_WIDTH)
        for period in range(1, 5):
            for clock_name, clock_mid in _CLOCK_MIDPOINTS.items():
                key = gamestate_key(diff, period, clock_mid)
                assert key.endswith(clock_name)
                points.append((key, np.array([diff, float(period), clock_mid])))
    return points


def models_to_shelf(
    events: pl.DataFrame,
    *,
    outcome_model: Optional[Pipeline] = None,
    rebound_model: Optional[Pipeline] = None,
) -> Shelf:
    """Bake fitted node models into a complete-coverage shelf.

    Fits the node models (unless supplied), evaluates them over EVERY cell
    of the gamestate grid, and returns a shelf whose PMFs are model
    predictions — smoothed where the data is thin, defined where the data
    is absent. Scalar parameters (FT pct, pace, global fallback) come from
    the empirical shelf of the same events.

    Args:
        events: Classified possession events (real data).
        outcome_model: Optional pre-fit OutcomeNode model.
        rebound_model: Optional pre-fit ReboundNode model.

    Returns:
        A :class:`Shelf` covering the full key grid, with per-key
        ``oreb_rates`` when the rebound model fits.

    Example:
        Model-backed simulation::

            from sportsdataverse.nba.nba_possession_sim import simulate_ensemble
            from sportsdataverse.nba.nba_possession_sim.node_models import models_to_shelf
            shelf = models_to_shelf(events)
            ens = simulate_ensemble(shelf, n_sim=500, seed=7)
            assert shelf.fallback_rate() == 0.0
    """
    empirical = build_shelf(events)
    outcome_model = outcome_model or fit_outcome_node_model(events)
    rebound_model = rebound_model if rebound_model is not None else fit_rebound_node_model(events)

    classes = [str(c) for c in outcome_model.classes_]
    points = _grid_points()
    matrix = np.vstack([features for _, features in points])
    probs = outcome_model.predict_proba(matrix)

    outcome_pmfs: Dict[str, Dict[str, float]] = {}
    for (key, _), row in zip(points, probs):
        by_class = {cls: float(p) for cls, p in zip(classes, row)}
        outcome_pmfs[key] = {o: by_class.get(o, 0.0) for o in OUTCOMES}

    oreb_rates: Optional[Dict[str, float]] = None
    if rebound_model is not None:
        oreb_probs = rebound_model.predict_proba(matrix)[:, 1]
        oreb_rates = {key: float(p) for (key, _), p in zip(points, oreb_probs)}

    meta: Dict[str, Any] = dict(empirical.meta)
    meta.update({"shelf_kind": "models2shelf", "n_keys": len(outcome_pmfs)})
    return Shelf(
        outcome_pmfs=outcome_pmfs,
        all_pmf=empirical.all_pmf,
        oreb_rate=empirical.oreb_rate,
        ft_pct=empirical.ft_pct,
        mean_possession_seconds=empirical.mean_possession_seconds,
        meta=meta,
        oreb_rates=oreb_rates,
    )
