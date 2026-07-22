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
    LearnedGamestateKeyer,
    clock_bin,
    gamestate_key,
    parse_clock,
    period_bin,
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


def _leaf_points(events: pl.DataFrame, keyer: LearnedGamestateKeyer) -> List[Tuple[str, np.ndarray]]:
    """(leaf key, representative feature vector) per learned-keyer leaf.

    The representative is the mean training state of the leaf — every leaf
    holds >= min_samples_leaf outcome events by construction, so each has
    a well-defined representative.
    """
    outcomes = events.filter(pl.col("kind") == "outcome")
    keyed = outcomes.with_columns(
        pl.struct(["score_diff", "period", "clock_seconds"])
        .map_elements(
            lambda s: keyer.key(s["score_diff"], s["period"], s["clock_seconds"]),
            return_dtype=pl.Utf8,
        )
        .alias("key")
    )
    reps = keyed.group_by("key").agg([pl.col(f).mean().alias(f) for f in _FEATURES]).sort("key")
    return [(str(row["key"]), np.array([float(row[f]) for f in _FEATURES])) for row in reps.iter_rows(named=True)]


def fit_pace_node(
    events: pl.DataFrame,
    *,
    keyer: Optional[LearnedGamestateKeyer] = None,
    shrinkage: float = 25.0,
) -> Dict[str, float]:
    """Fit the pace node: per-gamestate mean possession seconds.

    Clock burn is the tree's last unfitted scalar — one global mean hides
    real state dependence (end-of-quarter compression, trailing-team
    hurry-up). Per-key mean burns are empirical-Bayes shrunk toward the
    global mean with ``n / (n + shrinkage)`` so thin cells stay estimable.

    Args:
        events: Classified possession events (real data).
        keyer: Optional learned keyer; defaults to the hand-cut key.
        shrinkage: Pseudo-observation weight of the global mean.

    Returns:
        ``{gamestate_key: mean possession seconds}`` for
        :attr:`~sportsdataverse.nba.nba_possession_sim.shelf.Shelf.pace_rates`.

    Raises:
        ValueError: When no usable clock deltas exist.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_possession_sim.node_models import fit_pace_node
            shelf.pace_rates = fit_pace_node(events)
    """
    outcomes = events.filter(pl.col("kind") == "outcome")
    deltas = (
        outcomes.sort(["game_id", "period", "clock_seconds"], descending=[False, False, True])
        .with_columns(
            (pl.col("clock_seconds").shift(1) - pl.col("clock_seconds")).over(["game_id", "period"]).alias("burn")
        )
        .filter((pl.col("burn") > 0) & (pl.col("burn") < 60))
    )
    if deltas.height == 0:
        raise ValueError("no usable clock deltas to fit the pace node")
    global_mean = float(deltas["burn"].mean())
    key_fn = keyer.key if keyer is not None else gamestate_key
    keyed = deltas.with_columns(
        pl.struct(["score_diff", "period", "clock_seconds"])
        .map_elements(lambda s: key_fn(s["score_diff"], s["period"], s["clock_seconds"]), return_dtype=pl.Utf8)
        .alias("key")
    )
    grouped = keyed.group_by("key").agg(pl.col("burn").mean().alias("mean"), pl.len().alias("n"))
    return {
        str(row["key"]): (row["n"] * float(row["mean"]) + shrinkage * global_mean) / (row["n"] + shrinkage)
        for row in grouped.iter_rows(named=True)
    }


def fit_aux_node_models(
    actions: pl.DataFrame,
    events: pl.DataFrame,
    *,
    keyer: Optional[LearnedGamestateKeyer] = None,
    shrinkage: float = 25.0,
) -> Dict[str, Dict[str, float]]:
    """Fit the aux nodes: period x clock-conditional expanded-node rates.

    Turns the global scalars (``timeout_rate``, ``def_foul_rate``,
    ``steal_share``, ``and1_rate``) into per-gamestate overrides. The raw
    side streams carry no offense-perspective differential, so the
    conditioning is deliberately the (period, clock-bin) marginal — every
    key sharing a (period, clock) cell gets that cell's rate; the block
    rate stays global (playbyplayv3 carries no block annotation).

    Args:
        actions: RAW playbyplayv3 action rows (any games).
        events: Classified possession events (defines the key space; used
            for learned-keyer leaf representatives).
        keyer: Optional learned keyer; defaults to the hand-cut grid.
        shrinkage: Pseudo-event weight of the global rate per stream.

    Returns:
        ``{gamestate_key: {rate_name: value}}`` for
        :attr:`~sportsdataverse.nba.nba_possession_sim.shelf.Shelf.aux_rates`.

    Raises:
        ValueError: When the action stream has no possession-ending events.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_possession_sim.node_models import (
                fit_aux_node_models,
            )
            shelf.aux_rates = fit_aux_node_models(raw_actions, events)
    """
    a_type = "actionType" if "actionType" in actions.columns else "action_type"
    sub = "subType" if "subType" in actions.columns else "sub_type"
    frame = (
        actions.select(
            pl.col(a_type).cast(pl.Utf8).alias("t"),
            pl.col(sub).cast(pl.Utf8).fill_null("").alias("s"),
            pl.col("description").cast(pl.Utf8).fill_null("").alias("d"),
            pl.col("period").cast(pl.Int64).alias("period"),
            pl.col("clock").cast(pl.Utf8).map_elements(parse_clock, return_dtype=pl.Float64).alias("clock_seconds"),
        )
        .with_columns(
            pl.col("period").map_elements(period_bin, return_dtype=pl.Int64).alias("pbin"),
            pl.col("clock_seconds").map_elements(clock_bin, return_dtype=pl.Utf8).alias("cbin"),
            pl.col("t").is_in(["Made Shot", "Missed Shot", "Turnover"]).alias("is_end"),
            (pl.col("t") == "Timeout").alias("is_timeout"),
            ((pl.col("t") == "Foul") & (pl.col("d").str.contains("(?i)shooting") == False)).alias("is_def_foul"),  # noqa: E712
            (pl.col("t") == "Turnover").alias("is_tov"),
            (
                (pl.col("t") == "Turnover")
                & (
                    pl.col("s").str.contains("(?i)lost ball")
                    | (
                        pl.col("s").str.contains("(?i)bad pass")
                        & (pl.col("s").str.contains("(?i)out of bounds") == False)  # noqa: E712
                    )
                )
            ).alias("is_live_tov"),
            (pl.col("t") == "Made Shot").alias("is_make"),
            ((pl.col("t") == "Free Throw") & pl.col("d").str.contains("1 of 1")).alias("is_and1_ft"),
        )
        .filter(
            # season releases occasionally ship all-null placeholder rows;
            # a null state cannot key a rate cell
            pl.col("pbin").is_not_null() & pl.col("cbin").is_not_null()
        )
    )
    totals = frame.select(
        pl.col("is_end").sum().alias("events"),
        pl.col("is_timeout").sum().alias("timeouts"),
        pl.col("is_def_foul").sum().alias("fouls"),
        pl.col("is_tov").sum().alias("tovs"),
        pl.col("is_live_tov").sum().alias("live"),
        pl.col("is_make").sum().alias("makes"),
        pl.col("is_and1_ft").sum().alias("and1s"),
    ).to_dicts()[0]
    if not totals["events"]:
        raise ValueError("no possession-ending events to fit the aux nodes")

    def _rate(num: float, den: float, prior: float) -> float:
        return (num + shrinkage * prior) / (den + shrinkage) if (den + shrinkage) > 0 else prior

    g_timeout = totals["timeouts"] / totals["events"]
    g_foul = totals["fouls"] / totals["events"]
    g_steal = (totals["live"] / totals["tovs"]) if totals["tovs"] else 0.5
    g_and1 = min(0.25, totals["and1s"] / totals["makes"]) if totals["makes"] else 0.06

    grouped = frame.group_by("pbin", "cbin").agg(
        pl.col("is_end").sum().alias("events"),
        pl.col("is_timeout").sum().alias("timeouts"),
        pl.col("is_def_foul").sum().alias("fouls"),
        pl.col("is_tov").sum().alias("tovs"),
        pl.col("is_live_tov").sum().alias("live"),
        pl.col("is_make").sum().alias("makes"),
        pl.col("is_and1_ft").sum().alias("and1s"),
    )
    cell_rates: Dict[Tuple[int, str], Dict[str, float]] = {}
    for row in grouped.iter_rows(named=True):
        cell_rates[(int(row["pbin"]), str(row["cbin"]))] = {
            "timeout_rate": _rate(row["timeouts"], row["events"], g_timeout),
            "def_foul_rate": _rate(row["fouls"], row["events"], g_foul),
            "steal_share": _rate(row["live"], row["tovs"], g_steal),
            "and1_rate": min(0.25, _rate(row["and1s"], row["makes"], g_and1)),
        }
    fallback = {
        "timeout_rate": g_timeout,
        "def_foul_rate": g_foul,
        "steal_share": g_steal,
        "and1_rate": g_and1,
    }
    points = _leaf_points(events, keyer) if keyer is not None else _grid_points()
    aux_rates: Dict[str, Dict[str, float]] = {}
    for key, features in points:
        cell = (period_bin(int(round(float(features[1])))), clock_bin(float(features[2])))
        aux_rates[key] = dict(cell_rates.get(cell, fallback))
    return aux_rates


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
    keyer: Optional[LearnedGamestateKeyer] = None,
    actions: Optional[pl.DataFrame] = None,
    fit_pace: bool = True,
) -> Shelf:
    """Bake fitted node models into a complete-coverage shelf.

    The fully extended tree in one call: fits the node models (unless
    supplied), evaluates them over EVERY cell of the key space — the
    hand-cut 144-key grid, or the learned keyer's leaves when ``keyer`` is
    given (leaf representatives are the leaves' mean training states) —
    and returns a shelf whose PMFs are model predictions: smoothed where
    the data is thin, defined where the data is absent. The pace node is
    fitted per-key by default, and the aux nodes (timeout / non-shooting
    foul / steal-share / and-1) become period x clock conditional when the
    raw ``actions`` stream is supplied. Scalar parameters (FT pct, global
    pace fallback, global PMF) come from the empirical shelf of the same
    events.

    Args:
        events: Classified possession events (real data).
        outcome_model: Optional pre-fit OutcomeNode model.
        rebound_model: Optional pre-fit ReboundNode model.
        keyer: Optional fitted
            :class:`~sportsdataverse.nba.nba_possession_sim.keygen.LearnedGamestateKeyer`
            — the shelf is keyed by its leaves and carries it for sim-time
            lookups.
        actions: Optional RAW playbyplayv3 rows; enables the fitted aux
            nodes (:func:`fit_aux_node_models`).
        fit_pace: Fit the per-key pace node (:func:`fit_pace_node`).

    Returns:
        A :class:`Shelf` covering the full key space, with per-key
        ``oreb_rates`` when the rebound model fits, ``pace_rates`` when
        ``fit_pace``, and ``aux_rates`` when ``actions`` is given.

    Example:
        Model-backed simulation::

            from sportsdataverse.nba.nba_possession_sim import simulate_ensemble
            from sportsdataverse.nba.nba_possession_sim.node_models import models_to_shelf
            shelf = models_to_shelf(events, actions=raw_actions)
            ens = simulate_ensemble(shelf, n_sim=500, seed=7)
            assert shelf.fallback_rate() == 0.0
    """
    empirical = build_shelf(events, keyer=keyer)
    outcome_model = outcome_model or fit_outcome_node_model(events)
    rebound_model = rebound_model if rebound_model is not None else fit_rebound_node_model(events)

    classes = [str(c) for c in outcome_model.classes_]
    points = _leaf_points(events, keyer) if keyer is not None else _grid_points()
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

    pace_rates = fit_pace_node(events, keyer=keyer) if fit_pace else None
    aux_rates = fit_aux_node_models(actions, events, keyer=keyer) if actions is not None else None

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
        aux=empirical.aux,
        keyer=keyer,
        aux_rates=aux_rates,
        pace_rates=pace_rates,
    )
