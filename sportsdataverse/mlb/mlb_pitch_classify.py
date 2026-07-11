"""Pitch (re)classification ⑤ — per-pitcher Gaussian-mixture clustering.

Compute-on-demand, no bundled artifact: pitch types are pitcher-specific
clusters in physics space, so a fresh, seeded per-pitcher
:class:`sklearn.mixture.GaussianMixture` fit corrects Savant mislabels /
fills gaps without a trained global classifier.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Union

import numpy as np
import polars as pl
from sklearn.mixture import GaussianMixture

if TYPE_CHECKING:  # pragma: no cover -- annotation-only imports
    import pandas as pd

__all__ = ["mlb_pitch_classify"]

_CLUSTER_FEATURES = ("velo_z", "spin_z", "pfx_x_z", "pfx_z_z")

_EMPTY_SCHEMA: dict = {
    "pitcher": pl.Int64,
    "pitch_type": pl.Utf8,
    "pitch_type_reclass": pl.Utf8,
    "reclass_confidence": pl.Float64,
}

#: pitchers with fewer than this many pitches pass through the Savant label
#: unchanged (not enough data for a stable per-pitcher GMM fit).
MIN_PITCHES_FOR_CLUSTERING = 30


def _passthrough(group: pl.DataFrame) -> pl.DataFrame:
    return group.with_columns(
        pl.col("pitch_type").alias("pitch_type_reclass"),
        pl.lit(1.0).alias("reclass_confidence"),
    )


def _cluster_one_pitcher(group: pl.DataFrame, *, max_components: int, seed: int) -> pl.DataFrame:
    # Rows with a null cluster feature can't be fit/scored by GaussianMixture
    # (it rejects NaN) -- pass them through unchanged rather than dropping
    # them from the output.
    has_null = pl.any_horizontal([pl.col(c).is_null() for c in _CLUSTER_FEATURES])
    null_rows = group.filter(has_null)
    group = group.filter(~has_null)

    if group.height < MIN_PITCHES_FOR_CLUSTERING:
        clustered = _passthrough(group)
        return (
            pl.concat([clustered, _passthrough(null_rows)], how="diagonal_relaxed") if null_rows.height else clustered
        )

    x = group.select(list(_CLUSTER_FEATURES)).to_numpy()
    # Cap by max_components only -- NOT by the observed (possibly mislabeled)
    # Savant pitch_type count. The whole point of reclassification is to
    # correct cases where several genuinely distinct physics clusters share
    # one Savant label; constraining the search to the label count would make
    # that impossible by construction.
    cap = max(1, min(max_components, group.height))

    best_model = None
    best_bic = np.inf
    for k in range(1, cap + 1):
        model = GaussianMixture(n_components=k, random_state=seed, n_init=1)
        model.fit(x)
        bic = model.bic(x)
        if bic < best_bic:
            best_bic = bic
            best_model = model

    assert best_model is not None
    responsibilities = best_model.predict_proba(x)
    cluster_ids = responsibilities.argmax(axis=1)
    confidence = responsibilities.max(axis=1)

    group = group.with_columns(
        pl.Series("_cluster_id", cluster_ids, dtype=pl.Int64),
        pl.Series("reclass_confidence", confidence, dtype=pl.Float64),
    )
    # Label each cluster by the modal Savant pitch_type within it. Two
    # DIFFERENT physics clusters can share the same modal label (the whole
    # point of reclassification is correcting cases where Savant mislabeled
    # several genuinely distinct pitches identically) -- disambiguate any
    # label collisions across clusters for this pitcher with a numeric
    # suffix so the reclass output still distinguishes the clusters.
    modal = (
        group.group_by("_cluster_id", "pitch_type")
        .agg(pl.len().alias("n"))
        .sort("n", descending=True)
        .group_by("_cluster_id")
        .agg(pl.col("pitch_type").first().alias("_modal_label"))
        .sort("_cluster_id")
    )
    seen: dict = {}
    labels = []
    for label in modal["_modal_label"].to_list():
        seen[label] = seen.get(label, 0) + 1
        labels.append(label if seen[label] == 1 else f"{label}_{seen[label]}")
    modal = modal.with_columns(pl.Series("pitch_type_reclass", labels, dtype=pl.Utf8)).drop("_modal_label")
    group = group.join(modal, on="_cluster_id", how="left").drop("_cluster_id")
    return pl.concat([group, _passthrough(null_rows)], how="diagonal_relaxed") if null_rows.height else group


def mlb_pitch_classify(
    pitches: pl.DataFrame, *, max_components: int = 6, seed: int = 0, return_as_pandas: bool = False
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Per-pitcher Gaussian-mixture pitch reclassification.

    Args:
        pitches: Output of :func:`sportsdataverse.mlb.mlb_pitch_features.pitch_features`
            (needs ``velo_z``, ``spin_z``, ``pfx_x_z``, ``pfx_z_z``).
        max_components: Cap on GMM components considered per pitcher (BIC
            picks the best ``1..min(max_components, n_pitch_types)``).
        seed: Random seed for reproducible cluster labels.
        return_as_pandas: When ``True``, return a ``pandas.DataFrame``.

    Returns:
        ``pitcher``, ``pitch_type``, ``pitch_type_reclass``,
        ``reclass_confidence`` (max posterior cluster responsibility).
        Pitchers with fewer than
        :data:`MIN_PITCHES_FOR_CLUSTERING` pitches pass through the Savant
        label unchanged with ``reclass_confidence = 1.0``. Empty input
        returns a zero-row frame with this schema.

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_pitch_features import pitch_features
            from sportsdataverse.mlb.mlb_pitch_classify import mlb_pitch_classify
            feats = pitch_features(raw_pitches)
            out = mlb_pitch_classify(feats, seed=0)
            print(out.filter(out["pitch_type"] != out["pitch_type_reclass"]).head())

        See Also:
            * `baseballr`_ -- companion R package for Statcast-based pitching analysis.

        .. _baseballr: https://baseballr.sportsdataverse.org
    """
    required = _CLUSTER_FEATURES + ("pitcher", "pitch_type")
    if pitches is None or pitches.height == 0 or not all(c in pitches.columns for c in required):
        out = pl.DataFrame(schema=_EMPTY_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    groups = []
    for pitcher_id in pitches["pitcher"].unique(maintain_order=True).to_list():
        group = pitches.filter(pl.col("pitcher") == pitcher_id)
        groups.append(_cluster_one_pitcher(group, max_components=max_components, seed=seed))

    out = pl.concat(groups, how="diagonal_relaxed").select(
        "pitcher", "pitch_type", "pitch_type_reclass", "reclass_confidence"
    )
    return out.to_pandas() if return_as_pandas else out
