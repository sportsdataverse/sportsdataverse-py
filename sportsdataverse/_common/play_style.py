"""Play-style archetypes (BB-D20): season-relative KMeans fit on training seasons, applied as-of.

The shared core of the offensive and defensive play-style archetypes for NBA players and teams
(:mod:`sportsdataverse.nba.nba_play_style`) and the college play-style archetypes
(:mod:`sportsdataverse.mbb.mbb_play_style`). Every league follows one recipe:

* **Season-relative features.** Each feature is z-scored within its season among that season's
  qualifiers (:func:`season_zscores`), so an archetype describes a style relative to the league
  that year. The three-point boom does not drift every player toward "shooter", and a season is
  scored from its own rows only: an in-season cut is standardized against the same cut.
* **Fit on training seasons, frozen.** :func:`fit_play_style` runs seeded KMeans on the training
  seasons' z-scores and freezes the centers into a bundled JSON artifact (one per league, side and
  entity, refit by ``tools/models/play_style/fit_play_style.py``). Holdout seasons never enter the
  fit.
* **Applied as-of.** :func:`assign_play_style` gives every qualifying row the nearest frozen
  center. The output also carries the runner-up archetype and the margin between the two
  distances, so a hybrid is visible rather than hidden behind a single label.
* **Stability reported.** :func:`stability_report` measures, on the holdout seasons, (1) the
  agreement (adjusted Rand index) between the frozen assignment and a fresh fit on the holdout
  rows, and (2) year-over-year persistence: the share of players or teams seen in consecutive
  seasons who keep their archetype, against the chance rate of the cluster shares. The fitter
  writes the report into the artifact and the tests pin gates from the observed values.

Labels are derived from each center by :func:`label_centers`: a cluster is named after its most
distinctive feature (the largest positive z), and a second feature is added only where two
clusters would otherwise share a name.

**Internal** -- not re-exported at the top-level ``sportsdataverse`` package.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import numpy as np
import polars as pl

from sportsdataverse.mbb.mbb_player_value_constants import _ari, bootstrap_ari, kmeans_fit

__all__ = [
    "ASSIGN_SCHEMA",
    "Z_CLIP",
    "assign_play_style",
    "chance_persistence",
    "fit_play_style",
    "label_centers",
    "season_zscores",
    "stability_report",
    "yoy_persistence",
]

#: Z-scores are clipped to +/- this many standard deviations, so one extreme player-season (a
#: rare WBB dunker, a 37% tip-in share) cannot pull a center or sit far from every one.
Z_CLIP = 3.0

#: The columns :func:`assign_play_style` appends to the id columns.
ASSIGN_SCHEMA: dict[str, pl.DataType] = {
    "archetype": pl.Utf8(),
    "cluster": pl.Int64(),
    "dist_to_center": pl.Float64(),
    "second_archetype": pl.Utf8(),
    "margin": pl.Float64(),
}


def season_zscores(df: pl.DataFrame, feature_cols: Sequence[str], *, by: str = "season") -> pl.DataFrame:
    """Z-score each feature within its season (population sd), clipped to +/- :data:`Z_CLIP`.

    A null feature becomes 0 (the season mean).

    A feature with no spread in a season (sd 0 or a single row) scores 0 for every row of it.

    Args:
        df: One row per entity-season with ``by`` and the ``feature_cols``.
        feature_cols: The features to standardize.
        by: The season column.

    Returns:
        ``df`` with each feature column replaced by its within-season z-score (Float64).
    """
    out = []
    for c in feature_cols:
        x = pl.col(c).cast(pl.Float64)
        mu = x.mean().over(by)
        sd = x.std(ddof=0).over(by)
        out.append(pl.when(sd > 0).then(((x - mu) / sd).clip(-Z_CLIP, Z_CLIP)).otherwise(0.0).fill_null(0.0).alias(c))
    return df.with_columns(out)


def label_centers(centers: np.ndarray, feature_cols: Sequence[str], phrases: Mapping[str, str]) -> list[str]:
    """Name each center after its most distinctive feature (largest positive z).

    Where several centers share that feature, each adds the phrase of the feature on which it
    stands furthest above the others in the group (``"wing defender / screen chaser"`` beside
    ``"wing defender / help-side defender"``).

    Args:
        centers: ``(k, d)`` cluster centers in z-space.
        feature_cols: The ``d`` feature names, in center order.
        phrases: Feature name to label phrase; every feature must have one.

    Returns:
        ``k`` unique labels.

    Raises:
        KeyError: If a feature has no phrase.
        ValueError: If two centers still share a label.
    """
    missing = [c for c in feature_cols if c not in phrases]
    if missing:
        raise KeyError(f"no label phrase for features {missing}")
    c = np.asarray(centers, float)
    top = c.argmax(1)
    labels = []
    for i, j in enumerate(top):
        group = [g for g in range(len(c)) if top[g] == j and g != i]
        if not group:
            labels.append(phrases[feature_cols[j]])
            continue
        lead = c[i] - c[group].max(0)
        lead[j] = -np.inf
        labels.append(f"{phrases[feature_cols[j]]} / {phrases[feature_cols[int(lead.argmax())]]}")
    if len(set(labels)) < len(labels):
        raise ValueError(f"centers share a label: {labels}")
    return labels


def _nearest(Z: np.ndarray, centers: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Squared-free euclidean distances ``(n, k)`` and the nearest center per row."""
    d = np.sqrt(((Z[:, None, :] - centers[None, :, :]) ** 2).sum(-1))
    return d, d.argmin(1)


def fit_play_style(
    features: pl.DataFrame,
    feature_cols: Sequence[str],
    *,
    k: int,
    train_seasons: Sequence[int],
    phrases: Mapping[str, str],
    seed: int = 0,
) -> dict:
    """Fit the frozen archetype centers on the training seasons' within-season z-scores.

    Args:
        features: Qualifying entity-seasons with ``season`` and the ``feature_cols``.
        feature_cols: The clustering features.
        k: The number of archetypes.
        train_seasons: The seasons the centers are fit on; every other season is held out.
        phrases: Feature name to label phrase (see :func:`label_centers`).
        seed: KMeans seed (10 restarts, best inertia).

    Returns:
        The artifact payload: ``feature_cols``, ``k``, ``seed``, ``train_seasons``,
        ``centers`` (``k`` lists in z-space), ``labels`` and ``n_train`` (training rows).

    Raises:
        ValueError: If the training seasons hold fewer rows than ``k``.
    """
    cols = list(feature_cols)
    train = season_zscores(features, cols).filter(pl.col("season").is_in(list(train_seasons)))
    if train.height < k:
        raise ValueError(f"{train.height} training rows for k={k}")
    centers, _ = kmeans_fit(train.select(cols).to_numpy(), k, seed=seed)
    return {
        "feature_cols": cols,
        "k": int(k),
        "seed": int(seed),
        "train_seasons": sorted(int(s) for s in train_seasons),
        "centers": np.asarray(centers).tolist(),
        "labels": label_centers(centers, cols, phrases),
        "n_train": int(train.height),
    }


def assign_play_style(features: pl.DataFrame, artifact: Mapping, id_cols: Sequence[str]) -> pl.DataFrame:
    """Assign each qualifying entity-season the nearest frozen archetype, as of its own season.

    Args:
        features: Qualifying entity-seasons with ``season``, the ``id_cols`` and the artifact's
            ``feature_cols``. Each season is standardized against its own rows only.
        artifact: A :func:`fit_play_style` payload.
        id_cols: The columns kept in front of the assignment (``season`` included).

    Returns:
        ``id_cols`` plus :data:`ASSIGN_SCHEMA`: the ``archetype`` label and ``cluster`` index,
        ``dist_to_center`` (small = prototypical), the runner-up ``second_archetype`` and the
        ``margin`` between the two distances (small = a hybrid). Empty input returns that schema.
    """
    schema = {c: features.schema[c] for c in id_cols} | ASSIGN_SCHEMA
    if features.is_empty():
        return pl.DataFrame(schema=schema)
    cols = list(artifact["feature_cols"])
    Z = season_zscores(features, cols).select(cols).to_numpy()
    centers = np.asarray(artifact["centers"], float)
    d, nearest = _nearest(Z, centers)
    two = np.argsort(d, axis=1)[:, :2]
    labels = list(artifact["labels"])
    rows = np.arange(len(d))
    return features.select(id_cols).with_columns(
        pl.Series("archetype", [labels[j] for j in nearest], dtype=pl.Utf8),
        pl.Series("cluster", nearest.astype(np.int64), dtype=pl.Int64),
        pl.Series("dist_to_center", d[rows, nearest], dtype=pl.Float64),
        pl.Series("second_archetype", [labels[j] for j in two[:, 1]], dtype=pl.Utf8),
        pl.Series("margin", d[rows, two[:, 1]] - d[rows, two[:, 0]], dtype=pl.Float64),
    )


def yoy_persistence(assigned: pl.DataFrame, entity: str) -> float:
    """Share of entities seen in consecutive seasons that keep their archetype.

    Args:
        assigned: :func:`assign_play_style` output with ``season``, ``entity`` and ``cluster``.
        entity: The entity id column (``player_id`` / ``team_id``).

    Returns:
        The persistence rate over every (season, season + 1) pair, or NaN with no pair.
    """
    a = assigned.select(entity, "season", "cluster")
    pairs = a.join(a.with_columns(pl.col("season") - 1), on=[entity, "season"], suffix="_next")
    if pairs.is_empty():
        return float("nan")
    return float((pairs["cluster"] == pairs["cluster_next"]).mean())


def chance_persistence(assigned: pl.DataFrame) -> float:
    """The persistence two independent draws from the cluster shares would show: ``sum(p_k^2)``."""
    p = assigned["cluster"].value_counts(normalize=True)["proportion"].to_numpy()
    return float((p**2).sum())


def stability_report(
    features: pl.DataFrame, artifact: Mapping, *, entity: str, holdout_seasons: Sequence[int], n_boot: int = 20
) -> dict:
    """Stability of the frozen archetypes on the holdout seasons (and of the fit on the training ones).

    Args:
        features: Qualifying entity-seasons for every season, as for :func:`assign_play_style`.
        artifact: A :func:`fit_play_style` payload.
        entity: The entity id column.
        holdout_seasons: The seasons outside the fit.
        n_boot: Bootstrap refits for the training-season ARI.

    Returns:
        ``holdout_ari`` (frozen centers vs a fresh fit with the same ``k`` and seed on the holdout
        rows), ``train_bootstrap_ari``, ``holdout_persistence`` (the share of entities
        that keep their archetype from the season before a holdout season into it, pooled over the
        holdout seasons, with ``n_persistence_pairs``) against ``holdout_chance_persistence``,
        ``persistence_by_pair`` (``"2023-2024"`` to its rate), ``holdout_cluster_shares`` per
        season and ``n_holdout``.
    """
    cols = list(artifact["feature_cols"])
    k, seed = int(artifact["k"]), int(artifact["seed"])
    z = season_zscores(features, cols)
    hold = z.filter(pl.col("season").is_in(list(holdout_seasons)))
    train = z.filter(pl.col("season").is_in(list(artifact["train_seasons"])))
    centers = np.asarray(artifact["centers"], float)
    Zh = hold.select(cols).to_numpy()
    _, frozen = _nearest(Zh, centers)
    _, refit = kmeans_fit(Zh, k, seed=seed)
    # Persistence pairs each holdout season with the season before it (which may be a training
    # season): both sides are scored with the frozen centers, each against its own season.
    seasons = sorted({*holdout_seasons, *(s - 1 for s in holdout_seasons)})
    both = assign_play_style(features.filter(pl.col("season").is_in(seasons)), artifact, [entity, "season"])
    assigned = both.filter(pl.col("season").is_in(list(holdout_seasons)))
    by_pair = {
        f"{s - 1}-{s}": round(yoy_persistence(both.filter(pl.col("season").is_in([s - 1, s])), entity), 4)
        for s in sorted(holdout_seasons)
    }
    a = both.select(entity, "season", "cluster")
    joined = a.join(a.with_columns(pl.col("season") - 1), on=[entity, "season"], suffix="_next").filter(
        (pl.col("season") + 1).is_in(list(holdout_seasons))
    )
    shares = (
        assigned.group_by("season", "archetype")
        .len()
        .with_columns((pl.col("len") / pl.col("len").sum().over("season")).round(4).alias("share"))
        .sort("season", "archetype")
    )
    return {
        "holdout_ari": round(_ari(frozen, refit), 4),
        "train_bootstrap_ari": round(
            bootstrap_ari(
                lambda X: kmeans_fit(X, k, seed=seed), train.select(cols).to_numpy(), n_boot=n_boot, seed=seed
            ),
            4,
        ),
        "holdout_persistence": round(float((joined["cluster"] == joined["cluster_next"]).mean()), 4)
        if joined.height
        else float("nan"),
        "n_persistence_pairs": int(joined.height),
        "holdout_chance_persistence": round(chance_persistence(assigned), 4),
        "persistence_by_pair": by_pair,
        "holdout_cluster_shares": {
            str(s): {r["archetype"]: r["share"] for r in shares.filter(pl.col("season") == s).iter_rows(named=True)}
            for s in sorted(holdout_seasons)
        },
        "n_holdout": int(hold.height),
    }
