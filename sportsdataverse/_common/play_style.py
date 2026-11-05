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
* **Chosen on training seasons only.** ``k`` (and, for teams, the feature set) is picked by
  :func:`selection_score`: an inner split of the training seasons (fit on all but the last two,
  score the last two) plus a bootstrap over players or teams. The holdout seasons only report.
* **Stability reported.** :func:`stability_report` measures, on the holdout seasons, (1) the
  agreement (adjusted Rand index) between the frozen assignment and a fresh fit on the holdout
  rows, against the same pipeline on features shuffled within season (a no-structure null), and
  (2) year-over-year persistence: the share of players or teams seen in consecutive seasons who
  keep their archetype, against the chance rate of the paired entities' cluster shares, both with
  intervals from resampling players or teams. The fitter writes the report into the artifact and
  the tests pin gates from the observed values.

Labels are derived from each center by :func:`label_centers`: a cluster is named after its most
distinctive feature (the largest positive z), and a second feature is added only where two
clusters would otherwise share a name.

**Internal** -- not re-exported at the top-level ``sportsdataverse`` package.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import numpy as np
import polars as pl

from sportsdataverse.mbb.mbb_player_value_constants import _ari, kmeans_fit

__all__ = [
    "ASSIGN_SCHEMA",
    "Z_CLIP",
    "assign_play_style",
    "check_features",
    "fit_play_style",
    "group_bootstrap_ari",
    "label_centers",
    "season_zscores",
    "selection_score",
    "stability_report",
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

    A null value becomes 0 (the season mean); a feature with no spread in a season (sd 0 or a single
    row) scores 0 for every row of it. Pass a whole qualifying season: the z-scores of a subset (one
    team's players) are relative to that subset.

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

    Raises:
        ValueError: If a feature is null for every row of a season (see :func:`check_features`).
    """
    schema = {c: features.schema[c] for c in id_cols} | ASSIGN_SCHEMA
    if features.is_empty():
        return pl.DataFrame(schema=schema)
    cols = list(artifact["feature_cols"])
    check_features(features, cols)
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


def check_features(features: pl.DataFrame, feature_cols: Sequence[str]) -> None:
    """Refuse a season in which a feature is null for every row.

    :func:`season_zscores` turns a null into the season mean, so a feature a release stopped
    carrying (a dropped column, a season below a source's floor) would silently fall out of the
    clustering for that season instead of failing.

    Args:
        features: Entity-seasons with ``season`` and the ``feature_cols``.
        feature_cols: The clustering features.

    Raises:
        ValueError: Naming each (season, feature) with no value.
    """
    missing = [c for c in feature_cols if c not in features.columns]
    if missing:
        raise ValueError(f"features missing: {missing}")
    empty = (
        features.group_by("season")
        .agg(pl.col(c).is_not_null().any() for c in feature_cols)
        .unpivot(index="season", variable_name="feature", value_name="has")
        .filter(pl.col("has") == False)  # noqa: E712
    )
    if empty.height:
        raise ValueError(f"a feature is null for a whole season: {sorted(empty.select('season', 'feature').rows())}")


def _refit_ari(Z: np.ndarray, centers: np.ndarray, k: int, seed: int) -> float:
    """ARI between the nearest of ``centers`` and a fresh seeded KMeans on the same rows."""
    return _ari(_nearest(Z, centers)[1], kmeans_fit(Z, k, seed=seed)[1])


def group_bootstrap_ari(Z: np.ndarray, ids: np.ndarray, *, k: int, seed: int, n_boot: int = 20) -> float:
    """Mean ARI between the full fit and refits on resampled entities (all of an entity's seasons together).

    A player or team appears in several training seasons, so resampling rows would treat its seasons
    as independent draws and overstate the stability; this resamples entities.

    Args:
        Z: ``(n, d)`` z-scores.
        ids: The ``n`` entity ids.
        k: Clusters.
        seed: Seed for the fits and the resampling.
        n_boot: Resamples.

    Returns:
        The mean adjusted Rand index of the full sample's assignment to each refit's centers
        against the full fit's labels.
    """
    rng = np.random.default_rng(seed)
    _, base = kmeans_fit(Z, k, seed=seed)
    uniq, inverse = np.unique(ids, return_inverse=True)
    rows_of = [np.flatnonzero(inverse == g) for g in range(len(uniq))]
    scores = []
    for _ in range(n_boot):
        rows = np.concatenate([rows_of[g] for g in rng.integers(0, len(uniq), len(uniq))])
        centers, _ = kmeans_fit(Z[rows], k, seed=seed)
        scores.append(_ari(base, _nearest(Z, centers)[1]))
    return float(np.mean(scores))


def selection_score(
    features: pl.DataFrame,
    feature_cols: Sequence[str],
    *,
    k: int,
    train_seasons: Sequence[int],
    entity: str,
    seed: int,
    n_boot: int = 20,
) -> dict:
    """Training-only evidence for choosing ``k`` and the feature set: the holdout seasons are not read.

    Args:
        features: Qualifying entity-seasons (any seasons; only ``train_seasons`` are read).
        feature_cols: The candidate features.
        k: The candidate number of archetypes.
        train_seasons: The training seasons.
        entity: The entity id column.
        seed: KMeans and resampling seed.
        n_boot: Entity resamples for the bootstrap ARI.

    Returns:
        ``inner_ari`` (centers fit on all but the last two training seasons, scored on those two
        against a fresh fit; null with fewer than four training seasons), ``train_bootstrap_ari``
        (:func:`group_bootstrap_ari` on the training seasons) and ``score``, their mean (the
        bootstrap alone without an inner split).
    """
    cols = list(feature_cols)
    seasons = sorted(int(s) for s in train_seasons)
    z = season_zscores(features.filter(pl.col("season").is_in(seasons)), cols)
    boot = group_bootstrap_ari(z.select(cols).to_numpy(), z[entity].to_numpy(), k=k, seed=seed, n_boot=n_boot)
    inner = None
    if len(seasons) >= 4:
        fit_rows = z.filter(pl.col("season").is_in(seasons[:-2])).select(cols).to_numpy()
        val_rows = z.filter(pl.col("season").is_in(seasons[-2:])).select(cols).to_numpy()
        inner = _refit_ari(val_rows, kmeans_fit(fit_rows, k, seed=seed)[0], k, seed)
    score = boot if inner is None else (inner + boot) / 2
    return {
        "inner_ari": None if inner is None else round(inner, 4),
        "train_bootstrap_ari": round(boot, 4),
        "score": round(score, 4),
    }


def _pairs(assigned: pl.DataFrame, entity: str, later_seasons: Sequence[int]) -> pl.DataFrame:
    """Entities assigned in a season and the one before it: ``cluster`` (before) and ``cluster_next``."""
    a = assigned.select(entity, "season", "cluster")
    return a.join(a.with_columns(pl.col("season") - 1), on=[entity, "season"], suffix="_next").filter(
        (pl.col("season") + 1).is_in(list(later_seasons))
    )


def _persistence(pairs: pl.DataFrame) -> tuple[float, float]:
    """Kept-archetype rate of ``pairs`` and its chance rate, ``sum_k p_before,k * p_after,k`` per pair of seasons."""
    if pairs.is_empty():
        return float("nan"), float("nan")
    kept = float((pairs["cluster"] == pairs["cluster_next"]).mean())
    chance = 0.0
    for _, g in pairs.group_by("season"):
        before = g["cluster"].value_counts(normalize=True).rename({"proportion": "p"})
        after = g["cluster_next"].value_counts(normalize=True).rename({"cluster_next": "cluster", "proportion": "q"})
        both = before.join(after, on="cluster")
        chance += float((both["p"] * both["q"]).sum()) * g.height / pairs.height
    return kept, chance


def _shuffled(z: pl.DataFrame, cols: Sequence[str], seed: int) -> pl.DataFrame:
    """Each feature permuted independently within season: the marginals kept, the joint structure gone."""
    return z.with_columns(pl.col(c).shuffle(seed=seed + i).over("season") for i, c in enumerate(cols))


def _interval(values: Sequence[float]) -> list[float]:
    return [
        round(float(np.percentile(values, 2.5)), 4),
        round(float(np.percentile(values, 97.5)), 4),
    ]


def _resampled_rows(ids: np.ndarray, rng: np.random.Generator, n: int) -> list[np.ndarray]:
    """``n`` row-index draws, each resampling entities with replacement (all of an entity's rows together)."""
    uniq, inverse = np.unique(ids, return_inverse=True)
    rows_of = [np.flatnonzero(inverse == g) for g in range(len(uniq))]
    return [np.concatenate([rows_of[g] for g in rng.integers(0, len(uniq), len(uniq))]) for _ in range(n)]


def _intervals(
    Zh: np.ndarray,
    hold_ids: np.ndarray,
    pairs: pl.DataFrame,
    artifact: Mapping,
    entity: str,
    n_ci: int,
) -> dict:
    """Entity-resampled 95% intervals of the holdout ARI (50 refits at most) and the persistence."""
    k, seed = int(artifact["k"]), int(artifact["seed"])
    centers = np.asarray(artifact["centers"], float)
    rng = np.random.default_rng(seed)
    aris = [_refit_ari(Zh[rows], centers, k, seed) for rows in _resampled_rows(hold_ids, rng, min(n_ci, 50))]
    kept = pairs["cluster"].to_numpy() == pairs["cluster_next"].to_numpy()
    draws = [kept[rows].mean() for rows in _resampled_rows(pairs[entity].to_numpy(), rng, n_ci)]
    return {
        "holdout_ari_ci": _interval(aris),
        "holdout_persistence_ci": _interval(draws),
    }


def stability_report(
    features: pl.DataFrame,
    artifact: Mapping,
    *,
    entity: str,
    holdout_seasons: Sequence[int],
    n_boot: int = 20,
    n_ci: int = 200,
) -> dict:
    """Stability of the frozen archetypes on the holdout seasons (and of the fit on the training ones).

    Args:
        features: Qualifying entity-seasons for every season, as for :func:`assign_play_style`.
        artifact: A :func:`fit_play_style` payload.
        entity: The entity id column.
        holdout_seasons: The seasons outside the fit.
        n_boot: Entity resamples for the training bootstrap ARI.
        n_ci: Entity resamples for the intervals (0 skips them).

    Returns:
        ``holdout_ari`` (frozen centers vs a fresh fit with the same ``k`` and seed on the holdout
        rows), ``null_holdout_ari`` (the same fit-and-compare on features shuffled within season),
        ``train_bootstrap_ari``, ``holdout_persistence`` (the share of entities that keep their
        archetype from the season before a holdout season into it, pooled, with
        ``n_persistence_pairs``) against ``holdout_chance_persistence`` (the paired entities'
        cluster shares), ``persistence_by_pair``, ``holdout_cluster_shares`` per season with
        their ``min_share`` / ``max_share``, ``n_holdout``, and with ``n_ci`` the entity-resampled
        ``holdout_ari_ci`` / ``holdout_persistence_ci``.
    """
    cols = list(artifact["feature_cols"])
    k, seed = int(artifact["k"]), int(artifact["seed"])
    train_seasons = list(artifact["train_seasons"])
    z = season_zscores(features, cols)
    hold = z.filter(pl.col("season").is_in(list(holdout_seasons)))
    train = z.filter(pl.col("season").is_in(train_seasons))
    Zh = hold.select(cols).to_numpy()
    shuffled = _shuffled(z, cols, seed)
    null_centers, _ = kmeans_fit(
        shuffled.filter(pl.col("season").is_in(train_seasons)).select(cols).to_numpy(),
        k,
        seed=seed,
    )
    null_rows = shuffled.filter(pl.col("season").is_in(list(holdout_seasons))).select(cols).to_numpy()
    # Persistence pairs each holdout season with the season before it (which may be a training
    # season): both sides are scored with the frozen centers, each against its own season.
    seasons = sorted({*holdout_seasons, *(s - 1 for s in holdout_seasons)})
    both = assign_play_style(features.filter(pl.col("season").is_in(seasons)), artifact, [entity, "season"])
    pairs = _pairs(both, entity, holdout_seasons)
    kept, chance = _persistence(pairs)
    shares = (
        both.filter(pl.col("season").is_in(list(holdout_seasons)))
        .group_by("season", "archetype")
        .len()
        .with_columns((pl.col("len") / pl.col("len").sum().over("season")).round(4).alias("share"))
        .sort("season", "archetype")
    )
    report = {
        "holdout_ari": round(_refit_ari(Zh, np.asarray(artifact["centers"], float), k, seed), 4),
        "null_holdout_ari": round(_refit_ari(null_rows, null_centers, k, seed), 4),
        "train_bootstrap_ari": round(
            group_bootstrap_ari(
                train.select(cols).to_numpy(),
                train[entity].to_numpy(),
                k=k,
                seed=seed,
                n_boot=n_boot,
            ),
            4,
        ),
        "holdout_persistence": round(kept, 4),
        "holdout_chance_persistence": round(chance, 4),
        "n_persistence_pairs": int(pairs.height),
        "persistence_by_pair": {
            f"{s - 1}-{s}": round(_persistence(pairs.filter(pl.col("season") == s - 1))[0], 4)
            for s in sorted(holdout_seasons)
        },
        "holdout_cluster_shares": {
            str(s): {r["archetype"]: r["share"] for r in shares.filter(pl.col("season") == s).iter_rows(named=True)}
            for s in sorted(holdout_seasons)
        },
        "min_share": float(shares["share"].min()),
        "max_share": float(shares["share"].max()),
        "n_holdout": int(hold.height),
    }
    if n_ci:
        report |= _intervals(Zh, hold[entity].to_numpy(), pairs, artifact, entity, n_ci)
    return report
