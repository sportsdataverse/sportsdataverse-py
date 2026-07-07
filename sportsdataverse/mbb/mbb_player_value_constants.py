"""Player-value spine harness: per-100 features, fitters, metrics, constants.

Phase 0 of the MBB/WBB player-value & projection stack. Single home for the
pieces every model (box-BPM, archetypes, recruiting, transfer, draft) shares:
the per-100 feature builder, seeded numpy fitters (ridge / KMeans /
logistic), validation metrics, the as-of leakage split, per-league constants,
and bundled-artifact IO. ``spearman_corr`` / ``mae`` are re-used **by
reference** from :mod:`sportsdataverse.mbb.mbb_prediction_constants` (one
implementation package-wide).

Naming note: this module's constants accessor is
:func:`get_player_value_constants` (NOT ``get_constants``) -- the prediction
stack already star-exports ``get_constants``/``LeagueConstants`` in
``sportsdataverse.mbb``, and a same-named export here would shadow it.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from importlib.resources import files
from typing import Callable

import numpy as np
import polars as pl
from scipy.optimize import minimize

from sportsdataverse.mbb.mbb_prediction_constants import mae as mae  # noqa: PLC0414 - re-export by reference
from sportsdataverse.mbb.mbb_prediction_constants import spearman_corr as spearman_corr  # noqa: PLC0414

__all__ = [
    "PLAYER_VALUE_CONSTANTS",
    "PlayerValueConstants",
    "aggregate_player_seasons",
    "as_of_season_split",
    "bootstrap_ari",
    "get_player_value_constants",
    "kmeans_fit",
    "load_artifact",
    "logistic_fit",
    "mae",
    "player_per100_features",
    "rank_corr",
    "ridge_cv_lambda",
    "ridge_fit",
    "roc_auc",
    "save_artifact",
    "spearman_corr",
]

rank_corr = spearman_corr


@dataclass(frozen=True)
class PlayerValueConstants:
    """Per-league constants for the player-value spine.

    Attributes:
        pace_baseline: League baseline possessions per game (per-100 scaling).
        bubble_recruit_rank: National recruit rank of a "bubble" high-major
            rotation player (recruiting-model reference point).
        bundle_prefix: Artifact filename prefix under ``mbb/models``
            (``"mbb"`` / ``"wbb"``).
    """

    pace_baseline: float
    bubble_recruit_rank: int
    bundle_prefix: str


PLAYER_VALUE_CONSTANTS: dict[str, PlayerValueConstants] = {
    "mens": PlayerValueConstants(pace_baseline=67.0, bubble_recruit_rank=150, bundle_prefix="mbb"),
    "womens": PlayerValueConstants(pace_baseline=70.0, bubble_recruit_rank=120, bundle_prefix="wbb"),
}


def get_player_value_constants(league: str) -> PlayerValueConstants:
    """Return the :class:`PlayerValueConstants` for a league.

    Args:
        league: ``"mens"`` or ``"womens"``.

    Returns:
        The league's :class:`PlayerValueConstants`.

    Raises:
        ValueError: If ``league`` is not a known key.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_player_value_constants import get_player_value_constants
            get_player_value_constants("mens").bundle_prefix
    """
    try:
        return PLAYER_VALUE_CONSTANTS[league]
    except KeyError:
        raise ValueError(f"unknown league {league!r}; expected one of {sorted(PLAYER_VALUE_CONSTANTS)}") from None


def roc_auc(y_true: np.ndarray, score: np.ndarray) -> float:
    """Area under the ROC curve via the rank-sum (Mann-Whitney) identity.

    Args:
        y_true: Binary outcomes (0/1).
        score: Predicted scores (any monotone scale).

    Returns:
        AUC in ``[0, 1]``; ``nan`` when only one class is present.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.mbb.mbb_player_value_constants import roc_auc
            roc_auc(np.array([0, 1]), np.array([0.2, 0.9]))
    """
    from scipy.stats import rankdata  # noqa: PLC0415

    y = np.asarray(y_true, float)
    r = rankdata(np.asarray(score, float))
    n_pos = y.sum()
    n_neg = len(y) - n_pos
    if n_pos == 0 or n_neg == 0:
        return float("nan")
    return float((r[y == 1].sum() - n_pos * (n_pos + 1) / 2) / (n_pos * n_neg))


def as_of_season_split(df: pl.DataFrame, target_season: int) -> pl.DataFrame:
    """Rows strictly before ``target_season`` -- the leakage boundary.

    Args:
        df: Frame with an integer ``season`` column.
        target_season: The season being predicted; its rows (and later) drop.

    Returns:
        The subset with ``season < target_season``.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_player_value_constants import as_of_season_split
            prior = as_of_season_split(df, 2026)
    """
    return df.filter(pl.col("season") < target_season)


_FEATURE_SCHEMA = {
    "player_id": pl.Utf8,
    "season": pl.Int64,
    "team_id": pl.Utf8,
    "min": pl.Float64,
    "usage": pl.Float64,
    "ts_pct": pl.Float64,
    "efg_pct": pl.Float64,
    "ast_pct": pl.Float64,
    "tov_pct": pl.Float64,
    "oreb_pct": pl.Float64,
    "dreb_pct": pl.Float64,
    "blk_pct": pl.Float64,
    "stl_pct": pl.Float64,
    "ftr": pl.Float64,
    "rim_share": pl.Float64,
    "mid_share": pl.Float64,
    "three_share": pl.Float64,
    "pts_per100": pl.Float64,
    "reb_per100": pl.Float64,
    "ast_per100": pl.Float64,
}


def _share(num: pl.Expr, denom: pl.Expr) -> pl.Expr:
    return pl.when(denom > 0).then(num / denom).otherwise(0.0).cast(pl.Float64)


def player_per100_features(season_stats: pl.DataFrame) -> pl.DataFrame:
    """Per-100 / rate features for every (player_id, season).

    Args:
        season_stats: One row per player-season with the canonical counting
            columns (``minutes, field_goals_made, field_goals_attempted,
            three_point_field_goals_made, free_throws_attempted, turnovers,
            points, fga_rim, fga_mid, fga_three, offensive_rebounds,
            defensive_rebounds, assists, blocks, steals``) -- built from the
            player-boxscore aggregation (see the Phase-0 fitters).

    Returns:
        One row per (player_id, season): ids as ``Utf8`` plus the 17 rate /
        per-100 features. Empty input returns the schema with zero rows.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_player_value_constants import player_per100_features
            feats = player_per100_features(season_stats)
    """
    if season_stats.is_empty():
        return pl.DataFrame(schema=_FEATURE_SCHEMA)
    fga = pl.col("field_goals_attempted")
    fta = pl.col("free_throws_attempted")
    poss_used = fga + 0.44 * fta + pl.col("turnovers")
    shots = pl.col("fga_rim") + pl.col("fga_mid") + pl.col("fga_three")
    reb = pl.col("offensive_rebounds") + pl.col("defensive_rebounds")
    out = season_stats.with_columns(
        pl.col("player_id").cast(pl.Int64, strict=False).cast(pl.Utf8),
        pl.col("team_id").cast(pl.Int64, strict=False).cast(pl.Utf8),
        pl.col("season").cast(pl.Int64),
    ).with_columns(
        pl.col("minutes").cast(pl.Float64).alias("min"),
        (100.0 * poss_used / pl.col("minutes")).cast(pl.Float64).alias("usage"),
        _share(pl.col("points"), 2 * (fga + 0.44 * fta)).alias("ts_pct"),
        _share(pl.col("field_goals_made") + 0.5 * pl.col("three_point_field_goals_made"), fga).alias("efg_pct"),
        _share(pl.col("assists"), fga).alias("ast_pct"),
        _share(pl.col("turnovers"), poss_used).alias("tov_pct"),
        _share(pl.col("offensive_rebounds"), reb).alias("oreb_pct"),
        _share(pl.col("defensive_rebounds"), reb).alias("dreb_pct"),
        _share(pl.col("blocks"), fga).alias("blk_pct"),
        _share(pl.col("steals"), fga).alias("stl_pct"),
        _share(fta, fga).alias("ftr"),
        _share(pl.col("fga_rim"), shots).alias("rim_share"),
        _share(pl.col("fga_mid"), shots).alias("mid_share"),
        _share(pl.col("fga_three"), shots).alias("three_share"),
        (100.0 * pl.col("points") / pl.col("minutes")).cast(pl.Float64).alias("pts_per100"),
        (100.0 * reb / pl.col("minutes")).cast(pl.Float64).alias("reb_per100"),
        (100.0 * pl.col("assists") / pl.col("minutes")).cast(pl.Float64).alias("ast_per100"),
    )
    return out.select(list(_FEATURE_SCHEMA))


def _load_player_box(seasons: "list[int]", league: str) -> pl.DataFrame:
    """Per-season loads + needed-columns select (release schemas drift by year)."""
    if league == "womens":
        from sportsdataverse.wbb.wbb_loaders import load_wbb_player_boxscore as _loader  # noqa: PLC0415
    else:
        from sportsdataverse.mbb.mbb_loaders import load_mbb_player_boxscore as _loader  # noqa: PLC0415

    keep = [
        "athlete_id",
        "athlete_display_name",
        "athlete_position_abbreviation",
        "season",
        "team_id",
        "minutes",
        *(_COUNT_COLS),
    ]
    frames = []
    for season in seasons:
        df = _loader([season])
        if not df.is_empty():
            frames.append(df.select([c for c in keep if c in df.columns]))
    return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()


def _load_shots(seasons: "list[int]", league: str) -> pl.DataFrame:
    """Per-season loads + needed-columns select (release schemas drift by year)."""
    if league == "womens":
        from sportsdataverse.wbb.wbb_loaders import load_wbb_shots as _loader  # noqa: PLC0415
    else:
        from sportsdataverse.mbb.mbb_loaders import load_mbb_shots as _loader  # noqa: PLC0415

    frames = []
    for season in seasons:
        df = _loader([season])
        if not df.is_empty():
            frames.append(df.select([c for c in ("athlete_id_1", "season", "type_text") if c in df.columns]))
    return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()


_COUNT_COLS = (
    "field_goals_made",
    "field_goals_attempted",
    "three_point_field_goals_made",
    "three_point_field_goals_attempted",
    "free_throws_made",
    "free_throws_attempted",
    "offensive_rebounds",
    "defensive_rebounds",
    "assists",
    "steals",
    "blocks",
    "turnovers",
    "points",
)


def aggregate_player_seasons(seasons: "list[int]", *, league: str = "mens") -> pl.DataFrame:
    """Canonical per-player-season counting frame from the boxscore release.

    Sums the per-game player boxscores into one row per (player_id, season,
    team_id) with the counting columns :func:`player_per100_features` expects.
    Shot-location splits come from the shots release (2025+), classified by
    ``type_text`` (layup/dunk/tip = rim, "three point" = three, other = mid);
    for seasons without shots data, three-point attempts come from the box and
    all remaining attempts fold into ``fga_mid``.

    Args:
        seasons: Seasons to aggregate.
        league: ``"mens"`` or ``"womens"``.

    Returns:
        One row per (player_id, season, team_id): ``player_id:Utf8, season,
        team_id:Utf8, player, minutes`` + the counting columns + ``fga_rim,
        fga_mid, fga_three``. Empty input returns zero rows.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_player_value_constants import (
                aggregate_player_seasons, player_per100_features,
            )
            feats = player_per100_features(aggregate_player_seasons([2025]))
    """
    box = _load_player_box(seasons, league)
    if box.is_empty():
        return pl.DataFrame()
    agg = (
        box.filter(pl.col("minutes").is_not_null() & (pl.col("minutes") > 0))
        .group_by("athlete_id", "season", "team_id")
        .agg(
            pl.col("athlete_display_name").first().alias("player"),
            (
                pl.col("athlete_position_abbreviation").drop_nulls().first()
                if "athlete_position_abbreviation" in box.columns
                else pl.lit(None, dtype=pl.Utf8)
            ).alias("position"),
            pl.col("minutes").cast(pl.Float64).sum().alias("minutes"),
            *[pl.col(c).cast(pl.Float64).sum() for c in _COUNT_COLS if c in box.columns],
        )
        .with_columns(
            pl.col("athlete_id").cast(pl.Int64, strict=False).cast(pl.Utf8).alias("player_id"),
            pl.col("team_id").cast(pl.Int64, strict=False).cast(pl.Utf8),
            pl.col("season").cast(pl.Int64),
        )
        .drop("athlete_id")
    )

    try:
        shots = _load_shots(seasons, league)
    except Exception:  # noqa: BLE001 - shots release floors at 2025
        shots = pl.DataFrame()
    if shots.is_empty():
        return agg.with_columns(
            pl.lit(0.0).alias("fga_rim"),
            (pl.col("field_goals_attempted") - pl.col("three_point_field_goals_attempted")).alias("fga_mid"),
            pl.col("three_point_field_goals_attempted").alias("fga_three"),
        )

    cls = (
        pl.when(pl.col("type_text").str.contains("(?i)layup|dunk|tip"))
        .then(pl.lit("rim"))
        .when(pl.col("type_text").str.contains("(?i)three point"))
        .then(pl.lit("three"))
        .otherwise(pl.lit("mid"))
    )
    shot_counts = (
        shots.filter(pl.col("athlete_id_1").is_not_null())
        .with_columns(
            pl.col("athlete_id_1").cast(pl.Int64, strict=False).cast(pl.Utf8).alias("player_id"),
            pl.col("season").cast(pl.Int64),
            cls.alias("_bucket"),
        )
        .group_by("player_id", "season")
        .agg(
            (pl.col("_bucket") == "rim").cast(pl.Int64).sum().cast(pl.Float64).alias("fga_rim"),
            (pl.col("_bucket") == "mid").cast(pl.Int64).sum().cast(pl.Float64).alias("fga_mid"),
            (pl.col("_bucket") == "three").cast(pl.Int64).sum().cast(pl.Float64).alias("fga_three"),
        )
    )
    assert agg.schema["player_id"] == shot_counts.schema["player_id"] == pl.Utf8
    return agg.join(shot_counts, on=["player_id", "season"], how="left").with_columns(
        pl.col("fga_rim", "fga_mid", "fga_three").fill_null(0.0)
    )


def _design(X: np.ndarray) -> np.ndarray:
    return np.hstack([np.ones((X.shape[0], 1)), np.asarray(X, float)])


def ridge_fit(X: np.ndarray, y: np.ndarray, lam: float) -> np.ndarray:
    """Closed-form ridge with an unpenalized intercept (coefficient 0).

    Args:
        X: Feature matrix ``(n, d)``.
        y: Targets ``(n,)``.
        lam: L2 penalty on the non-intercept coefficients.

    Returns:
        Coefficient vector of length ``d + 1`` (intercept first).

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.mbb.mbb_player_value_constants import ridge_fit
            beta = ridge_fit(np.random.rand(50, 3), np.random.rand(50), lam=1.0)
    """
    Xd = _design(X)
    pen = lam * np.eye(Xd.shape[1])
    pen[0, 0] = 0.0
    return np.asarray(np.linalg.solve(Xd.T @ Xd + pen, Xd.T @ np.asarray(y, float)))


def ridge_cv_lambda(X: np.ndarray, y: np.ndarray, groups: np.ndarray, lams: "list[float]") -> float:
    """Pick lambda by leave-one-group-out CV (groups = seasons/classes).

    Args:
        X: Feature matrix.
        y: Targets.
        groups: Group label per row (e.g. season); each held out once.
        lams: Candidate penalties.

    Returns:
        The candidate with the lowest mean held-out MSE.

    Example:
        Quick start::

            lam = ridge_cv_lambda(X, y, seasons, [0.1, 1, 10, 100])
    """
    y = np.asarray(y, float)
    best_lam, best_err = lams[0], np.inf
    for lam in lams:
        errs = []
        for g in np.unique(groups):
            tr, te = groups != g, groups == g
            b = ridge_fit(X[tr], y[tr], lam)
            errs.append(float(np.mean((_design(X[te]) @ b - y[te]) ** 2)))
        if (m := float(np.mean(errs))) < best_err:
            best_lam, best_err = lam, m
    return best_lam


def kmeans_fit(
    X: np.ndarray, k: int, seed: int, n_init: int = 10, max_iter: int = 100
) -> "tuple[np.ndarray, np.ndarray]":
    """Seeded Lloyd's KMeans, best-of-``n_init`` by inertia.

    Args:
        X: Feature matrix ``(n, d)`` (standardize first).
        k: Number of clusters.
        seed: RNG seed (deterministic output).
        n_init: Independent restarts.
        max_iter: Lloyd iterations per restart.

    Returns:
        ``(centers[k, d], labels[n])``.

    Example:
        Quick start::

            centers, labels = kmeans_fit(Z, k=8, seed=0)
    """
    X = np.asarray(X, float)
    rng = np.random.default_rng(seed)
    best: "tuple[float, np.ndarray, np.ndarray] | None" = None
    for _ in range(n_init):
        centers = X[rng.choice(len(X), k, replace=False)].copy()
        labels: np.ndarray = np.zeros(len(X), dtype=np.int64)
        for _ in range(max_iter):
            d = ((X[:, None, :] - centers[None, :, :]) ** 2).sum(-1)
            labels = d.argmin(1)
            new = np.array([X[labels == j].mean(0) if (labels == j).any() else centers[j] for j in range(k)])
            if np.allclose(new, centers):
                centers = new
                break
            centers = new
        inertia = float(((X - centers[labels]) ** 2).sum())
        if best is None or inertia < best[0]:
            best = (inertia, centers, labels)
    assert best is not None
    return best[1], best[2]


def logistic_fit(X: np.ndarray, y: np.ndarray, lam: float = 1.0) -> np.ndarray:
    """L2-penalized logistic regression via L-BFGS (intercept unpenalized).

    Args:
        X: Feature matrix ``(n, d)``.
        y: Binary outcomes (0/1).
        lam: L2 penalty on the non-intercept coefficients.

    Returns:
        Coefficient vector of length ``d + 1`` (intercept first).

    Example:
        Quick start::

            coef = logistic_fit(X, drafted, lam=1.0)
    """
    Xd = _design(X)
    yv = np.asarray(y, float)

    def nll(b: np.ndarray) -> float:
        z = Xd @ b
        p = 1.0 / (1.0 + np.exp(-z))
        eps = 1e-12
        ll = -(yv * np.log(p + eps) + (1 - yv) * np.log(1 - p + eps)).sum()
        return float(ll + lam * (b[1:] ** 2).sum())

    res = minimize(nll, np.zeros(Xd.shape[1]), method="L-BFGS-B")
    return np.asarray(res.x)


def _ari(a: np.ndarray, b: np.ndarray) -> float:
    """Adjusted Rand index between two label vectors."""
    a = np.asarray(a)
    b = np.asarray(b)
    n = len(a)
    ua, ub = np.unique(a), np.unique(b)
    m = np.zeros((len(ua), len(ub)))
    for i, x in enumerate(ua):
        for j, y in enumerate(ub):
            m[i, j] = np.sum((a == x) & (b == y))
    comb = lambda x: x * (x - 1) / 2.0  # noqa: E731
    sum_ij = comb(m).sum()
    sum_a = comb(m.sum(1)).sum()
    sum_b = comb(m.sum(0)).sum()
    expected = sum_a * sum_b / comb(n)
    max_index = 0.5 * (sum_a + sum_b)
    if max_index == expected:
        return 1.0
    return float((sum_ij - expected) / (max_index - expected))


def bootstrap_ari(
    fit_fn: "Callable[[np.ndarray], tuple[np.ndarray, np.ndarray]]",
    X: np.ndarray,
    n_boot: int = 20,
    seed: int = 0,
) -> float:
    """Cluster stability: mean ARI between the full fit and bootstrap refits.

    Args:
        fit_fn: ``X -> (centers, labels)`` (e.g. a seeded ``kmeans_fit``
            partial).
        X: Feature matrix.
        n_boot: Bootstrap resamples.
        seed: RNG seed.

    Returns:
        Mean adjusted Rand index of the resample fits' assignments (of the
        FULL sample, via nearest refit center) vs the full-fit labels.

    Example:
        Quick start::

            from functools import partial
            score = bootstrap_ari(lambda Z: kmeans_fit(Z, 8, seed=0), Z, n_boot=20, seed=0)
    """
    X = np.asarray(X, float)
    rng = np.random.default_rng(seed)
    centers, base_labels = fit_fn(X)
    scores = []
    for _ in range(n_boot):
        idx = rng.integers(0, len(X), len(X))
        c_b, _ = fit_fn(X[idx])
        d = ((X[:, None, :] - c_b[None, :, :]) ** 2).sum(-1)
        scores.append(_ari(base_labels, d.argmin(1)))
    return float(np.mean(scores))


def _models_dir_file(name: str) -> "object":
    return files("sportsdataverse.mbb") / "models" / f"{name}.json"


def load_artifact(name: str) -> dict:
    """Read a bundled player-value artifact (``mbb/models/<name>.json``).

    Args:
        name: Artifact stem, e.g. ``"mbb_box_bpm"``.

    Returns:
        The parsed JSON dict.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_player_value_constants import load_artifact
            art = load_artifact("mbb_box_bpm")
    """
    return dict(json.loads(_models_dir_file(name).read_text(encoding="utf-8")))  # type: ignore[attr-defined]


def save_artifact(name: str, obj: dict) -> None:
    """Write a bundled artifact (dev/fitter use -- writes into the source tree).

    Args:
        name: Artifact stem, e.g. ``"mbb_box_bpm"``.
        obj: JSON-serializable artifact payload.

    Example:
        Quick start::

            save_artifact("mbb_box_bpm", {"league": "mens", "coef": [0.1]})
    """
    from pathlib import Path  # noqa: PLC0415

    path = Path(str(_models_dir_file(name)))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2) + "\n", encoding="utf-8")
