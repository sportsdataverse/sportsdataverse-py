"""Shared harness for the NBA/WNBA/G-League draft-and-projection spine (T3.4).

One home for every algorithm shared across the four draft/projection models
(``nba_draft_model``, ``nba_aging_curve``, ``nba_availability``,
``nba_rookie_projection``) and their WNBA by-reference shims:

- League-agnostic numpy fitters (``ridge_fit``, ``logistic_fit_irls``).
- Validation metrics (``spearman_corr``, ``auc``, ``mae``, ``calibration_table``).
- The as-of-class leakage boundary (``as_of_class_split``).
- The all-era, box-score-derived **career-value** common currency
  (``box_value_per100`` / ``career_value_from_seasons``), scale-anchored to the
  shipped ``nba_bpm`` on the 2016+ overlap era (see ``dev/nba_draft/fit_box_value.py``).
- The combine-feature builder (``build_combine_features``).
- Per-league fitted constants (``LEAGUE_CONSTANTS`` / ``get_constants``).

Design: the **math** here is league-agnostic; the **fitted numbers** (box-value
coefficients, replacement level) live in ``LEAGUE_CONSTANTS`` per league and are
never hard-coded inside an algorithm function. See
``C:/Users/saiem/Documents/ClaudeCowork/specs/2026-07-07-nba-draft-projection-design.md``
section 3.2 for the algorithm/constants boundary rationale.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import polars as pl
from scipy.stats import rankdata

# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------


def spearman_corr(a: np.ndarray, b: np.ndarray) -> float:
    """Spearman rank correlation between two 1-D arrays.

    Args:
        a: First array.
        b: Second array, same length as ``a``.

    Returns:
        Spearman's rho as a plain ``float``.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.nba.nba_draft_constants import spearman_corr
            spearman_corr(np.array([1.0, 2.0, 3.0]), np.array([10.0, 20.0, 15.0]))
    """
    ra, rb = rankdata(a), rankdata(b)
    return float(np.corrcoef(ra, rb)[0, 1])


def mae(a: np.ndarray, b: np.ndarray) -> float:
    """Mean absolute error between two 1-D arrays.

    Args:
        a: First array (e.g. predictions).
        b: Second array (e.g. realized values), same length as ``a``.

    Returns:
        Mean absolute error as a plain ``float``.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.nba.nba_draft_constants import mae
            mae(np.array([1.0, 2.0]), np.array([1.5, 2.5]))
    """
    return float(np.mean(np.abs(np.asarray(a, dtype=float) - np.asarray(b, dtype=float))))


def auc(y_true: np.ndarray, p_pred: np.ndarray) -> float:
    """Area under the ROC curve via the Mann-Whitney U statistic.

    Args:
        y_true: Binary ground-truth labels (0/1).
        p_pred: Predicted probabilities, same length as ``y_true``.

    Returns:
        AUC as a plain ``float``; ``0.5`` (no discrimination) when one class
        is absent so callers never divide by zero.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.nba.nba_draft_constants import auc
            auc(np.array([0, 0, 1, 1]), np.array([0.1, 0.2, 0.8, 0.9]))
    """
    y = np.asarray(y_true)
    p = np.asarray(p_pred, dtype=float)
    pos, neg = p[y == 1], p[y == 0]
    if pos.size == 0 or neg.size == 0:
        return 0.5
    r = rankdata(p)
    return float((r[y == 1].sum() - pos.size * (pos.size + 1) / 2) / (pos.size * neg.size))


def calibration_table(y_true: np.ndarray, p_pred: np.ndarray, n_bins: int = 10) -> pl.DataFrame:
    """Bin predicted probabilities and compare mean-predicted vs mean-actual.

    Args:
        y_true: Binary or fractional ground-truth outcomes in ``[0, 1]``.
        p_pred: Predicted probabilities/rates in ``[0, 1]``.
        n_bins: Number of equal-width probability bins.

    Returns:
        Frame ``bin_mid:Float64, mean_pred:Float64, mean_actual:Float64, n:Int64``,
        one row per non-empty bin (``height <= n_bins``).

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.nba.nba_draft_constants import calibration_table
            y = np.random.default_rng(0).integers(0, 2, 200)
            p = np.random.default_rng(1).random(200)
            calibration_table(y, p, n_bins=10)
    """
    df = pl.DataFrame({"y": np.asarray(y_true, dtype=float), "p": np.asarray(p_pred, dtype=float)})
    df = df.with_columns((pl.col("p").clip(0.0, 0.9999) * n_bins).floor().cast(pl.Int64).alias("bin"))
    return (
        df.group_by("bin")
        .agg(pl.col("p").mean().alias("mean_pred"), pl.col("y").mean().alias("mean_actual"), pl.len().alias("n"))
        .sort("bin")
        .with_columns(((pl.col("bin") + 0.5) / n_bins).alias("bin_mid"))
        .select("bin_mid", "mean_pred", "mean_actual", "n")
    )


# ---------------------------------------------------------------------------
# Fitters
# ---------------------------------------------------------------------------


def _add_intercept(X: np.ndarray) -> np.ndarray:
    return np.column_stack([np.ones(X.shape[0]), X])


def ridge_fit(X: np.ndarray, y: np.ndarray, lam: float) -> np.ndarray:
    """Closed-form ridge regression (normal equations), intercept unpenalized.

    Args:
        X: Feature matrix, shape ``(n_samples, n_features)``.
        y: Target vector, shape ``(n_samples,)``.
        lam: Ridge penalty strength (``0.0`` = ordinary least squares).

    Returns:
        Coefficient vector of length ``n_features + 1``: ``beta[0]`` is the
        intercept, ``beta[1:]`` are the feature slopes.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.nba.nba_draft_constants import ridge_fit
            X = np.random.default_rng(0).normal(size=(50, 2))
            y = X @ np.array([1.0, -2.0]) + 3.0
            beta = ridge_fit(X, y, lam=1e-3)
    """
    Xi = _add_intercept(np.asarray(X, dtype=float))
    p = Xi.shape[1]
    reg = lam * np.eye(p)
    reg[0, 0] = 0.0
    return np.asarray(np.linalg.solve(Xi.T @ Xi + reg, Xi.T @ np.asarray(y, dtype=float)))


def logistic_fit_irls(X: np.ndarray, y: np.ndarray, *, max_iter: int = 50, tol: float = 1e-8) -> np.ndarray:
    """Logistic regression via iteratively reweighted least squares (IRLS).

    Args:
        X: Feature matrix, shape ``(n_samples, n_features)``.
        y: Binary target vector, shape ``(n_samples,)``.
        max_iter: Maximum IRLS iterations.
        tol: Convergence tolerance on the max coefficient change.

    Returns:
        Coefficient vector of length ``n_features + 1`` (intercept first).

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.nba.nba_draft_constants import logistic_fit_irls
            X = np.random.default_rng(1).normal(size=(100, 2))
            y = (X[:, 0] > 0).astype(int)
            beta = logistic_fit_irls(X, y)
    """
    Xi = _add_intercept(np.asarray(X, dtype=float))
    yv = np.asarray(y, dtype=float)
    beta = np.zeros(Xi.shape[1])
    for _ in range(max_iter):
        eta = Xi @ beta
        mu = 1 / (1 + np.exp(-eta))
        w = np.clip(mu * (1 - mu), 1e-9, None)
        z = eta + (yv - mu) / w
        beta_new = np.linalg.solve((Xi * w[:, None]).T @ Xi + 1e-6 * np.eye(Xi.shape[1]), (Xi * w[:, None]).T @ z)
        if np.max(np.abs(beta_new - beta)) < tol:
            beta = beta_new
            break
        beta = beta_new
    return np.asarray(beta)


# ---------------------------------------------------------------------------
# Leakage boundary
# ---------------------------------------------------------------------------


def as_of_class_split(
    df: pl.DataFrame, cutoff_year: int, *, year_col: str = "draft_year"
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Split a frame into a training window and a strictly-future holdout.

    This is the as-of-class leakage boundary used by every predictive model
    in the spine: training uses classes/seasons at or before ``cutoff_year``,
    holdout is strictly after it.

    Args:
        df: Frame carrying ``year_col``.
        cutoff_year: Last year included in the training split.
        year_col: Column name to split on (``"draft_year"`` or ``"season"``).

    Returns:
        ``(train, holdout)`` tuple: ``train`` has ``year_col <= cutoff_year``,
        ``holdout`` has ``year_col > cutoff_year``.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.nba.nba_draft_constants import as_of_class_split
            df = pl.DataFrame({"draft_year": [2015, 2016, 2017], "v": [1, 2, 3]})
            train, holdout = as_of_class_split(df, cutoff_year=2016)
    """
    return df.filter(pl.col(year_col) <= cutoff_year), df.filter(pl.col(year_col) > cutoff_year)


# ---------------------------------------------------------------------------
# League constants
# ---------------------------------------------------------------------------

BOX_VALUE_FEATURES: list[str] = ["pts100", "reb100", "ast100", "stl100", "blk100", "tov100", "ts_pct", "usg"]


@dataclass(frozen=True)
class LeagueConstants:
    """Per-league fitted constants for the draft/projection spine.

    Attributes:
        replacement: Replacement-level ``box_value_per100`` (VORP convention).
        box_value_coef: ``[intercept, *BOX_VALUE_FEATURES coefficients]`` (9 floats),
            fit vs the shipped ``nba_bpm`` (see ``dev/nba_draft/fit_box_value.py``).
        peak_age: Aging-curve peak age for this league. Mirrors the fitted
            ``{prefix}_aging_curve.json`` artifact's ``peak_age`` (nba/wnba
            = 29, from ``dev/nba_draft/fit_aging_curve.py``); gleague has no
            fitted curve yet so its value is a seeded placeholder. Informational
            only -- the runtime reads ``peak_age`` from the loaded artifact,
            not this field.
        games_full_season: Games in a full regular season (82 NBA/WNBA-scaled
            below; WNBA plays a shorter season).
        artifact_prefix: Bundled-artifact filename prefix under
            ``sportsdataverse/nba/models/`` (``"nba"``/``"wnba"``/``"gleague"``).
    """

    replacement: float
    box_value_coef: list[float] = field(default_factory=list)
    peak_age: float = 27.0
    games_full_season: int = 82
    artifact_prefix: str = "nba"


# NBA box_value_coef/replacement are fit in dev/nba_draft/fit_box_value.py
# against the shipped nba_bpm (2016-17..2019-20 overlap, combine-class
# players) -- see that script's printed diagnostics for provenance.
# WNBA/G-League seed from the NBA fit until Phase 5 re-fits them on
# wnba_stats data (documented reduced-combine caveat in wnba_draft_constants.py).
LEAGUE_CONSTANTS: dict[str, LeagueConstants] = {
    "nba": LeagueConstants(
        replacement=-22.6616,
        box_value_coef=[
            -134.4456,
            0.8133,
            0.1469,
            0.7374,
            0.6847,
            0.4375,
            -0.3049,
            -1.9082,
            0.5861,
        ],
        peak_age=29.0,
        games_full_season=82,
        artifact_prefix="nba",
    ),
    "wnba": LeagueConstants(
        replacement=-22.6616,
        box_value_coef=[
            -134.4456,
            0.8133,
            0.1469,
            0.7374,
            0.6847,
            0.4375,
            -0.3049,
            -1.9082,
            0.5861,
        ],
        peak_age=29.0,
        games_full_season=40,
        artifact_prefix="wnba",
    ),
    "gleague": LeagueConstants(
        replacement=-22.6616,
        box_value_coef=[
            -134.4456,
            0.8133,
            0.1469,
            0.7374,
            0.6847,
            0.4375,
            -0.3049,
            -1.9082,
            0.5861,
        ],
        peak_age=25.0,
        games_full_season=50,
        artifact_prefix="gleague",
    ),
}


def get_constants(league: str) -> LeagueConstants:
    """Look up the fitted constants table for a league.

    Args:
        league: One of ``"nba"``, ``"wnba"``, ``"gleague"``.

    Returns:
        The league's :class:`LeagueConstants`.

    Raises:
        ValueError: If ``league`` is not a known key.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_draft_constants import get_constants
            get_constants("nba").peak_age
    """
    try:
        return LEAGUE_CONSTANTS[league]
    except KeyError as exc:
        raise ValueError(f"Unknown league {league!r}; expected one of {sorted(LEAGUE_CONSTANTS)}") from exc


# ---------------------------------------------------------------------------
# Career-value box formula
# ---------------------------------------------------------------------------


def box_value_per100(feats: pl.DataFrame, *, league: str = "nba") -> pl.Series:
    """Linear box-score value score per 100 possessions.

    Args:
        feats: Frame carrying all of :data:`BOX_VALUE_FEATURES`.
        league: League key for :func:`get_constants`.

    Returns:
        ``Float64`` series, the dot product of ``[1, *BOX_VALUE_FEATURES]``
        with the league's fitted ``box_value_coef``.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.nba.nba_draft_constants import box_value_per100
            feats = pl.DataFrame({
                "pts100": [25.0], "reb100": [7.0], "ast100": [5.0], "stl100": [1.2],
                "blk100": [0.8], "tov100": [2.5], "ts_pct": [0.58], "usg": [26.0],
            })
            box_value_per100(feats)
    """
    coef = get_constants(league).box_value_coef
    intercept, slopes = coef[0], coef[1:]
    expr = pl.lit(intercept)
    for name, c in zip(BOX_VALUE_FEATURES, slopes):
        expr = expr + pl.col(name).fill_null(0.0) * c
    return feats.select(expr.alias("box_value_per100"))["box_value_per100"]


def career_value_from_seasons(season_stats: pl.DataFrame, *, league: str = "nba") -> pl.DataFrame:
    """Aggregate per-season box rates into an all-era career-value label.

    ``season_vorp = (box_value_per100 - replacement) * minutes / 1000``;
    ``career_value = sum(season_vorp)`` over all rows for a player. This is
    the common-currency label consumed by the draft (①) and rookie/soph (④)
    models, fit and cited in ``dev/nba_draft/fit_box_value.py``.

    Args:
        season_stats: Per-``(player_id, season)`` rows carrying
            :data:`BOX_VALUE_FEATURES` + ``minutes``.
        league: League key for :func:`get_constants`.

    Returns:
        Frame ``player_id, career_value:Float64, seasons_played:Int64,
        total_minutes:Float64`` — one row per distinct ``player_id``.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.nba.nba_draft_constants import career_value_from_seasons
            season_stats = pl.DataFrame({
                "player_id": ["1"], "pts100": [25.0], "reb100": [7.0], "ast100": [5.0],
                "stl100": [1.2], "blk100": [0.8], "tov100": [2.5], "ts_pct": [0.58],
                "usg": [26.0], "minutes": [2000.0],
            })
            career_value_from_seasons(season_stats)
    """
    replacement = get_constants(league).replacement
    scored = season_stats.with_columns(box_value_per100(season_stats, league=league).alias("_box_value"))
    scored = scored.with_columns(
        ((pl.col("_box_value") - replacement) * pl.col("minutes") / 1000.0).alias("_season_vorp")
    )
    return (
        scored.group_by("player_id")
        .agg(
            pl.col("_season_vorp").sum().alias("career_value"),
            pl.len().alias("seasons_played"),
            pl.col("minutes").sum().alias("total_minutes"),
        )
        .with_columns(pl.col("seasons_played").cast(pl.Int64))
        .select("player_id", "career_value", "seasons_played", "total_minutes")
    )


# ---------------------------------------------------------------------------
# Combine-feature builder
# ---------------------------------------------------------------------------

COMBINE_FEATURES: list[str] = [
    "height_wo_shoes",
    "weight",
    "wingspan",
    "standing_reach",
    "body_fat_pct",
    "hand_length",
    "hand_width",
    "lane_agility",
    "three_quarter_sprint",
    "standing_vertical",
    "max_vertical",
    "spot_fifteen_corner_left_pct",
    "offdrib_fifteen_top_pct",
    "bmi",
    "wingspan_diff",
]


def _assert_utf8_player_id(df: pl.DataFrame, name: str) -> None:
    if "player_id" in df.columns and df.schema["player_id"] != pl.Utf8:
        raise ValueError(f"{name}.player_id must be Utf8, got {df.schema['player_id']}")


def build_combine_features(
    anthro: pl.DataFrame,
    drills: pl.DataFrame,
    spot: pl.DataFrame,
    nonstat: pl.DataFrame,
    *,
    league: str = "nba",
) -> pl.DataFrame:
    """Join the four combine frames into one per-prospect feature vector.

    Outer-joins ``anthro``/``drills``/``spot``/``nonstat`` on ``player_id`` (+
    ``draft_year`` when present), asserts ``player_id`` is ``Utf8`` on every
    input, derives ``bmi`` and ``wingspan_diff``, and imputes missing drill/
    shooting measurements with ``0.0`` (a neutral, documented placeholder —
    the bundled draft artifact's ``feature_median`` supersedes this at score
    time via ``nba_draft_model``'s ``fill_null``).

    Args:
        anthro: ``nba_stats_draftcombineplayeranthro``-shaped frame.
        drills: ``nba_stats_draftcombinedrillresults``-shaped frame.
        spot: ``nba_stats_draftcombinespotshooting``-shaped frame.
        nonstat: ``nba_stats_draftcombinenonstationaryshooting``-shaped frame.
        league: League key (unused today, reserved for league-specific
            imputation constants).

    Returns:
        Frame ``player_id, draft_year`` (when present in ``anthro``) plus
        :data:`COMBINE_FEATURES`. Empty ``anthro`` -> zero-row frame with the
        full schema.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.nba.nba_draft_constants import build_combine_features
            anthro = pl.DataFrame({
                "player_id": ["1"], "draft_year": [2019], "height_wo_shoes": [78.0],
                "weight": [210.0], "wingspan": [82.0], "standing_reach": [102.0],
                "body_fat_pct": [7.0], "hand_length": [9.0], "hand_width": [10.0],
            })
            empty = pl.DataFrame({"player_id": []}, schema={"player_id": pl.Utf8})
            build_combine_features(anthro, empty, empty, empty)
    """
    del league  # reserved for future league-specific imputation
    for frame, name in ((anthro, "anthro"), (drills, "drills"), (spot, "spot"), (nonstat, "nonstat")):
        _assert_utf8_player_id(frame, name)

    if anthro.is_empty():
        schema = {"player_id": pl.Utf8, "draft_year": pl.Int64, **{c: pl.Float64 for c in COMBINE_FEATURES}}
        return pl.DataFrame(schema=schema)

    join_keys = ["player_id"] + (["draft_year"] if "draft_year" in anthro.columns else [])
    joined = anthro
    for other in (drills, spot, nonstat):
        if other.is_empty():
            continue
        other_keys = [k for k in join_keys if k in other.columns]
        joined = joined.join(other, on=other_keys, how="left")

    for col in COMBINE_FEATURES:
        if col not in joined.columns:
            joined = joined.with_columns(pl.lit(None).cast(pl.Float64).alias(col))

    joined = joined.with_columns(
        (pl.col("weight") / (pl.col("height_wo_shoes") ** 2) * 703.0).alias("bmi"),
        (pl.col("wingspan") - pl.col("height_wo_shoes")).alias("wingspan_diff"),
    )
    joined = joined.with_columns([pl.col(c).fill_null(0.0).cast(pl.Float64) for c in COMBINE_FEATURES])

    keep = ["player_id"] + (["draft_year"] if "draft_year" in joined.columns else []) + COMBINE_FEATURES
    return joined.select(keep)
