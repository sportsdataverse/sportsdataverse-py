"""NFL draft outcome model: combine measurables + draft position + position
one-hots -> expected career value (ridge closed-form) and P(multi-year starter)
(ridge-regularized logistic via IRLS). Compute-on-demand, no bundled artifacts.

Career value target is nflverse ``w_av`` (PFR weighted career Approximate
Value; the ``car_av`` name is kept for the public schema). Methodology
citation: Pro-Football-Reference Approximate Value (no code copied).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, List, Literal, Optional, Tuple, Union, overload

import numpy as np
import polars as pl

from sportsdataverse.nfl.nfl_loaders import load_nfl_combine, load_nfl_draft_picks

if TYPE_CHECKING:  # pragma: no cover
    import pandas as pd

MEASURABLES: List[str] = ["forty", "bench", "vertical", "broad_jump", "cone", "shuttle", "ht", "wt"]

_FEATURE_SCHEMA: dict = {
    "gsis_id": pl.Utf8,
    "season": pl.Int64,
    "position": pl.Utf8,
    "round": pl.Int64,
    "pick": pl.Int64,
    **{c: pl.Float64 for c in MEASURABLES},
    **{f"{c}_imputed": pl.Int8 for c in MEASURABLES},
    "car_av": pl.Float64,
    "seasons_started": pl.Float64,
}


def _parse_height(col: str) -> pl.Expr:
    feet = pl.col(col).str.extract(r"^(\d+)-", 1).cast(pl.Float64)
    inches = pl.col(col).str.extract(r"-(\d+)$", 1).cast(pl.Float64)
    return (feet * 12.0 + inches).alias("ht")


def assemble_draft_features(combine: pl.DataFrame, draft: pl.DataFrame) -> pl.DataFrame:
    """Assemble the per-prospect draft feature frame.

    Left-joins combine measurables (via ``pfr_id``) onto draft picks, parses
    height strings (``"6-2"`` -> inches), and imputes missing measurables to
    the position-season median (retaining ``<col>_imputed`` flag columns).
    The career-value label ``car_av`` is sourced from nflverse ``w_av``
    (null -> 0.0 for drafted players who never accrued AV).

    Args:
        combine (pl.DataFrame): ``load_nfl_combine()`` frame (``pfr_id, ht, wt,
            forty, bench, vertical, broad_jump, cone, shuttle``).
        draft (pl.DataFrame): ``load_nfl_draft_picks()`` frame (``gsis_id,
            pfr_player_id, season, round, pick, position, w_av,
            seasons_started``).

    Returns:
        pl.DataFrame: One row per drafted prospect with ``gsis_id:Utf8,
        season:Int64, position:Utf8, round:Int64, pick:Int64``, Float64
        measurables + ``<col>_imputed`` flags, ``car_av:Float64,
        seasons_started:Float64``. Empty/malformed input returns a zero-row
        frame with that schema.

    Example:
        Quick start::

            import sportsdataverse.nfl as nfl
            from sportsdataverse.nfl.nfl_draft_model import assemble_draft_features
            feats = assemble_draft_features(nfl.load_nfl_combine(), nfl.load_nfl_draft_picks())

    See Also:
        * `Pro Football Reference`_ -- Approximate Value methodology
        * `nflverse`_ -- full data ecosystem (R + Python)

    .. _Pro Football Reference: https://www.pro-football-reference.com
    .. _nflverse: https://nflverse.nflverse.com
    """
    required = {"gsis_id", "season", "round", "pick", "position"}
    if draft.height == 0 or not required.issubset(draft.columns):
        return pl.DataFrame(schema=_FEATURE_SCHEMA)
    av_col = "w_av" if "w_av" in draft.columns else "car_av"
    dr = draft.select(
        pl.col("gsis_id").cast(pl.Utf8),
        pl.col("pfr_player_id").cast(pl.Utf8),
        pl.col("season").cast(pl.Int64),
        pl.col("position").cast(pl.Utf8),
        pl.col("round").cast(pl.Int64),
        pl.col("pick").cast(pl.Int64),
        pl.col(av_col).cast(pl.Float64).fill_null(0.0).alias("car_av"),
        pl.col("seasons_started").cast(pl.Float64),
    ).filter(pl.col("gsis_id").is_not_null())
    if combine.height > 0 and "pfr_id" in combine.columns:
        cb = (
            combine.with_columns(
                _parse_height("ht") if combine.schema.get("ht") == pl.Utf8 else pl.col("ht").cast(pl.Float64)
            )
            .select(
                pl.col("pfr_id").cast(pl.Utf8),
                *[
                    (pl.col(c).cast(pl.Float64) if c in combine.columns else pl.lit(None, dtype=pl.Float64)).alias(c)
                    for c in MEASURABLES
                ],
            )
            .filter(pl.col("pfr_id").is_not_null())
            .unique(subset=["pfr_id"], keep="first")
        )
        assert dr.schema["pfr_player_id"] == cb.schema["pfr_id"]
        feats = dr.join(cb, left_on="pfr_player_id", right_on="pfr_id", how="left").drop("pfr_player_id")
    else:
        feats = dr.drop("pfr_player_id").with_columns(*[pl.lit(None, dtype=pl.Float64).alias(c) for c in MEASURABLES])
    # impute to the position-season median, then the position median, then the
    # global median; retain flags
    for c in MEASURABLES:
        feats = feats.with_columns(pl.col(c).is_null().cast(pl.Int8).alias(f"{c}_imputed"))
        feats = feats.with_columns(
            pl.col(c)
            .fill_null(pl.col(c).median().over("position", "season"))
            .fill_null(pl.col(c).median().over("position"))
            .fill_null(pl.col(c).median())
            .fill_null(0.0)
        )
    return feats.select(list(_FEATURE_SCHEMA.keys()))


def _ridge_fit(X: np.ndarray, y: np.ndarray, lam: float) -> np.ndarray:
    """Closed-form ridge: ``(X'X + lam*I)^-1 X'y`` (intercept column unpenalized
    is not needed — features are standardized and y is centered by the caller)."""
    n_feat = X.shape[1]
    return np.linalg.solve(X.T @ X + lam * np.eye(n_feat), X.T @ y)


def _logistic_fit(X: np.ndarray, y: np.ndarray, lam: float, n_iter: int = 25) -> np.ndarray:
    """Ridge-regularized logistic regression via IRLS (numpy only)."""
    n, d = X.shape
    beta = np.zeros(d)
    for _ in range(n_iter):
        p = 1.0 / (1.0 + np.exp(-np.clip(X @ beta, -30, 30)))
        w = np.clip(p * (1 - p), 1e-6, None)
        z = X @ beta + (y - p) / w
        wx = X * w[:, None]
        beta_new = np.linalg.solve(X.T @ wx + lam * np.eye(d), X.T @ (w * z))
        if np.max(np.abs(beta_new - beta)) < 1e-8:
            beta = beta_new
            break
        beta = beta_new
    return beta


def _design(
    feats: pl.DataFrame,
    positions: List[str],
    stats: Optional[dict] = None,
) -> Tuple[np.ndarray, dict]:
    """Build the standardized design matrix (intercept, z-scored numerics,
    imputation flags, position one-hots). ``stats`` holds train means/stds so
    the prediction design reuses the train standardization."""
    numeric = MEASURABLES + ["round", "pick", "log_pick"]
    df = feats.with_columns(pl.col("pick").cast(pl.Float64).log().alias("log_pick"))
    cols = {}
    for c in numeric:
        v = df[c].cast(pl.Float64).to_numpy().astype(float)
        cols[c] = v
    if stats is None:
        stats = {c: (float(np.nanmean(v)), float(np.nanstd(v) or 1.0)) for c, v in cols.items()}
    mats = [np.ones(df.height)]
    for c in numeric:
        mu, sd = stats[c]
        mats.append((cols[c] - mu) / (sd if sd > 0 else 1.0))
    for c in MEASURABLES:
        mats.append(df[f"{c}_imputed"].cast(pl.Float64).to_numpy().astype(float))
    pos = df["position"].to_list()
    for p in positions:
        mats.append(np.array([1.0 if x == p else 0.0 for x in pos]))
    return np.column_stack(mats), stats


_PROJ_SCHEMA: dict = {
    "gsis_id": pl.Utf8,
    "target_class": pl.Int64,
    "position": pl.Utf8,
    "pred_car_av": pl.Float64,
    "hit_prob": pl.Float64,
    "outcome_rank": pl.Int64,
}

# hit := seasons_started >= 3 (multi-year starter), per the spine design.
HIT_SEASONS_STARTED: float = 3.0

# training maturity horizon: outcomes are only trusted for classes at least
# this many seasons before the target class
MATURITY_YEARS: int = 5


@overload
def nfl_draft_projection(
    seasons: List[int], target_class: int, *, lam: float = ..., return_as_pandas: Literal[False] = ...
) -> pl.DataFrame: ...


@overload
def nfl_draft_projection(
    seasons: List[int], target_class: int, *, lam: float = ..., return_as_pandas: Literal[True]
) -> "pd.DataFrame": ...


def nfl_draft_projection(
    seasons: List[int], target_class: int, *, lam: float = 1.0, return_as_pandas: bool = False
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Draft outcome projection for one draft class.

    Trains the closed-form ridge (expected ``car_av``) and the IRLS logistic
    (``hit_prob`` = P(``seasons_started >= 3``)) on **matured** classes
    (``season <= target_class - 5``) and scores the ``target_class``
    prospects. Features: standardized combine measurables (+ imputation
    flags), draft ``round``/``pick``/``log(pick)``, position one-hots.

    Args:
        seasons (List[int]): Draft classes to load (training classes beyond
            the maturity boundary are filtered out automatically).
        target_class (int): The draft class to score.
        lam (float): Ridge regularization strength.
        return_as_pandas (bool): If True, returns a pandas dataframe.

    Returns:
        pl.DataFrame: One row per ``target_class`` prospect: ``gsis_id:Utf8,
        target_class:Int64, position:Utf8, pred_car_av:Float64,
        hit_prob:Float64, outcome_rank:Int64`` (dense rank, best first).
        Empty training or prediction slice returns a zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.nfl.nfl_draft_model import nfl_draft_projection
            proj = nfl_draft_projection(list(range(2000, 2020)), 2019)
            proj.sort("outcome_rank").head()

    See Also:
        * `Pro Football Reference`_ -- Approximate Value methodology
        * `nflverse`_ -- full data ecosystem (R + Python)

    .. _Pro Football Reference: https://www.pro-football-reference.com
    .. _nflverse: https://nflverse.nflverse.com
    """
    combine = load_nfl_combine()
    draft = load_nfl_draft_picks()
    if "season" in draft.columns and seasons:
        draft = draft.filter(pl.col("season").is_in(list(seasons) + [target_class]))
    feats = assemble_draft_features(combine, draft)
    return project_draft_class(feats, target_class, lam=lam, return_as_pandas=return_as_pandas)  # type: ignore[return-value]


def project_draft_class(
    feats: pl.DataFrame, target_class: int, *, lam: float = 1.0, return_as_pandas: bool = False
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Fit on matured classes and score one target class (offline core).

    Same contract as :func:`nfl_draft_projection` but takes an already
    assembled feature frame (see :func:`assemble_draft_features`) — used by the
    offline oracle tests and callers who want to supply extra feature columns.

    Args:
        feats (pl.DataFrame): Assembled feature frame.
        target_class (int): The draft class to score.
        lam (float): Ridge regularization strength.
        return_as_pandas (bool): If True, returns a pandas dataframe.

    Returns:
        pl.DataFrame: Same schema as :func:`nfl_draft_projection`.

    Example:
        Quick start::

            from sportsdataverse.nfl.nfl_draft_model import assemble_draft_features, project_draft_class
            proj = project_draft_class(feats, 2019)
    """
    train = feats.filter(pl.col("season") <= target_class - MATURITY_YEARS)
    pred = feats.filter(pl.col("season") == target_class)
    if train.height == 0 or pred.height == 0:
        result = pl.DataFrame(schema=_PROJ_SCHEMA)
        return result.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else result
    positions = sorted([p for p in train["position"].unique().to_list() if p is not None])
    X_train, stats = _design(train, positions)
    X_pred, _ = _design(pred, positions, stats=stats)
    y = train["car_av"].to_numpy().astype(float)
    y_mean = float(y.mean())
    beta = _ridge_fit(X_train, y - y_mean, lam)
    pred_av = X_pred @ beta + y_mean
    hit = (train["seasons_started"].fill_null(0.0).to_numpy().astype(float) >= HIT_SEASONS_STARTED).astype(float)
    beta_h = _logistic_fit(X_train, hit, lam)
    hit_prob = 1.0 / (1.0 + np.exp(-np.clip(X_pred @ beta_h, -30, 30)))
    result = (
        pred.select("gsis_id", "position")
        .with_columns(
            pl.lit(target_class, dtype=pl.Int64).alias("target_class"),
            pl.Series("pred_car_av", pred_av).cast(pl.Float64),
            pl.Series("hit_prob", hit_prob).cast(pl.Float64),
        )
        .with_columns(pl.col("pred_car_av").rank(method="dense", descending=True).cast(pl.Int64).alias("outcome_rank"))
        .select(list(_PROJ_SCHEMA.keys()))
        .sort("outcome_rank")
    )
    return result.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else result
