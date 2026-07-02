"""Trained SPM: box-score features regressed onto our RAPM target (per-100)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Union

import numpy as np
import pandas as pd
import polars as pl
from sklearn.linear_model import Ridge

from sportsdataverse.nba.nba_box_logs import _STATS, box_features
from sportsdataverse.nba.nba_model_validation import RatingsFit

SPM_FEATURES: List[str] = list(_STATS)


@dataclass(frozen=True)
class SpmCoefficients:
    """Fitted SPM coefficients (box features -> offense/defense RAPM, per-100).

    Attributes:
        o_coef: Coefficient vector for the offense regression (shape ``[n_features]``).
        d_coef: Coefficient vector for the defense regression (shape ``[n_features]``).
        o_intercept: Intercept for the offense regression.
        d_intercept: Intercept for the defense regression.
        feature_names: Ordered list of feature column names corresponding to the
            coefficient vectors.
    """

    o_coef: np.ndarray
    d_coef: np.ndarray
    o_intercept: float
    d_intercept: float
    feature_names: List[str]


def _feature_matrix(box_features: pl.DataFrame, feature_names: List[str]) -> np.ndarray:
    """Extract a float64 numpy matrix from *box_features* for *feature_names*."""
    return box_features.select(feature_names).to_numpy().astype(np.float64)


def train_spm(
    box_features: pl.DataFrame,
    rapm_target: pl.DataFrame,
    *,
    feature_names: Optional[List[str]] = None,
    alpha: float = 100.0,
) -> SpmCoefficients:
    """Ridge-fit box features onto ``o_rapm`` and ``d_rapm`` (two regressions).

    The two models share the same feature matrix but separate target vectors,
    producing independent offense and defense coefficient vectors.

    Args:
        box_features: Per-player per-100 features.  Must contain ``player_id``
            and every column in *feature_names*.
        rapm_target: Per-player RAPM target frame with columns
            ``player_id``, ``o_rapm``, and ``d_rapm``.  Only the rows whose
            ``player_id`` appears in *box_features* are used (inner join).
        feature_names: Ordered list of feature columns to regress on.
            Defaults to ``SPM_FEATURES`` (= ``_STATS`` from ``nba_box_logs``).
        alpha: Ridge regularization strength (``sklearn.linear_model.Ridge``).
            Lower values approach OLS; higher values shrink toward zero.

    Returns:
        ``SpmCoefficients`` with offense and defense coefficient vectors,
        intercepts, and the ordered ``feature_names``.

    Raises:
        ValueError: If *feature_names* lists a column absent from *box_features*
            or *rapm_target* is missing required columns.

    Example:
        Fit on pooled multi-season features + RAPM::

            from sportsdataverse.nba import train_spm
            coef = train_spm(box_feats, rapm_ratings)

        With custom regularization::

            coef = train_spm(box_feats, rapm_ratings, alpha=50.0)
    """
    names: List[str] = feature_names if feature_names is not None else SPM_FEATURES
    joined = box_features.join(
        rapm_target.select(["player_id", "o_rapm", "d_rapm"]),
        on="player_id",
        how="inner",
    )
    X = _feature_matrix(joined, names)
    o_y = joined["o_rapm"].to_numpy().astype(np.float64)
    d_y = joined["d_rapm"].to_numpy().astype(np.float64)
    o_model = Ridge(alpha=alpha).fit(X, o_y)
    d_model = Ridge(alpha=alpha).fit(X, d_y)
    return SpmCoefficients(
        o_coef=np.asarray(o_model.coef_, dtype=np.float64),
        d_coef=np.asarray(d_model.coef_, dtype=np.float64),
        o_intercept=float(o_model.intercept_),
        d_intercept=float(d_model.intercept_),
        feature_names=list(names),
    )


def nba_spm(
    box_features: pl.DataFrame,
    coefficients: SpmCoefficients,
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Apply fitted SPM coefficients to per-100 box features -> OSPM/DSPM/SPM.

    Applies a linear scoring rule:

    .. code-block:: text

        ospm = X @ o_coef + o_intercept
        dspm = X @ d_coef + d_intercept
        spm  = ospm + dspm

    Args:
        box_features: Per-player per-100 features.  Must contain ``player_id``,
            every column in ``coefficients.feature_names``, ``min``, and ``gp``.
        coefficients: A ``SpmCoefficients`` instance from ``train_spm``.
        return_as_pandas: When ``True``, return a ``pandas.DataFrame`` instead of
            a ``polars.DataFrame``.

    Returns:
        Per-player frame with columns
        ``player_id`` (Int64), ``ospm`` (Float64), ``dspm`` (Float64),
        ``spm`` (Float64), ``min`` (Float64), ``gp`` (Int64).

    Example:
        Score a season::

            from sportsdataverse.nba import nba_spm
            ratings = nba_spm(box_feats, coef)
            print(ratings.sort("spm", descending=True).head())

        Pipeline next step::

            ratings.filter(pl.col("min") >= 500).sort("spm", descending=True)

        See Also:
            * `nba_rapm`_ — RAPM target the SPM is trained on
            * `hoopR`_ — companion R package for NBA data

        .. _nba_rapm: sportsdataverse.nba.nba_rapm
        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    X = _feature_matrix(box_features, coefficients.feature_names)
    ospm = X @ coefficients.o_coef + coefficients.o_intercept
    dspm = X @ coefficients.d_coef + coefficients.d_intercept
    out = (
        box_features.select(["player_id", "min", "gp"])
        .with_columns(
            pl.col("player_id").cast(pl.Int64),
            pl.col("gp").cast(pl.Int64),
            pl.Series("ospm", ospm).cast(pl.Float64),
            pl.Series("dspm", dspm).cast(pl.Float64),
            pl.Series("spm", (ospm + dspm)).cast(pl.Float64),
        )
        .select(["player_id", "ospm", "dspm", "spm", "min", "gp"])
    )
    if return_as_pandas:
        return out.to_pandas()
    return out


class NbaSpmModel:
    """A ``RatingsModel`` that scores a fold via fitted SPM coefficients.

    Restricts its box aggregation to the fold's ``game_id`` (the leakage guard),
    then applies the (globally pre-fit) SPM coefficients.

    Args:
        coefficients: A ``SpmCoefficients`` instance from ``train_spm``.
        player_logs: Per-player-per-game box lines used to build fold features.
        team_logs: Per-team-per-game lines used to estimate per-game possessions.

    Example:
        Validate SPM head-to-head with plain RAPM::

            from sportsdataverse.nba import NbaSpmModel, train_spm
            from sportsdataverse.nba.nba_model_validation import validate_model
            model = NbaSpmModel(coef, logs["player"], logs["team"])
            report = validate_model(model, season_frames, model_name="spm")
    """

    def __init__(
        self,
        coefficients: SpmCoefficients,
        player_logs: pl.DataFrame,
        team_logs: pl.DataFrame,
    ) -> None:
        self._coef = coefficients
        self._player_logs = player_logs
        self._team_logs = team_logs

    def fit_ratings(self, possessions: pl.DataFrame) -> RatingsFit:
        """Aggregate the fold's box (restricted to its game_ids) and apply SPM coeffs.

        Args:
            possessions: The fold's possession+lineup frame.  Only the ``game_id``
                values it contains are used to filter ``player_logs`` and
                ``team_logs`` (the leakage guard).

        Returns:
            ``RatingsFit`` with ``o_ratings`` and ``d_ratings`` dicts mapping
            player_id to per-100 OSPM/DSPM. Returns empty dicts when no
            box features can be built from the fold's games.
        """
        game_ids = possessions["game_id"].unique().to_list()
        bf = box_features(self._player_logs, self._team_logs, game_ids=game_ids)
        if bf.is_empty():
            return RatingsFit(o_ratings={}, d_ratings={})
        scored = nba_spm(bf, self._coef)
        o = {int(p): float(v) for p, v in zip(scored["player_id"], scored["ospm"])}
        d = {int(p): float(v) for p, v in zip(scored["player_id"], scored["dspm"])}
        return RatingsFit(o_ratings=o, d_ratings=d)
