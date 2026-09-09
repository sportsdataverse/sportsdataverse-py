"""User-facing CFB model calculators.

Hand a data frame to one of these and get model output back, whether the rows
came from a real game or were typed by hand to ask a hypothetical -- the same
shape ``sportsdataverse.nfl`` already provides via
``calculate_expected_points()``.

Every calculator validates its input against the model's OWN published card
rather than a feature list restated here. Six such restatements exist in this
package and are exactly the drift that produced cfbfastR-cfb-data#70.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import polars as pl
from xgboost import DMatrix

from sportsdataverse.cfb.model_cards import card_era_contract, card_features


def predict_from_card(df: pl.DataFrame, model: str, booster: Any) -> np.ndarray:
    """Score ``df`` with ``booster``, validated and ordered by the model's card.

    Args:
        df: Frame carrying at least the model's declared features. Extra
            columns are ignored, so a full pbp frame passes through unchanged.
        model: Bundle stem, used to look up the card and to name the model in
            any error.
        booster: The loaded ``xgboost.Booster``.

    Returns:
        The booster's raw predictions.

    Raises:
        ValueError: When any declared feature is absent from ``df``. Every
            missing column is named in one message -- the failure this surface
            exists to remove is a caller unable to tell what their frame lacks.

    Example:
        Quick start::

            from sportsdataverse.cfb.model_calculators import predict_from_card
            predict_from_card(pbp, "xpass_model", booster)
    """
    feats = card_features(model)
    missing = [f for f in feats if f not in df.columns]
    if missing:
        raise ValueError(
            f"{model} needs {len(missing)} column(s) not present: {', '.join(missing)}. "
            f"Its card declares: {', '.join(feats)}"
        )
    # Selected in the CARD's order, never the frame's: XGBoost aligns a DMatrix
    # by position, so frame order would silently score against wrong columns.
    matrix = df.select(feats).to_pandas()
    return booster.predict(DMatrix(matrix, feature_names=feats))


def add_era_columns(df: pl.DataFrame, model: str, season: int | None = None) -> pl.DataFrame:
    """Add the era column(s) ``model`` consumes, using ITS card's cuts.

    The cuts are read from the published contract, never restated here. That is
    the fix for cfbfastR-cfb-data#70, where both consumers kept a private copy of
    the era boundary, both drifted to a 2017 cut the trainer never used, and
    2018-2020 scored an era off the models trained with them.

    Args:
        df: Frame carrying a ``season`` column, or any frame when ``season`` is
            given explicitly.
        model: Bundle stem, used to look up the era contract.
        season: Season to use when ``df`` has no ``season`` column -- the
            hand-built-row case.

    Returns:
        ``df`` with the contract's columns added. Returned unchanged when the
        model declares no era contract, or when the columns are already present.

    Raises:
        ValueError: When the model needs an era column but neither a ``season``
            column nor a ``season`` argument is available to derive it.

    Example:
        Quick start::

            from sportsdataverse.cfb.model_calculators import add_era_columns
            add_era_columns(pl.DataFrame({"season": [2018]}), "xpass_model")
    """
    contract = card_era_contract(model)
    if not contract:
        return df
    columns = contract["columns"]
    if all(c in df.columns for c in columns):
        return df
    lo, mid, hi = contract["cuts"]
    if season is not None:
        season_expr = pl.lit(season, dtype=pl.Int64)
    elif "season" in df.columns:
        season_expr = pl.col("season").cast(pl.Int64)
    else:
        raise ValueError(
            f"{model} needs an era column; supply a 'season' column or the season= argument"
        )
    bucket = (
        pl.when(season_expr <= lo)
        .then(0)
        .when(season_expr <= mid)
        .then(1)
        .when(season_expr <= hi)
        .then(2)
        .otherwise(3)
    )
    if contract["encoding"] == "ordinal":
        return df.with_columns(bucket.cast(pl.Int32).alias(columns[0]))
    return df.with_columns(
        [(bucket == i).cast(pl.Int32).alias(col) for i, col in enumerate(columns)]
    )
