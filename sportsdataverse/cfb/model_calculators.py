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

from sportsdataverse.cfb.model_cards import card_features


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
