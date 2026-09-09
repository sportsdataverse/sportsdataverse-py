"""Validation and ordering for the card-driven predict path."""

from __future__ import annotations

import numpy as np
import polars as pl
import pytest
import xgboost as xgb

from sportsdataverse.cfb import model_calculators as mc


def _booster(features):
    X = np.zeros((4, len(features)), dtype=float)
    y = np.array([0, 1, 0, 1])
    dm = xgb.DMatrix(X, label=y, feature_names=list(features))
    return xgb.train({"objective": "binary:logistic", "max_depth": 1}, dm, num_boost_round=1)


def test_missing_columns_are_named_in_one_error(monkeypatch):
    """The failure this surface exists to remove is a caller unable to tell what
    their frame is missing -- so name every absent column, not just the first."""
    monkeypatch.setattr(mc, "card_features", lambda m: ["down", "distance", "yards_to_goal"])
    df = pl.DataFrame({"down": [1.0]})
    with pytest.raises(ValueError) as exc:
        mc.predict_from_card(df, "xpass_model", _booster(["down", "distance", "yards_to_goal"]))
    msg = str(exc.value)
    assert "distance" in msg and "yards_to_goal" in msg
    assert "xpass_model" in msg, "the error must say which model wanted them"


def test_columns_are_ordered_by_the_card_not_the_frame(monkeypatch):
    """A frame in a different column order must score identically. If the
    DMatrix were built from frame order this silently scores garbage."""
    feats = ["down", "distance", "yards_to_goal"]
    monkeypatch.setattr(mc, "card_features", lambda m: feats)
    b = _booster(feats)
    ordered = pl.DataFrame({"down": [3.0], "distance": [7.0], "yards_to_goal": [42.0]})
    shuffled = ordered.select(["yards_to_goal", "down", "distance"])
    assert mc.predict_from_card(ordered, "xpass_model", b) == pytest.approx(
        mc.predict_from_card(shuffled, "xpass_model", b)
    )


def test_extra_columns_are_ignored(monkeypatch):
    """A real pbp frame carries hundreds of columns the model never saw."""
    feats = ["down", "distance"]
    monkeypatch.setattr(mc, "card_features", lambda m: feats)
    df = pl.DataFrame({"down": [1.0], "distance": [10.0], "play_text": ["irrelevant"]})
    assert len(mc.predict_from_card(df, "xpass_model", _booster(feats))) == 1


def test_an_empty_frame_returns_an_empty_result(monkeypatch):
    feats = ["down", "distance"]
    monkeypatch.setattr(mc, "card_features", lambda m: feats)
    df = pl.DataFrame({"down": [], "distance": []}, schema={"down": pl.Float64, "distance": pl.Float64})
    assert len(mc.predict_from_card(df, "xpass_model", _booster(feats))) == 0
