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


def test_ordinal_era_is_derived_from_the_cards_cuts(monkeypatch):
    """cfbfastR-cfb-data#70: consumers kept a private era cut of 2017 the trainer
    never used, so 2018-2020 scored an era off. The cut now comes from the card."""
    monkeypatch.setattr(mc, "card_era_contract", lambda m: {
        "encoding": "ordinal", "columns": ["era"], "cuts": [2006, 2013, 2020]})
    out = mc.add_era_columns(pl.DataFrame({"season": [2005, 2010, 2018, 2024]}), "xpass_model")
    assert out["era"].to_list() == [0, 1, 2, 3]


def test_2018_through_2020_land_in_bucket_2_not_3(monkeypatch):
    """The exact regression from cfbfastR-cfb-data#70, pinned."""
    monkeypatch.setattr(mc, "card_era_contract", lambda m: {
        "encoding": "ordinal", "columns": ["era"], "cuts": [2006, 2013, 2020]})
    out = mc.add_era_columns(pl.DataFrame({"season": [2018, 2019, 2020]}), "xpass_model")
    assert out["era"].to_list() == [2, 2, 2]


def test_one_hot_era_produces_all_four_columns(monkeypatch):
    monkeypatch.setattr(mc, "card_era_contract", lambda m: {
        "encoding": "one_hot", "columns": ["era0", "era1", "era2", "era3"],
        "cuts": [2006, 2013, 2020]})
    out = mc.add_era_columns(pl.DataFrame({"season": [2018]}), "fg_model")
    assert out.select(["era0", "era1", "era2", "era3"]).row(0) == (0, 0, 1, 0)


def test_a_model_with_no_era_contract_is_left_untouched(monkeypatch):
    monkeypatch.setattr(mc, "card_era_contract", lambda m: None)
    df = pl.DataFrame({"season": [2018], "yards_to_goal": [30.0]})
    assert mc.add_era_columns(df, "ep_model").columns == df.columns


def test_an_existing_era_column_is_not_overwritten(monkeypatch):
    """A pbp frame already carries era; recomputing it would fight the pipeline."""
    monkeypatch.setattr(mc, "card_era_contract", lambda m: {
        "encoding": "ordinal", "columns": ["era"], "cuts": [2006, 2013, 2020]})
    df = pl.DataFrame({"season": [2018], "era": [99]})
    assert mc.add_era_columns(df, "xpass_model")["era"].to_list() == [99]


def test_a_hand_built_row_can_supply_the_season_argument(monkeypatch):
    """The hypothetical case: no season column, season passed explicitly."""
    monkeypatch.setattr(mc, "card_era_contract", lambda m: {
        "encoding": "ordinal", "columns": ["era"], "cuts": [2006, 2013, 2020]})
    out = mc.add_era_columns(pl.DataFrame({"yards_to_goal": [30.0]}), "xpass_model", season=2018)
    assert out["era"].to_list() == [2]


def test_no_season_at_all_is_a_clear_error(monkeypatch):
    monkeypatch.setattr(mc, "card_era_contract", lambda m: {
        "encoding": "ordinal", "columns": ["era"], "cuts": [2006, 2013, 2020]})
    with pytest.raises(ValueError, match="season"):
        mc.add_era_columns(pl.DataFrame({"yards_to_goal": [30.0]}), "xpass_model")
