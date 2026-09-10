"""The model card is the contract; nothing may restate it.

Six hardcoded feature lists in this package duplicate a published card's
``features`` array. That duplication is the same class of bug as
cfbfastR-cfb-data#70, where both consumers kept private copies of the era cuts
and drifted to a 2017 boundary the trainer never used. These accessors are what
replaces them.
"""

from __future__ import annotations

import pytest

from sportsdataverse.cfb import model_cards


@pytest.fixture(autouse=True)
def _clear_cache():
    model_cards.load_model_card.cache_clear()
    yield
    model_cards.load_model_card.cache_clear()


def _stub(monkeypatch, payload):
    monkeypatch.setattr(model_cards, "_read_card_json", lambda model: payload)


def test_features_come_back_in_the_cards_declared_order(monkeypatch):
    """Feature ORDER is load-bearing for XGBoost -- a sorted or re-derived list
    silently mis-aligns the DMatrix and the model scores garbage."""
    feats = ["TimeSecsRem", "yards_to_goal", "distance", "down_1"]
    _stub(monkeypatch, {"features": feats, "model_type": "ep"})
    assert model_cards.card_features("ep_model") == feats


def test_era_contract_is_returned_when_present(monkeypatch):
    contract = {
        "encoding": "one_hot",
        "columns": ["era0", "era1", "era2", "era3"],
        "cuts": [2006, 2013, 2020],
    }
    _stub(monkeypatch, {"features": ["yards_to_goal"], "era_contract": contract})
    assert model_cards.card_era_contract("fg_model") == contract


def test_a_model_with_no_era_feature_returns_none(monkeypatch):
    """None is CORRECT for ep_model, wp_naive, wp_spread and cfb_cp_model --
    they have no era feature. Inventing a contract would tell callers otherwise."""
    _stub(monkeypatch, {"features": ["TimeSecsRem"], "era_contract": None})
    assert model_cards.card_era_contract("ep_model") is None


def test_a_card_without_features_is_rejected(monkeypatch):
    """A card with no features validates nothing -- fail loudly, not silently."""
    _stub(monkeypatch, {"model_type": "ep"})
    with pytest.raises(ValueError, match="no features"):
        model_cards.card_features("ep_model")


def test_a_missing_card_names_the_path(monkeypatch):
    monkeypatch.setattr(model_cards, "_MODEL_DIR", model_cards._MODEL_DIR / "nope")
    with pytest.raises(FileNotFoundError, match="no published card"):
        model_cards.load_model_card("ep_model")


def test_the_card_is_read_once_and_cached(monkeypatch):
    calls = []

    def counting(model):
        calls.append(model)
        return {"features": ["a"]}

    monkeypatch.setattr(model_cards, "_read_card_json", counting)
    model_cards.card_features("ep_model")
    model_cards.card_features("ep_model")
    assert len(calls) == 1, "card re-read; every calculator call would hit disk"
