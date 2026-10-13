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


@pytest.mark.parametrize("model", ["fd_model", "fg_model", "qbr_model", "two_pt_model", "xpass_model"])
def test_the_catalogued_era_cuts_are_the_bundled_cards_cuts(model):
    """The QBR / fourth-down / two-point era dummies read cfb/model_vars.py CFB_MODEL_ERA_CUTS (task 3.11);
    each era-aware model was trained on its card's ``era.cuts``, so the two must agree."""
    from sportsdataverse.cfb.model_vars import CFB_MODEL_ERA_CUTS

    assert tuple(model_cards.card_era_contract(model)["cuts"]) == CFB_MODEL_ERA_CUTS


# --- task 3.8 (C-M11, C-L28): holdout status + importance artifact per bundled booster ------------
_BOOSTERS = sorted(p.stem for p in model_cards._MODEL_DIR.glob("*.ubj"))


def _importance_tool():
    import importlib.util
    from pathlib import Path

    path = Path(__file__).resolve().parents[2] / "tools" / "cfb_model_importance.py"
    spec = importlib.util.spec_from_file_location("cfb_model_importance", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_every_bundled_booster_ships_a_card_and_an_importance_artifact():
    assert len(_BOOSTERS) >= 11
    for stem in _BOOSTERS:
        assert (model_cards._MODEL_DIR / f"{stem}.card.json").exists(), stem
        assert (model_cards._MODEL_DIR / f"{stem}.importance.json").exists(), stem


@pytest.mark.parametrize("model", _BOOSTERS)
def test_every_card_states_its_holdout_status_truthfully(model):
    """A card names a holdout disjoint from (after) its training span, or says in_sample and why --
    never both, never neither: a score inside the training span must not read as validation."""
    card = model_cards.load_model_card(model)
    has_holdout = "holdout_seasons" in card
    assert has_holdout != (card.get("in_sample") is True), model
    if has_holdout:
        lo, hi = card["holdout_seasons"]
        assert lo <= hi and card["training_seasons"][1] < lo, model
    else:
        assert len(card.get("in_sample_reason") or "") > 40, model


@pytest.mark.parametrize("model", _BOOSTERS)
def test_the_importance_artifact_is_the_bundled_boosters_own(model):
    """Recomputed from the shipped bytes: a swapped booster without a regenerated artifact, or a
    hand-edited number, fails. Feature order must be the card's."""
    import json

    committed = json.loads((model_cards._MODEL_DIR / f"{model}.importance.json").read_text(encoding="utf-8"))
    fresh = _importance_tool().importance(model_cards._MODEL_DIR / f"{model}.ubj")
    assert committed["sha256"] == fresh["sha256"]
    assert [f["feature"] for f in committed["features"]] == model_cards.card_features(model)
    assert [f["feature"] for f in committed["features"]] == [f["feature"] for f in fresh["features"]]
    for c, f in zip(committed["features"], fresh["features"]):
        for key in ("gain", "total_gain", "weight", "cover", "share_total_gain"):
            assert c[key] == pytest.approx(f[key], rel=1e-9, abs=1e-12), (c["feature"], key)


@pytest.mark.parametrize("model", _BOOSTERS)
def test_week_and_label_spans_are_ordered_pairs_that_agree_with_the_card(model):
    """``holdout_weeks`` / ``label_seasons`` are [lo, hi] pairs; the weeks match ``holdout_scope``, and
    ``label_seasons`` sits inside the training span and matches ``label_rows_by_season`` (review R847-7/8)."""
    card = model_cards.load_model_card(model)
    for key in ("holdout_weeks", "label_seasons"):
        if key in card:
            lo, hi = card[key]
            assert lo <= hi, (model, key)
    if "holdout_weeks" in card:
        lo, hi = card["holdout_weeks"]
        assert f"weeks {lo}-{hi}" in card["holdout_scope"], model
    if "label_seasons" in card:
        lo, hi = card["label_seasons"]
        t_lo, t_hi = card["training_seasons"]
        assert t_lo <= lo and hi <= t_hi, model
        by_season = {int(s): n for s, n in card["label_rows_by_season"].items()}
        assert (min(by_season), max(by_season)) == (lo, hi), model
        assert sum(by_season.values()) == card["n_training_rows"], model
        assert "extrapolation" in card["in_sample_reason"], model
