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
    monkeypatch.setattr(
        mc, "card_era_contract", lambda m: {"encoding": "ordinal", "columns": ["era"], "cuts": [2006, 2013, 2020]}
    )
    out = mc.add_era_columns(pl.DataFrame({"season": [2005, 2010, 2018, 2024]}), "xpass_model")
    assert out["era"].to_list() == [0, 1, 2, 3]


def test_2018_through_2020_land_in_bucket_2_not_3(monkeypatch):
    """The exact regression from cfbfastR-cfb-data#70, pinned."""
    monkeypatch.setattr(
        mc, "card_era_contract", lambda m: {"encoding": "ordinal", "columns": ["era"], "cuts": [2006, 2013, 2020]}
    )
    out = mc.add_era_columns(pl.DataFrame({"season": [2018, 2019, 2020]}), "xpass_model")
    assert out["era"].to_list() == [2, 2, 2]


def test_one_hot_era_produces_all_four_columns(monkeypatch):
    monkeypatch.setattr(
        mc,
        "card_era_contract",
        lambda m: {"encoding": "one_hot", "columns": ["era0", "era1", "era2", "era3"], "cuts": [2006, 2013, 2020]},
    )
    out = mc.add_era_columns(pl.DataFrame({"season": [2018]}), "fg_model")
    assert out.select(["era0", "era1", "era2", "era3"]).row(0) == (0, 0, 1, 0)


def test_a_model_with_no_era_contract_is_left_untouched(monkeypatch):
    monkeypatch.setattr(mc, "card_era_contract", lambda m: None)
    df = pl.DataFrame({"season": [2018], "yards_to_goal": [30.0]})
    assert mc.add_era_columns(df, "ep_model").columns == df.columns


def test_an_existing_era_column_is_not_overwritten(monkeypatch):
    """A pbp frame already carries era; recomputing it would fight the pipeline."""
    monkeypatch.setattr(
        mc, "card_era_contract", lambda m: {"encoding": "ordinal", "columns": ["era"], "cuts": [2006, 2013, 2020]}
    )
    df = pl.DataFrame({"season": [2018], "era": [99]})
    assert mc.add_era_columns(df, "xpass_model")["era"].to_list() == [99]


def test_a_hand_built_row_can_supply_the_season_argument(monkeypatch):
    """The hypothetical case: no season column, season passed explicitly."""
    monkeypatch.setattr(
        mc, "card_era_contract", lambda m: {"encoding": "ordinal", "columns": ["era"], "cuts": [2006, 2013, 2020]}
    )
    out = mc.add_era_columns(pl.DataFrame({"yards_to_goal": [30.0]}), "xpass_model", season=2018)
    assert out["era"].to_list() == [2]


def test_no_season_at_all_is_a_clear_error(monkeypatch):
    monkeypatch.setattr(
        mc, "card_era_contract", lambda m: {"encoding": "ordinal", "columns": ["era"], "cuts": [2006, 2013, 2020]}
    )
    with pytest.raises(ValueError, match="season"):
        mc.add_era_columns(pl.DataFrame({"yards_to_goal": [30.0]}), "xpass_model")


# ---------------------------------------------------------------------------
# The ten public calculators, against the real packaged models.
# ---------------------------------------------------------------------------


def test_xpass_returns_the_frame_plus_one_probability_column():
    """The contract: return the caller's frame with model output appended, so a
    hand-built row and a pbp frame behave identically."""
    df = pl.DataFrame(
        {
            "season": [2024],
            "down": [3.0],
            "distance": [8.0],
            "yards_to_goal": [55.0],
            "pos_score_diff": [-4.0],
            "TimeSecsRem": [900.0],
            "period": [3.0],
        }
    )
    out = mc.calculate_xpass(df)
    assert out.height == 1
    assert 0.0 <= out["xpass"][0] <= 1.0
    for c in df.columns:
        assert c in out.columns, f"calculator dropped the caller's column {c}"


def test_a_hand_built_row_scores_without_any_pbp_machinery():
    """The hypothetical case: someone types a situation and asks the model."""
    out = mc.calculate_field_goal_probability(pl.DataFrame({"season": [2024], "yards_to_goal": [25.0]}))
    assert 0.0 <= out["fg_prob"][0] <= 1.0


def test_field_goal_probability_moves_with_the_era():
    """The era one-hot must actually reach the model. Kickers improved over the
    covered seasons, so a fixed distance should not score identically in 2005
    and 2024 -- if it does, the era columns are being ignored."""

    def fg(year):
        return mc.calculate_field_goal_probability(pl.DataFrame({"season": [year], "yards_to_goal": [25.0]}))[
            "fg_prob"
        ][0]

    assert fg(2005) != fg(2024)
    assert fg(2005) < fg(2024)


def test_a_missing_column_names_what_is_absent():
    with pytest.raises(ValueError, match="yards_to_goal"):
        mc.calculate_field_goal_probability(pl.DataFrame({"season": [2024]}))


def test_return_as_pandas_is_honoured():
    import pandas as pd

    out = mc.calculate_field_goal_probability(
        pl.DataFrame({"season": [2024], "yards_to_goal": [25.0]}), return_as_pandas=True
    )
    assert isinstance(out, pd.DataFrame)


def test_expected_points_emits_class_probabilities_and_a_points_expectation():
    """EP is multi:softprob over seven next-score classes; the probabilities and
    the collapsed expectation must both surface, and the classes must sum to 1."""
    out = mc.calculate_expected_points(
        pl.DataFrame(
            {
                "TimeSecsRem": [1800.0],
                "yards_to_goal": [75.0],
                "distance": [10.0],
                "down_1": [1],
                "down_2": [0],
                "down_3": [0],
                "down_4": [0],
                "pos_score_diff_start": [0.0],
            }
        )
    )
    classes = ["td_prob", "opp_td_prob", "fg_prob", "opp_fg_prob", "safety_prob", "opp_safety_prob", "no_score_prob"]
    for c in classes:
        assert c in out.columns
    total = sum(out[c][0] for c in classes)
    assert total == pytest.approx(1.0, abs=1e-4), f"class probabilities sum to {total}"
    assert -10.0 <= out["ep"][0] <= 10.0


def test_win_probability_picks_the_spread_model_when_a_spread_is_present(monkeypatch):
    """wp_naive is wp_spread minus spread_time, so the presence of that column
    is what decides which contract applies."""
    seen = []
    monkeypatch.setattr(mc, "_calculate", lambda df, model, out, **kw: seen.append(model) or df)
    mc.calculate_win_probability(pl.DataFrame({"spread_time": [1.0]}))
    mc.calculate_win_probability(pl.DataFrame({"down": [1.0]}))
    assert seen == ["wp_spread", "wp_naive"]


def test_epa_requires_the_after_play_value():
    """EPA is a difference; this scores rows, not sequences. Silently inventing
    ep_end would produce a number that looks like EPA and is not."""
    with pytest.raises(ValueError, match="ep_end"):
        mc.calculate_epa(pl.DataFrame({"yards_to_goal": [75.0]}))


def test_wpa_requires_the_after_play_value():
    with pytest.raises(ValueError, match="wp_end"):
        mc.calculate_wpa(pl.DataFrame({"down": [1.0]}))


def test_epa_is_the_difference_and_reuses_an_existing_ep():
    """An ep already present must not be recomputed -- that would fight a pbp
    frame whose ep came from the pipeline."""
    df = pl.DataFrame({"ep": [2.0], "ep_end": [5.0]})
    assert mc.calculate_epa(df)["epa"].to_list() == [3.0]


def test_every_calculator_preserves_input_columns():
    """Chaining two calculators must be lossless."""
    df = pl.DataFrame({"season": [2024], "yards_to_goal": [25.0], "marker": ["keep"]})
    out = mc.calculate_field_goal_probability(df)
    assert out["marker"].to_list() == ["keep"]


# ---------------------------------------------------------------------------
# No feature list may drift from the card it restates.
# ---------------------------------------------------------------------------

CARD_BACKED = [
    ("ep_model", "sportsdataverse.cfb.cfb_fourth_down", "EP_FEATURES"),
    ("wp_spread", "sportsdataverse.cfb.cfb_fourth_down", "WP_SPREAD_FEATURES"),
    ("fd_model", "sportsdataverse.cfb.cfb_fourth_down", "FD_FEATURES"),
    ("two_pt_model", "sportsdataverse.cfb.cfb_two_point", "TWO_PT_FEATURES"),
    ("cfb_cp_model", "sportsdataverse.cfb.cfb_pbp", "CP_FEATURES"),
    ("xpass_model", "sportsdataverse.cfb.cfb_pbp", "XPASS_FEATURES"),
]


@pytest.mark.parametrize("model,module,const", CARD_BACKED)
def test_no_feature_list_drifts_from_its_card(model, module, const):
    """Every feature-list constant must equal its card, in order.

    These six restated what the cards publish. Left unpinned they drift -- which
    is exactly how cfbfastR-cfb-data#70 happened one layer down, with the era
    cuts. This fails the moment a retrain changes a model's feature set.
    """
    import importlib

    from sportsdataverse.cfb.model_cards import card_features

    local = list(getattr(importlib.import_module(module), const))
    assert local == card_features(model), f"{const} drifted from {model}'s card"


def test_the_air_yards_superset_still_composes_from_the_card():
    """CP_AIR_YARDS_FEATURES is CP_FEATURES plus three throw-depth columns.
    Making CP_FEATURES card-backed must not break that composition."""
    from sportsdataverse.cfb.cfb_pbp import CP_AIR_YARDS_FEATURES, CP_FEATURES

    assert CP_AIR_YARDS_FEATURES[: len(CP_FEATURES)] == CP_FEATURES
    assert set(CP_AIR_YARDS_FEATURES) - set(CP_FEATURES) == {"air_yards", "pass_is_middle", "qb_hurry"}


# ---------------------------------------------------------------------------
# Regressions found in review of PR #475.
# ---------------------------------------------------------------------------


def test_a_real_pbp_frame_scores_without_pre_renaming():
    """A pbp frame carries start.TimeSecsRem / start.yardsToEndzone; the cards
    declare TimeSecsRem / yards_to_goal. Before normalization the calculators
    only accepted already-renamed frames -- which is not what a caller holding
    play-by-play has, and the PR claimed otherwise."""
    pbp = pl.DataFrame(
        {
            "season": [2024],
            "start.TimeSecsRem": [900.0],
            "start.yardsToEndzone": [55.0],
            "start.distance": [8.0],
            "start.down": [3.0],
            "pos_score_diff_start": [-4.0],
            "period": [3.0],
        }
    )
    out = mc.calculate_xpass(pbp)
    assert 0.0 <= out["xpass"][0] <= 1.0
    # copies, never renames: the caller's own columns must survive
    assert "start.TimeSecsRem" in out.columns


def test_normalization_does_not_clobber_an_already_named_column():
    """A hand-built frame using card names must pass through untouched."""
    df = pl.DataFrame(
        {
            "TimeSecsRem": [111.0],
            "start.TimeSecsRem": [999.0],
            "down": [1.0],
            "distance": [10.0],
            "yards_to_goal": [50.0],
            "pos_score_diff": [0.0],
            "period": [1.0],
            "season": [2024],
        }
    )
    out = mc.normalize_pbp_columns(df, "xpass_model")
    assert out["TimeSecsRem"].to_list() == [111.0]


def test_the_frames_own_season_beats_the_season_argument():
    """`season=` is documented as the fallback for a frame with no season column.
    Letting it override stamped one era across a multi-season frame and scored
    most rows against the wrong inputs -- silently, since no column was missing.
    """
    multi = pl.DataFrame({"season": [2005, 2024], "yards_to_goal": [25.0, 25.0]})
    out = mc.calculate_field_goal_probability(multi, season=2024)
    vals = out["fg_prob"].to_list()
    assert vals[0] != vals[1], "one era was stamped across both rows"
    # and the per-row eras must match what each season maps to
    eras = mc.add_era_columns(multi, "fg_model", season=2024)
    assert eras.select(["era0", "era1", "era2", "era3"]).row(0) == (1, 0, 0, 0)
    assert eras.select(["era0", "era1", "era2", "era3"]).row(1) == (0, 0, 0, 1)


def test_expected_points_scores_a_raw_pbp_frame():
    """EP is trained on one-hot downs, so aliasing start.down -> down left a raw
    pbp frame four features short. The first pbp fix normalized names but not
    this derivation, and calculate_expected_points still raised."""
    pbp = pl.DataFrame(
        {
            "season": [2024],
            "start.TimeSecsRem": [900.0],
            "start.yardsToEndzone": [75.0],
            "start.distance": [10.0],
            "start.down": [1.0],
            "pos_score_diff_start": [0.0],
        }
    )
    out = mc.calculate_expected_points(pbp)
    assert -10.0 <= out["ep"][0] <= 10.0
    assert [out[c][0] for c in ("down_1", "down_2", "down_3", "down_4")] == [1, 0, 0, 0]


def test_down_one_hots_are_derived_per_down():
    pbp = pl.DataFrame({"start.down": [1.0, 2.0, 3.0, 4.0]})
    out = mc.normalize_pbp_columns(pbp, "ep_model")
    assert out["down_1"].to_list() == [1, 0, 0, 0]
    assert out["down_4"].to_list() == [0, 0, 0, 1]


def test_caller_supplied_down_one_hots_are_never_overwritten():
    """A pbp frame may already carry them from the pipeline."""
    df = pl.DataFrame(
        {
            "start.down": [1.0],
            "down_1": [0],
            "down_2": [1],
            "down_3": [0],
            "down_4": [0],
        }
    )
    out = mc.normalize_pbp_columns(df, "ep_model")
    assert out["down_1"].to_list() == [0]
    assert out["down_2"].to_list() == [1]
