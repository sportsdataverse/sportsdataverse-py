from __future__ import annotations

import importlib

import polars as pl
import pytest


@pytest.fixture
def synthetic_combine(monkeypatch: pytest.MonkeyPatch) -> None:
    mod = importlib.import_module("sportsdataverse.nba.nba_draft_model")

    anthro = pl.DataFrame(
        {
            "player_id": [1, 2, 3],
            "height_wo_shoes": [84.0, 78.0, 74.0],
            "weight": [250.0, 210.0, 180.0],
            "wingspan": [90.0, 82.0, 76.0],
            "standing_reach": [110.0, 102.0, 96.0],
            "body_fat_pct": [8.0, 7.0, 6.0],
            "hand_length": [9.5, 9.0, 8.0],
            "hand_width": [10.5, 10.0, 9.0],
        }
    )
    empty = pl.DataFrame({"player_id": []}, schema={"player_id": pl.Utf8})

    monkeypatch.setattr(mod, "nba_stats_draftcombineplayeranthro", lambda season_year: anthro)
    monkeypatch.setattr(mod, "nba_stats_draftcombinedrillresults", lambda season_year: empty)
    monkeypatch.setattr(mod, "nba_stats_draftcombinespotshooting", lambda season_year: empty)
    monkeypatch.setattr(mod, "nba_stats_draftcombinenonstationaryshooting", lambda season_year: empty)

    # Hand-written artifact: only `height_wo_shoes` and `wingspan_diff` drive
    # the score, coefficients hand-verifiable.
    art = {
        "league": "nba",
        "features": ["height_wo_shoes", "wingspan_diff"],
        "value_coef": [1.0, 2.0],
        "value_intercept": 0.0,
        "prob_coef": [0.1, 0.1],
        "prob_intercept": -5.0,
        "feature_median": {"height_wo_shoes": 78.0, "wingspan_diff": 4.0},
    }
    monkeypatch.setattr(mod, "_load_artifact", lambda league: art)


def test_draft_model_scores_and_ranks(synthetic_combine: None) -> None:
    from sportsdataverse.nba.nba_draft_model import nba_draft_model

    out = nba_draft_model(2019)
    assert out.schema["player_id"] == pl.Utf8
    assert out.schema["draft_year"] == pl.Int64
    assert out.schema["proj_career_value"] == pl.Float64
    assert out.schema["draft_prob"] == pl.Float64
    assert out.schema["projected_pick"] == pl.Int64
    assert out.schema["pro_tier"] == pl.Utf8
    assert out.height == 3

    # player 1 (84in height, 6in wingspan_diff) scores highest -> pick 1
    top = out.sort("projected_pick").row(0, named=True)
    assert top["player_id"] == "1"
    assert set(out["projected_pick"].to_list()) == {1, 2, 3}


def test_draft_model_return_as_pandas(synthetic_combine: None) -> None:
    from sportsdataverse.nba.nba_draft_model import nba_draft_model

    out = nba_draft_model(2019, return_as_pandas=True)
    import pandas as pd

    assert isinstance(out, pd.DataFrame)


def test_draft_model_empty_years_returns_schema() -> None:
    from sportsdataverse.nba.nba_draft_model import _SCHEMA, nba_draft_model

    out = nba_draft_model([])
    assert out.height == 0
    assert list(out.schema.keys()) == list(_SCHEMA.keys())


def test_bundled_nba_draft_value_artifact_loads() -> None:
    from sportsdataverse.nba.nba_draft_model import _load_artifact

    art = _load_artifact("nba")
    assert "features" in art and "value_coef" in art and "prob_coef" in art
    assert len(art["features"]) == len(art["value_coef"]) == len(art["prob_coef"])
