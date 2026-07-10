from __future__ import annotations

import importlib

import polars as pl
import pytest


def test_wnba_aging_curve_is_nba_core_bound_to_wnba() -> None:
    from sportsdataverse.wnba.wnba_aging_curve import wnba_aging_curve

    curve = wnba_aging_curve()
    assert curve.schema["age"] == pl.Int64
    assert curve.schema["rel_value"] == pl.Float64
    assert wnba_aging_curve.__wrapped_core__.keywords["league"] == "wnba"  # type: ignore[attr-defined]


def test_wnba_career_trajectory_matches_nba_core_shape() -> None:
    from sportsdataverse.wnba.wnba_aging_curve import wnba_career_trajectory

    player_values = pl.DataFrame({"player_id": ["1"], "age": [27], "value": [10.0]})
    out = wnba_career_trajectory(player_values)
    assert "age_adjusted_value" in out.columns
    assert "proj_next_value" in out.columns


def test_wnba_availability_is_nba_core_bound_to_wnba() -> None:
    from sportsdataverse.wnba.wnba_availability import wnba_availability

    assert wnba_availability.__wrapped_core__.keywords["league"] == "wnba"  # type: ignore[attr-defined]


def test_wnba_availability_empty_seasons_returns_schema() -> None:
    from sportsdataverse.wnba.wnba_availability import wnba_availability

    out = wnba_availability([])
    assert out.height == 0
    assert list(out.schema.keys()) == ["player_id", "season", "avail_pct"]


@pytest.fixture
def synthetic_drafthistory(monkeypatch: pytest.MonkeyPatch) -> None:
    mod = importlib.import_module("sportsdataverse.wnba.wnba_draft_model")
    history = pl.DataFrame(
        {
            "person_id": [1, 2, 3],
            "season": [2023, 2023, 2023],
            "overall_pick": [1, 5, 20],
            "round_number": [1, 1, 2],
        }
    )
    monkeypatch.setattr(mod, "wnba_stats_drafthistory", lambda: history)
    art = {
        "features": ["overall_pick", "round_number"],
        "value_coef": [-1.0, 0.0],
        "value_intercept": 100.0,
        "prob_coef": [-0.1, 0.0],
        "prob_intercept": 2.0,
        "feature_median": {"overall_pick": 18.0, "round_number": 2.0},
        "feature_mean": [18.0, 2.0],
        "feature_sd": [10.0, 1.0],
    }
    monkeypatch.setattr(mod, "_load_artifact", lambda: art)


def test_wnba_draft_model_scores_from_drafthistory(synthetic_drafthistory: None) -> None:
    from sportsdataverse.wnba.wnba_draft_model import wnba_draft_model

    out = wnba_draft_model(2023)
    assert out.height == 3
    assert out.schema["player_id"] == pl.Utf8
    assert out.schema["draft_year"] == pl.Int64
    # lower overall_pick (better) -> higher proj_career_value given value_coef < 0
    top = out.sort("projected_pick").row(0, named=True)
    assert top["player_id"] == "1"


def test_wnba_draft_model_empty_years_returns_schema() -> None:
    from sportsdataverse.wnba.wnba_draft_model import _SCHEMA, wnba_draft_model

    out = wnba_draft_model([])
    assert out.height == 0
    assert list(out.schema.keys()) == list(_SCHEMA.keys())


@pytest.fixture
def synthetic_wnba_pipeline(monkeypatch: pytest.MonkeyPatch) -> None:
    mod = importlib.import_module("sportsdataverse.wnba.wnba_rookie_projection")
    draft_board = pl.DataFrame(
        {
            "player_id": ["1", "2"],
            "draft_year": [2023, 2023],
            "proj_career_value": [150.0, 40.0],
            "draft_prob": [0.9, 0.5],
            "projected_pick": [1, 2],
            "pro_tier": ["lottery", "first_round"],
        }
    )
    curve = pl.DataFrame({"age": [22, 23, 27], "rel_value": [0.6, 0.65, 1.0]})
    avail = pl.DataFrame({"player_id": ["1", "2"], "season": [2023, 2023], "avail_pct": [0.85, 0.7]})
    monkeypatch.setattr(mod, "wnba_draft_model", lambda draft_year, **kw: draft_board)
    monkeypatch.setattr(mod, "wnba_aging_curve", lambda **kw: curve)
    monkeypatch.setattr(mod, "wnba_availability", lambda seasons, **kw: avail)
    art = {"rookie_fraction": 0.1, "residual": {"lottery": 2.0, "first_round": 0.5}}
    monkeypatch.setattr(mod, "_load_residual_artifact", lambda: art)


def test_wnba_rookie_projection_composes(synthetic_wnba_pipeline: None) -> None:
    from sportsdataverse.wnba.wnba_rookie_projection import wnba_rookie_projection

    out = wnba_rookie_projection(2023)
    assert out.height == 2
    for col in ["proj_rookie_value", "proj_soph_value", "proj_rookie_min", "proj_avail_pct", "pro_tier"]:
        assert col in out.columns
    p1 = out.filter(pl.col("player_id") == "1").row(0, named=True)
    p2 = out.filter(pl.col("player_id") == "2").row(0, named=True)
    assert p1["proj_rookie_value"] > p2["proj_rookie_value"]
