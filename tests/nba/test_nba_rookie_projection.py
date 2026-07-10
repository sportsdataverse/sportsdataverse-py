from __future__ import annotations

import importlib

import polars as pl
import pytest


@pytest.fixture
def synthetic_pipeline(monkeypatch: pytest.MonkeyPatch) -> None:
    mod = importlib.import_module("sportsdataverse.nba.nba_rookie_projection")

    draft_board = pl.DataFrame(
        {
            "player_id": ["1", "2"],
            "draft_year": [2019, 2019],
            "proj_career_value": [200.0, 50.0],
            "draft_prob": [0.95, 0.6],
            "projected_pick": [1, 2],
            "pro_tier": ["lottery", "first_round"],
        }
    )
    aging_curve = pl.DataFrame(
        {"age": [19, 20, 27], "rel_value": [0.55, 0.62, 1.0]},
    )
    avail = pl.DataFrame({"player_id": ["1", "2"], "season": [2019, 2019], "avail_pct": [0.9, 0.8]})

    monkeypatch.setattr(mod, "nba_draft_model", lambda draft_year, **kw: draft_board)
    monkeypatch.setattr(mod, "nba_aging_curve", lambda **kw: aging_curve)
    monkeypatch.setattr(mod, "nba_availability", lambda seasons, **kw: avail)

    art = {"rookie_fraction": 0.1, "residual": {"lottery": 5.0, "first_round": 1.0}}
    monkeypatch.setattr(mod, "_load_residual_artifact", lambda league: art)


def test_rookie_projection_composes_and_orders(synthetic_pipeline: None) -> None:
    from sportsdataverse.nba.nba_rookie_projection import nba_rookie_projection

    out = nba_rookie_projection(2019)
    assert out.height == 2
    for col in ["proj_rookie_value", "proj_soph_value", "proj_rookie_min", "proj_avail_pct", "pro_tier"]:
        assert col in out.columns

    p1 = out.filter(pl.col("player_id") == "1").row(0, named=True)
    p2 = out.filter(pl.col("player_id") == "2").row(0, named=True)
    # higher proj_career_value -> higher proj_rookie_value
    assert p1["proj_rookie_value"] > p2["proj_rookie_value"]
    # availability reported separately, matches the (mocked) avail frame
    assert p1["proj_avail_pct"] == pytest.approx(0.9)
    assert p2["proj_avail_pct"] == pytest.approx(0.8)


def test_rookie_projection_empty_draft_year() -> None:
    from sportsdataverse.nba.nba_rookie_projection import _SCHEMA, nba_rookie_projection

    out = nba_rookie_projection([])
    assert out.height == 0
    assert list(out.schema.keys()) == list(_SCHEMA.keys())
