"""Tests for the NFL-draft outcome loader + projection (T2.2 Phase 5)."""

from __future__ import annotations

import importlib

import polars as pl

_mod = importlib.import_module("sportsdataverse.cfb.cfb_draft_projection")
from sportsdataverse.cfb.cfb_draft_projection import load_draft_outcomes


def _fake_picks(**kwargs) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "season": [2023, 2023, 2010],
            "round": [1, 1, 2],
            "pick": [1, 2, 40],
            "pfr_player_name": ["Bryce Young", "C.J. Stroud", "Old Guy"],
            "college": ["Alabama", "Ohio St.", "Nowhere"],
            "cfb_player_id": ["4685720", None, None],
            "position": ["QB", "QB", "RB"],
        }
    )


def test_draft_outcomes_contract(monkeypatch) -> None:
    monkeypatch.setattr(_mod, "load_nfl_draft_picks", _fake_picks)
    out = load_draft_outcomes(2023)
    assert out.height == 2  # 2010 filtered out
    assert out.schema["draft_year"] == pl.Int64
    assert out.schema["player_id"] == pl.Utf8
    assert out.schema["round"] == pl.Int64 and out.schema["pick"] == pl.Int64
    row = out.row(0, named=True)
    assert row["player_name"] == "Bryce Young" and row["college"] == "Alabama"
    assert row["player_id"] == "4685720"


def test_draft_outcomes_multi_year_and_empty(monkeypatch) -> None:
    monkeypatch.setattr(_mod, "load_nfl_draft_picks", _fake_picks)
    both = load_draft_outcomes([2010, 2023])
    assert set(both["draft_year"].unique().to_list()) == {2010, 2023}
    monkeypatch.setattr(_mod, "load_nfl_draft_picks", lambda **k: pl.DataFrame())
    empty = load_draft_outcomes(2023)
    assert empty.height == 0
    for col in ("draft_year", "college", "player_id", "player_name", "round", "pick"):
        assert col in empty.columns


def _synth_player_years() -> pl.DataFrame:
    # 40 players/year x 6 draft years; drafted iff stars >= 4 (separable)
    import itertools

    rows = []
    for year, i in itertools.product(range(2018, 2024), range(40)):
        stars = 5 if i < 4 else (4 if i < 10 else 3 if i < 30 else 2)
        rows.append(
            {
                "draft_year": year,
                "team_id": "T" + str(i % 8),
                "player_id": f"{year}-{i}",
                "player_name": f"Player {year} {i}",
                "recruit_stars": float(stars),
                "talent_points": {5: 100.0, 4: 70.0, 3: 45.0, 2: 25.0}[stars],
                "career_production_z": (stars - 3) * 0.8,
                "class_year": float(i % 4 + 1),
                "drafted": 1 if stars >= 4 else 0,
            }
        )
    return pl.DataFrame(rows)


def test_draft_projection_monotone_and_boundary(monkeypatch) -> None:
    from sportsdataverse.cfb.cfb_projection_constants import roc_auc
    from sportsdataverse.cfb.cfb_draft_projection import cfb_draft_projection

    frame = _synth_player_years()
    monkeypatch.setattr(_mod, "_player_feature_frame", lambda years, division: frame)
    out = cfb_draft_projection(2023)
    players = out["players"]
    assert set(players["draft_year"].unique().to_list()) == {2023}
    # monotone in stars
    by_stars = (
        players.join(frame.select("player_id", "recruit_stars"), on="player_id", how="left", suffix="_f")
        .group_by("recruit_stars")
        .agg(pl.col("draft_prob").mean())
        .sort("recruit_stars")
    )
    probs = by_stars["draft_prob"].to_list()
    assert probs == sorted(probs), f"draft_prob not monotone in stars: {probs}"
    # separable synthetic -> high AUC on the held-out year
    labels = frame.filter(pl.col("draft_year") == 2023)
    j = players.join(labels.select("player_id", "drafted"), on="player_id", how="inner")
    auc = roc_auc(j["drafted"].to_numpy(), j["draft_prob"].to_numpy())
    assert auc >= 0.95, f"held-out AUC {auc}"
    # team roll-up sums player probs
    teams = out["teams"]
    assert abs(teams["proj_draft_picks"].sum() - players["draft_prob"].sum()) < 1e-6


def test_draft_projection_empty(monkeypatch) -> None:
    from sportsdataverse.cfb.cfb_draft_projection import cfb_draft_projection

    empty = _synth_player_years().clear()
    monkeypatch.setattr(_mod, "_player_feature_frame", lambda years, division: empty)
    out = cfb_draft_projection(2023)
    assert out["players"].height == 0 and out["teams"].height == 0
