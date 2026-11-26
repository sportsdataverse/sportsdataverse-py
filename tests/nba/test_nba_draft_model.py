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


def test_draft_model_gleague_bridge_empty_frame_no_crash(
    synthetic_combine: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """gleague_bridge=True with an empty G-League frame -> full schema, no crash.

    Uses ``importlib.import_module`` (not ``import ... as``) -- this package's
    convention of a module named identically to its public function
    (``nba_draft_model.py``'s ``nba_draft_model``) means the package
    ``__init__.py``'s ``from .nba_draft_model import nba_draft_model``
    overwrites the *package attribute* ``sportsdataverse.nba.nba_draft_model``
    with the function; a plain ``import sportsdataverse.nba.nba_draft_model
    as mod`` statement resolves through that shadowed attribute and silently
    binds ``mod`` to the function instead of the module.
    """
    mod = importlib.import_module("sportsdataverse.nba.nba_draft_model")

    monkeypatch.setattr(
        mod,
        "nba_stats_leaguedashplayerstats",
        lambda season, league_id=None: pl.DataFrame(),
    )
    out = mod.nba_draft_model(2019, gleague_bridge=True)
    assert out.height == 3
    assert list(out.schema.keys()) == list(mod._SCHEMA.keys())


# The columns each combine wrapper returns, under the wrapper's own names (native nba_stats schemas).
_SHARED = ["temp_player_id", "first_name", "last_name", "player_name", "position"]
_WRAPPER_COLS = {
    "nba_stats_draftcombineplayeranthro": {
        c: c
        for c in [
            "height_wo_shoes",
            "weight",
            "wingspan",
            "standing_reach",
            "body_fat_pct",
            "hand_length",
            "hand_width",
        ]
    },
    "nba_stats_draftcombinedrillresults": {
        "lane_agility": "lane_agility_time",
        "three_quarter_sprint": "three_quarter_sprint",
        "standing_vertical": "standing_vertical_leap",
        "max_vertical": "max_vertical_leap",
    },
    "nba_stats_draftcombinespotshooting": {"spot_fifteen_corner_left_pct": "fifteen_corner_left_pct"},
    "nba_stats_draftcombinenonstationaryshooting": {"offdrib_fifteen_top_pct": "off_drib_fifteen_top_key_pct"},
}


def test_live_path_imputes_training_medians_on_real_holdout(monkeypatch: pytest.MonkeyPatch) -> None:
    """The live path (wrappers -> ``build_combine_features`` -> ``_score``) must score the
    2016-2019 holdout exactly as ``_score`` does on the joined fixture.

    Each stand-in wrapper returns the fixture's real values under the wrapper's column names,
    plus the name columns all four share. Combine drills and shooting are 56-92% missing; they
    must reach ``_score`` as null so the artifact's training medians fill them. A zero-fill in
    the feature builder ranked the holdout at 0.45 Spearman against this reference and moved
    ``pro_tier`` for half the prospects.
    """
    from sportsdataverse.nba.nba_draft_constants import COMBINE_FEATURES

    mod = importlib.import_module("sportsdataverse.nba.nba_draft_model")
    combine = pl.read_parquet("tests/fixtures/nba_draft/combine_2016_2019.parquet").filter(pl.col("draft_year") >= 2016)

    def wrapper(cols: dict):
        return lambda season_year: combine.filter(pl.col("draft_year") == int(season_year)).select(
            "player_id",
            *[pl.lit("x").alias(c) for c in _SHARED],
            *[pl.col(model).alias(native) for model, native in cols.items()],
        )

    for name, cols in _WRAPPER_COLS.items():
        monkeypatch.setattr(mod, name, wrapper(cols))

    raw = [c for c in COMBINE_FEATURES if c not in ("bmi", "wingspan_diff")]
    reference = mod._score(
        combine.with_columns([pl.col(c).cast(pl.Float64, strict=False) for c in raw]).with_columns(
            (pl.col("weight") / (pl.col("height_wo_shoes") ** 2) * 703.0).alias("bmi"),
            (pl.col("wingspan") - pl.col("height_wo_shoes")).alias("wingspan_diff"),
        ),
        mod._load_artifact("nba"),
    )
    live = mod.nba_draft_model([2016, 2017, 2018, 2019])

    assert live.height == reference.height == 248
    both = live.join(reference, on=["player_id", "draft_year"], how="inner", suffix="_ref")
    assert both.height == 248
    assert (both["proj_career_value"] - both["proj_career_value_ref"]).abs().max() < 1e-9
    assert (both["draft_prob"] - both["draft_prob_ref"]).abs().max() < 1e-12
    assert (both["pro_tier"] == both["pro_tier_ref"]).all()


def test_live_path_reads_real_wrapper_payloads(monkeypatch: pytest.MonkeyPatch) -> None:
    """Captured stats.nba.com combine payloads, parsed by the real wrappers, reach ``_score``
    with every measurement under its model name and the source values intact."""
    import json
    from pathlib import Path

    from sportsdataverse.nba.nba_stats import parse_nba_stats_result_sets

    fixtures = Path("tests/fixtures/nba_stats/endpoints")
    stats = importlib.import_module("sportsdataverse.nba.nba_stats")
    mod = importlib.import_module("sportsdataverse.nba.nba_draft_model")
    monkeypatch.setattr(
        stats, "_get", lambda url, **_: json.loads((fixtures / f"{url.rsplit('/', 1)[1]}.json").read_text("utf-8"))
    )
    seen: dict = {}
    real_score = mod._score

    def spy(feats: pl.DataFrame, art: dict) -> pl.DataFrame:
        seen["feats"] = feats
        return real_score(feats, art)

    monkeypatch.setattr(mod, "_score", spy)

    out = mod.nba_draft_model(2023)
    assert out.height > 0 and out["proj_career_value"].is_finite().all()

    feats = seen["feats"]
    renamed_with_values: set = set()
    for name, cols in _WRAPPER_COLS.items():
        src = parse_nba_stats_result_sets(
            json.loads((fixtures / f"{name.removeprefix('nba_stats_')}.json").read_text("utf-8"))
        ).select(
            pl.col("player_id").cast(pl.Int64).cast(pl.Utf8),
            *[pl.col(native).cast(pl.Float64, strict=False).alias(f"src_{native}") for native in cols.values()],
        )
        got = feats.join(src, on="player_id", how="inner")
        assert got.height > 0, name
        for model, native in cols.items():
            want = got[f"src_{native}"]
            if model != native and want.is_not_null().any():
                renamed_with_values.add(model)
            assert (got[model].is_null() == want.is_null()).all(), (name, model)
            assert ((got[model] - want).abs().fill_null(0.0) < 1e-9).all(), (name, model)
    # the capture has values under renamed wrapper columns, so a lost rename fails the null check above
    assert {"lane_agility", "standing_vertical", "max_vertical"} <= renamed_with_values
