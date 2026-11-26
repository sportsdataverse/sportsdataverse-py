"""``nba_draft_model``: schema, the release's season stamp, league guard, and the
pre-draft college / EuroLeague columns replayed from captured 2019 slices."""

from __future__ import annotations

import importlib

import polars as pl
import pytest

FX = "tests/fixtures/nba_draft/pre_draft_2019"


@pytest.fixture
def draft_2019(monkeypatch: pytest.MonkeyPatch):
    """The 2019 class as the nba_stats_draft release publishes it (season stamped 2020)."""
    mod = importlib.import_module("sportsdataverse.nba.nba_draft_model")
    raw = pl.read_parquet(f"{FX}/draft_raw.parquet")
    calls: list = []

    def fake(seasons, return_as_pandas=False):
        calls.append(seasons)
        return raw

    monkeypatch.setattr(mod, "load_nba_stats_draft", fake)
    return mod, calls


@pytest.fixture
def replay_sources(monkeypatch: pytest.MonkeyPatch) -> None:
    feats = importlib.import_module("sportsdataverse.nba._nba_draft_features")
    frames = {
        k: pl.read_parquet(f"{FX}/{k}.parquet") for k in ("core", "value", "ratings", "crosswalk", "rapm", "euro_stats")
    }

    def euro(comp, start):
        e = frames["euro_stats"]
        return e.filter((pl.col("_comp") == comp) & (pl.col("_start") == start)).drop("_comp", "_start")

    loaders = {k: (lambda seasons, k=k: frames[k]) for k in ("core", "value", "ratings", "crosswalk", "rapm")}
    loaders["euro_stats"] = euro
    monkeypatch.setattr(feats, "_default_loaders", lambda: loaders)


def test_schema_and_season_shift(draft_2019) -> None:
    mod, calls = draft_2019
    out = mod.nba_draft_model(2019)
    assert calls == [[2019]]
    assert dict(out.schema) == mod._SCHEMA
    assert out.height == 60 and out["draft_year"].unique().to_list() == [2019]
    assert out["overall_pick"].to_list() == list(range(1, 61))
    assert out["player_id"].str.contains(r"\.").sum() == 0  # ids never pass through a float
    assert out.filter(pl.col("overall_pick") == 1)["player_name"].item() == "Zion Williamson"
    assert out["proj_career_value"].is_sorted(descending=True)
    tiers = out.group_by("pro_tier").len().sort("pro_tier")
    assert dict(zip(tiers["pro_tier"], tiers["len"])) == {"first_round": 16, "lottery": 14, "second_round": 30}


def test_return_as_pandas(draft_2019) -> None:
    import pandas as pd

    mod, _ = draft_2019
    assert isinstance(mod.nba_draft_model(2019, return_as_pandas=True), pd.DataFrame)


def test_empty_and_unpublished_years_return_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    mod = importlib.import_module("sportsdataverse.nba.nba_draft_model")
    assert list(mod.nba_draft_model([]).schema) == list(mod._SCHEMA)
    monkeypatch.setattr(mod, "load_nba_stats_draft", lambda seasons, return_as_pandas=False: pl.DataFrame())
    out = mod.nba_draft_model(2031, pre_draft_features=True)
    assert out.height == 0 and "col_bpm_last" in out.columns


def test_non_nba_league_raises() -> None:
    mod = importlib.import_module("sportsdataverse.nba.nba_draft_model")
    with pytest.raises(ValueError, match="wnba_draft_model"):
        mod.nba_draft_model(2019, league="wnba")


def test_pre_draft_features_replay_matches_capture(draft_2019, replay_sources) -> None:
    """Replaying the captured source slices reproduces the live capture's columns exactly."""
    from sportsdataverse.nba._nba_draft_features import FEATURE_SCHEMA

    mod, _ = draft_2019
    out = mod.nba_draft_model(2019, pre_draft_features=True)
    expected = pl.read_parquet(f"{FX}/expected.parquet")
    cols = list(FEATURE_SCHEMA)
    got = out.select("player_id", *cols).sort("player_id")
    exp = expected.select("player_id", *cols).sort("player_id")
    assert got.schema == exp.schema
    assert got.equals(exp, null_equal=True)
    # match counts as captured: 52 college links (all with box BPM, 51 with RAPM), 4 EuroLeague/EuroCup links
    assert out["college_espn_id"].is_not_null().sum() == 52
    assert out["col_bpm_last"].is_not_null().sum() == 52
    assert out["col_rapm_last"].is_not_null().sum() == 51
    assert out["euro_code"].is_not_null().sum() == 4
    zion = out.filter(pl.col("overall_pick") == 1).row(0, named=True)
    assert zion["college_match"] == "id" and zion["col_seasons"] == 1 and zion["col_bpm_last"] > 10


def test_pre_draft_features_never_change_the_value(draft_2019, replay_sources) -> None:
    mod, _ = draft_2019
    plain = mod.nba_draft_model(2019)
    rich = mod.nba_draft_model(2019, pre_draft_features=True)
    assert plain["proj_career_value"].equals(rich["proj_career_value"])


def test_gleague_bridge_empty_frame_no_crash(draft_2019, monkeypatch: pytest.MonkeyPatch) -> None:
    mod, _ = draft_2019
    monkeypatch.setattr(mod, "nba_stats_leaguedashplayerstats", lambda season, league_id=None: pl.DataFrame())
    out = mod.nba_draft_model(2019, gleague_bridge=True)
    assert out.height == 60 and list(out.schema) == list(mod._SCHEMA)
