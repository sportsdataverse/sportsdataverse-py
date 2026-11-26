"""``nba_rookie_projection``: rookie pick curves on the real 2019 class, availability join, schema."""

from __future__ import annotations

import importlib

import polars as pl
import pytest


@pytest.fixture
def board_2019(monkeypatch: pytest.MonkeyPatch):
    dm = importlib.import_module("sportsdataverse.nba.nba_draft_model")
    raw = pl.read_parquet("tests/fixtures/nba_draft/pre_draft_2019/draft_raw.parquet")
    monkeypatch.setattr(dm, "load_nba_stats_draft", lambda seasons, return_as_pandas=False: raw)
    mod = importlib.import_module("sportsdataverse.nba.nba_rookie_projection")
    seen: list = []

    def avail(seasons, **kw):
        seen.append(seasons)
        # one real-shaped row: the 2019 #1 pick in his debut season (END year 2020)
        return pl.DataFrame({"player_id": ["1629627"], "season": [2020], "avail_pct": [0.3]})

    monkeypatch.setattr(mod, "nba_availability", avail)
    return mod, seen


def test_rookie_projection_schema_and_order(board_2019) -> None:
    mod, seen = board_2019
    out = mod.nba_rookie_projection(2019)
    assert dict(out.schema) == mod._SCHEMA
    assert out.height == 60
    assert seen == [[2020]], "availability must be asked for the debut season's END year"
    for col in ("proj_rookie_value", "proj_soph_value", "proj_rookie_min"):
        assert out[col].is_sorted(descending=True), f"{col} must fall with the pick"
    assert (out["proj_rookie_min"] >= 0).all()
    zion = out.filter(pl.col("overall_pick") == 1).row(0, named=True)
    assert zion["proj_avail_pct"] == pytest.approx(0.3)
    assert out.filter(pl.col("overall_pick") == 60)["proj_avail_pct"].item() == pytest.approx(0.75)


def test_availability_is_never_folded_into_value(board_2019) -> None:
    mod, _ = board_2019
    out = mod.nba_rookie_projection(2019)
    art = mod._load_rookie_artifact()
    direct = mod._score(out.select("player_id", "draft_year", "overall_pick"), art)
    assert direct["proj_rookie_value"].equals(out["proj_rookie_value"])


def test_rookie_projection_empty_draft_year() -> None:
    from sportsdataverse.nba.nba_rookie_projection import _SCHEMA, nba_rookie_projection

    out = nba_rookie_projection([])
    assert out.height == 0 and list(out.schema) == list(_SCHEMA)


def test_rookie_projection_non_nba_league_raises() -> None:
    from sportsdataverse.nba.nba_rookie_projection import nba_rookie_projection

    with pytest.raises(ValueError, match="wnba"):
        nba_rookie_projection(2019, league="wnba")
