"""Real-data gate for the stats.ncaa.org player id -> ESPN athlete id season crosswalk (BB-D37).

The offline tests run :func:`ncaa_espn_player_crosswalk` on real 2023-24 captures (Big Ten +
SWAC, both leagues; ``tests/fixtures/ncaa_espn_player_crosswalk/README.md``) with the three
release reads monkeypatched to the fixtures, and check the result against the published
``load_{mbb,wbb}_player_aliases`` rows for the same players. The live test runs whole seasons.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.mbb import mbb_ncaa_espn_crosswalk as xw
from sportsdataverse.mbb import ncaa_espn_player_crosswalk
from sportsdataverse.scrape.ncaa import espn_player_xwalk as px
from tests.conftest import skip_if_no_live

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "ncaa_espn_player_crosswalk"
LEAGUES = ("mbb", "wbb")

#: Observed, not aspirational: whole seasons match 99.66-99.93% of roster players (mbb 2024
#: 99.82%, 2025 99.93%; wbb 2024 99.66%, 2025 99.71%) and the fixture 99.8% / 100%. Held a
#: little under so one upstream roster quirk is not an instant red, while a broken normalizer,
#: a lost jersey or an id-dtype slip (which matches nothing) fails at once.
MATCH_FLOOR = 0.99


def _fixture(league: str, name: str) -> pl.DataFrame:
    return pl.read_parquet(FIXTURES / f"{league}_2024_{name}.parquet")


@pytest.fixture(params=LEAGUES)
def offline(request, monkeypatch) -> "tuple[str, pl.DataFrame]":
    league = request.param
    monkeypatch.setattr(xw, "_ncaa_rosters", lambda lg, season: _fixture(lg, "ncaa_team_rosters"))
    monkeypatch.setattr(xw, "ncaa_espn_team_crosswalk", lambda lg: _fixture(lg, "team_crosswalk"))
    monkeypatch.setattr(px, "espn_athlete_side", lambda lg, season: _fixture(lg, "espn_athletes"))
    return league, ncaa_espn_player_crosswalk(2024, league)


def test_schema_and_one_dtype_per_id(offline):
    _, df = offline
    assert dict(df.schema) == xw._PLAYER_SCHEMA
    assert df.height == _fixture(offline[0], "ncaa_team_rosters").height
    assert df["season"].unique().to_list() == [2024]


def test_match_rate_floor(offline):
    _, df = offline
    rate = df["espn_athlete_id"].is_not_null().mean()
    assert rate >= MATCH_FLOOR, f"matched {rate:.4f} < floor {MATCH_FLOOR}"
    # every ESPN team resolved, and the NCAA side never invents a team
    assert df["espn_team_id"].null_count() == 0


def test_method_reason_and_confidence_are_consistent(offline):
    _, df = offline
    assert ((df["match_method"].is_null()) != (df["unmatched_reason"].is_null())).all()
    assert (df["espn_athlete_id"].is_not_null() == df["match_method"].is_not_null()).all()
    expected = df["match_method"].replace_strict(px.MATCH_CONFIDENCE, default=None, return_dtype=pl.Float64)
    assert df["match_confidence"].equals(expected)
    matched = df.drop_nulls("espn_athlete_id")
    assert matched["espn_athlete_name"].null_count() == 0
    # one ESPN athlete per NCAA player inside a team
    assert matched.select("espn_team_id", "espn_athlete_id").is_unique().all()


def test_agrees_with_published_player_aliases(offline):
    league, df = offline
    aliases = _fixture(league, "player_aliases").select(
        pl.col("source_player_id").alias("ncaa_player_id"),
        pl.col("athlete_id").cast(pl.Int64).alias("alias_athlete_id"),
        pl.col("match_method").alias("alias_method"),
        pl.col("match_confidence").alias("alias_confidence"),
    )
    assert df.schema["ncaa_player_id"] == aliases.schema["ncaa_player_id"]
    both = df.join(aliases, on="ncaa_player_id", how="inner")
    assert both.height == aliases.height == df["espn_athlete_id"].is_not_null().sum()
    assert (both["espn_athlete_id"] == both["alias_athlete_id"]).all()
    assert (both["match_method"] == both["alias_method"]).all()
    assert (both["match_confidence"] == both["alias_confidence"]).all()


def test_missing_season_returns_documented_empty_frame(monkeypatch):
    monkeypatch.setattr(xw, "_ncaa_rosters", lambda lg, season: pl.DataFrame())
    df = ncaa_espn_player_crosswalk(2009, "wbb")
    assert df.height == 0 and dict(df.schema) == xw._PLAYER_SCHEMA


def test_rejects_unknown_league():
    with pytest.raises(ValueError, match="league must be one of"):
        ncaa_espn_player_crosswalk(2024, league="nba")


@skip_if_no_live
@pytest.mark.parametrize("league", LEAGUES)
def test_live_whole_season_match_rate(league):
    df = ncaa_espn_player_crosswalk(2024, league)
    assert df.height > 4000
    rate = df["espn_athlete_id"].is_not_null().mean()
    assert rate >= MATCH_FLOOR, f"{league} 2024 matched {rate:.4f} < floor {MATCH_FLOOR}"
