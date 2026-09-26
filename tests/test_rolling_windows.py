"""Rolling event-count windows over real released pbp (one CFB QB's 36 games, 2021-2024)."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.rolling_windows import EVENT_SCHEMA, football_events

FIX = Path(__file__).parent / "fixtures" / "rolling_windows"
QB = "4433971"  # Kyle McCord: Ohio State 2021-23, Syracuse 2024


@pytest.fixture(scope="module")
def cfb_pbp() -> pl.DataFrame:
    return pl.read_parquet(FIX / "cfb_pbp_4433971_2021_2024.parquet")


@pytest.fixture(scope="module")
def cfb_dates() -> pl.DataFrame:
    s = pl.read_parquet(FIX / "cfb_schedule_4433971_2021_2024.parquet")
    return s.select(
        pl.col("game_id").cast(pl.Int64),
        game_date=pl.col("start_date").str.slice(0, 10).str.to_date(),
    )


def test_dropbacks_per_season_match_the_published_pbp(cfb_pbp, cfb_dates):
    ev = football_events(cfb_pbp, cfb_dates)
    qb = ev.filter((pl.col("entity_id") == QB) & (pl.col("window_unit") == "dropback") & (pl.col("metric") == "epa"))
    # same population as sdv-db player_routes: EPA_scrimmage not null, down 1-4, REG+POST
    assert dict(qb.group_by("season").len().iter_rows()) == {2021: 41, 2022: 20, 2023: 357, 2024: 611}


def test_every_unit_and_metric_is_present_with_the_event_schema(cfb_pbp, cfb_dates):
    ev = football_events(cfb_pbp, cfb_dates)
    assert ev.schema == pl.Schema(EVENT_SCHEMA)
    assert set(ev["window_unit"].unique()) == {"dropback", "target", "carry", "play"}
    assert set(ev["metric"].unique()) == {"epa", "success_rate"}
    assert set(ev.filter(pl.col("metric") == "success_rate")["value"].unique()) <= {0.0, 1.0}
    assert ev.filter(pl.col("entity_type") == "team")["entity_id"].str.contains(r"^\d+$").all()


def test_ids_are_strings_and_a_float_id_raises(cfb_pbp, cfb_dates):
    ev = football_events(cfb_pbp, cfb_dates)
    assert ev["entity_id"].str.contains(r"^\d+$").all()  # never "4433971.0"
    # NFL's released pbp carries passer ids as String: same events either way
    as_str = cfb_pbp.with_columns(pl.col("passer_player_id").cast(pl.Utf8))
    assert football_events(as_str, cfb_dates).height == ev.height
    with pytest.raises(TypeError, match="passer_player_id"):
        football_events(cfb_pbp.with_columns(pl.col("passer_player_id").cast(pl.Float64)), cfb_dates)


def test_a_play_without_a_game_date_raises(cfb_pbp, cfb_dates):
    with pytest.raises(ValueError, match="game_date"):
        football_events(cfb_pbp, cfb_dates.head(3))


def test_empty_pbp_returns_the_event_schema(cfb_dates):
    assert football_events(pl.DataFrame(), cfb_dates).schema == pl.Schema(EVENT_SCHEMA)
