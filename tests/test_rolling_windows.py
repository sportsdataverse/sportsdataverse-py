"""Rolling event-count windows over real released pbp (one CFB QB's 36 games, 2021-2024)."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.rolling_windows import EVENT_SCHEMA, OUTPUT_SCHEMA, football_events, rolling_windows

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


@pytest.fixture(scope="module")
def cfb_events(cfb_pbp, cfb_dates):
    return football_events(cfb_pbp, cfb_dates)


@pytest.fixture(scope="module")
def rw2024(cfb_events):
    return rolling_windows(cfb_events, 2024)


def _row(df: pl.DataFrame, **kw) -> dict:
    out = df.filter(pl.all_horizontal([pl.col(k) == v for k, v in kw.items()]))
    assert out.height == 1, out
    return out.row(0, named=True)


# hand count over the fixture: his dropbacks sorted by (date, game_id, game_play_number),
# 1,029 of them (41 + 20 + 357 + 611), 418 before 2024
@pytest.mark.parametrize(
    ("n", "cur", "prev", "start", "career"),
    [
        (50, 1.000268, 0.250366, 0.355281, 0.289759),
        (100, 0.625317, 0.457801, 0.464828, 0.291879),
        (300, 0.360402, 0.241778, 0.350092, 0.309420),
    ],
)
def test_qb_dropback_windows_match_a_hand_count(rw2024, n, cur, prev, start, career):
    r = _row(rw2024, entity_id=QB, window_unit="dropback", metric="epa", window_n=n)
    assert r["n"] == n
    assert r["cur"] == pytest.approx(cur, abs=1e-6)
    assert r["prev"] == pytest.approx(prev, abs=1e-6)
    assert r["season_start"] == pytest.approx(start, abs=1e-6)
    assert r["career_baseline"] == pytest.approx(career, abs=1e-6)
    assert r["delta_prev"] == pytest.approx(cur - prev, abs=1e-6)
    assert r["delta_season"] == pytest.approx(cur - start, abs=1e-6)
    assert r["entity_name"] == "Kyle McCord" and r["team_id"] == "183"
    assert r["last_event_date"] == date(2024, 12, 28)
    assert r["as_of_date"] == date(2024, 12, 28)
    assert r["season"] == 2024


def test_output_schema_and_one_row_per_key(rw2024):
    assert rw2024.schema == pl.Schema(OUTPUT_SCHEMA)
    key = ["entity_type", "entity_id", "window_unit", "window_n", "metric"]
    assert rw2024.select(key).is_duplicated().sum() == 0


def test_short_history_is_null_and_unranked(rw2024):
    short = rw2024.filter(pl.col("n") < pl.col("window_n"))
    assert short.height > 0
    for c in ("prev", "season_start", "career_baseline", "delta_prev_rank"):
        assert short[c].null_count() == short.height, c
    assert (
        rw2024.filter(pl.col("prev").is_null())["delta_prev_rank"].null_count()
        == rw2024.filter(pl.col("prev").is_null()).height
    )


def test_rank_is_min_rank_of_delta_prev_descending(rw2024):
    g = rw2024.filter(
        (pl.col("window_unit") == "target")
        & (pl.col("window_n") == 30)
        & (pl.col("metric") == "epa")
        & pl.col("delta_prev_rank").is_not_null()
    )
    assert (
        g.height == 4
    )  # receivers active in 2024 with a full current + previous 30-target window (smoke-run 2026-09-26)
    deltas = g["delta_prev"].to_list()
    for d, rank in zip(deltas, g["delta_prev_rank"].to_list()):
        assert rank == 1 + sum(1 for o in deltas if o > d)


def test_rows_only_for_entities_with_a_2024_event(cfb_events, rw2024):
    active = set(cfb_events.filter(pl.col("season") == 2024)["entity_id"].unique())
    assert set(rw2024["entity_id"].unique()) <= active
    assert _row(rw2024, entity_type="team", entity_id="183", window_unit="play", metric="epa", window_n=150)["n"] == 150


def test_windows_override_and_empty_season(cfb_events):
    only = rolling_windows(cfb_events, 2024, windows={"dropback": (10,)})
    assert set(only["window_unit"].unique()) == {"dropback"} and set(only["window_n"].unique()) == {10}
    none = rolling_windows(cfb_events, 2030)
    assert none.height == 0 and none.schema == pl.Schema(OUTPUT_SCHEMA)
