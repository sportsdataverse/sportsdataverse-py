"""Rolling event-count windows over real released pbp (one CFB QB's 36 games, 2021-2024)."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest
from polars.testing import assert_frame_equal

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


def test_qualified_flags_full_vs_short_windows(rw2024):
    full = rw2024.filter(pl.col("qualified"))
    short = rw2024.filter(~pl.col("qualified"))
    assert full.height > 0 and short.height > 0
    assert (full["n"] == full["window_n"]).all()
    assert (short["n"] < short["window_n"]).all()


def _synthetic_events(rows: list[dict]) -> pl.DataFrame:
    """A minimal EVENT_SCHEMA frame from plain dicts, for cases too fiddly to hand-pick from the fixture."""
    return pl.DataFrame(rows, schema=EVENT_SCHEMA, orient="row")


def test_tied_deltas_split_by_float_noise_share_the_lowest_rank():
    # 0.6 - 0.4 and 0.5 - 0.3 are both "really" 0.2, but IEEE 754 subtraction gives
    # 0.19999999999999996 for the first and 0.2 (exact) for the second -- a real
    # divergence at ~1e-16 that .round(12) must collapse before ranking.
    assert (0.6 - 0.4) != (0.5 - 0.3)
    assert round(0.6 - 0.4, 12) == round(0.5 - 0.3, 12) == 0.2
    ev = _synthetic_events(
        [
            dict(
                season=2024,
                entity_type="player",
                entity_id=eid,
                entity_name=name,
                team_id="1",
                window_unit="dropback",
                metric="epa",
                game_id=g,
                event_date=d,
                seq=1,
                value=v,
            )
            for eid, name, g, d, v in [
                ("A1", "Entity A", "g1", date(2024, 1, 1), 0.4),
                ("A1", "Entity A", "g2", date(2024, 1, 8), 0.6),
                ("B1", "Entity B", "g1", date(2024, 1, 1), 0.3),
                ("B1", "Entity B", "g2", date(2024, 1, 8), 0.5),
            ]
        ]
    )
    rw = rolling_windows(ev, 2024, windows={"dropback": (1,)})
    a = _row(rw, entity_id="A1", window_unit="dropback", metric="epa", window_n=1)
    b = _row(rw, entity_id="B1", window_unit="dropback", metric="epa", window_n=1)
    assert a["delta_prev"] == pytest.approx(0.2, abs=1e-12)
    assert b["delta_prev"] == pytest.approx(0.2, abs=1e-12)
    assert a["delta_prev_rank"] == b["delta_prev_rank"] == 1


def test_nan_events_are_dropped_and_never_rank_first():
    ev = _synthetic_events(
        [
            # C1's latest event is NaN: dropped entirely, so its window falls back
            # to the two real events behind it (cur=0.1, prev=0.2, delta=-0.1).
            dict(
                season=2024,
                entity_type="player",
                entity_id="C1",
                entity_name="Entity C",
                team_id="1",
                window_unit="dropback",
                metric="epa",
                game_id="g1",
                event_date=date(2024, 1, 1),
                seq=1,
                value=0.2,
            ),
            dict(
                season=2024,
                entity_type="player",
                entity_id="C1",
                entity_name="Entity C",
                team_id="1",
                window_unit="dropback",
                metric="epa",
                game_id="g2",
                event_date=date(2024, 1, 8),
                seq=1,
                value=0.1,
            ),
            dict(
                season=2024,
                entity_type="player",
                entity_id="C1",
                entity_name="Entity C",
                team_id="1",
                window_unit="dropback",
                metric="epa",
                game_id="g3",
                event_date=date(2024, 1, 15),
                seq=1,
                value=float("nan"),
            ),
            # D1 has the biggest real riser and should take rank 1 instead.
            dict(
                season=2024,
                entity_type="player",
                entity_id="D1",
                entity_name="Entity D",
                team_id="2",
                window_unit="dropback",
                metric="epa",
                game_id="g1",
                event_date=date(2024, 1, 1),
                seq=1,
                value=0.1,
            ),
            dict(
                season=2024,
                entity_type="player",
                entity_id="D1",
                entity_name="Entity D",
                team_id="2",
                window_unit="dropback",
                metric="epa",
                game_id="g2",
                event_date=date(2024, 1, 8),
                seq=1,
                value=0.9,
            ),
        ]
    )
    rw = rolling_windows(ev, 2024, windows={"dropback": (1,)})
    c = _row(rw, entity_id="C1", window_unit="dropback", metric="epa", window_n=1)
    d = _row(rw, entity_id="D1", window_unit="dropback", metric="epa", window_n=1)
    assert c["cur"] == pytest.approx(0.1) and c["prev"] == pytest.approx(0.2)  # the NaN event never entered a window
    assert c["delta_prev_rank"] != 1
    assert d["delta_prev_rank"] == 1


def test_no_leakage_from_future_seasons(cfb_events):
    key = ["window_unit", "window_n", "metric", "entity_type", "entity_id"]
    full = rolling_windows(cfb_events, 2023).sort(key)
    pre_only = rolling_windows(cfb_events.filter(pl.col("season") <= 2023), 2023).sort(key)
    assert_frame_equal(full, pre_only)


def test_duplicate_game_dates_raises(cfb_pbp, cfb_dates):
    dup_dates = pl.concat([cfb_dates, cfb_dates.head(1)])
    with pytest.raises(ValueError, match="game_id"):
        football_events(cfb_pbp, dup_dates)


def test_rows_only_for_entities_with_a_2024_event(cfb_events, rw2024):
    active = set(cfb_events.filter(pl.col("season") == 2024)["entity_id"].unique())
    assert set(rw2024["entity_id"].unique()) <= active
    assert _row(rw2024, entity_type="team", entity_id="183", window_unit="play", metric="epa", window_n=150)["n"] == 150


def test_windows_override_and_empty_season(cfb_events):
    only = rolling_windows(cfb_events, 2024, windows={"dropback": (10,)})
    assert set(only["window_unit"].unique()) == {"dropback"} and set(only["window_n"].unique()) == {10}
    none = rolling_windows(cfb_events, 2030)
    assert none.height == 0 and none.schema == pl.Schema(OUTPUT_SCHEMA)


def test_package_level_export_and_reference_docs():
    import sportsdataverse

    assert sportsdataverse.rolling_windows is rolling_windows
    assert sportsdataverse.football_events is football_events
    doc = (Path(__file__).parents[1] / "docs" / "docs" / "reference" / "python-helpers.md").read_text(encoding="utf-8")
    assert "{#rolling_windows}" in doc and "{#football_events}" in doc
