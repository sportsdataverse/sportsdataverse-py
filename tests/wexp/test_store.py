"""Unit tests for the vintage-keyed feature store (leakage refusal at the API)."""

import polars as pl
import pytest

from sportsdataverse.wexp.store import VintageStore


def _vintage_frame():
    # as_of_week=W rows are built from weeks < W (EXCLUSIVE convention).
    return pl.DataFrame(
        {
            "season": pl.Series([2023] * 4, dtype=pl.Int32),
            "as_of_week": pl.Series([2, 3, 5, 3], dtype=pl.Int32),
            "team_id": ["10", "10", "10", "20"],
            "rating": [1.0, 2.0, 5.0, -1.0],
        }
    )


def _games():
    return pl.DataFrame(
        {
            "game_id": ["g1", "g2", "g3"],
            "season": pl.Series([2023] * 3, dtype=pl.Int32),
            "week": pl.Series([3, 4, 1], dtype=pl.Int32),
            "home_team_id": ["10", "10", "10"],
        }
    )


def test_register_refuses_undated_frame():
    store = VintageStore()
    with pytest.raises(ValueError, match="as_of_week"):
        store.register("bad", pl.DataFrame({"season": [2023], "team_id": ["1"], "x": [1.0]}), entity_key="team_id")


def test_register_refuses_duplicate_vintage_rows():
    store = VintageStore()
    dup = _vintage_frame().vstack(_vintage_frame().slice(0, 1))
    with pytest.raises(ValueError, match="duplicate"):
        store.register("dup", dup, entity_key="team_id")


def test_join_uses_latest_leak_free_vintage_and_never_the_future():
    store = VintageStore()
    store.register("ratings", _vintage_frame(), entity_key="team_id")
    out = store.join_asof(_games(), "ratings", on={"home_team_id": "team_id"}, prefix="home_")
    by_game = {r["game_id"]: r for r in out.iter_rows(named=True)}
    # week 3 game -> as_of_week 3 (built from weeks 1-2): allowed, value 2.0
    assert by_game["g1"]["home_rating"] == 2.0
    # week 4 game -> backward fill to as_of_week 3 (no wk-4 snapshot); NOT the wk-5 future row
    assert by_game["g2"]["home_rating"] == 2.0
    # week 1 game -> no vintage exists yet: null, never a future value
    assert by_game["g3"]["home_rating"] is None
    # original game columns survive
    assert set(_games().columns) <= set(out.columns)


def test_join_refuses_unregistered_and_dtype_mismatch():
    store = VintageStore()
    store.register("ratings", _vintage_frame(), entity_key="team_id")
    with pytest.raises(KeyError):
        store.join_asof(_games(), "nope", on={"home_team_id": "team_id"})
    bad_games = _games().with_columns(pl.col("home_team_id").cast(pl.Int64))
    with pytest.raises(ValueError, match="dtype"):
        store.join_asof(bad_games, "ratings", on={"home_team_id": "team_id"})
