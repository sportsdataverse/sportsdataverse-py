"""Gates for schedule-as-truth completeness, on real ESPN scoreboards.

Three leagues' committed scoreboard captures prove the parser shape is
universal; the reconciliation gates use those REAL game ids — identity is
full coverage, dropped ids are named exactly (the silent-shortfall incident
class), unknown extras warn without blocking, and id-dtype mismatches raise.
"""

from __future__ import annotations

import json
import pathlib

import polars as pl
import pytest

from sportsdataverse.modeling.integrity import (
    schedule_completeness,
    schedule_frame_from_espn_scoreboard,
)

LEAGUES = ("nba", "nfl", "mbb")


def _schedule(league: str) -> pl.DataFrame:
    payload = json.loads(pathlib.Path(f"tests/fixtures/espn/scoreboard_{league}.json").read_text(encoding="utf-8"))
    return schedule_frame_from_espn_scoreboard(payload)


@pytest.mark.parametrize("league", LEAGUES)
def test_scoreboard_parses_universally(league: str) -> None:
    schedule = _schedule(league)
    assert schedule.height > 0
    assert schedule.schema["game_id"] == pl.Utf8
    assert schedule.schema["completed"] == pl.Boolean
    assert (schedule["game_id"].str.len_chars() > 0).all()
    assert schedule["season"].drop_nulls().n_unique() >= 1
    # empty payloads keep the schema
    assert schedule_frame_from_espn_scoreboard({}).schema == schedule.schema


@pytest.mark.parametrize("league", LEAGUES)
def test_identity_is_full_coverage(league: str) -> None:
    schedule = _schedule(league)
    published = schedule.filter(pl.col("completed") == True).select("game_id")  # noqa: E712
    result = schedule_completeness(published, schedule)
    assert result.ok and result.coverage == 1.0
    assert result.missing_ids == [] and result.extra_ids == []
    assert result.n_scheduled_final == published.height


def test_missing_final_games_are_named_and_block() -> None:
    schedule = _schedule("nfl")
    final_ids = schedule.filter(pl.col("completed") == True)["game_id"].to_list()  # noqa: E712
    dropped = final_ids[:2]
    published = pl.DataFrame({"game_id": [g for g in final_ids if g not in dropped]})
    result = schedule_completeness(published, schedule)
    assert not result.ok
    assert result.missing_ids == sorted(dropped)  # the exact incident evidence
    assert result.coverage == pytest.approx((len(final_ids) - 2) / len(final_ids))
    with pytest.raises(ValueError, match="scheduled final games"):
        result.raise_if_incomplete()
    # the per-partition breakdown localizes the shortfall
    by_season = schedule_completeness(published, schedule, partition_key="season").by_partition
    assert by_season is not None and int(by_season["n_missing"].sum()) == 2


def test_unknown_extras_warn_but_do_not_block() -> None:
    schedule = _schedule("nba")
    published = pl.DataFrame(
        {"game_id": schedule.filter(pl.col("completed") == True)["game_id"].to_list() + ["9999999999"]}  # noqa: E712
    )
    result = schedule_completeness(published, schedule)
    assert result.ok  # completeness holds
    assert result.extra_ids == ["9999999999"]  # provenance question, surfaced
    result.raise_if_incomplete()  # does not raise


def test_dtype_mismatch_is_an_error() -> None:
    schedule = _schedule("nba")
    published = pl.DataFrame({"game_id": [1, 2, 3]})  # Int64 vs the schedule's Utf8
    with pytest.raises(ValueError, match="dtype mismatch"):
        schedule_completeness(published, schedule)
    with pytest.raises(ValueError, match="missing from the published"):
        schedule_completeness(pl.DataFrame({"other": ["x"]}), schedule)
