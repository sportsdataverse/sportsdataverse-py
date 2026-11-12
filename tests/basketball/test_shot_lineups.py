"""BB-D36: the ten players on the floor for every shot, on real captures (tests/fixtures/bb_shot_lineups)."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from sportsdataverse._common.shot_lineups import (
    SLOTS,
    ncaa_shot_lineups,
    pro_shot_lineups,
    shot_lineup_coverage,
    shot_lineup_schema,
)

F = Path(__file__).resolve().parents[1] / "fixtures" / "bb_shot_lineups"
OFF = [f"off_player_{i}" for i in range(1, 6)]


def _college(lg: str) -> tuple[pl.DataFrame, pl.DataFrame]:
    return pl.read_parquet(F / f"{lg}_shots.parquet"), pl.read_parquet(F / f"{lg}_pbp.parquet")


def _pro(lg: str) -> tuple[pl.DataFrame, pl.DataFrame]:
    return pl.read_parquet(F / f"{lg}_stats_shots.parquet"), pl.read_parquet(F / f"{lg}_stats_poss.parquet")


def _shooter_in_five(df: pl.DataFrame) -> pl.Expr:
    return pl.any_horizontal([pl.col(c) == pl.col("shooter_id") for c in OFF])


@pytest.mark.parametrize("lg", ["mbb", "wbb"])
def test_college_every_chart_shot_finds_its_event(lg):
    shots, pbp = _college(lg)
    out = ncaa_shot_lineups(shots, pbp)
    assert out.schema == pl.Schema(shot_lineup_schema(pl.Utf8()))
    assert out.height == shots.height
    assert out["matched"].all()
    # the shots under a minute carry tenths (0.7) the pbp clock drops (0:00): matched on floored seconds
    assert out.filter(pl.col("clock_seconds") % 1 != 0).height > 0
    # D-I against D-I: all ten ids known on every shot
    both = out.filter(pl.col("team_id").is_not_null() & pl.col("opp_team_id").is_not_null())
    assert both.height > 0 and both["full_lineup"].all()
    cov = shot_lineup_coverage(out).row(0, named=True)
    assert cov["eligible_full_share"] == 1.0
    # the shooter is one of his side's five (real lineup-engine gaps keep a few out: 305/315 MBB, 271/273 WBB)
    named = out.filter(pl.col("shooter_id").is_not_null())
    assert named.filter(_shooter_in_five(named)).height / named.height >= 0.96


def test_college_lineups_are_the_events_and_a_non_d1_side_has_no_ids():
    shots, pbp = _college("mbb")
    out = ncaa_shot_lineups(shots, pbp)
    # contest 6607173, Lindenwood (home, 2815) vs East-West U. (non-D-I): Malek Archie's make with 19:26 left
    row = out.filter(pl.col("game_id") == "6607173", pl.col("period") == 1, pl.col("clock_seconds") == 1166.0).row(
        0, named=True
    )
    ev = pbp.filter(
        pl.col("contest_id") == "6607173", pl.col("clock") == "19:26", pl.col("player_1_id") == "11563901"
    ).row(0, named=True)
    assert [row[c] for c in OFF] == [ev[f"home_{i}_player_id"] for i in range(1, 6)]
    assert [row["team_id"], row["opp_team_id"], row["shooter_id"], row["made"]] == [2815, None, "11563901", True]
    assert row["def_player_1"] is None and not row["full_lineup"]
    ewu = out.filter(pl.col("game_id") == "6607173", pl.col("team_id").is_null())
    assert ewu.height > 0 and ewu["matched"].all() and ewu["opp_team_id"].eq(2815).all()
    assert not ewu["full_lineup"].any()  # its own five have no stats.ncaa.org ids


def test_college_a_shot_with_no_clock_is_unmatched():
    """The 2018-19..2025-26 releases carry no chart clock: no lineup, so the season is not eligible."""
    shots, pbp = _college("wbb")
    out = ncaa_shot_lineups(shots.with_columns(pl.lit(None, pl.Utf8).alias("sec_left")), pbp)
    assert out.height == shots.height and not out["matched"].any() and not out["full_lineup"].any()
    assert out.select(SLOTS).null_count().row(0) == (out.height,) * 10
    cov = shot_lineup_coverage(out).row(0, named=True)
    assert [cov["eligible_shots"], cov["eligible_full_share"]] == [0, None]


@pytest.mark.parametrize("lg", ["nba", "wnba"])
def test_pro_every_shot_takes_its_possession(lg):
    shots, poss = _pro(lg)
    out = pro_shot_lineups(shots, poss)
    assert out.schema == pl.Schema(shot_lineup_schema(pl.Int64()))
    assert out.height == shots.height
    assert out["matched"].all() and out["full_lineup"].all()
    # the defending team is the possession's defense, never the shooter's own team
    assert (out["opp_team_id"] != out["team_id"]).all()
    assert (
        out.filter(_shooter_in_five(out)).height / out.height >= 0.98
    )  # 169/171 NBA, 135/136 WNBA (subs inside a possession)
    # each lineup is the possession's that holds the shot clock, for the shooting team
    p = poss.with_columns(pl.col("offense_team_id").cast(pl.Int64))
    for r in out.head(25).iter_rows(named=True):
        hold = p.filter(
            pl.col("period") == r["period"],
            pl.col("offense_team_id") == r["team_id"],
            pl.col("end_seconds_remaining") <= r["clock_seconds"],
            pl.col("start_seconds_remaining") >= r["clock_seconds"],
        ).sort("start_seconds_remaining")
        assert [r[c] for c in OFF] == [hold.row(0, named=True)[f"off_player_{i}"] for i in range(1, 6)]


def test_pro_a_game_missing_from_the_possessions_is_unmatched_and_not_eligible():
    shots, poss = _pro("nba")
    out = pro_shot_lineups(shots, poss.head(0))
    assert not out["matched"].any() and out["opp_team_id"].is_null().all()
    assert shot_lineup_coverage(out).row(0, named=True)["eligible_shots"] == 0
