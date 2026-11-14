"""Where the 2025-26 NBA ``player_impact`` axes centre, and the WAR possession count (GOP #414 Q28).

Real captures (``tests/fixtures/bb_impact_ranks/README.md``): the published 2025-26 regular-season
``player_impact`` rows (built 2026-11-12, after #1137 and #1148) and the 30 teams' regular-season
totals. Each method says where its league mean sits; these pin that on the table the site reads:

* BPM 2.0 (#1148): minute-weighted ``obpm`` / ``dbpm`` / ``bpm`` each about 0.
* RAPM: an intercept-fitted ridge where every row has five players a side, so each side's
  coefficients sum to exactly 0 (the unweighted mean); weighted by possessions it leans positive,
  because the heavy-minute players are the better ones and the penalty pulls the thin ones to 0.
* WAR: ``(rapm - R) * poss / 100 / pts_per_win`` over possessions counted once. The release
  counted them twice (``off_poss + def_poss``), doubling every WAR.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl
import pytest

from sportsdataverse.nba.nba_war import calibrate_pts_per_win, nba_war

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "bb_impact_ranks"
R = -2.0  # hoopR-nba-stats-data's DEFAULT_REPLACEMENT_LEVEL, the release's


@pytest.fixture(scope="module")
def impact() -> pl.DataFrame:
    return pl.read_parquet(FIX / "nba_player_impact_2026_regular_season.parquet")


@pytest.fixture(scope="module")
def teams() -> pl.DataFrame:
    return pl.read_parquet(FIX / "nba_team_totals_2026.parquet")


def _wmean(df: pl.DataFrame, col: str, weight: pl.Expr) -> float:
    return df.select((pl.col(col) * weight).sum() / weight.sum()).item()


def test_bpm_family_centres_on_the_league(impact) -> None:
    for c in ("obpm", "dbpm", "bpm"):
        assert abs(_wmean(impact, c, pl.col("min"))) < 0.05, c
    rotation = impact.filter(pl.col("off_poss") >= 2000)  # the scatter's population
    assert 0.3 < float((rotation["dbpm"] > 0).mean()) < 0.7  # was every one (median +2.86)
    bkn = impact.filter((pl.col("team_id") == 1610612751) & (pl.col("min") >= 500))
    assert bkn["obpm"].min() > -6  # was -9 to -14 across the rotation


def test_rapm_sides_sum_to_zero_and_lean_positive_by_possession(impact) -> None:
    for side, poss in (("o_rapm", "off_poss"), ("d_rapm", "def_poss")):
        assert abs(impact[side].mean()) < 1e-6, side  # the ridge identity, every player of the fit
        assert 0.0 < _wmean(impact, side, pl.col(poss)) < 1.0, side  # 2025-26: +0.35 / +0.09


def test_war_counts_each_possession_once(impact, teams) -> None:
    ppw = calibrate_pts_per_win(
        teams.select("team_id", pl.col("w").alias("wins"), pl.col("plus_minus").alias("total_margin"))
    )
    assert 35 < ppw < 40
    rating = impact.select("player_id", pl.col("rapm").alias("rating"))
    once = nba_war(rating, impact.select("player_id", ((pl.col("off_poss") + pl.col("def_poss")) / 2).alias("poss")),
                   replacement_level=R, pts_per_win=ppw)  # fmt: skip
    twice = nba_war(rating, impact.select("player_id", (pl.col("off_poss") + pl.col("def_poss")).alias("poss")),
                    replacement_level=R, pts_per_win=ppw)  # fmt: skip
    # the release is the doubled count, to the last digit
    pub = impact.select("player_id", "war").join(twice, on="player_id", suffix="_twice")
    assert (pub["war"] - pub["war_twice"]).abs().max() < 1e-9

    # league: a team of R players is 5R per 100, so the WAR above it is the league's wins over
    # that team's, plus what the ratings' own possession-weighted mean adds
    team_poss = impact["off_poss"].sum() / 5
    repl_wins = teams.height * 41 + 5 * R * team_poss / 100 / ppw
    lean = _wmean(impact, "rapm", pl.col("off_poss") + pl.col("def_poss")) * team_poss * 5 / 100 / ppw
    expected = teams["w"].sum() - repl_wins + lean
    assert once["war"].sum() == pytest.approx(expected, rel=0.01)  # 791
    assert twice["war"].sum() == pytest.approx(2 * expected, rel=0.01)  # 1,582: the release

    # per team: a shrunken rating's WAR gains under one per win; the doubled count gains 1.55
    def slope(war: pl.DataFrame) -> float:
        g = impact.select("player_id", "team_id").join(war, on="player_id").group_by("team_id").agg(pl.col("war").sum())
        g = g.join(teams.select("team_id", "w"), on="team_id")
        return float(np.polyfit(g["w"].to_numpy(), g["war"].to_numpy(), 1)[0])

    assert 0.5 < slope(once) < 1.0
    assert slope(twice) > 1.4
