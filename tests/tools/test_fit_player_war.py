"""BB-D26: the WAR replacement level and points per win re-fit offline from committed records.

``tests/fixtures/bb_player_war/{lg}_player_seasons.parquet`` are the player-seasons
``tools/fit_player_war.py --write`` saved from the production functions (README there);
``tests/fixtures/bb_team_luck/{lg}_season_records.parquet`` are BB-D22's final scores. The fit
is re-run on the TRAIN seasons and must land on the frozen
:data:`~sportsdataverse._common.player_war.REPLACEMENT_LEVEL` /
:data:`~sportsdataverse._common.player_war.PTS_PER_WIN`; the HOLDOUT is then scored.

Holdout gates are the observed values rounded up at the third decimal (never loosen one to
pass -- refit and re-derive it). Season level minus R, mean: NBA 0.193 (2020-2026), WNBA 0.633
(2018-2025), MBB 0.316 (2020-2026), WBB 0.417 (2022-2026). Points-per-win MAE in wins: NBA 2.717
(210 team-seasons), WNBA 1.400 (97), MBB 1.684 (2493), WBB 1.675 (1802).
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse._common.player_war import (
    PTS_PER_WIN,
    REPLACEMENT_LEVEL,
    REPLACEMENT_SHARE,
    replacement_level_by_season,
)

_ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("fit_player_war", _ROOT / "tools" / "fit_player_war.py")
fit = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fit)

#: league -> (holdout mean |season level - R| gate, holdout seasons, ppw MAE gate, team-seasons).
GATES = {
    "nba": (0.194, 7, 2.718, 210),
    "wnba": (0.634, 8, 1.401, 97),
    "mbb": (0.317, 7, 1.685, 2493),
    "wbb": (0.418, 5, 1.676, 1802),
}
#: league -> (holdout signed mean of season level - R, the latest season's gap), observed: the
#: pool level keeps drifting up, so a frozen R runs low on the newest seasons (college most).
#: Gates: observed + 0.001; a refit that moves SPANS forward must re-derive them.
SIGNED = {
    "nba": (0.108, 0.014),  # +0.107; 2026 +0.013 (2025: +0.794)
    "wnba": (0.183, 1.260),  # +0.182; 2025 +1.259
    "mbb": (0.235, 0.121),  # +0.234; 2026 +0.120
    "wbb": (0.418, 0.351),  # +0.417 (all five seasons above R); 2026 +0.350
}


def _span(df: pl.DataFrame, span: range) -> pl.DataFrame:
    return df.filter(pl.col("season").is_in(list(span)))


@pytest.mark.parametrize("league", sorted(GATES))
def test_refit_lands_on_the_frozen_constants_and_holds_out(league: str) -> None:
    train, holdout = fit.SPANS[league]
    assert not set(train) & set(holdout) and max(train) < min(holdout)
    ps = pl.read_parquet(fit.FIXTURES / f"{league}_player_seasons.parquet")
    assert set(ps["season"].unique()) == set(train) | set(holdout)
    assert fit.fit_replacement_level(_span(ps, train)) == REPLACEMENT_LEVEL[league]
    gap_gate, n_seasons, mae_gate, n_teams = GATES[league]
    mean_gap, _, n = fit.score_replacement_level(_span(ps, holdout), REPLACEMENT_LEVEL[league])
    assert n == n_seasons and mean_gap <= gap_gate
    levels = replacement_level_by_season(_span(ps, holdout))["replacement_level"] - REPLACEMENT_LEVEL[league]
    signed_gate, latest_gate = SIGNED[league]
    assert float(levels.mean()) <= signed_gate and float(levels[-1]) <= latest_gate
    records = pl.read_parquet(fit.RECORDS / f"{league}_season_records.parquet")
    assert fit.fit_pts_per_win(_span(records, train)) == PTS_PER_WIN[league]
    mae, n = fit.score_pts_per_win(_span(records, holdout), PTS_PER_WIN[league])
    assert n == n_teams and mae <= mae_gate


@pytest.mark.parametrize("league", sorted(GATES))
def test_the_pool_is_the_least_used_share(league: str) -> None:
    ps = pl.read_parquet(fit.FIXTURES / f"{league}_player_seasons.parquet")
    pools = replacement_level_by_season(ps)
    # observed: every season's pool fills 0.145-0.150 of its possessions
    assert (pools["pool_share"] <= REPLACEMENT_SHARE).all() and (pools["pool_share"] >= 0.144).all()
    season = int(ps["season"].max())
    one = ps.filter((pl.col("season") == season) & (pl.col("poss") > 0)).sort("poss")
    n = int(pools.filter(pl.col("season") == season)["pool_players"].item())
    # the pool is the n players with the fewest possessions, and the next one would overflow it
    assert one["poss"].head(n + 1).sum() > REPLACEMENT_SHARE * one["poss"].sum() >= one["poss"].head(n).sum()


def test_nba_level_sits_at_basketball_reference_convention() -> None:
    # Basketball-Reference's VORP puts replacement at -2.0 BPM; the 15% pool lands within 0.05
    assert abs(REPLACEMENT_LEVEL["nba"] - -2.0) < 0.05
