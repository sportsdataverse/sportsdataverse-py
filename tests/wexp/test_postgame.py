"""Post-game deserved-win probability tests on the real 2-game drive fixture.

Real 2019 facts (espn_cfb_schedules): 401110720 Alabama (333, listed
home) 42-3 Duke (150); 401112224 Wisconsin (275, home) 35-14 Michigan
(130). Observed (gate-setting): Alabama pg_we 0.976 analytic / 0.986
resample — decisive on drives too; Wisconsin only 0.68 / 0.71 — the
35-14 scoreboard flattered a moderate drive-efficiency edge, exactly
the luck-stripping this metric exists for. Methods agree within 0.03.
"""

from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.wexp.engines import cfb_drive_deltas
from sportsdataverse.wexp.postgame import postgame_we

FIXDIR = Path(__file__).resolve().parents[1] / "fixtures" / "wexp"

GAMES = pl.DataFrame({"game_id": ["401110720", "401112224"], "home_team_id": ["333", "275"]})


@pytest.fixture(scope="module")
def drives() -> pl.DataFrame:
    return cfb_drive_deltas(pl.read_parquet(FIXDIR / "cfb_pbp_drive_sample.parquet"))


def test_analytic_deserved_win(drives):
    we = postgame_we(drives, GAMES, method="analytic")
    assert we.height == 2
    by = {r["game_id"]: r for r in we.iter_rows(named=True)}
    assert by["401110720"]["pg_we"] > 0.95  # observed 0.976: deserved blowout
    assert 0.55 < by["401112224"]["pg_we"] < 0.85  # observed 0.68: score flattered
    assert by["401110720"]["perf_margin"] > by["401112224"]["perf_margin"] > 0
    assert by["401110720"]["n_drives_home"] >= 10


def test_resample_agrees_with_analytic(drives):
    """G2 bootstrap ~ G3 normal on decisive games; fixed seed = reproducible."""
    ana = postgame_we(drives, GAMES, method="analytic")
    boot = postgame_we(drives, GAMES, method="resample", n_boot=4000, seed=7)
    joined = ana.join(boot, on="game_id", suffix="_b")
    assert (joined["pg_we"] - joined["pg_we_b"]).abs().max() < 0.03
    again = postgame_we(drives, GAMES, method="resample", n_boot=4000, seed=7)
    assert boot["pg_we"].equals(again["pg_we"])


def test_missing_side_dropped_and_bad_method():
    drives = pl.DataFrame(
        {
            "game_id": [1, 1],
            "off": [10, 10],  # only one offense observed
            "delta": [1.0, -0.5],
        }
    )
    games = pl.DataFrame({"game_id": ["1"], "home_team_id": ["10"]})
    assert postgame_we(drives, games).height == 0  # dropped, not imputed
    with pytest.raises(ValueError, match="method"):
        postgame_we(drives, games, method="magic")
