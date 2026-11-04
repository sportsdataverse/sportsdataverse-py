"""Guards of tools/fit_team_strength_ridge.py, on the BB-D3 WNBA 2025 fixture (no network)."""

import importlib.util
import math
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.nba.nba_team_season_rates import nba_possession_rate_rows

_ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location(
    "fit_team_strength_ridge", _ROOT / "tools" / "fit_team_strength_ridge.py"
)
fit = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fit)

RATES = _ROOT / "tests" / "fixtures" / "bb_team_season_rates"


def test_held_out_team_without_training_rows_gets_the_prior_mean() -> None:
    rows = nba_possession_rate_rows(
        pl.read_parquet(RATES / "wnba_possessions_2025.parquet"),
        pl.read_parquet(RATES / "wnba_schedule_2025.parquet"),
        league_id="10",
    )
    team = rows["offense_team_id"].drop_nulls()[0]
    in_game = (pl.col("offense_team_id") == team) | (pl.col("defense_team_id") == team)
    # The team keeps only its fold-0 games, so fold 0's training folds never see it.
    rows = rows.filter(~in_game | (pl.col("game_id").cast(pl.Int64) % fit._FOLDS == 0))
    assert rows.filter(in_game).height > 0
    sse, n = fit.season_cv(rows, [100.0])[100.0]
    assert n == rows.filter(pl.col("offense_team_id").is_not_null() & pl.col("defense_team_id").is_not_null()).height
    assert math.isfinite(sse)


@pytest.mark.parametrize("seasons", [["2025"], ["2025", "2025"]])
def test_fewer_than_two_distinct_seasons_is_rejected(seasons: list[str], capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exc:
        fit.main(["--league", "wnba", "--seasons", *seasons])
    assert exc.value.code == 2
    assert "at least two distinct seasons" in capsys.readouterr().err
