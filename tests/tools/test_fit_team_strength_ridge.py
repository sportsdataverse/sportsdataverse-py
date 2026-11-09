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


@pytest.mark.parametrize(
    ("seasons", "message"),
    [
        (["2025"], "at least two distinct seasons"),
        (["2025", "2025"], "repeats a season"),
        (["2024", "2024", "2025"], "repeats a season"),
    ],
)
def test_bad_seasons_are_rejected(seasons: list[str], message: str, capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exc:
        fit.main(["--league", "wnba", "--seasons", *seasons])
    assert exc.value.code == 2
    assert message in capsys.readouterr().err


def test_season_without_possessions_is_rejected(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # The real mapper and season_cv run on a season the release has no possessions for, so the
    # guard is shown to fire before the per-season division, not just when season_cv is stubbed.
    empty = pl.read_parquet(RATES / "wnba_possessions_2025.parquet").head(0)
    schedule = pl.read_parquet(RATES / "wnba_schedule_2025.parquet")
    monkeypatch.setattr(fit, "_league_loaders", lambda league_id: (lambda s: empty, lambda s: schedule, 0))
    with pytest.raises(SystemExit) as exc:
        fit.main(["--league", "wnba", "--seasons", "2024", "2025"])
    assert exc.value.code == 2
    assert "season 2024: no possessions" in capsys.readouterr().err
