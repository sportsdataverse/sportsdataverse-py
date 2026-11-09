"""Guards of tools/fit_team_strength_ridge.py, on the BB-D3 WNBA 2025 fixture (no network)."""

import importlib.util
import math
from pathlib import Path

import numpy as np
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


def test_grouped_weighted_cv_matches_the_ungrouped_fit() -> None:
    # season_cv fits identical training possessions as one row weighted by their count. That must
    # give the same held-out error as fitting every possession on its own; drop the weight and the
    # collapsed means count once each, so the error moves.
    rows = nba_possession_rate_rows(
        pl.read_parquet(RATES / "wnba_possessions_2025.parquet"),
        pl.read_parquet(RATES / "wnba_schedule_2025.parquet"),
        league_id="10",
    )
    grid = [100.0, 800.0]
    got = fit.season_cv(rows, grid)
    design = fit.ridge_design(rows).with_columns((pl.col("game_id").cast(pl.Int64) % fit._FOLDS).alias("_fold"))
    for lam in grid:
        sse = 0.0
        for k in range(fit._FOLDS):
            train, test = design.filter(pl.col("_fold") != k), design.filter(pl.col("_fold") == k)
            coef, b0, hfa = fit.opponent_adjusted_ridge(
                train,
                off_col="offense_id",
                def_col="defense_id",
                home_col="offense_id",
                resp_col="y",
                lam=lam,
                hfa_col="hfa",
            )
            off = dict(zip(coef["team_id"], coef["off_coef"]))
            de = dict(zip(coef["team_id"], coef["def_coef"]))
            pred = (
                b0
                + np.array([off.get(t, 0.0) for t in test["offense_id"]])
                + np.array([de.get(t, 0.0) for t in test["defense_id"]])
                + hfa * test["hfa"].to_numpy()
            )
            sse += float(((test["y"].to_numpy() - pred) ** 2).sum())
        assert got[lam][0] == pytest.approx(sse, rel=1e-9)
