"""BB-D22: the Pythagorean exponents re-fit offline from the committed season records.

``tests/fixtures/bb_team_luck/{lg}_season_records.parquet`` are the per-team-season records
``tools/fit_team_luck_pythag.py --write`` saved from the real final scores (README there). The
fit is re-run here on the TRAIN seasons and must land on the frozen
:data:`~sportsdataverse._common.team_luck.PYTHAG_EXPONENT`; the HOLDOUT seasons are then scored.

Holdout MAE in wins at the frozen exponent (gate = observed, rounded up at the third decimal;
never loosen a gate to pass -- refit and re-derive it): NBA 2.360 (2017-2026, 300 team-seasons),
WNBA 1.363 (2016-2025, 121), MBB 1.629 (2018-2026, 3197), WBB 1.499 (2018-2026, 3151). The fit
beats Hollinger's 16.5 (NBA 2.791, WNBA 2.205) and KenPom's 11.5 (MBB 2.126, WBB 2.762) on the
same holdout, and matches Oliver's 14 / Morey's 13.91 in the NBA within 0.004 wins.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from sportsdataverse._common.team_luck import PYTHAG_EXPONENT, pythag_win_pct

_ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("fit_team_luck_pythag", _ROOT / "tools" / "fit_team_luck_pythag.py")
fit = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fit)

LUCK = _ROOT / "tests" / "fixtures" / "bb_team_luck"
#: league -> (holdout MAE gate, holdout team-seasons, a published exponent the fit must beat on the holdout).
GATES = {"nba": (2.361, 300, 16.5), "wnba": (1.364, 121, 16.5), "mbb": (1.630, 3197, 11.5), "wbb": (1.500, 3151, 11.5)}


@pytest.mark.parametrize("league", sorted(GATES))
def test_refit_lands_on_the_frozen_exponent_and_holds_out(league: str) -> None:
    train, holdout = fit.SPANS[league]
    assert not set(train) & set(holdout) and max(train) < min(holdout)
    records = pl.read_parquet(LUCK / f"{league}_season_records.parquet")
    assert set(records["season"].unique()) == set(train) | set(holdout)
    x = fit.fit_exponent(records.filter(pl.col("season").is_in(list(train))))
    assert x == PYTHAG_EXPONENT[league]
    held = records.filter(pl.col("season").is_in(list(holdout)))
    mae, _, n = fit.score(held, x)
    gate, n_gate, published = GATES[league]
    assert n == n_gate and mae <= gate
    assert mae < fit.score(held, published)[0]
    if league == "nba":  # Oliver's 14, Morey's 13.91
        assert all(abs(mae - fit.score(held, ref)[0]) < 0.005 for ref in (14.0, 13.91))


@pytest.mark.parametrize("league", sorted(GATES))
def test_the_fit_scores_what_team_luck_publishes(league: str) -> None:
    records = fit.qualified(pl.read_parquet(LUCK / f"{league}_season_records.parquet"))
    x = PYTHAG_EXPONENT[league]
    core = records.select((pythag_win_pct(x) * pl.col("games")).alias("e"))["e"].to_numpy()
    tool = fit.expected_wins(*(records[c].to_numpy().astype(float) for c in ("pts", "opp_pts", "games")), x)
    assert np.allclose(core, tool, rtol=1e-12)


def test_nba_exponent_agrees_with_the_literature() -> None:
    # Oliver's 14 (Basketball on Paper) and Morey's 13.91, fitted on other NBA seasons.
    assert abs(PYTHAG_EXPONENT["nba"] - 14.0) < 0.1


def test_qualifier_keeps_short_seasons_out() -> None:
    records = pl.read_parquet(LUCK / "mbb_season_records.parquet").filter(pl.col("season") == 2025)
    kept = fit.qualified(records)
    # ESPN carries hundreds of non-D1 opponents with a game or two; D1 teams play ~30.
    assert kept.height < records.height and kept["games"].min() >= 0.5 * records["games"].max()
    assert 350 <= kept.height <= 370
