"""Current impact (``nba_current_impact``, BB-D39): the filter's math, its as-of boundary and its shocks.

Fixtures, all real (README in ``tests/fixtures/bb_current_impact``): the regular-season possessions of
2013-14 from April 1 and 2014-15 before November 8 (with game dates and team ids), and the 365-day window
priors ``tools/fit_current_impact.py inputs`` built at the evaluation dates in that span. The window-prior
tests read the BB-D38 2023-24 slice (``tests/fixtures/bb_box_prior_v2``).
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from sportsdataverse.nba.nba_box_prior import train_box_prior
from sportsdataverse.nba.nba_current_impact import (
    CURRENT_IMPACT_SCHEMA,
    CurrentImpactFilter,
    CurrentImpactParams,
    current_impact_prior,
    nba_current_impact,
    run_current_impact,
)
from sportsdataverse.nba.nba_rapm import build_rapm_design
from sportsdataverse.nba.nba_reml_rapm import fit_reml_prior_ridge

_FX = Path(__file__).resolve().parents[1] / "fixtures"
_CI, _D38 = _FX / "bb_current_impact", _FX / "bb_box_prior_v2"
_ASOF = dt.date(2014, 11, 5)


@pytest.fixture(scope="module")
def poss() -> pl.DataFrame:
    return pl.read_parquet(_CI / "nba_poss_2014_2015_slice.parquet")


@pytest.fixture(scope="module")
def priors() -> pl.DataFrame:
    return pl.read_parquet(_CI / "nba_window_priors_2014_2015_slice.parquet")


@pytest.fixture(scope="module")
def current(poss, priors) -> pl.DataFrame:
    return nba_current_impact(poss, priors, _ASOF)


def _by_pid(frame: pl.DataFrame) -> dict[int, float]:
    return dict(zip(frame["player_id"].to_list(), frame["current_impact"].to_list(), strict=True))


def test_no_shocks_is_the_prior_informed_ridge(poss, priors) -> None:
    """No drift or shocks, one season, one prior: the filter mean is fit_reml_prior_ridge at noise/entry^2."""
    one = poss.filter(pl.col("season") == 2015)
    pr = priors.filter(pl.col("prior_date") == priors["prior_date"].max())
    entry, noise = 2.0, 1.3  # 1.3 = CURRENT_IMPACT_NOISE_VAR_FALLBACK, used for the input's first season
    par = CurrentImpactParams(entry_sd=entry, season_sd=0.0, team_sd=0.0, drift_sd=0.0, level_sd=10.0)
    cut = dt.date(2015, 1, 1)
    _, r = run_current_impact(one, pr.with_columns(pl.lit(dt.date(2000, 1, 1)).alias("prior_date")), [cut], params=par)[
        cut
    ]
    X, y, pids = build_rapm_design(one)
    prior = {a: (b, c) for a, b, c in pr.select("player_id", "o_prior", "d_prior").iter_rows()}
    mu = np.array([prior.get(p, (0, 0))[0] for p in pids] + [-prior.get(p, (0, 0))[1] for p in pids]) / 100
    fit = fit_reml_prior_ridge(X, y, mu, lambdas=(noise / (entry / 100) ** 2,) * 2)
    P = len(pids)
    assert np.abs(np.array([r[p][0] for p in pids]) - 100 * fit.coef[:P]).max() < 1e-4
    assert np.abs(np.array([r[p][1] for p in pids]) + 100 * fit.coef[P:]).max() < 1e-4


def test_schema_and_empty(poss, priors, current) -> None:
    assert current.schema == pl.Schema(CURRENT_IMPACT_SCHEMA)
    assert current.height > 400 and current["as_of"].unique().to_list() == [_ASOF]
    assert (current["current_impact"] - current["o_current_impact"] - current["d_current_impact"]).abs().max() < 1e-12
    assert (current["last_game_date"] < _ASOF).all()
    early = nba_current_impact(poss, priors, poss["game_date"].min())
    assert early.height == 0 and early.schema == pl.Schema(CURRENT_IMPACT_SCHEMA)


def test_games_on_or_after_the_date_never_move_it(poss, priors, current) -> None:
    """Rewriting every possession dated on or after as_of leaves the rating identical; one before moves it."""
    late = pl.col("game_date") >= _ASOF
    assert poss.filter(late).height > 1000
    flipped = poss.with_columns(pl.when(late).then(3 - pl.col("points")).otherwise(pl.col("points")).alias("points"))
    pri_late = priors.with_columns(pl.when(pl.col("prior_date") > _ASOF).then(pl.col("o_prior") + 9).otherwise(pl.col("o_prior")).alias("o_prior"))  # fmt: skip
    again = nba_current_impact(flipped, pri_late, _ASOF)
    assert again.equals(current)
    day = poss.filter(~late)["game_date"].max()
    early = poss.with_columns(pl.when(pl.col("game_date") == day).then(3 - pl.col("points")).otherwise(pl.col("points")).alias("points"))  # fmt: skip
    moved = nba_current_impact(early, priors, _ASOF)
    assert (moved["current_impact"] - current["current_impact"]).abs().max() > 0.01


@pytest.mark.parametrize(
    ("field", "value"), [("drift_sd", 0.1), ("season_sd", 0.0), ("entry_sd", 0.5), ("team_sd", 1.0)]
)
def test_every_shock_moves_the_rating(poss, priors, current, field, value) -> None:
    """Changing each SD changes the output: none is a silent no-op on real games.

    The fitted team (0.026) and drift (0.0115) SDs move no rating in this slice by more than 0.002 per 100,
    so those two are checked at 1.0 and 0.1 instead: that they are wired, not that the fitted values matter
    (the gate's ablations say they barely do).
    """
    par = CurrentImpactParams(**{**CurrentImpactParams().__dict__, field: value})
    other = nba_current_impact(poss, priors, _ASOF, params=par)
    assert other["player_id"].equals(current["player_id"])
    assert (other["current_impact"] - current["current_impact"]).abs().max() > 1e-2


def test_the_slice_has_team_changes(poss) -> None:
    """The team-SD test above can only fail if some player changes team in the slice (offseason moves)."""
    long = pl.concat([poss.select(pl.col(f"off_player_{i}").alias("pid"), pl.col("offense_team_id").alias("t")) for i in range(1, 6)])  # fmt: skip
    assert long.group_by("pid").agg(pl.col("t").n_unique().alias("k")).filter(pl.col("k") > 1).height > 10


def test_carry_across_the_season_boundary(poss, priors) -> None:
    """Opening night of 2014-15 rates last season's players: window prior plus the carried deviation."""
    d0 = poss.filter(pl.col("season") == 2015)["game_date"].min()
    r = nba_current_impact(poss, priors, d0)
    assert r.height > 300 and (r["season"] == 2014).all()
    dev = r["current_impact"] - r["o_prior"] - r["d_prior"]
    assert dev.abs().max() > 0.1  # the filter carried something beyond the prior
    pr0 = priors.filter(pl.col("prior_date") == d0)
    assert pr0.height > 300  # last season's box lines: the window prior is not empty on opening night


def _d38_model() -> object:
    pan = pl.read_parquet(_D38 / "nba_panel_2022_2023.parquet")
    return train_box_prior(pan, season=2024)


def test_window_prior_reads_only_games_before_the_date() -> None:
    """current_impact_prior ignores box lines on or after as_of and changes when an earlier one does."""
    lp = pl.read_parquet(_D38 / "nba_player_logs_2024_slice.parquet")
    lt = pl.read_parquet(_D38 / "nba_team_logs_2024_slice.parquet")
    pos = pl.read_parquet(_D38 / "nba_possessions_2024_slice.parquet")
    bio = pl.read_parquet(_D38 / "nba_bio_2023_2024.parquet").filter(pl.col("season") == 2024)
    model = _d38_model()
    dates = lp["game_date"].unique().sort()
    d = dt.date.fromisoformat(dates[len(dates) // 2])
    base = current_impact_prior(lp, lt, pos, bio, model, d)
    assert base.height > 100 and (base["prior_date"] == d).all()
    on = pl.col("game_date").str.to_date() >= d
    same = current_impact_prior(lp.with_columns(pl.when(on).then(pl.col("pts") + 40).otherwise(pl.col("pts")).alias("pts")), lt, pos, bio, model, d)  # fmt: skip
    assert same.sort("player_id")["player_id"].equals(base.sort("player_id")["player_id"])
    assert (
        same.sort("player_id")["o_prior"] - base.sort("player_id")["o_prior"]
    ).abs().max() < 1e-9  # polars sums reorder
    before = pl.col("game_date").str.to_date() < d
    moved = current_impact_prior(lp.with_columns(pl.when(before).then(pl.col("pts") + 40).otherwise(pl.col("pts")).alias("pts")), lt, pos, bio, model, d)  # fmt: skip
    assert (moved.sort("player_id")["o_prior"] - base.sort("player_id")["o_prior"]).abs().max() > 0.1
    assert current_impact_prior(lp, lt, pos, bio, model, dates[0]).height == 0


def test_priors_dated_after_the_date_never_move_it(poss, priors) -> None:
    """The prior in force is the latest dated on or before as_of; a later one is never read."""
    d = dt.date(2014, 11, 1)
    later = pl.col("prior_date") > d
    assert priors.filter(later).height > 300  # the 2014-11-04 prior exists, so this test can fail
    base = nba_current_impact(poss, priors, d)
    bumped = priors.with_columns(
        pl.when(later).then(pl.col("o_prior") + 9).otherwise(pl.col("o_prior")).alias("o_prior")
    )
    assert nba_current_impact(poss, bumped, d).equals(base)
    for day, want in ((d, dt.date(2014, 10, 28)), (dt.date(2014, 11, 4), dt.date(2014, 11, 4))):
        r = nba_current_impact(poss, priors, day).filter(pl.col("season") == 2015, pl.col("off_poss") > 0)
        p = priors.filter(pl.col("prior_date") == want).select("player_id", pl.col("o_prior").alias("want"))
        j = r.join(p, on="player_id", how="inner")
        assert j.height > 100 and (j["o_prior"] - j["want"]).abs().max() < 1e-12, (day, want)


def test_one_pass_over_several_dates_equals_one_date(poss, priors) -> None:
    """The gate's one-pass, many-dates run gives each date what a single as-of call gives."""
    d1, d2 = dt.date(2014, 11, 1), dt.date(2014, 11, 7)
    both = nba_current_impact(poss, priors, [d1, d2]).filter(pl.col("as_of") == d1)
    one = nba_current_impact(poss, priors, d1)
    assert both["player_id"].equals(one["player_id"])
    assert (both["current_impact"] - one["current_impact"]).abs().max() < 1e-12


def test_each_season_uses_the_previous_seasons_noise(poss, priors, monkeypatch) -> None:
    """start_season gets None (the fallback) for the first season, then the variance of the previous one's points."""
    seen: list[float | None] = []
    orig = CurrentImpactFilter.start_season

    def spy(self, pids, noise_var=None):
        seen.append(noise_var)
        return orig(self, pids, noise_var)

    monkeypatch.setattr(CurrentImpactFilter, "start_season", spy)
    nba_current_impact(poss, priors, dt.date(2014, 11, 7))
    assert seen[0] is None
    assert seen[1] == pytest.approx(poss.filter(pl.col("season") == 2014)["points"].var())


def test_window_prior_rejects_mismatched_game_ids() -> None:
    lp = pl.read_parquet(_D38 / "nba_player_logs_2024_slice.parquet")
    lt = pl.read_parquet(_D38 / "nba_team_logs_2024_slice.parquet")
    pos = pl.read_parquet(_D38 / "nba_possessions_2024_slice.parquet")
    bio = pl.read_parquet(_D38 / "nba_bio_2023_2024.parquet").filter(pl.col("season") == 2024)
    d = dt.date.fromisoformat(lp["game_date"].max())
    with pytest.raises(ValueError, match="dtypes differ"):
        current_impact_prior(lp, lt, pos.with_columns(pl.col("game_id").cast(pl.Int64)), bio, _d38_model(), d)
    with pytest.raises(ValueError, match="no possession matches"):
        current_impact_prior(lp, lt, pos.with_columns(pl.lit("x").alias("game_id")), bio, _d38_model(), d)
