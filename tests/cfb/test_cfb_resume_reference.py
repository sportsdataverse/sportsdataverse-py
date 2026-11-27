"""Reference-team schedule strength, record difficulty and wins above bubble on the real 2023 season.

Fixtures (tests/fixtures/cfb_prediction): ``pbp_2023_sample.parquet`` -> ``efficiency_ratings``; ``results_2023.parquet``
(every 2023 game, all divisions).
"""

from __future__ import annotations

import datetime
from pathlib import Path

import numpy as np
import polars as pl
import pytest
from scipy.stats import norm

from sportsdataverse.cfb.cfb_prediction_constants import get_constants
from sportsdataverse.cfb.cfb_ratings import efficiency_ratings
from sportsdataverse.cfb.cfb_resume import (
    _REFERENCES,
    _normalize_schedule,
    _poisson_binomial_tail,
    _reference_ratings,
    _resume_core,
)

_FIX = Path(__file__).resolve().parents[1] / "fixtures" / "cfb_prediction"
_CFG = get_constants("modern")
# Michigan (15-0), Washington (14-1), Iowa (10-4)
_TEAMS = ("130", "264", "2294")


@pytest.fixture(scope="module")
def ratings() -> pl.DataFrame:
    return efficiency_ratings(pl.read_parquet(_FIX / "pbp_2023_sample.parquet")).select("team_id", "adj_net")


@pytest.fixture(scope="module")
def schedule() -> pl.DataFrame:
    return _normalize_schedule(pl.read_parquet(_FIX / "results_2023.parquet"))


@pytest.fixture(scope="module")
def resume(ratings: pl.DataFrame, schedule: pl.DataFrame) -> pl.DataFrame:
    return _resume_core(ratings, schedule, _CFG, 2023)


def _games(ratings: pl.DataFrame, schedule: pl.DataFrame, team: str, r_ref: float) -> tuple[np.ndarray, int]:
    """The team's completed games: the reference team's win probability in each (written out again) and its wins."""
    p, wins = [], 0
    rmap = dict(zip(ratings["team_id"].to_list(), ratings["adj_net"].to_list(), strict=True))
    for g in schedule.filter((pl.col("home_team_id") == team) | (pl.col("away_team_id") == team)).iter_rows(named=True):
        home = g["home_team_id"] == team
        opp = g["away_team_id"] if home else g["home_team_id"]
        h = 0.0 if g["neutral_site"] else (1.0 if home else -1.0)
        mine, theirs = (g["home_score"], g["away_score"]) if home else (g["away_score"], g["home_score"])
        wins += int(mine > theirs)
        r_opp = rmap.get(opp)
        p.append(
            1.0
            if r_opp is None
            else norm.cdf((_CFG.net_points_scale * (r_ref - r_opp) + h * _CFG.hfa_points) / _CFG.margin_sd)
        )
    return np.array(p), wins


def test_poisson_binomial_matches_monte_carlo_on_real_teams(ratings: pl.DataFrame, schedule: pl.DataFrame) -> None:
    """Gate (a): exact tail within 0.002 of 100,000 simulated seasons, three teams x four references."""
    refs = _reference_ratings(ratings, schedule)
    rng = np.random.default_rng(2023)
    for team in _TEAMS:
        for ref, r_ref in refs.items():
            p, wins = _games(ratings, schedule, team, r_ref)
            sims = (rng.random((100_000, len(p))) < p).sum(axis=1)
            assert abs(_poisson_binomial_tail(p, wins) - (sims >= wins).mean()) <= 0.002, (team, ref)


def test_columns_are_the_written_out_probabilities(ratings, schedule, resume) -> None:
    refs = _reference_ratings(ratings, schedule)
    for team in _TEAMS:
        row = resume.filter(pl.col("team_id") == team).row(0, named=True)
        for ref, r_ref in refs.items():
            p, wins = _games(ratings, schedule, team, r_ref)
            assert row[f"sos_{ref}_past"] == pytest.approx(p.mean(), abs=1e-12)
            assert row[f"sos_{ref}_season"] == pytest.approx(p.mean(), abs=1e-12)  # every 2023 game is played
            assert row[f"sos_{ref}_remaining"] is None
            assert row[f"record_difficulty_{ref}"] == pytest.approx(_poisson_binomial_tail(p, wins), abs=1e-12)
        p, wins = _games(ratings, schedule, team, refs["bubble"])
        assert row["wins_above_bubble"] == pytest.approx(wins - p.sum(), abs=1e-12)


def test_reference_order_and_unbeaten_record(resume, ratings, schedule) -> None:
    refs = _reference_ratings(ratings, schedule)
    assert refs["average"] == 0.0
    assert refs["top5"] > refs["bubble"] > refs["top25"]
    mich = resume.filter(pl.col("team_id") == "130").row(0, named=True)
    # a harder reference wins fewer of the same games, so an unbeaten record is rarer against it
    assert mich["sos_top5_past"] > mich["sos_bubble_past"] > mich["sos_top25_past"] > mich["sos_average_past"]
    assert mich["record_difficulty_top5"] > mich["record_difficulty_bubble"] > mich["record_difficulty_average"]


def test_as_of_date_splits_past_from_remaining(ratings, schedule) -> None:
    cut = datetime.date(2023, 10, 1)
    out = (
        _resume_core(ratings, schedule, _CFG, 2023, as_of_date=cut)
        .filter(pl.col("team_id") == "130")
        .row(0, named=True)
    )
    refs = _reference_ratings(ratings, schedule)
    games = schedule.filter((pl.col("home_team_id") == "130") | (pl.col("away_team_id") == "130"))
    before = games.filter(pl.col("date") < cut).height
    assert 0 < before < games.height
    p, _ = _games(ratings, schedule, "130", refs["bubble"])
    order = games.with_row_index().filter(pl.col("date") < cut)["index"].to_numpy()
    rest = np.setdiff1d(np.arange(games.height), order)
    assert out["sos_bubble_past"] == pytest.approx(p[order].mean(), abs=1e-12)
    assert out["sos_bubble_remaining"] == pytest.approx(p[rest].mean(), abs=1e-12)


def test_new_columns_in_the_empty_schema() -> None:
    from sportsdataverse.cfb.cfb_resume import _RESUME_SCHEMA

    for ref in _REFERENCES:
        assert f"record_difficulty_{ref}" in _RESUME_SCHEMA
        assert all(f"sos_{ref}_{s}" in _RESUME_SCHEMA for s in ("past", "remaining", "season"))
    assert _RESUME_SCHEMA["wins_above_bubble"] == pl.Float64
