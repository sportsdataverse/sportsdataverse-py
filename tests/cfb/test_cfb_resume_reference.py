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
    # a stronger reference wins more of the same games, so matching an unbeaten record is likelier for it
    assert mich["sos_top5_past"] > mich["sos_bubble_past"] > mich["sos_top25_past"] > mich["sos_average_past"]
    assert mich["record_difficulty_top5"] > mich["record_difficulty_bubble"] > mich["record_difficulty_average"]


def test_as_of_date_splits_past_from_remaining(ratings, schedule) -> None:
    """A game ON the cutoff date is remaining (strict <); every past-game column uses only the earlier games."""
    cut = datetime.date(2023, 9, 30)  # Michigan played at Nebraska that day
    out = (
        _resume_core(ratings, schedule, _CFG, 2023, as_of_date=cut)
        .filter(pl.col("team_id") == "130")
        .row(0, named=True)
    )
    refs = _reference_ratings(ratings, schedule)
    games = schedule.filter((pl.col("home_team_id") == "130") | (pl.col("away_team_id") == "130"))
    assert games.filter(pl.col("date") == cut).height == 1
    early = schedule.filter(pl.col("date") < cut)
    late = schedule.filter(pl.col("date") >= cut)
    p_all, _ = _games(ratings, schedule, "130", refs["bubble"])
    p_past, wins = _games(ratings, early, "130", refs["bubble"])
    p_rest, _ = _games(ratings, late, "130", refs["bubble"])
    assert len(p_past) == 4
    assert len(p_past) + len(p_rest) == len(p_all)
    assert out["sos_bubble_past"] == pytest.approx(p_past.mean(), abs=1e-12)
    assert out["sos_bubble_remaining"] == pytest.approx(p_rest.mean(), abs=1e-12)
    assert out["record_difficulty_bubble"] == pytest.approx(_poisson_binomial_tail(p_past, wins), abs=1e-12)
    assert out["wins_above_bubble"] == pytest.approx(wins - p_past.sum(), abs=1e-12)


def test_an_unrated_fcs_opponent_is_a_sure_win(ratings, schedule, resume) -> None:
    """Ohio State 2023 hosted Youngstown State (FCS, unrated): that game is P = 1 for every reference."""
    rated = set(ratings["team_id"].to_list())
    refs = _reference_ratings(ratings, schedule)
    games = schedule.filter((pl.col("home_team_id") == "194") | (pl.col("away_team_id") == "194"))
    opps = [a if h == "194" else h for h, a in zip(games["home_team_id"], games["away_team_id"], strict=True)]
    n_fcs = sum(o not in rated for o in opps)
    assert n_fcs == 1
    rmap = dict(zip(ratings["team_id"].to_list(), ratings["adj_net"].to_list(), strict=True))
    row = resume.filter(pl.col("team_id") == "194").row(0, named=True)
    for ref, r_ref in refs.items():
        fbs = 0.0
        for g, o in zip(games.iter_rows(named=True), opps, strict=True):
            if o in rated:
                h = 0.0 if g["neutral_site"] else (1.0 if g["home_team_id"] == "194" else -1.0)
                fbs += norm.cdf((_CFG.net_points_scale * (r_ref - rmap[o]) + h * _CFG.hfa_points) / _CFG.margin_sd)
        assert row[f"sos_{ref}_past"] * len(opps) == pytest.approx(fbs + n_fcs, abs=1e-9), ref


def test_reference_ratings_keep_fbs_teams_only(ratings, schedule) -> None:
    """With division columns, a rated team the schedule lists outside FBS does not set a reference rating."""
    best = ratings.sort("adj_net", descending=True)["team_id"][0]
    tagged = schedule.with_columns(
        home_division=pl.when(pl.col("home_team_id") == best).then(pl.lit("fcs")).otherwise(pl.lit("fbs")),
        away_division=pl.when(pl.col("away_team_id") == best).then(pl.lit("fcs")).otherwise(pl.lit("fbs")),
    )
    values = sorted(ratings.filter(pl.col("team_id") != best)["adj_net"].to_list(), reverse=True)
    refs = _reference_ratings(ratings, tagged)
    assert refs["top5"] == values[4] and refs["bubble"] == values[11] and refs["top25"] == values[24]
    assert refs["top5"] != _reference_ratings(ratings, schedule)["top5"]


def test_new_columns_in_the_empty_schema() -> None:
    from sportsdataverse.cfb.cfb_resume import _RESUME_SCHEMA

    for ref in _REFERENCES:
        assert f"record_difficulty_{ref}" in _RESUME_SCHEMA
        assert all(f"sos_{ref}_{s}" in _RESUME_SCHEMA for s in ("past", "remaining", "season"))
    assert _RESUME_SCHEMA["wins_above_bubble"] == pl.Float64
