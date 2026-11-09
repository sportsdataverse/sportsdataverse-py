"""NBA / WNBA neutral-site games: the curated list, the bubble rule, and where the products use them.

Real fixtures only: ``tests/fixtures/bb_pro_season`` (stats schedules with dates, NBA 2023-24 to
2026-27, WNBA 2024-25) and ``tests/fixtures/bb_team_season_rates`` (possessions). The sources of
every listed game are in ``sportsdataverse/nba/data/neutral_site_games.csv``.
"""

from __future__ import annotations

import datetime as dt
import importlib
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from sportsdataverse.nba import nba_neutral_site_games
from sportsdataverse.nba.nba_neutral_site import BUBBLES, NEUTRAL_GAMES_SCHEMA, _neutral_site

ROOT = Path(__file__).resolve().parents[1] / "fixtures"
RATES, PRO = ROOT / "bb_team_season_rates", ROOT / "bb_pro_season"
tsr = importlib.import_module("sportsdataverse.nba.nba_team_season_rates")
sim_mod = importlib.import_module("sportsdataverse.nba.nba_season_sim")


def test_the_list_is_well_formed():
    nba, wnba = nba_neutral_site_games(), nba_neutral_site_games(league_id="10")
    for df in (nba, wnba):
        assert dict(df.schema) == NEUTRAL_GAMES_SCHEMA
        assert df["game_id"].is_unique().all()
        assert df["source_url"].str.starts_with("http").all()
    assert set(nba["reason"]) == {"international", "cup_knockout"}
    # the stats id carries the season: 00 2 YY (regular season) / 00 6 YY (Cup final), YY = start year
    start = nba["game_id"].str.slice(3, 2).cast(pl.Int64)
    assert ((start + 2001) % 100 == nba["season"] % 100).all()
    kind = nba["game_id"].str.slice(2, 1)
    assert (kind.replace_strict({"2": "regular-season", "6": "nba-cup"}) == nba["season_type"]).all()
    assert nba.filter(pl.col("reason") == "cup_knockout").height == 9  # 3 seasons x 2 semifinals + final
    with pytest.raises(ValueError, match="league_id"):
        nba_neutral_site_games(league_id="20")


@pytest.mark.parametrize(
    ("lg", "league_id", "season"), [("nba", "00", 2024), ("nba", "00", 2026), ("wnba", "10", 2025)]
)
def test_listed_games_are_on_the_schedule_and_flagged(lg, league_id, season):
    sched = pl.read_parquet(PRO / f"{lg}_schedule_{season}.parquet")
    listed = nba_neutral_site_games([season], league_id=league_id)
    assert listed.height > 0
    on = sched.join(listed.select("game_id", pl.col("game_date").alias("_d")), on="game_id", how="inner")
    assert on.height == listed.height  # every listed id is a game of that season
    assert (on["game_date"].str.slice(0, 10).str.to_date() == on["_d"]).all()
    flagged = sched.filter(_neutral_site(sched, league_id))["game_id"].sort().to_list()
    assert flagged == listed["game_id"].sort().to_list()  # outside 2020 nothing else is neutral


def test_the_bubble_is_a_date_rule():
    lo, hi = BUBBLES["00"]
    days = [lo - dt.timedelta(days=1), lo, hi, hi + dt.timedelta(days=1)]
    frame = pl.DataFrame({"game_id": ["x"] * 4, "game_date": [d.isoformat() for d in days]})
    assert frame.select(_neutral_site(frame, "00"))["game_id"].to_list() == [False, True, True, False]
    # a frame without dates (a trimmed fixture) gets the list only: a 2025-26 Cup semifinal
    semi = pl.DataFrame({"game_id": ["0022501229"]})
    assert semi.select(_neutral_site(semi, "00")).item()


def test_possession_rows_drop_the_home_term_for_neutral_games():
    """BB-D3 / BB-D19: neutral possessions get no home term, so the ridge's home term moves."""
    sched = pl.read_parquet(PRO / "nba_schedule_2026.parquet")
    rows = tsr.nba_possession_rate_rows(pl.read_parquet(RATES / "nba_possessions_2026.parquet"), sched)
    listed = set(nba_neutral_site_games([2026])["game_id"])
    in_rows = set(rows.filter(pl.col("neutral_site") == True)["game_id"])  # noqa: E712
    assert in_rows and in_rows <= listed  # the Cup final is not regular season
    from sportsdataverse._common.team_strength import RIDGE_LAMBDA, possession_ridge_ratings

    fit = possession_ridge_ratings(rows, lam=RIDGE_LAMBDA["00"])["hfa"][0]
    home_only = possession_ridge_ratings(rows.with_columns(pl.lit(False).alias("neutral_site")), lam=RIDGE_LAMBDA["00"])
    assert fit != home_only["hfa"][0]


def test_the_sim_plays_neutral_games_without_the_home_term():
    """BB-D18: a neutral game's simulated margin is the home game's less the home term, nothing else."""
    games = sim_mod._regular_games(pl.read_parquet(PRO / "nba_schedule_2026.parquet"), dt.date(2025, 10, 1))
    rows = tsr.nba_possession_rate_rows(
        pl.read_parquet(RATES / "nba_possessions_2026.parquet"), pl.read_parquet(PRO / "nba_schedule_2026.parquet")
    )
    model = sim_mod.as_of_game_model(rows)
    teams = sorted(set(games["home_team_id"]) | set(games["away_team_id"]))
    idx = {t: i for i, t in enumerate(teams)}
    h = np.array([idx[t] for t in games["home_team_id"]])
    a = np.array([idx[t] for t in games["away_team_id"]])
    net, sd = sim_mod._team_arrays(model, teams)
    zero = np.zeros((len(teams), len(teams)), dtype=np.int16)
    played = (zero, np.zeros(len(teams)), np.zeros((len(teams), len(teams))))
    neutral = games["neutral_site"].to_numpy()
    assert neutral.sum() == 5  # Mexico City, Berlin, London and the two Las Vegas semifinals

    def margins(flag: np.ndarray | bool) -> np.ndarray:
        rng = np.random.default_rng(0)
        out = sim_mod._simulate_games(
            model, net, sd, h, a, played, neutral=flag, n_sims=4, wnba=False, uncertain=False, rng=rng
        )
        return out[4]

    gap = margins(False) - margins(neutral)
    assert np.allclose(gap[:, neutral], 2 * model.hfa * model.poss / 100)
    assert np.allclose(gap[:, ~neutral], 0.0)


def test_rated_priors_take_our_flag_over_espns(monkeypatch: pytest.MonkeyPatch) -> None:
    """ESPN's flag is wrong both ways: a stats-matched game takes the list's flag; an unmatched one keeps ESPN's."""
    from sportsdataverse.errors import SeasonNotFoundError
    from sportsdataverse.nba import nba_rated_prior
    from sportsdataverse.wnba import wnba_loaders

    fix = ROOT / "nba_prediction"
    rows = pl.read_parquet(fix / "wnba_rated_prior_asof_2025.parquet")
    games = pl.read_parquet(fix / "wnba_rated_prior_asof_games_2025.parquet")
    espn = games.select(
        pl.lit(2025).alias("season"),
        pl.int_range(pl.len()).alias("game_id"),
        "game_date",
        pl.int_range(pl.len()).alias("home_id"),
        pl.int_range(pl.len()).alias("away_id"),
        pl.lit(True).alias("neutral_site"),  # ESPN marks every game neutral, as it marks the All-Star games
        pl.col("_h").alias("home_display_name"),
        pl.col("_a").alias("away_display_name"),
    )
    all_star = espn.head(1).with_columns(
        pl.lit(10_000, dtype=espn.schema["game_id"]).alias("game_id"), pl.lit("team clark").alias("home_display_name")
    )  # no stats game: nothing of ours to replace ESPN's flag with
    monkeypatch.setattr(wnba_loaders, "load_wnba_schedule", lambda seasons: pl.concat([espn, all_star]))

    def fake(league_id: str, season: int) -> tuple[pl.DataFrame, pl.DataFrame]:
        if season != 2025:
            raise SeasonNotFoundError(f"no {season} in the fixture")
        return rows, games

    monkeypatch.setattr(nba_rated_prior, "_possession_rows", fake)
    out = nba_rated_prior.nba_rated_priors(2025, league_id="10")
    matched = out.filter(pl.col("game_id") != 10_000)
    assert matched.height == games.height and not matched["neutral_site"].any()  # none of these is on the list
    assert out.filter(pl.col("game_id") == 10_000)["neutral_site"].to_list() == [True]
