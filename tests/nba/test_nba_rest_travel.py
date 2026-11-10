"""BB-D23: rest, back-to-backs and travel before each pro game, and their effect in the season sim.

Fixtures (none synthetic): ``tests/fixtures/bb_pro_season`` schedules and possessions and BB-D3's
``tests/fixtures/bb_team_season_rates`` possessions (README in each).

What is checked against the real world:

* **Sites:** the 2026-01-15 Berlin and 2026-01-18 London games (Grizzlies and Magic) are placed
  abroad from ``neutral_site_games.csv``: about 7,800 km and 6-7 time zones out, about 930 km and
  one zone between them, and the same back home.
* **Clocks:** Phoenix keeps standard time, so it is an hour off Los Angeles in winter and level
  with it in summer.
* **Back-to-backs:** the 2025-26 schedule's second nights match consecutive dates.
* **Effect:** the season sim's game model (the as-of ridge, weekly cutoffs) with the rest and
  travel margin scores better log loss and margin MAE on the NBA 2023-24 and 2025-26 regular
  seasons than without it. Both seasons are in the shipped fit (in-sample); the out-of-sample
  evidence is the walk-forward in ``tools/fit_rest_travel.py``, cited at
  :data:`~sportsdataverse.nba.nba_rest_travel.REST_TRAVEL_CONSTANTS`. Gate = observed + 0.002 log
  loss, and the gain at least half the observed one.
* **Applied:** ``nba_season_sim`` passes a nonzero margin to the simulated NBA games and none to
  the WNBA's, and it moves the projection.
"""

from __future__ import annotations

import datetime as dt
import importlib
import warnings
from pathlib import Path

import numpy as np
import polars as pl
import pytest
from scipy.stats import norm

from sportsdataverse.nba import nba_rest_travel as rt

ROOT = Path(__file__).resolve().parents[1] / "fixtures"
RATES, PRO = ROOT / "bb_team_season_rates", ROOT / "bb_pro_season"
tsr = importlib.import_module("sportsdataverse.nba.nba_team_season_rates")
sim_mod = importlib.import_module("sportsdataverse.nba.nba_season_sim")
ORL, MEM = 1610612753, 1610612763
BERLIN, LONDON = "0022500578", "0022500602"


def _schedule(lg: str, season: int) -> pl.DataFrame:
    return pl.read_parquet(PRO / f"{lg}_schedule_{season}.parquet")


def _seq(schedule: pl.DataFrame) -> pl.DataFrame:
    return schedule.filter(pl.col("season_type").is_in(["regular-season", "nba-cup"])).select(
        pl.col("season").cast(pl.Int64),
        pl.col("game_id").cast(pl.Utf8),
        pl.col("game_date").str.slice(0, 10).str.to_date(),
        pl.col("home_team_id").cast(pl.Int64),
        pl.col("away_team_id").cast(pl.Int64),
    )


@pytest.fixture(scope="module")
def nba_2026() -> pl.DataFrame:
    seq = _seq(_schedule("nba", 2026))
    return rt.team_game_rest_travel(seq, "00").join(seq.select("game_id", "game_date"), on="game_id", validate="m:1")


def test_games_abroad_are_placed_abroad(nba_2026: pl.DataFrame) -> None:
    trip = nba_2026.filter(pl.col("team_id").is_in([ORL, MEM])).sort("team_id", "game_date")
    berlin = trip.filter(pl.col("game_id") == BERLIN)
    assert berlin["travel_km"].min() > 7_500 and berlin["travel_km"].max() < 8_100
    assert sorted(berlin["tz_shift"].to_list()) == [6.0, 7.0]  # Orlando (ET) and Memphis (CT) to Berlin
    london = trip.filter(pl.col("game_id") == LONDON)
    assert london["travel_km"].to_list() == pytest.approx([930.0, 930.0], abs=40)
    assert london["tz_shift"].to_list() == [1.0, 1.0]
    home = trip.filter(pl.col("game_date") > dt.date(2026, 1, 18)).group_by("team_id").first()
    assert home["travel_km"].min() > 6_500  # the first game back crosses the Atlantic again
    assert nba_2026.height == 2 * _seq(_schedule("nba", 2026)).height
    assert nba_2026.null_count().sum_horizontal().item() == 0


@pytest.mark.parametrize("league_id", ["00", "10"])
def test_every_neutral_city_has_a_site(league_id: str) -> None:
    """A neutral game added to neutral_site_games.csv without a site row would switch the margin off."""
    from sportsdataverse.nba.nba_neutral_site import nba_neutral_site_games

    lg = {"00": "nba", "10": "wnba"}[league_id]
    sites = rt.game_sites().filter((pl.col("league") == lg) & (pl.col("kind") == "neutral"))
    cities = set(nba_neutral_site_games(league_id=league_id)["city"].to_list())
    assert cities <= set(sites["place"].to_list()), cities - set(sites["place"].to_list())
    assert sites["place"].n_unique() == sites.height


def test_utc_offsets_follow_each_city_and_date() -> None:
    winter, summer = dt.date(2026, 1, 10), dt.date(2025, 7, 10)
    assert rt._utc_offset_hours("America/Phoenix", winter) - rt._utc_offset_hours("America/Los_Angeles", winter) == 1
    assert rt._utc_offset_hours("America/Phoenix", summer) == rt._utc_offset_hours("America/Los_Angeles", summer)


def test_back_to_backs_are_consecutive_dates(nba_2026: pl.DataFrame) -> None:
    gap = nba_2026.sort("team_id", "game_date").with_columns(
        (pl.col("game_date") - pl.col("game_date").shift(1).over("team_id")).dt.total_days().alias("gap")
    )
    assert (gap["b2b"] == (gap["gap"] == 1).fill_null(False).cast(pl.Int64)).all()
    assert (gap.filter(pl.col("three_in_four") == 1)["b2b"].sum()) > 0
    # 2025-26: 441 second nights, 13 to 17 a team
    per_team = gap.group_by("team_id").agg(pl.col("b2b").sum())
    assert (per_team["b2b"].sum(), per_team["b2b"].min(), per_team["b2b"].max()) == (441, 13, 17)
    # three games in four nights: 662 (18 to 26 a team); four in five would count more
    gap2 = gap.with_columns(
        (pl.col("game_date") - pl.col("game_date").shift(2).over("team_id")).dt.total_days().alias("gap2")
    )
    assert (gap2["three_in_four"] == (gap2["gap2"] <= 3).fill_null(False).cast(pl.Int64)).all()
    assert int(gap2["three_in_four"].sum()) == 662


def _rows(lg: str, year: int) -> pl.DataFrame:
    name = f"{lg}_possessions_{year}.parquet"
    path = next(d / name for d in (PRO, RATES) if (d / name).exists())
    return tsr.nba_possession_rate_rows(pl.read_parquet(path), _schedule(lg, year), league_id="00")


def _season_scores(season: int) -> dict[str, float]:
    """Log loss and margin MAE every 28 days from the first game, with and without the rest margin."""
    sched = _schedule("nba", season)
    games = sim_mod._with_rest_travel(sim_mod._regular_games(sched, None), sched, "00").filter(
        pl.col("home_pts").is_not_null()
    )
    rows = _rows("nba", season).join(games.select("game_id", "game_date"), on="game_id", how="inner", validate="m:1")
    assert games.height == 1230 and rows.get_column("game_id").n_unique() == 1230  # every game, none dropped
    prior = None
    if (PRO / f"nba_possessions_{season - 1}.parquet").exists():
        teams = sorted(set(games["home_team_id"].to_list()))
        prior = sim_mod.preseason_prior(_rows("nba", season - 1), teams, league_id="00")
    start, end = games["game_date"].min(), games["game_date"].max()
    base, adj, y, margin = [], [], [], []
    cut = start
    while cut <= end:
        model = sim_mod.as_of_game_model(rows.filter(pl.col("game_date") < cut).drop("game_date"), prior=prior)
        net = dict(zip(model.ratings["team_id"].to_list(), model.ratings["adj_net_rtg"].to_list(), strict=True))
        fut = games.filter(pl.col("game_date") >= cut)
        m = model.margin(
            np.array([net.get(t, 0.0) for t in fut["home_team_id"].to_list()]),
            np.array([net.get(t, 0.0) for t in fut["away_team_id"].to_list()]),
            fut["neutral_site"].to_numpy(),
        )
        base.append(m)
        adj.append(m + fut["margin_adj"].to_numpy())
        margin.append((fut["home_pts"] - fut["away_pts"]).to_numpy().astype(float))
        y.append((fut["home_pts"] > fut["away_pts"]).to_numpy())
        cut += dt.timedelta(days=28)
    yy, mm = np.concatenate(y), np.concatenate(margin)
    assert yy.size > 4_400  # observed 4,487 (2023-24) and 4,490: each game at every cutoff before it

    def ll(m: np.ndarray) -> float:
        p = np.clip(norm.cdf(m / sim_mod.SIM_CONSTANTS["00"][1]), 1e-15, 1 - 1e-15)
        return float(-(yy * np.log(p) + (~yy) * np.log(1 - p)).mean())

    b, a = np.concatenate(base), np.concatenate(adj)
    return {
        "ll_base": ll(b),
        "ll_adj": ll(a),
        "mae_base": float(np.abs(mm - b).mean()),
        "mae_adj": float(np.abs(mm - a).mean()),
    }


# Observed 2026-11-10, log loss / margin MAE without -> with: 2023-24 (no prior: 2022-23 is not
# committed) 0.64174 / 11.793 -> 0.63813 / 11.708; 2025-26 (prior from 2024-25) 0.61357 / 12.297
# -> 0.61134 / 12.242. Gate: (observed + 0.002, half the observed log-loss gain); never lowered.
EFFECT_GATES = {2024: (0.6402, 0.0018), 2026: (0.6134, 0.0011)}


@pytest.mark.parametrize("season", list(EFFECT_GATES))
def test_rest_and_travel_improve_the_game_model(season: int) -> None:
    s = _season_scores(season)
    gate, gain = EFFECT_GATES[season]
    assert s["ll_adj"] <= gate, s
    assert s["ll_base"] - s["ll_adj"] >= gain, s
    assert s["mae_adj"] < s["mae_base"], s


def _sim(lg: str, season: int, **kwargs):  # type: ignore[no-untyped-def]
    from tests.nba.test_nba_season_sim import _sim as sim  # the release loaders patched to the fixtures

    return sim(lg, season, **kwargs)


def test_the_season_sim_applies_it(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, np.ndarray] = {}
    real = sim_mod._simulate_games

    def spy(*args, **kwargs):  # type: ignore[no-untyped-def]
        seen[kwargs.get("wnba") and "wnba" or "nba"] = np.asarray(kwargs["adj"], dtype=float)
        return real(*args, **kwargs)

    monkeypatch.setattr(sim_mod, "_simulate_games", spy)
    as_of = dt.date(2026, 1, 16)
    with_adj = _sim("nba", 2026, as_of_date=as_of, n_sims=2_000)
    _sim("wnba", 2025, as_of_date=dt.date(2025, 7, 16), n_sims=500)
    # observed: 619 games, sd 1.29 points (602 nonzero: zero for two teams in the same state)
    assert seen["nba"].size > 500 and float(np.std(seen["nba"])) > 0.5
    assert not np.any(seen["wnba"])
    monkeypatch.setitem(rt.REST_TRAVEL_CONSTANTS, "00", dict.fromkeys(rt.FEATURES, 0.0))
    without = _sim("nba", 2026, as_of_date=as_of, n_sims=2_000)
    assert not np.any(seen["nba"])
    diff = (with_adj.sort("team_id")["proj_wins"] - without.sort("team_id")["proj_wins"]).abs()
    assert float(diff.max()) > 0.1, diff.max()  # observed 0.48 wins


def test_the_live_season_places_every_game() -> None:
    """2026-27 a fortnight in (TBD Cup knockout games listed): every game placed, no fallback."""
    sched = _schedule("nba", 2027)
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        games = sim_mod._with_rest_travel(sim_mod._regular_games(sched, None), sched, "00")
    assert games.height == 1200 and games["margin_adj"].null_count() == 0
    assert float(games["margin_adj"].std()) > 0.5  # type: ignore[operator]


def test_a_missing_site_warns_and_adjusts_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    sites = rt.game_sites().filter(pl.col("team") != "ORL")
    monkeypatch.setattr(rt, "game_sites", lambda: sites)
    sched = _schedule("nba", 2026)
    with pytest.warns(UserWarning, match="no rest-and-travel adjustment"):
        games = sim_mod._with_rest_travel(sim_mod._regular_games(sched, None), sched, "00")
    assert games.height == 1230 and (games["margin_adj"] == 0.0).all()
