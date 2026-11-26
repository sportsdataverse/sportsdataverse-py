"""NBA player-prop rates shrink toward a minutes-and-role tier prior (``rate_prior_minutes``).

Corpus: ``player_box_rate_prior_2025_2026.parquet``, the real ESPN release rows of Boston (2) and New York (18):
all of 2024-25 (regular season and playoffs) and 2025-26 before 2025-11-08. The oracle below recomputes tiers and
pooled rates in plain Python, independent of the polars code under test.
"""

from __future__ import annotations

import importlib
import math
from collections import defaultdict
from datetime import date
from pathlib import Path

import numpy as np
import polars as pl
import pytest
from scipy import stats as st

from sportsdataverse.nba.nba_prediction_constants import get_constants

props = importlib.import_module("sportsdataverse.nba.nba_player_props")
FIX = Path(__file__).resolve().parents[1] / "fixtures" / "nba_prediction" / "player_box_rate_prior_2025_2026.parquet"
CUT = date(2025, 11, 1)
STATS = ("pts", "reb", "ast", "fg3m", "stl", "blk", "tov", "fgm", "ftm")


@pytest.fixture
def logs(monkeypatch):
    box = pl.read_parquet(FIX)
    loaders = importlib.import_module("sportsdataverse.nba.nba_loaders")
    monkeypatch.setattr(
        loaders, "load_nba_player_boxscore", lambda seasons: box.filter(pl.col("season").is_in(seasons))
    )
    cur = props._load_player_logs(2026, "00").filter(pl.col("game_date") < CUT)
    prev_all = props._load_player_logs(2025, "00")
    return cur, prev_all


def _oracle(cur: pl.DataFrame, prev: pl.DataFrame, k: dict[str, float]) -> dict[str, dict[str, float]]:
    def profile(frame):
        per = defaultdict(lambda: {"min": 0.0, "g": 0, "starts": 0, **{s: 0.0 for s in STATS}})
        for r in frame.iter_rows(named=True):
            if r["minutes"] is None or r["minutes"] <= 0:
                continue
            p = per[r["player_id"]]
            p["min"] += r["minutes"]
            p["g"] += 1
            p["starts"] += bool(r["starter"])
            for s in STATS:
                p[s] += r[s]
        return per

    def tier(p):
        mpg = p["min"] / p["g"]
        return 2 * sum(mpg >= e for e in (12.0, 20.0, 28.0)) + (p["starts"] / p["g"] >= 0.5)

    pooled = defaultdict(lambda: {"min": 0.0, **{s: 0.0 for s in STATS}})
    current = profile(cur)
    for frame in (current, profile(prev)):
        for p in frame.values():
            t = pooled[tier(p)]
            t["min"] += p["min"]
            for s in STATS:
                t[s] += p[s]
    out = {}
    for pid, p in current.items():
        t = pooled[tier(p)]
        out[pid] = {s: (p[s] + k[s] * t[s] / t["min"]) / (p["min"] + k[s]) for s in STATS}
    return out


def test_shrunk_rates_match_the_tier_prior_formula(logs) -> None:
    cur, prev_all = logs
    prev = prev_all.filter(pl.col("season_type") == 2)
    k = get_constants("00").rate_prior_minutes
    assert set(k) == set(STATS) and all(v > 0 for v in k.values())
    want = _oracle(cur, prev, k)
    got = props.player_rates(cur, prior_logs=prev, league_id="00")
    assert got.height == len(want) > 20
    for r in got.iter_rows(named=True):
        for s in STATS:
            assert r[f"{s}_per_min"] == pytest.approx(want[r["player_id"]][s], rel=1e-12), (r["player_id"], s)


def test_shrinkage_moves_short_samples_and_raw_rates_stay_raw(logs) -> None:
    cur, prev_all = logs
    prev = prev_all.filter(pl.col("season_type") == 2)
    shrunk = props.player_rates(cur, prior_logs=prev, league_id="00")
    raw = props.player_rates(cur)
    both = shrunk.join(raw, on="player_id", suffix="_raw")
    short = both.filter(pl.col("games") <= 3)
    assert short.height >= 3
    # pts k = 50 pseudo-minutes against at most ~100 real minutes: the shrinkage share is >= 1/3
    assert (short["pts_per_min"] - short["pts_per_min_raw"]).abs().max() > 0.02
    totals = (
        cur.filter(pl.col("minutes") > 0)
        .group_by("player_id")
        .agg((pl.col("stl").sum() / pl.col("minutes").sum()).alias("r"))
    )
    assert raw.join(totals, on="player_id")["stl_per_min"].to_list() == pytest.approx(
        raw.join(totals, on="player_id")["r"].to_list()
    )
    # no fitted k for the WNBA or the G-League: their rates are the raw rates
    for league_id in ("10", "20"):
        assert props.player_rates(cur, prior_logs=prev, league_id=league_id).equals(raw)


def test_nba_player_props_pools_the_previous_regular_season_only(monkeypatch, logs) -> None:
    cur, prev_all = logs
    ratings = pl.DataFrame({"team_id": ["2", "18"], "adj_pace": [100.0, 98.0]})
    monkeypatch.setattr(props, "_load_ratings", lambda season, league_id, as_of_date=None: ratings)
    out = props.nba_player_props(2026, "0", "2", "18", league_id="00", as_of_date=CUT)
    pace_factor = (100.0 * 98.0 / get_constants("00").avg_pace) / get_constants("00").avg_pace
    k = get_constants("00").rate_prior_minutes
    regular = _oracle(cur, prev_all.filter(pl.col("season_type") == 2), k)
    with_playoffs = _oracle(cur, prev_all, k)
    mpg = dict(cur.filter(pl.col("minutes") > 0).group_by("player_id").agg(pl.col("minutes").mean()).iter_rows())
    assert out.height == len(regular)
    moved = 0
    for r in out.iter_rows(named=True):
        pid = r["player_id"]
        for s in STATS:
            assert r[f"stat_{s}_exp"] == pytest.approx(regular[pid][s] * mpg[pid] * pace_factor, rel=1e-12)
        moved += not math.isclose(regular[pid]["pts"], with_playoffs[pid]["pts"], rel_tol=1e-6)
    assert moved > 0  # the playoff rows would change the pooled tier rates, so the filter is observable


def test_a_stat_the_release_lacks_gets_no_rate(logs) -> None:
    """The loader null-fills a missing release column; that stat's rate (and line) is null, not 0."""
    cur, prev_all = logs
    blank = cur.with_columns(pl.lit(None, dtype=pl.Float64).alias("stl"))
    for kwargs in ({}, {"prior_logs": prev_all.filter(pl.col("season_type") == 2), "league_id": "00"}):
        r = props.player_rates(blank, **kwargs)
        assert r["stl_per_min"].null_count() == r.height
        assert r["blk_per_min"].null_count() == 0


def test_a_stat_last_season_lacks_pools_this_season_only(logs) -> None:
    """A stat null-filled in the prior frame must not drag the tier rate toward 0: it pools this season alone."""
    cur, prev_all = logs
    prev = prev_all.filter(pl.col("season_type") == 2)
    blank = props.player_rates(
        cur, prior_logs=prev.with_columns(pl.lit(None, dtype=pl.Float64).alias("stl")), league_id="00"
    )
    alone = props.player_rates(cur, league_id="00")
    full = props.player_rates(cur, prior_logs=prev, league_id="00")
    assert blank["stl_per_min"].to_list() == pytest.approx(alone["stl_per_min"].to_list(), rel=1e-12)
    assert blank["blk_per_min"].to_list() == pytest.approx(full["blk_per_min"].to_list(), rel=1e-12)
    assert blank["blk_per_min"].to_list() != pytest.approx(alone["blk_per_min"].to_list(), rel=1e-6)


def test_shrinkage_needs_starter_values(logs) -> None:
    cur, prev_all = logs
    with pytest.raises(ValueError, match="needs a starter column in box_logs"):
        props.player_rates(cur.drop("starter"), league_id="00")
    # the loader null-fills a starter column the release lacks: all-null must raise too, not tier everyone bench
    no_starts = prev_all.with_columns(pl.lit(None, dtype=pl.Boolean).alias("starter"))
    with pytest.raises(ValueError, match="needs a starter column in prior_logs"):
        props.player_rates(cur, prior_logs=no_starts, league_id="00")
    assert props.player_rates(cur.drop("starter")).height > 0  # raw rates never read it


def test_nba_player_props_raises_when_the_prior_season_is_empty(monkeypatch, logs) -> None:
    """A prior season without season_type (null-filled) would filter to nothing: raise, never project prior-free."""
    box = pl.read_parquet(FIX)
    loaders = importlib.import_module("sportsdataverse.nba.nba_loaders")
    monkeypatch.setattr(
        loaders,
        "load_nba_player_boxscore",
        lambda seasons: (
            box.filter(pl.col("season").is_in(seasons)).drop("season_type")
            if 2025 in seasons
            else box.filter(pl.col("season").is_in(seasons))
        ),
    )
    monkeypatch.setattr(
        props,
        "_load_ratings",
        lambda season, league_id, as_of_date=None: pl.DataFrame({"team_id": ["2"], "adj_pace": [100.0]}),
    )
    with pytest.raises(ValueError, match="no 2025 regular-season box logs"):
        props.nba_player_props(2026, "0", "2", "18", league_id="00", as_of_date=CUT)


def _crps(mu: float, stat: str, y: float) -> float:
    fam, par = props.prop_distribution(mu, stat)
    if fam == "normal":
        sd = par["sd"]
        z = (y - par["mu"]) / sd
        return sd * (z * (2 * st.norm.cdf(z) - 1) + 2 * st.norm.pdf(z) - 1 / math.sqrt(math.pi))
    k = np.arange(51)
    cdf = st.poisson.cdf(k, par["mu"]) if fam == "poisson" else st.nbinom.cdf(k, par["r"], par["p"])
    return float(((cdf - (k >= y)) ** 2).sum())


# Observed 2026-11-25 (179 played rows; Boston / New York, 2025-10-21..2025-11-07, a fit season, so this is a
# regression gate, not evidence): relative CRPS gain pts .025, reb .061, ast .059, fg3m .069, stl .144, blk .142,
# tov .145, fgm .038, ftm .109. Floors are those values rounded down.
EARLY_SEASON_GAIN_FLOOR = {
    "pts": 0.02,
    "reb": 0.06,
    "ast": 0.05,
    "fg3m": 0.06,
    "stl": 0.14,
    "blk": 0.14,
    "tov": 0.14,
    "fgm": 0.03,
    "ftm": 0.10,
}


def test_shrunk_lines_beat_raw_lines_early_in_the_season(logs) -> None:
    """Walk the fixture's first 2025-26 weeks as of each date: the shipped (shrunk) predictive scores better."""
    cur, prev_all = logs
    prev = prev_all.filter(pl.col("season_type") == 2)
    played = props._load_player_logs(2026, "00").filter(pl.col("minutes") > 0)
    total = {s: [0.0, 0.0] for s in STATS}
    n = 0
    for d in played["game_date"].unique().sort().to_list():
        hist = played.filter(pl.col("game_date") < d)
        if hist.height == 0:
            continue
        raw = props.player_rates(hist)
        shrunk = props.player_rates(hist, prior_logs=prev, league_id="00")
        day = (
            played.filter(pl.col("game_date") == d).join(raw, on="player_id").join(shrunk, on="player_id", suffix="_s")
        )
        for r in day.iter_rows(named=True):
            n += 1
            for s in STATS:
                total[s][0] += _crps(r[f"{s}_per_min"] * r["minutes_pg"], s, r[s])
                total[s][1] += _crps(r[f"{s}_per_min_s"] * r["minutes_pg"], s, r[s])
    assert n == 179
    gain = {s: (a - b) / a for s, (a, b) in total.items()}
    assert all(gain[s] >= EARLY_SEASON_GAIN_FLOOR[s] for s in STATS), gain


@pytest.mark.parametrize("stat", ["stl", "blk", "tov", "fgm", "ftm"])
def test_extra_stats_have_an_nba_distribution_and_no_other_league_one(stat: str) -> None:
    disp = get_constants("00").stat_dispersion[stat]
    fam, par = props.prop_distribution(2.0, stat)
    if disp <= 1.0001:
        assert fam == "poisson" and par["mu"] == pytest.approx(2.0)
        sf = st.poisson.sf(1, 2.0)
    else:
        r, p = par["r"], par["p"]
        assert fam == "nbinom"
        assert r * (1 - p) / p == pytest.approx(2.0)  # mean
        assert r * (1 - p) / p**2 == pytest.approx(disp * 2.0)  # var = dispersion * mean
        sf = st.nbinom.sf(1, r, p)
    assert props.prob_over(2.0, 1.5, stat) == pytest.approx(sf, rel=1e-12)
    for league_id in ("10", "20"):
        with pytest.raises(ValueError, match="no fitted dispersion"):
            props.prop_distribution(2.0, stat, league_id=league_id)
