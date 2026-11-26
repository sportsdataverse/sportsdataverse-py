"""NBA player props: the "recent window is a new rate" mixture (``recent_mixture``).

Corpus: the real ESPN release rows of Boston (2) and New York (18): 2023-24 regular season
(``player_box_rate_prior_2024.parquet``, the 2024-25 tier prior) and 2024-25 with playoffs
(``player_box_rate_prior_2025_2026.parquet``). The oracle recomputes tiers, pooled rates, last-10
windows and weights in plain Python, independent of the polars code under test.
"""

from __future__ import annotations

import importlib
import math
import statistics
from collections import defaultdict
from datetime import date
from pathlib import Path

import numpy as np
import polars as pl
import pytest
from scipy import stats as st

from sportsdataverse.nba.nba_prediction_constants import get_constants

props = importlib.import_module("sportsdataverse.nba.nba_player_props")
FIX = Path(__file__).resolve().parents[1] / "fixtures" / "nba_prediction"
STATS = ("pts", "reb", "ast", "fg3m", "stl", "blk", "tov", "fgm", "ftm")
CUT = date(2025, 2, 15)
PACE = {"2": 100.0, "18": 98.0}


@pytest.fixture
def stubbed(monkeypatch):
    box = pl.concat(
        [
            pl.read_parquet(FIX / "player_box_rate_prior_2024.parquet"),
            pl.read_parquet(FIX / "player_box_rate_prior_2025_2026.parquet"),
        ]
    )
    loaders = importlib.import_module("sportsdataverse.nba.nba_loaders")
    monkeypatch.setattr(
        loaders, "load_nba_player_boxscore", lambda seasons: box.filter(pl.col("season").is_in(seasons))
    )
    ratings = pl.DataFrame({"team_id": list(PACE), "adj_pace": list(PACE.values())})
    monkeypatch.setattr(props, "_load_ratings", lambda season, league_id, as_of_date=None: ratings)
    return box


def _oracle(cur: pl.DataFrame, prev: pl.DataFrame) -> dict[str, dict]:
    """Per player: the recent (last-10) shrunk rates, last-10 mean minutes and the mixture weight."""
    c = get_constants("00")
    k, mix = c.rate_prior_minutes, c.recent_mixture

    def played(frame):
        rows = defaultdict(list)
        for r in frame.sort("game_date", "game_id").iter_rows(named=True):
            if r["minutes"] is not None and r["minutes"] > 0:
                rows[r["player_id"]].append(r)
        return rows

    def tier(games):
        mpg = sum(g["minutes"] for g in games) / len(games)
        starts = sum(bool(g["starter"]) for g in games) / len(games)
        return 2 * sum(mpg >= e for e in (12.0, 20.0, 28.0)) + (starts >= 0.5)

    pool = defaultdict(lambda: defaultdict(float))
    current = played(cur)
    for frame in (current, played(prev)):
        for games in frame.values():
            t = pool[tier(games)]
            t["min"] += sum(g["minutes"] for g in games)
            for s in STATS:
                t[s] += sum(g[s] for g in games)
    out = {}
    for pid, games in current.items():
        t, last = pool[tier(games)], games[-int(mix["games"]) :]
        mins = sum(g["minutes"] for g in last)
        mpg, mpg10 = statistics.fmean(g["minutes"] for g in games), mins / len(last)
        sd = statistics.stdev(g["minutes"] for g in games) if len(games) > 1 else float("nan")
        sd = max(sd, mix["sd_floor"]) if math.isfinite(sd) else mix["sd_fallback"]
        w = 1 / (1 + math.exp(-(mix["a"] + mix["b"] * abs(mpg10 - mpg) / sd))) if len(games) > mix["games"] else 0.0
        rate = {s: (sum(g[s] for g in last) + k[s] * t[s] / t["min"]) / (mins + k[s]) for s in STATS}
        out[pid] = {"w": w, "mpg10": mpg10, "rate": rate, "games": len(games)}
    return out


def test_recent_lines_and_weights_match_the_oracle(stubbed) -> None:
    out = props.nba_player_props(2025, "0", "2", "18", league_id="00", as_of_date=CUT)
    cur = props._load_player_logs(2025, "00").filter(pl.col("game_date") < CUT)
    prev = props._load_player_logs(2024, "00").filter(pl.col("season_type") == 2)
    want = _oracle(cur, prev)
    pace = PACE["2"] * PACE["18"] / get_constants("00").avg_pace / get_constants("00").avg_pace
    assert out.height == len(want) > 20
    mixed = 0
    for r in out.iter_rows(named=True):
        o = want[r["player_id"]]
        assert r["recent_weight"] == pytest.approx(o["w"], rel=1e-12, abs=1e-15)
        for s in STATS:
            assert r[f"stat_{s}_recent_exp"] == pytest.approx(o["rate"][s] * o["mpg10"] * pace, rel=1e-12)
        mixed += o["games"] > 10 and abs(r["stat_pts_recent_exp"] - r["stat_pts_exp"]) > 0.5
    assert mixed >= 5  # the recent line is a different line for players with a long sample


def test_short_samples_and_other_leagues_get_no_mixture(stubbed) -> None:
    out = props.nba_player_props(2025, "0", "2", "18", league_id="00", as_of_date=date(2024, 11, 1))
    assert (out["recent_weight"] == 0.0).all()  # nobody has 11 games yet: the two windows coincide
    for s in STATS:
        assert out[f"stat_{s}_recent_exp"].to_list() == pytest.approx(out[f"stat_{s}_exp"].to_list(), rel=1e-12)
    rates = pl.DataFrame({"player_id": ["P"], "games": [30], "minutes_pg": [30.0]})
    for league_id in ("10", "20"):
        none = props._with_recent(rates, pl.DataFrame(), None, league_id, 1.0)
        assert none["recent_weight"].null_count() == 1 and none["stat_pts_recent_exp"].null_count() == 1


def _abs_moment(m: float, v: float) -> float:
    s = math.sqrt(v)
    return s * math.sqrt(2 / math.pi) * math.exp(-m * m / (2 * v)) + m * (1 - 2 * st.norm.cdf(-m / s))


def _crps(fam: str, par: dict, y: float) -> float:
    """Exact CRPS of a prop_distribution output (Normal mixtures closed form; counts over the support)."""
    comps = list(zip(par["weights"], par["components"], strict=True)) if fam == "mixture" else [(1.0, (fam, par))]
    if comps[0][1][0] == "normal":
        mv = [(w, p["mu"], p["sd"] ** 2) for w, (_, p) in comps]
        return sum(w * _abs_moment(m - y, v) for w, m, v in mv) - 0.5 * sum(
            wi * wj * _abs_moment(mi - mj, vi + vj) for wi, mi, vi in mv for wj, mj, vj in mv
        )
    k = np.arange(51)
    cdf = sum(
        w * (st.poisson.cdf(k, p["mu"]) if f == "poisson" else st.nbinom.cdf(k, p["r"], p["p"])) for w, (f, p) in comps
    )
    return float(((cdf - (k >= y)) ** 2).sum())


# Observed 2026-11-25 on the 2024-25 Boston / New York rows with recent_weight > 0 (a fit season, so a
# regression gate, not evidence): relative CRPS gain of the mixture over the single (shrunk) predictive,
# pts .0057, reb .0097, ast .0077 over 1,276 rows. Floors are those values rounded down; never lower a gate --
# debug the model.
GAIN_FLOOR = {"pts": 0.005, "reb": 0.009, "ast": 0.007}


def test_the_mixture_beats_the_single_predictive_on_real_rows(stubbed) -> None:
    played = props._load_player_logs(2025, "00").filter((pl.col("minutes") > 0) & (pl.col("season_type") == 2))
    total = {s: [0.0, 0.0] for s in GAIN_FLOOR}
    n = 0
    for d in played["game_date"].unique().sort().to_list():
        out = props.nba_player_props(2025, "0", "2", "18", league_id="00", as_of_date=d)
        day = played.filter(pl.col("game_date") == d).join(out.filter(pl.col("recent_weight") > 0), on="player_id")
        for r in day.iter_rows(named=True):
            n += 1
            for s in GAIN_FLOOR:
                single = props.prop_distribution(r[f"stat_{s}_exp"], s)
                mix = props.prop_distribution(
                    r[f"stat_{s}_exp"], s, recent_value=r[f"stat_{s}_recent_exp"], recent_weight=r["recent_weight"]
                )
                total[s][0] += _crps(*single, r[s])
                total[s][1] += _crps(*mix, r[s])
    assert n > 1000
    gain = {s: (a - b) / a for s, (a, b) in total.items()}
    assert all(gain[s] >= GAIN_FLOOR[s] for s in GAIN_FLOOR), (n, gain)


def test_mixture_prices_as_the_weighted_components() -> None:
    fam, par = props.prop_distribution(20.0, "pts", recent_value=26.0, recent_weight=0.3)
    mix = get_constants("00").recent_mixture
    assert fam == "mixture" and par["weights"] == pytest.approx([0.7, 0.3])
    assert [p["sd"] for _, p in par["components"]] == pytest.approx(
        [mix["pts_sd_a"] + mix["pts_sd_b"] * math.sqrt(m) for m in (20.0, 26.0)]
    )
    want = 0.7 * st.norm.sf(22.5, 20.0, par["components"][0][1]["sd"]) + 0.3 * st.norm.sf(
        22.5, 26.0, par["components"][1][1]["sd"]
    )
    assert props.prob_over(20.0, 22.5, "pts", recent_value=26.0, recent_weight=0.3) == pytest.approx(want, rel=1e-12)
    # weight 0 / None is the single shipped distribution
    assert props.prob_over(20.0, 22.5, "pts", recent_value=26.0, recent_weight=0.0) == props.prob_over(
        20.0, 22.5, "pts"
    )
    fam, par = props.prop_distribution(4.0, "reb", recent_value=6.0, recent_weight=0.5)
    assert props.prob_over(4.0, 4.5, "reb", recent_value=6.0, recent_weight=0.5) == pytest.approx(
        0.5 * props.prob_over(4.0, 4.5, "reb") + 0.5 * props.prob_over(6.0, 4.5, "reb"), rel=1e-12
    )
    with pytest.raises(ValueError, match="fitted mixture"):
        props.prop_distribution(20.0, "pts", league_id="10", recent_value=26.0, recent_weight=0.3)
    with pytest.raises(ValueError, match="recent_value"):
        props.prop_distribution(20.0, "pts", recent_weight=0.3)


def test_recent_lines_never_see_the_target_or_a_later_game(monkeypatch, stubbed) -> None:
    """Own-line and later-line probes for the new columns: inflate pts and minutes on and after the cut."""
    before = props.nba_player_props(2025, "0", "2", "18", league_id="00", as_of_date=CUT)
    late = (pl.col("season") == 2025) & (pl.col("game_date") >= CUT)
    poisoned = stubbed.with_columns(
        pl.when(late).then(pl.col("points") * 10).otherwise(pl.col("points")).alias("points"),
        pl.when(late).then(pl.col("minutes") + 20).otherwise(pl.col("minutes")).alias("minutes"),
    )
    loaders = importlib.import_module("sportsdataverse.nba.nba_loaders")
    monkeypatch.setattr(
        loaders, "load_nba_player_boxscore", lambda seasons: poisoned.filter(pl.col("season").is_in(seasons))
    )
    after = props.nba_player_props(2025, "0", "2", "18", league_id="00", as_of_date=CUT)
    cols = ["recent_weight", *(f"stat_{s}_recent_exp" for s in STATS)]
    assert before.select("player_id", *cols).equals(after.select("player_id", *cols))


def test_the_weight_starts_at_the_eleventh_game(stubbed) -> None:
    """10 earlier games: the windows coincide, weight 0; 11: the mixture is on (the ``games > 10`` cutoff)."""
    played = props._load_player_logs(2025, "00").filter(pl.col("minutes") > 0)
    seen = {10: False, 11: False}
    for d in played["game_date"].unique().sort().to_list():
        out = props.nba_player_props(2025, "0", "2", "18", league_id="00", as_of_date=d)
        for g in (10, 11):
            rows = out.join(props.player_rates(played.filter(pl.col("game_date") < d)), on="player_id").filter(
                pl.col("games") == g
            )
            if rows.height:
                seen[g] = True
                assert ((rows["recent_weight"] > 0) == (g == 11)).all(), (d, g)
        if all(seen.values()):
            break
    assert all(seen.values()), seen
