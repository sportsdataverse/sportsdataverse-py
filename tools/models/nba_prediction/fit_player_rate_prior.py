"""Fit the NBA player-prop rate prior: ``rate_prior_minutes`` (k per stat) and ``stat_dispersion``.

Run (network: the ESPN player box, schedule and team box releases):

    uv run python tools/models/nba_prediction/fit_player_rate_prior.py

Fit seasons: 2024-25 and 2025-26 regular seasons (2023-24 enters only as the 2024-25 tier prior).

1. ``stat_dispersion`` for stl/blk/tov/fgm/ftm, by ``fit_player_props.py``'s method: rotation
   player-seasons (>= 15 games, >= 15 mpg), median single-game var/mean.
2. ``rate_prior_minutes``: one row per played regular-season player-game whose player has >= 1
   earlier game that season, every input pregame (his earlier games only; the tier prior from games
   before the date plus the whole previous regular season; pace from the as-of team ratings). For
   each stat, k minimizes the mean CRPS of the shipped predictive (Normal points with the league SD
   law; counts Negative-Binomial with var = dispersion * mean) over a grid of pseudo-minutes, then a
   x1.25 grid around the best value. Paste both into ``LEAGUE_CONSTANTS["00"]``.

Held-out check of the shipped constants (run once per window, after the fit is frozen):

    uv run python tools/models/nba_prediction/fit_player_rate_prior.py --holdout 2022

scores raw against shrunk rates on that season's rows (the previous season as the tier prior), with
bootstrap SEs clustered by game day and, separately, by player. 2022 (2021-22) is the window the
committed constants were checked on, 2026-11-25.

The recent-window mixture (``recent_mixture``), on the same fit rows with E1 (shrunk) rates:

    uv run python tools/models/nba_prediction/fit_player_rate_prior.py --mixture
    uv run python tools/models/nba_prediction/fit_player_rate_prior.py --holdout-mixture 2019

``--mixture`` fits the weight ``logistic(a + b |dmin| / sd)`` (grid, then Nelder-Mead on the summed
relative CRPS of pts / reb / ast over rows with >= 11 earlier games, components on the single points
SD law), then the components' points SD law with a and b frozen (Nelder-Mead on points CRPS).
``--holdout-mixture`` scores the committed mixture against the single shrunk predictive on one
held-out season; 2019 (2018-19) is the window it was checked on, 2026-11-25.
"""

from __future__ import annotations

import math
import sys

import numpy as np
import polars as pl
from scipy import stats as st
from scipy.optimize import minimize

from sportsdataverse._temporal import as_date
from sportsdataverse.nba.nba_loaders import load_nba_player_boxscore, load_nba_schedule, load_nba_team_boxscore
from sportsdataverse.nba.nba_player_props import _STATS, _rate_tier, player_rates
from sportsdataverse.nba.nba_prediction_constants import as_of_ratings_split, get_constants
from sportsdataverse.nba.nba_team_ratings import _normalize_schedule, adjust_pace, raw_game_efficiency

FIT_SEASONS = (2025, 2026)
SRC = {
    "pts": "points",
    "reb": "rebounds",
    "ast": "assists",
    "fg3m": "three_point_field_goals_made",
    "stl": "steals",
    "blk": "blocks",
    "tov": "turnovers",
    "fgm": "field_goals_made",
    "ftm": "free_throws_made",
}
EXTRA = ("stl", "blk", "tov", "fgm", "ftm")
GRID = (0, 5, 10, 20, 40, 80, 160, 320, 640, 1280, 2560, 5120)
KMAX = 50


def box(season: int) -> pl.DataFrame:
    b = load_nba_player_boxscore([season])
    return (
        b.select(
            pl.col("game_id").cast(pl.Int64, strict=False).cast(pl.Utf8),
            as_date(pl.col("game_date")).alias("game_date"),
            pl.col("season_type").cast(pl.Int64),
            pl.col("athlete_id").cast(pl.Int64, strict=False).cast(pl.Utf8).alias("player_id"),
            pl.col("team_id").cast(pl.Int64, strict=False).cast(pl.Utf8),
            (pl.col("home_away") == "home").alias("is_home"),
            pl.col("opponent_team_id").cast(pl.Int64, strict=False).cast(pl.Utf8).alias("opp_team_id"),
            pl.col("minutes").cast(pl.Float64),
            pl.col("starter").cast(pl.Float64).fill_null(0.0),
            *[pl.col(c).cast(pl.Float64).fill_null(0.0).alias(s) for s, c in SRC.items()],
        )
        .drop_nulls(["minutes", "player_id", "game_date"])
        .filter(pl.col("minutes") > 0)
        .unique(["game_id", "player_id"], keep="first")
        .sort("player_id", "game_date", "game_id")
    )


def tier_sums(per: pl.DataFrame) -> pl.DataFrame:
    """Pool per-player totals (``_min``, ``_<stat>``, tiered by ``_mpg``/``_ss``) by tier."""
    return (
        per.with_columns(_rate_tier(pl.col("_mpg"), pl.col("_ss")))
        .group_by("_tier")
        .agg(pl.col("_min").sum(), *[pl.col(f"_{s}").sum() for s in _STATS])
    )


def season_rows(season: int) -> pl.DataFrame:
    """Pregame rows for one season (see the module docstring)."""
    cur, prev = box(season), box(season - 1).filter(pl.col("season_type") == 2)
    prev_sums = tier_sums(
        prev.group_by("player_id").agg(
            pl.col("minutes").sum().alias("_min"),
            pl.col("minutes").mean().alias("_mpg"),
            pl.col("starter").mean().alias("_ss"),
            *[pl.col(s).sum().alias(f"_{s}") for s in _STATS],
        )
    )
    feats = cur.with_columns(
        pl.int_range(pl.len()).over("player_id").alias("g"),
        *[(pl.col(c).cum_sum().over("player_id") - pl.col(c)).alias(f"c_{c}") for c in ("minutes", "starter", *_STATS)],
        *[
            pl.col(c).shift(1).rolling_sum(10, min_samples=1).over("player_id").alias(f"l10_{c}")
            for c in ("minutes", *_STATS)
        ],
        (pl.col("minutes").pow(2).cum_sum().over("player_id") - pl.col("minutes").pow(2)).alias("c_min2"),
    ).with_columns(
        (pl.col("c_minutes") / pl.col("g")).alias("mpg"),
        (pl.col("l10_minutes") / pl.min_horizontal(pl.col("g"), pl.lit(10))).alias("mpg10"),
        ((pl.col("c_min2") - pl.col("c_minutes").pow(2) / pl.col("g")) / (pl.col("g") - 1))
        .clip(0.0)
        .sqrt()
        .alias("min_sd"),
        _rate_tier(pl.col("c_minutes") / pl.col("g"), pl.col("c_starter") / pl.col("g")),
    )
    state = cur.with_columns(
        pl.col("minutes").cum_sum().over("player_id").alias("_min"),
        (pl.col("minutes").cum_sum().over("player_id") / pl.int_range(1, pl.len() + 1).over("player_id")).alias("_mpg"),
        (pl.col("starter").cum_sum().over("player_id") / pl.int_range(1, pl.len() + 1).over("player_id")).alias("_ss"),
        *[pl.col(s).cum_sum().over("player_id").alias(f"_{s}") for s in _STATS],
    )
    sched = _normalize_schedule(load_nba_schedule([season]))
    team_box = load_nba_team_boxscore([season])
    avg = get_constants("00").avg_pace
    targets = feats.filter((pl.col("season_type") == 2) & (pl.col("g") >= 1))
    out = []
    dates = targets["game_date"].unique().sort().to_list()
    for d in dates:
        last = (
            state.filter(pl.col("game_date") < d)
            .group_by("player_id")
            .agg(pl.all().sort_by("game_date", "game_id").last())
        )
        prior = (
            pl.concat([tier_sums(last), prev_sums.select(tier_sums(last).columns)])
            .group_by("_tier")
            .agg(pl.all().sum())
            .select("_tier", *[(pl.col(f"_{s}") / pl.col("_min")).alias(f"prior_{s}") for s in _STATS])
        )
        pace = adjust_pace(raw_game_efficiency(as_of_ratings_split(sched, d), team_box))
        p = {
            t: v
            for t, v in zip(pace["team_id"].to_list(), pace["adj_pace"].to_list(), strict=True)
            if v is not None and math.isfinite(v)
        }
        day = targets.filter(pl.col("game_date") == d).join(prior, on="_tier", how="left")
        # team_pace_projection / avg_pace: a team with no ratings row yet gets the league pace
        pf = [
            p.get(t, avg) * p.get(o, avg) / avg / avg for t, o in zip(day["team_id"], day["opp_team_id"], strict=True)
        ]
        out.append(day.with_columns(pl.Series("pace_factor", pf, dtype=pl.Float64)))
        if d == dates[len(dates) // 4]:  # this pooling must be the package's: check it against player_rates
            k = get_constants("00").rate_prior_minutes
            ship = player_rates(cur.filter(pl.col("game_date") < d), prior_logs=prev, league_id="00")
            both = day.join(ship, on="player_id")
            assert both.height == day.height, (d, both.height, day.height)
            for s in _STATS:
                mine = (both[f"c_{s}"] + k[s] * both[f"prior_{s}"]) / (both["c_minutes"] + k[s])
                assert np.allclose(mine.to_numpy(), both[f"{s}_per_min"].to_numpy(), rtol=1e-12), (d, s)
            # and the recent window / minutes SD must be the package's recent_games=10 and std()
            hist = cur.filter(pl.col("game_date") < d)
            recent = player_rates(hist, prior_logs=prev, league_id="00", recent_games=10)
            sd = hist.group_by("player_id").agg(pl.col("minutes").std().alias("_sd"))
            both = day.join(recent, on="player_id").join(sd, on="player_id")
            assert both.height == day.height, (d, both.height, day.height)
            assert np.allclose(both["mpg10"].to_numpy(), both["minutes_pg"].to_numpy(), rtol=1e-12), d
            assert np.allclose(both["min_sd"].to_numpy(), both["_sd"].to_numpy(), rtol=1e-9, equal_nan=True), d
            for s in _STATS:
                mine = (both[f"l10_{s}"] + k[s] * both[f"prior_{s}"]) / (both["l10_minutes"] + k[s])
                assert np.allclose(mine.to_numpy(), both[f"{s}_per_min"].to_numpy(), rtol=1e-12), (d, s, "recent")
    return pl.concat(out)


def crps_normal(mu: np.ndarray, sd: np.ndarray, y: np.ndarray) -> np.ndarray:
    z = (y - mu) / sd
    return sd * (z * (2 * st.norm.cdf(z) - 1) + 2 * st.norm.pdf(z) - 1 / math.sqrt(math.pi))


def crps_count(mu: np.ndarray, disp: float, y: np.ndarray) -> np.ndarray:
    k = np.arange(KMAX + 1)[None, :]
    m = np.maximum(mu, 1e-6)[:, None]
    if disp <= 1.0001:
        F = st.poisson.cdf(k, m)
    else:
        r = m / (disp - 1.0)
        F = st.nbinom.cdf(k, r, r / (r + m))
    return ((F - (k >= y[:, None])) ** 2).sum(1)


def fit_dispersion() -> dict[str, float]:
    per = pl.concat(
        [
            box(s)
            .filter(pl.col("season_type") == 2)
            .group_by("player_id")
            .agg(
                pl.len().alias("g"),
                pl.col("minutes").mean().alias("mpg"),
                *[pl.col(s).mean().alias(f"{s}_mu") for s in EXTRA],
                *[pl.col(s).var().alias(f"{s}_var") for s in EXTRA],
            )
            .filter((pl.col("g") >= 15) & (pl.col("mpg") >= 15))
            for s in FIT_SEASONS
        ]
    )
    disp = {s: round(float((per[f"{s}_var"] / per[f"{s}_mu"].clip(1e-6)).median()), 3) for s in EXTRA}
    print(f"stat_dispersion ({per.height} rotation player-seasons): {disp}")
    return disp


def fit_k(rows: pl.DataFrame, disp: dict[str, float]) -> dict[str, float]:
    c = get_constants("00")
    mins = rows["c_minutes"].to_numpy()
    scale = (rows["mpg"] * rows["pace_factor"]).to_numpy()
    ks = {}
    for s in _STATS:
        y, tot, prior = (rows[x].to_numpy().astype(float) for x in (s, f"c_{s}", f"prior_{s}"))

        def crps(k: float) -> float:
            mu = np.maximum((tot + k * prior) / (mins + k) * scale, 1e-6)
            if s == "pts":
                return float(crps_normal(mu, c.pts_sd_a + c.pts_sd_b * np.sqrt(mu), y).mean())
            return float(crps_count(mu, disp.get(s, c.count_dispersion), y).mean())

        best = GRID[int(np.argmin([crps(k) for k in GRID]))]
        fine = sorted({round(best * 1.25**j, 1) for j in range(-4, 5)}) if best else [0, 1, 2, 3, 4]
        ks[s] = float(fine[int(np.argmin([crps(k) for k in fine]))])
        print(f"k[{s}] = {ks[s]:g}  (mean CRPS {crps(0):.5f} raw -> {crps(ks[s]):.5f})")
    return ks


def boot_se(d: np.ndarray, cluster: np.ndarray, b: int = 2000, seed: int = 7) -> float:
    """Bootstrap SE of mean(d), resampling whole clusters."""
    _, inv = np.unique(cluster, return_inverse=True)
    tot, cnt = np.bincount(inv, weights=d), np.bincount(inv).astype(float)
    idx = np.random.default_rng(seed).integers(0, tot.size, (b, tot.size))
    return float((tot[idx].sum(1) / cnt[idx].sum(1)).std(ddof=1))


def holdout(season: int) -> None:
    """Raw vs shrunk (the committed constants) on one held-out season, per stat."""
    c = get_constants("00")
    rows = season_rows(season)
    mins = rows["c_minutes"].to_numpy()
    scale = (rows["mpg"] * rows["pace_factor"]).to_numpy()
    day, player = rows["game_date"].cast(pl.Utf8).to_numpy(), rows["player_id"].to_numpy()
    print(f"holdout {season}: {rows.height} player-games, {np.unique(day).size} days, {np.unique(player).size} players")
    for s in _STATS:
        y, tot, prior = (rows[x].to_numpy().astype(float) for x in (s, f"c_{s}", f"prior_{s}"))
        crps = []
        for k in (0.0, c.rate_prior_minutes[s]):
            mu = np.maximum((tot + k * prior) / (mins + k) * scale, 1e-6)
            if s == "pts":
                crps.append(crps_normal(mu, c.pts_sd_a + c.pts_sd_b * np.sqrt(mu), y))
            else:
                crps.append(crps_count(mu, c.stat_dispersion.get(s, c.count_dispersion), y))
        d = crps[0] - crps[1]
        se_day, se_player = boot_se(d, day), boot_se(d, player)
        print(
            f"  {s:5s} CRPS {crps[0].mean():.4f} -> {crps[1].mean():.4f}  gain {d.mean():+.4f}  "
            f"SE day {se_day:.4f} (z {d.mean() / se_day:.1f})  SE player {se_player:.4f} (z {d.mean() / se_player:.1f})"
        )


def _abs_moment(m: np.ndarray, v: np.ndarray) -> np.ndarray:
    sd = np.sqrt(v)
    return sd * math.sqrt(2 / math.pi) * np.exp(-(m**2) / (2 * v)) + m * (1 - 2 * st.norm.cdf(-m / sd))


def crps_mix(ws: list, mus: list, y: np.ndarray, stat: str, law: tuple[float, float]) -> np.ndarray:
    """Exact CRPS of a mixture of the shipped predictives (points components on the SD law ``law``)."""
    c = get_constants("00")
    mus = [np.maximum(m, 1e-6) for m in mus]
    if stat == "pts":
        vs = [(law[0] + law[1] * np.sqrt(m)) ** 2 for m in mus]
        out = sum(w * _abs_moment(m - y, v) for w, m, v in zip(ws, mus, vs, strict=True))
        pairs = [
            (wi * wj, mi - mj, vi + vj)
            for wi, mi, vi in zip(ws, mus, vs, strict=True)
            for wj, mj, vj in zip(ws, mus, vs, strict=True)
        ]
        return out - 0.5 * sum(w * _abs_moment(m, v) for w, m, v in pairs)
    k = np.arange(KMAX + 1)[None, :]
    disp = c.stat_dispersion.get(stat, c.count_dispersion)
    cdf = np.zeros((y.size, KMAX + 1))
    for w, m in zip(ws, mus, strict=True):
        mm = m[:, None]
        if disp <= 1.0001:
            cdf += w[:, None] * st.poisson.cdf(k, mm)
        else:
            r = mm / (disp - 1.0)
            cdf += w[:, None] * st.nbinom.cdf(k, r, r / (r + mm))
    return ((cdf - (k >= y[:, None])) ** 2).sum(1)


def mixture_parts(rows: pl.DataFrame) -> tuple[dict, np.ndarray]:
    """Season and last-10 component means per stat (E1 rates, the committed k) and |dmin|."""
    k = get_constants("00").rate_prior_minutes
    pace = rows["pace_factor"].to_numpy()
    comps = {}
    for s in _STATS:
        tot, l10, prior = (rows[x].to_numpy().astype(float) for x in (f"c_{s}", f"l10_{s}", f"prior_{s}"))
        season = (tot + k[s] * prior) / (rows["c_minutes"].to_numpy() + k[s]) * rows["mpg"].to_numpy() * pace
        recent = (l10 + k[s] * prior) / (rows["l10_minutes"].to_numpy() + k[s]) * rows["mpg10"].to_numpy() * pace
        comps[s] = [season, recent]
    return comps, np.abs(rows["mpg10"].to_numpy() - rows["mpg"].to_numpy())


def _weights(dmin: np.ndarray, sd: np.ndarray, a: float, b: float, mix_on: np.ndarray) -> list:
    w = np.where(mix_on, 1.0 / (1.0 + np.exp(-(a + b * dmin / sd))), 0.0)
    return [1 - w, w]


def _sd(rows: pl.DataFrame, floor: float, fallback: float) -> np.ndarray:
    raw = rows["min_sd"].to_numpy()
    return np.where(np.isfinite(raw), np.maximum(raw, floor), fallback)


def fit_mixture(rows: pl.DataFrame) -> dict[str, float]:
    c = get_constants("00")
    raw = rows["min_sd"].to_numpy()
    fallback = round(float(np.median(raw[np.isfinite(raw)])), 3)
    sub = rows.filter(pl.col("g") >= 11)
    comps, dmin = mixture_parts(sub)
    sd, on = _sd(sub, 1.0, fallback), np.ones(sub.height, bool)
    ys = {s: sub[s].to_numpy().astype(float) for s in ("pts", "reb", "ast")}
    single = (c.pts_sd_a, c.pts_sd_b)
    base = {s: crps_mix(_weights(dmin, sd, 0, 0, ~on), comps[s], ys[s], s, single).mean() for s in ys}

    def obj(p: np.ndarray) -> float:
        ws = _weights(dmin, sd, p[0], p[1], on)
        return float(sum(crps_mix(ws, comps[s], ys[s], s, single).mean() / base[s] for s in ys))

    grid = [(a, b) for a in (-8, -6, -4, -3, -2, -1, 0, 1) for b in (0.0, 0.25, 0.5, 1.0, 2.0, 4.0)]
    opts = {"xatol": 1e-3, "fatol": 1e-7, "maxiter": 400}
    ab = minimize(obj, min(grid, key=obj), method="Nelder-Mead", options=opts).x
    ws = _weights(dmin, sd, ab[0], ab[1], on)

    def pts_obj(q: np.ndarray) -> float:
        return float(crps_mix(ws, comps["pts"], ys["pts"], "pts", (q[0], q[1])).mean()) if min(q) > 0 else 1e9

    law = minimize(pts_obj, single, method="Nelder-Mead", options={"xatol": 1e-4, "fatol": 1e-8}).x
    out = {"a": round(float(ab[0]), 3), "b": round(float(ab[1]), 3), "sd_floor": 1.0, "sd_fallback": fallback}
    out |= {"pts_sd_a": round(float(law[0]), 4), "pts_sd_b": round(float(law[1]), 4)}
    print(f"recent_mixture ({sub.height} player-games with >= 11 earlier games): {out}")
    return out


def holdout_mixture(season: int) -> None:
    """The committed mixture against the single shrunk predictive on one held-out season, per stat."""
    c = get_constants("00")
    mix = c.recent_mixture
    rows = season_rows(season)
    comps, dmin = mixture_parts(rows)
    on = rows["g"].to_numpy() > mix["games"]
    ws = _weights(dmin, _sd(rows, mix["sd_floor"], mix["sd_fallback"]), mix["a"], mix["b"], on)
    day = rows["game_date"].cast(pl.Utf8).to_numpy()
    print(f"holdout-mixture {season}: {rows.height} player-games, {np.unique(day).size} days, {int(on.sum())} mixed")
    for s in _STATS:
        y = rows[s].to_numpy().astype(float)
        one = crps_mix([np.ones(rows.height)], [comps[s][0]], y, s, (c.pts_sd_a, c.pts_sd_b))
        both = np.where(on, crps_mix(ws, comps[s], y, s, (mix["pts_sd_a"], mix["pts_sd_b"])), one)
        d = one - both
        print(
            f"  {s:5s} CRPS {one.mean():.4f} -> {both.mean():.4f}  gain {d.mean():+.4f}  z (day) {d.mean() / boot_se(d, day):.1f}"
        )


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--holdout":
        holdout(int(sys.argv[2]))
        raise SystemExit(0)
    if len(sys.argv) == 3 and sys.argv[1] == "--holdout-mixture":
        holdout_mixture(int(sys.argv[2]))
        raise SystemExit(0)
    if len(sys.argv) == 2 and sys.argv[1] == "--mixture":
        fit_mixture(pl.concat([season_rows(s) for s in FIT_SEASONS], how="vertical_relaxed"))
        raise SystemExit(0)
    dispersion = fit_dispersion()
    fit_rows = pl.concat([season_rows(s) for s in FIT_SEASONS], how="vertical_relaxed")
    print(f"{fit_rows.height} player-games, {fit_rows['game_date'].n_unique()} days")
    print({"stat_dispersion": dispersion, "rate_prior_minutes": fit_k(fit_rows, dispersion)})
