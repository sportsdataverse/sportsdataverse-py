# tools/fit_preseason_prior.py
"""Fit the basketball preseason prior on an as-of-date holdout, and the college sim's rating noise.

The fitted values are frozen into
``sportsdataverse.mbb.mbb_prediction_constants.PRESEASON_PRIOR`` (college) and
``sportsdataverse.nba.nba_season_sim.PRIOR_CONSTANTS`` / ``SIM_CONSTANTS`` (NBA / WNBA), each
with a comment citing this script.

**The prior.** Each team's prior-season final rating, regressed toward the league mean by a
factor ``regression``, is the mean the in-season ratings shrink toward:

* college: the ratings engine's fixed point
  (:func:`sportsdataverse.mbb.mbb_team_ratings.adjust_efficiency`) counts the prior as
  ``weight`` extra games at the prior rating, so a team with ``g`` games is ``g / (g + weight)``
  this season;
* NBA / WNBA: the as-of possession ridge shrinks toward the prior instead of toward 0, with
  ``lam`` possessions of prior on each side
  (:func:`sportsdataverse._common.team_strength.possession_ridge_ratings` with ``prior=``).

A program new to the league has no prior-season rating. College: it starts at the median
percentile that new Division I programs finished at in the train seasons, read off last
season's Division I ratings. NBA / WNBA: the train seasons have no expansion team, so the
default is the bottom quartile of last season's net ratings (0.25), not fitted.

**The holdout.** For each season, cutoffs every 7 days from the first game (week 0 is the
preseason: no game played). The ratings use the games strictly before the cutoff and score every
later regular-season game (college: both teams Division I, conference tournaments included).
Win probability is the products' own: college ``Phi((em_scale * dEM + hfa) / margin_sd)`` with
the shipped constants; pro ``Phi(margin / sigma)``. ``regression`` and ``weight`` / ``lam`` (and
pro ``sigma``) minimize pooled log loss on the TRAIN seasons; the HOLDOUT seasons, disjoint, are
scored once against the shipped behaviour (no prior).

**College rating noise.** The season sim draws one AdjEM shift per team per simulated season,
``N(0, rating_sd ** 2 / (g + weight))``. A game's marginal win probability is then
``Phi(mu / sqrt(margin_sd ** 2 + em_scale ** 2 * rating_sd ** 2 * (1 / (g_h + weight) +
1 / (g_a + weight))))``; ``rating_sd`` minimizes its train log loss with the prior fixed.

Inputs are the ESPN college schedules / team boxes and the ``{nba,wnba}_stats_possessions`` /
``_schedules`` releases (network), cached as parquet under ``--cache``.

Usage::

    uv run python tools/fit_preseason_prior.py --league mbb \
        --train 2012 2013 2014 2015 2016 2017 2018 2019 2020 2022 --holdout 2023 2024 2025 2026
    uv run python tools/fit_preseason_prior.py --league nba --train 2018 2019 2022 2023 --holdout 2024 2025 2026
"""

from __future__ import annotations

import argparse
import importlib
import datetime as dt
import json
import sys
from collections.abc import Callable, Iterator
from pathlib import Path

import numpy as np
import polars as pl
from scipy import sparse
from scipy.stats import norm

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fit_pro_season_ratings import load_season  # noqa: E402

from sportsdataverse._common.metrics import brier_score, log_loss_score  # noqa: E402
from sportsdataverse._common.team_strength import possession_ridge_ratings, ridge_design  # noqa: E402
from sportsdataverse.mbb.mbb_college_season import (  # noqa: E402
    college_teams,
    game_noise_sd,
    normalize_college_schedule,
    rating_shift_sd,
)
from sportsdataverse.mbb.mbb_prediction_constants import get_constants  # noqa: E402
from sportsdataverse.mbb.mbb_team_ratings import (  # noqa: E402
    _league_loaders,
    adjust_efficiency,
    preseason_prior,
    raw_game_efficiency,
)

nba_season_sim = importlib.import_module(
    "sportsdataverse.nba.nba_season_sim"
)  # the package re-exports a function by that name

REGRESSION = np.round(np.arange(0.0, 1.0001, 0.1), 2).tolist()
WEIGHTS = [1.0, 2.0, 3.0, 4.0, 6.0, 8.0, 10.0, 12.0, 16.0, 20.0, 25.0]
LAMS = [200.0, 400.0, 800.0, 1600.0, 3200.0, 6400.0, 12800.0]
SIGMAS = np.round(np.arange(10.0, 18.01, 0.1), 2)
RATING_SDS = np.arange(0.0, 40.01, 1.0)
SHIFT_MULTIPLIERS = [0.0, 0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 1.75]
TOTAL_SIMS = 400
PRO_NEW_TEAM_QUANTILE = 0.25
EARLY_WEEKS = 6
_COLLEGE = {"mbb": "mens", "wbb": "womens"}
_PRO = {"nba": "00", "wnba": "10"}


def _ll(y: np.ndarray, p: np.ndarray) -> np.ndarray:
    p = np.clip(p, 1e-15, 1 - 1e-15)
    return -(y * np.log(p) + (1 - y) * np.log(1 - p))


def _score(y: np.ndarray, p: np.ndarray) -> dict[str, float]:
    return {"log_loss": round(log_loss_score(y, p), 5), "brier": round(brier_score(y, p), 5), "n": int(y.size)}


def paired(frame: pl.DataFrame, a: str, b: str) -> dict[str, float]:
    """Mean per-prediction log-loss difference ``a - b``, SE clustered by game (a game is scored at every cutoff)."""
    y = frame["y"].to_numpy()
    d = _ll(y, frame[a].to_numpy()) - _ll(y, frame[b].to_numpy())
    mean = float(d.mean())
    sums = pl.DataFrame({"g": frame["game_id"], "d": d}).group_by("g").agg(pl.col("d").sum(), pl.len().alias("n"))
    se = float(np.sqrt(((sums["d"].to_numpy() - sums["n"].to_numpy() * mean) ** 2).sum()) / d.size)
    return {"diff": round(mean, 5), "se": round(se, 5), "games": sums.height}


def cutoffs(start: dt.date, end: dt.date) -> Iterator[tuple[int, dt.date]]:
    """``(week, cutoff)`` every 7 days from ``start`` (week 0: nothing played yet) through ``end``."""
    k = 0
    while start + dt.timedelta(days=7 * k) <= end:
        yield k, start + dt.timedelta(days=7 * k)
        k += 1


# --- college --------------------------------------------------------------------------------


def load_college(lg: str, season: int, cache: Path) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """(normalized schedule, game efficiency rows of every completed game, Division I teams), cached."""
    paths = [cache / f"{lg}_{kind}_{season}.parquet" for kind in ("sched", "eff", "teams")]
    if all(p.exists() for p in paths):
        return pl.read_parquet(paths[0]), pl.read_parquet(paths[1]), pl.read_parquet(paths[2])
    load_schedule, load_box = _league_loaders(_COLLEGE[lg])
    sched = normalize_college_schedule(load_schedule([season]))
    done = sched.filter(
        (pl.col("completed") == True) & pl.col("home_score").is_not_null() & pl.col("away_score").is_not_null()  # noqa: E712
    )
    eff = raw_game_efficiency(done, load_box([season]))
    teams = college_teams(sched)
    cache.mkdir(parents=True, exist_ok=True)
    for frame, p in zip((sched, eff, teams), paths, strict=True):
        frame.write_parquet(p)
    return sched, eff, teams


def new_team_quantile(lg: str, seasons: list[int], cache: Path) -> dict[str, float]:
    """The median final-AdjEM percentile, among Division I, of programs in their first Division I season."""
    pct = []
    for s in seasons:
        _, eff, teams = load_college(lg, s, cache)
        _, _, prev_teams = load_college(lg, s - 1, cache)
        final = adjust_efficiency(eff, league=_COLLEGE[lg]).join(teams.select("team_id"), on="team_id", how="semi")
        ranked = final.with_columns((pl.col("adj_em").rank("average") / pl.len()).alias("pct"))
        new = ranked.join(prev_teams.select("team_id"), on="team_id", how="anti")
        pct += new.get_column("pct").to_list()
    if not pct:
        raise ValueError(f"no first-year Division I program in the train seasons {seasons}; add a season that has one")
    return {"quantile": round(float(np.median(pct)), 2), "programs": len(pct)}


def college_season(
    lg: str, season: int, cache: Path, methods: dict[str, tuple[float, float]], new_q: float
) -> pl.DataFrame:
    """One row per (cutoff, scored game): outcome, games so far per side, and ``mu`` per method.

    ``methods`` maps a name to ``(regression, weight)``; weight 0 is the shipped engine (no prior).
    """
    league = _COLLEGE[lg]
    c = get_constants(league)
    sched, eff, teams = load_college(lg, season, cache)
    _, prev_eff, prev_teams = load_college(lg, season - 1, cache)
    priors = {
        r: preseason_prior(prev_eff, teams, prev_teams, league=league, regression=r, new_team_quantile=new_q)
        for r in {m[0] for m in methods.values()}
    }
    d1 = set(teams.get_column("team_id").to_list())
    scored = sched.filter(
        (pl.col("completed") == True)  # noqa: E712
        & (pl.col("season_type") == 2)
        & pl.col("home_score").is_not_null()
        & pl.col("away_score").is_not_null()
        & pl.col("home_team_id").is_in(sorted(d1))
        & pl.col("away_team_id").is_in(sorted(d1))
    )
    start = eff.get_column("date").min()
    end = scored.get_column("date").max()
    out = []
    for week, cut in cutoffs(start, end):  # type: ignore[arg-type]
        past = eff.filter(pl.col("date") < cut)
        fut = scored.filter(pl.col("date") >= cut)
        if fut.height == 0:
            break
        games = dict(past.group_by("team_id").len().iter_rows())
        h, a = fut.get_column("home_team_id").to_list(), fut.get_column("away_team_id").to_list()
        frame: dict[str, object] = {
            "season": season,
            "week": week,
            "game_id": fut.get_column("game_id").to_list(),
            "y": (fut.get_column("home_score") > fut.get_column("away_score")).cast(pl.Float64).to_list(),
            "g_h": [games.get(t, 0) for t in h],
            "g_a": [games.get(t, 0) for t in a],
        }
        loc = c.hfa * (~fut.get_column("neutral_site").fill_null(False).to_numpy())
        for name, (r, w) in methods.items():
            rat = adjust_efficiency(past, league=league, prior=priors[r] if w > 0 else None, prior_weight=w)
            em = dict(zip(rat["team_id"].to_list(), rat["adj_em"].to_list(), strict=True))
            # the sim's rule: an unrated team plays as the weakest rated Division I team
            fill = min((em[t] for t in d1 if t in em), default=0.0)
            dem = np.array([em.get(x, fill) for x in h]) - np.array([em.get(x, fill) for x in a])
            frame[name] = c.em_scale * dem + loc
        out.append(pl.DataFrame(frame))
    return pl.concat(out)


def _crps(samples: np.ndarray, y: np.ndarray) -> np.ndarray:
    """CRPS per column of ``samples`` (sims x teams) against ``y``: mean |X - y| less half the mean |X - X*|."""
    srt = np.sort(samples, axis=0)
    m = srt.shape[0]
    w = (2.0 * np.arange(1, m + 1) - m - 1)[:, None]
    return np.abs(samples - y[None, :]).mean(axis=0) - (w * srt).sum(axis=0) / (m * m)


def college_totals(
    lg: str, season: int, cache: Path, prior_rw: tuple[float, float], new_q: float, scales: list[float], n_sims: int
) -> pl.DataFrame:
    """Season-total check of the sim's rating shift.

    Per cutoff and shift scale: the CRPS of each Division I team's remaining regular-season wins
    (the sim's game model, its shift and its per-game noise) against the wins it went on to get,
    and whether they fell in the 10-90% band. Common random numbers across ``scales``, seeded by
    season and week.
    """
    league = _COLLEGE[lg]
    c = get_constants(league)
    r, w = prior_rw
    sched, eff, teams = load_college(lg, season, cache)
    _, prev_eff, prev_teams = load_college(lg, season - 1, cache)
    prior = preseason_prior(prev_eff, teams, prev_teams, league=league, regression=r, new_team_quantile=new_q)
    ids = teams.get_column("team_id").to_list()
    idx = {t: i for i, t in enumerate(ids)}
    n = len(ids)
    reg = sched.filter(
        (pl.col("completed") == True)  # noqa: E712
        & (pl.col("season_type") == 2)
        & ~((pl.col("type_abbreviation") == "TRNMNT") & (pl.col("conference_competition") == True))  # noqa: E712
        & pl.col("home_score").is_not_null()
        & pl.col("away_score").is_not_null()
        & (pl.col("home_team_id").is_in(ids) | pl.col("away_team_id").is_in(ids))
    )
    out = []
    for week, cut in cutoffs(eff.get_column("date").min(), reg.get_column("date").max()):  # type: ignore[arg-type]
        fut = reg.filter(pl.col("date") >= cut)
        past = eff.filter(pl.col("date") < cut)
        rat = adjust_efficiency(past, league=league, prior=prior, prior_weight=w)
        em = dict(zip(rat["team_id"].to_list(), rat["adj_em"].to_list(), strict=True))
        games = dict(past.group_by("team_id").len().iter_rows())
        emin = min((em[t] for t in ids if t in em), default=0.0)  # a non-D-I opponent plays as the weakest D-I
        h = np.array([idx.get(t, -1) for t in fut["home_team_id"].to_list()])
        a = np.array([idx.get(t, -1) for t in fut["away_team_id"].to_list()])
        em_t = np.array([em.get(t, emin) for t in ids])
        em_h = np.where(h >= 0, em_t[np.maximum(h, 0)], emin)
        em_a = np.where(a >= 0, em_t[np.maximum(a, 0)], emin)
        mu = c.em_scale * (em_h - em_a) + c.hfa * (~fut["neutral_site"].fill_null(False).to_numpy())
        home_won = (fut["home_score"] > fut["away_score"]).to_numpy()
        g = fut.height
        inc_h = np.zeros((g, n), dtype=np.float32)
        inc_a = np.zeros((g, n), dtype=np.float32)
        inc_h[np.flatnonzero(h >= 0), h[h >= 0]] = 1.0
        inc_a[np.flatnonzero(a >= 0), a[a >= 0]] = 1.0
        actual = home_won.astype(np.float32) @ inc_h + (~home_won).astype(np.float32) @ inc_a
        played = np.array([games.get(t, 0) for t in ids])
        rng = np.random.default_rng([season, week])
        z = rng.standard_normal((n_sims, n))
        u = rng.random((n_sims, g))
        has = (inc_h.sum(axis=0) + inc_a.sum(axis=0)) > 0
        for scale in scales:
            tau = rating_shift_sd(played, league, rating_sd=scale, weight=w)
            tau_h, tau_a = np.where(h >= 0, tau[np.maximum(h, 0)], 0.0), np.where(a >= 0, tau[np.maximum(a, 0)], 0.0)
            sh = z * tau
            d = np.where(h >= 0, sh[:, np.maximum(h, 0)], 0.0) - np.where(a >= 0, sh[:, np.maximum(a, 0)], 0.0)
            hw = (u < norm.cdf((mu + c.em_scale * d) / game_noise_sd(tau_h, tau_a, league))).astype(np.float32)
            wins = hw @ inc_h + (1.0 - hw) @ inc_a
            lo, hi = np.quantile(wins, 0.1, axis=0), np.quantile(wins, 0.9, axis=0)
            out.append(
                pl.DataFrame(
                    {
                        "season": season,
                        "week": week,
                        "scale": scale,
                        "team_id": [ids[i] for i in np.flatnonzero(has)],
                        "crps": _crps(wins, actual)[has],
                        "covered": ((actual >= lo) & (actual <= hi))[has],
                    }
                )
            )
    return pl.concat(out)


def _college_p(frame: pl.DataFrame, name: str, league: str, rating_sd: float = 0.0, weight: float = 0.0) -> np.ndarray:
    c = get_constants(league)
    var = c.margin_sd**2
    if rating_sd > 0:
        v = 1.0 / (frame["g_h"].to_numpy() + weight) + 1.0 / (frame["g_a"].to_numpy() + weight)
        var = var + (c.em_scale * rating_sd) ** 2 * v
    return norm.cdf(frame[name].to_numpy() / np.sqrt(var))


def fit_college(lg: str, train: list[int], holdout: list[int], cache: Path) -> dict[str, object]:
    league = _COLLEGE[lg]
    nq = new_team_quantile(lg, train, cache)
    grid = {"none": (0.0, 0.0), **{f"r{r:g}_w{w:g}": (r, w) for r in REGRESSION for w in WEIGHTS}}
    tr = pl.concat([college_season(lg, s, cache, grid, nq["quantile"]) for s in train])
    y = tr["y"].to_numpy()
    train_ll = {m: log_loss_score(y, _college_p(tr, m, league)) for m in grid}
    best = min((m for m in grid if m != "none"), key=train_ll.__getitem__)
    r, w = grid[best]
    # marginal widening (the evidence for keeping each game's margin sd at margin_sd)
    sd_ll = {float(s): log_loss_score(y, _college_p(tr, best, league, s, w)) for s in RATING_SDS}
    # the shift's scale, on season totals: sigma_team = margin_sd / (em_scale * sqrt 2) is one
    # team's single-game AdjEM noise implied by the game model; the grid multiplies it
    c = get_constants(league)
    sigma_team = c.margin_sd / (c.em_scale * np.sqrt(2.0))
    scales = [round(m * sigma_team, 2) for m in SHIFT_MULTIPLIERS]
    tot_tr = pl.concat([college_totals(lg, s, cache, (r, w), nq["quantile"], scales, TOTAL_SIMS) for s in train])
    crps_tr = dict(tot_tr.group_by("scale").agg(pl.col("crps").mean()).iter_rows())
    rating_sd = min(crps_tr, key=crps_tr.__getitem__)
    tot_ho = pl.concat(
        [college_totals(lg, s, cache, (r, w), nq["quantile"], [0.0, rating_sd], TOTAL_SIMS) for s in holdout]
    )
    methods = {"none": (0.0, 0.0), "prior": (r, w)}
    ho = pl.concat([college_season(lg, s, cache, methods, nq["quantile"]) for s in holdout])
    ho = ho.with_columns(
        pl.Series("p_none", _college_p(ho, "none", league)),
        pl.Series("p_prior", _college_p(ho, "prior", league)),
    )
    early_tr = tr.filter(pl.col("week") <= EARLY_WEEKS)

    def totals(frame: pl.DataFrame) -> dict[str, object]:
        out: dict[str, object] = {}
        for (sc,), sub in frame.group_by("scale", maintain_order=True):
            early = sub.filter(pl.col("week") <= EARLY_WEEKS)
            out[f"{sc:g}"] = {
                "crps": round(float(sub["crps"].mean()), 4),  # type: ignore[arg-type]
                "band_10_90_coverage": round(float(sub["covered"].mean()), 3),  # type: ignore[arg-type]
                "weeks_0_6_crps": round(float(early["crps"].mean()), 4),  # type: ignore[arg-type]
                "weeks_0_6_coverage": round(float(early["covered"].mean()), 3),  # type: ignore[arg-type]
            }
        return out

    return {
        "league": lg,
        "train": train,
        "holdout": holdout,
        "new_team": nq,
        "chosen_on_train": {"regression": r, "weight": w, "rating_sd": rating_sd, "sigma_team": round(sigma_team, 3)},
        "train_log_loss": {
            "none": round(train_ll["none"], 5),
            "prior": round(train_ll[best], 5),
            "weeks_0_6_none": round(log_loss_score(early_tr["y"].to_numpy(), _college_p(early_tr, "none", league)), 5),
            "weeks_0_6_prior": round(log_loss_score(early_tr["y"].to_numpy(), _college_p(early_tr, best, league)), 5),
            "regression_profile": {f"{x:g}": round(train_ll[f"r{x:g}_w{w:g}"], 5) for x in REGRESSION},
            "weight_profile": {f"{x:g}": round(train_ll[f"r{r:g}_w{x:g}"], 5) for x in WEIGHTS},
            "marginal_widening_profile": {f"{s:g}": round(v, 5) for s, v in sd_ll.items() if s % 4 == 0},
        },
        "season_totals_train": totals(tot_tr),
        "season_totals_holdout": totals(tot_ho),
        **_holdout_report(ho, ["p_none", "p_prior"]),
    }


# --- NBA / WNBA ------------------------------------------------------------------------------


def _pro_prior_vectors(prior: nba_season_sim.PreseasonPrior | None, index: dict[int, int]) -> np.ndarray:
    n_t = len(index)
    b0 = np.zeros(2 * n_t + 2)
    if prior is not None:
        for t, o, d in prior.ratings.select("team_id", "off_dev", "def_dev").iter_rows():
            if t in index:
                b0[index[t]], b0[n_t + index[t]] = o, d
    return b0


def pro_season(
    league_id: str, season: int, cache: Path, methods: dict[str, tuple[float, float]], check: bool = False
) -> pl.DataFrame:
    """One row per (cutoff, future game): outcome and the predicted home margin per method.

    ``methods`` maps a name to ``(regression, lam)``; regression ``None`` (encoded as -1) is the
    shipped model (no prior). The ridge is solved from sufficient statistics; ``check`` asserts
    it against :func:`possession_ridge_ratings` at one cutoff.
    """
    rows, games = load_season(league_id, season, cache)
    prev_rows, _ = load_season(league_id, season - 1, cache)
    teams = sorted(set(games["home_team_id"].to_list()) | set(games["away_team_id"].to_list()))
    index = {t: i for i, t in enumerate(teams)}
    n_t = len(teams)
    priors = {
        r: nba_season_sim.preseason_prior(
            prev_rows, teams, league_id=league_id, regression=r, new_team_quantile=PRO_NEW_TEAM_QUANTILE
        )
        for r in {m[0] for m in methods.values() if m[0] >= 0}
    }
    any_prior = next(iter(priors.values()), None)
    rows = rows.filter(pl.col("offense_team_id").is_not_null() & pl.col("defense_team_id").is_not_null()).sort("date")
    design = ridge_design(rows)
    assert design.height == rows.height
    oi = np.array([index[int(t)] for t in design["offense_id"].to_list()])
    di = np.array([index[int(t)] for t in design["defense_id"].to_list()])
    m = design.height
    data = np.ones(3 * m)
    X = sparse.csr_matrix(
        (
            np.concatenate([data, design["hfa"].to_numpy()]),
            (
                np.concatenate([np.arange(m), np.arange(m), np.arange(m), np.arange(m)]),
                np.concatenate([oi, n_t + di, np.full(m, 2 * n_t), np.full(m, 2 * n_t + 1)]),
            ),
        ),
        shape=(m, 2 * n_t + 2),
    )
    y = design["y"].to_numpy()
    dates = rows["date"].to_numpy()
    gid = rows["game_id"].to_numpy()
    R = np.eye(2 * n_t + 2)
    R[2 * n_t, 2 * n_t] = R[2 * n_t + 1, 2 * n_t + 1] = 0.0
    start, end = games["date"].min(), games["date"].max()
    out = []
    for week, cut in cutoffs(start, end):  # type: ignore[arg-type]
        k = int(np.searchsorted(dates, np.datetime64(cut)))
        fut = games.filter(pl.col("date") >= cut)
        if fut.height == 0:
            break
        h = np.array([index[t] for t in fut["home_team_id"].to_list()])
        a = np.array([index[t] for t in fut["away_team_id"].to_list()])
        frame: dict[str, object] = {
            "season": season,
            "week": week,
            "game_id": fut["game_id"].to_list(),
            "y": (fut["margin"] > 0).cast(pl.Float64).to_list(),
        }
        if k:
            Xk = X[:k]
            xtx = (Xk.T @ Xk).toarray()
            xty = Xk.T @ y[:k]
            poss = k / (2.0 * len(set(gid[:k].tolist())))
        for name, (r, lam) in methods.items():
            prior = priors.get(r) if r >= 0 else None
            b0 = _pro_prior_vectors(prior, index)
            if k == 0:
                net = b0[:n_t] - b0[n_t : 2 * n_t]
                hfa, pp = (prior.hfa, prior.poss) if prior is not None else (0.0, 100.0)
            else:
                beta = np.linalg.solve(xtx + lam * R, xty - xtx @ b0) + b0
                net, hfa, pp = beta[:n_t] - beta[n_t : 2 * n_t], beta[2 * n_t + 1], poss
            frame[name] = (net[h] - net[a] + 2.0 * hfa) * pp / 100.0
            if check and week == 3 and prior is not None:
                lib = possession_ridge_ratings(rows.head(k), lam=lam, prior=prior.ratings)
                got = dict(zip(lib["team_id"].to_list(), lib["adj_net_rtg"].to_list(), strict=True))
                diff = max(abs(got[t] - net[index[t]]) for t in got)
                assert diff < 1e-6, f"sufficient-statistics ridge differs from the library by {diff}"
                check = False
        out.append(pl.DataFrame(frame))
    assert any_prior is None or any_prior.ratings.height == n_t
    return pl.concat(out)


def fit_pro(lg: str, train: list[int], holdout: list[int], cache: Path) -> dict[str, object]:
    """The constrained fit (shipped) next to the joint scan (reported).

    Shipped: the regression factor alone, on train, at the sim's own penalty and probit sd
    (``SIM_CONSTANTS``, fit on the same train seasons by ``tools/fit_pro_season_ratings.py``). The
    penalty is the prior's blend weight, in possessions. The joint scan also picks the penalty
    and sd on train; its holdout is reported so the choice between them is visible.
    """
    league_id = _PRO[lg]
    lam0, sd0 = nba_season_sim.SIM_CONSTANTS[league_id]
    grid = {"none": (-1.0, lam0), **{f"r{r:g}_l{lam:g}": (r, lam) for r in REGRESSION for lam in LAMS}}
    tr = pl.concat([pro_season(league_id, s, cache, grid, check=(i == 0)) for i, s in enumerate(train)])
    y = tr["y"].to_numpy()

    def best_sigma(name: str) -> tuple[float, float]:
        mm = tr[name].to_numpy()
        losses = [log_loss_score(y, norm.cdf(mm / s)) for s in SIGMAS]
        i = int(np.argmin(losses))
        return float(SIGMAS[i]), losses[i]

    def at_shipped(name: str) -> float:
        return log_loss_score(y, norm.cdf(tr[name].to_numpy() / sd0))

    fits = {name: best_sigma(name) for name in grid}
    joint = min((m for m in grid if m != "none"), key=lambda m: fits[m][1])
    shipped_ll = {r: at_shipped(f"r{r:g}_l{lam0:g}") for r in REGRESSION}
    r = min(shipped_ll, key=shipped_ll.__getitem__)
    rj, lamj = grid[joint]
    methods = {"none": (-1.0, lam0), "prior": (r, lam0), "joint": (rj, lamj)}
    ho = pl.concat([pro_season(league_id, s, cache, methods) for s in holdout])
    ho = ho.with_columns(
        pl.Series("p_none", norm.cdf(ho["none"].to_numpy() / sd0)),
        pl.Series("p_prior", norm.cdf(ho["prior"].to_numpy() / sd0)),
        pl.Series("p_joint", norm.cdf(ho["joint"].to_numpy() / fits[joint][0])),
    )
    return {
        "league": lg,
        "train": train,
        "holdout": holdout,
        "shipped_penalty_and_sd": {"lam": lam0, "sigma": sd0},
        "chosen_on_train": {"regression": r, "lam": lam0, "sigma": sd0, "new_team_quantile": PRO_NEW_TEAM_QUANTILE},
        "joint_scan_on_train": {"regression": rj, "lam": lamj, "sigma": fits[joint][0]},
        "train_log_loss": {
            "none": round(at_shipped("none"), 5),
            "prior": round(shipped_ll[r], 5),
            "joint": round(fits[joint][1], 5),
            "regression_profile": {f"{x:g}": round(v, 5) for x, v in shipped_ll.items()},
            "joint_lam_profile": {
                f"{x:g}": [fits[f"r{rj:g}_l{x:g}"][0], round(fits[f"r{rj:g}_l{x:g}"][1], 5)] for x in LAMS
            },
        },
        **_holdout_report(ho, ["p_none", "p_prior", "p_joint"]),
    }


def _holdout_report(ho: pl.DataFrame, cols: list[str]) -> dict[str, object]:
    def block(sub: pl.DataFrame) -> dict[str, object]:
        yy = sub["y"].to_numpy()
        return {c: _score(yy, sub[c].to_numpy()) for c in cols}

    early = ho.filter(pl.col("week") <= EARLY_WEEKS)
    return {
        "holdout_all": block(ho),
        "holdout_weeks_0_6": block(early),
        "holdout_by_week": {str(k): block(ho.filter(pl.col("week") == k)) for k in range(EARLY_WEEKS + 1)},
        "holdout_by_season_weeks_0_6": {
            str(s): block(early.filter(pl.col("season") == s)) for s in sorted(set(ho["season"].to_list()))
        },
        "holdout_paired_weeks_0_6": {f"{c} - p_none": paired(early, c, "p_none") for c in cols[1:]},
        "holdout_paired_all": {f"{c} - p_none": paired(ho, c, "p_none") for c in cols[1:]},
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    ap.add_argument("--league", choices=sorted([*_COLLEGE, *_PRO]), required=True)
    ap.add_argument("--train", type=int, nargs="+", required=True, help="seasons by END year")
    ap.add_argument("--holdout", type=int, nargs="+", required=True, help="seasons by END year")
    ap.add_argument("--cache", type=Path, default=Path(".cache/preseason_prior"))
    ap.add_argument("--out", type=Path, default=None, help="write the result json here")
    args = ap.parse_args(argv)
    if set(args.train) & set(args.holdout):
        ap.error("train and holdout seasons must be disjoint")
    if len(set(args.train)) != len(args.train) or len(set(args.holdout)) != len(args.holdout):
        ap.error("--train and --holdout must not repeat a season")
    fit: Callable[[str, list[int], list[int], Path], dict[str, object]]
    if args.league in _COLLEGE:
        fit, cache = fit_college, args.cache
    else:
        fit, cache = fit_pro, args.cache.parent / "pro_season"  # the fit_pro_season_ratings cache
    result = fit(args.league, args.train, args.holdout, cache)
    text = json.dumps(result, indent=2, default=str)
    print(text)
    if args.out:
        args.out.write_text(text + "\n", encoding="utf-8", newline="\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
