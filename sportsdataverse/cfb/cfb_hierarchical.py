"""Hierarchical CFB team strength, the opt-in pregame path (``cfb_predict_games(model="hierarchical")``).

A two-level Gaussian model of game margins:
- team strengths shrink toward a conference mean plus a carried-over share of last season's
  strength, with one home-field term;
- the posterior mean is a ridge whose penalty is the variance ratio, so it is fit in closed
  form by EM on the variance components (``method="eb"`` of the cfbfastR-cfb-data stage 34
  model ``cfb_higher_models.hierarchical``, which this module ports).

There is no stored artifact: the model refits from completed games at call time, one EM fit
per season of the carryover chain plus one for the as-of slot.

The corpus is every completed game BEFORE the slot: seasons ``first_season`` to
``season - 1``, then ``season``'s slots ``t' < t``. Slot ``t`` is the regular-season week, or
``100 + week`` for the postseason, because ESPN restarts ``week`` at 1 for bowls. The EM's
variance components average over the whole team universe, so a corpus that grew after the
slot would move every strength.

The stage 34 reference, measured against the served closed form on identical as-of games:
- 2015-2025 (6,201 games): MAE 12.84 vs 13.33, 11/11 seasons.
- 2005-2013, out of sample (4,774 games): MAE 12.62 vs 13.55, 9/9 seasons.

Those numbers come from the reference's walk-forward, which builds ONE corpus over the whole run. Its team
universe and conferences therefore include games after each slot. This path's before-the-slot corpus is
the version a live caller can compute, and the pre-registered 2026 forward check scores it. It has not
been rescored historically.

It is opt-in and not the default.
"""

from __future__ import annotations

from typing import Literal, overload

import numpy as np
import pandas as pd
import polars as pl

__all__ = ["cfb_hierarchical_ratings"]

#: Priors and EM starts, copied verbatim from the gated stage 34 model.
PRIOR_HFA_LOC, PRIOR_HFA_SCALE = 2.5, 3.0
_EM_ITERS = 40
_START_SIGMA, _START_TAU2, _START_SCONF2, _START_RHO = 16.4, 100.0, 25.0, 0.5
#: Chain start of the gated model (its walk-forward seeds the carryover at 2014 with zeros).
FIRST_SEASON = 2014
#: Win-probability spread: the residual sd of the 2015-2025 walk-forward margins (6,201 games,
#: 2026-11-26 rescore). Fixed for the served path by the 2026-11-27 promotion pre-registration.
HIER_MARGIN_SD = 16.15

_HIER_COLUMNS = ["season", "asof_t", "team_id", "theta", "hfa", "tau_team", "sigma_obs"]


def _slot() -> pl.Expr:
    return pl.when(pl.col("season_type") == "postseason").then(pl.col("week") + 100).otherwise(pl.col("week"))


def _corpus(
    schedule: pl.DataFrame, season: int, t: int, first_season: int
) -> tuple[pl.DataFrame, np.ndarray, int, dict[int, np.ndarray]]:
    """Encode completed games before the slot into fixed team / conference index space."""
    g = (
        schedule.filter(
            pl.col("season_type").is_in(["regular", "postseason"])
            & (pl.col("fbs_participant") == True)  # noqa: E712
            & pl.col("home_points").is_not_null()
            & pl.col("away_points").is_not_null()
        )
        .with_columns(_slot().cast(pl.Int64).alias("t"))
        .filter(pl.col("season").is_between(first_season, season) & ((pl.col("season") < season) | (pl.col("t") < t)))
        .select(
            pl.col("game_id").cast(pl.Int64),
            pl.col("season").cast(pl.Int64),
            "t",
            pl.col("home_id").cast(pl.Int64),
            pl.col("away_id").cast(pl.Int64),
            pl.col("neutral_site").cast(pl.Boolean),
            pl.col("home_conference").cast(pl.Utf8),
            pl.col("away_conference").cast(pl.Utf8),
            (pl.col("home_points") - pl.col("away_points")).cast(pl.Float64).alias("margin"),
        )
        .drop_nulls(["home_id", "away_id", "margin"])
    )
    team_ids = np.sort(np.unique(np.concatenate([g["home_id"].to_numpy(), g["away_id"].to_numpy()])))
    tix = {int(x): i for i, x in enumerate(team_ids)}
    long = pl.concat(
        [
            g.select("season", pl.col("home_id").alias("team_id"), pl.col("home_conference").alias("conf")),
            g.select("season", pl.col("away_id").alias("team_id"), pl.col("away_conference").alias("conf")),
        ]
    ).with_columns(pl.col("conf").fill_null("OTHER"))
    conf_names = sorted(set(long["conf"].to_list()) | {"OTHER"})
    cix = {c: i for i, c in enumerate(conf_names)}
    modal = (
        long.group_by(["season", "team_id", "conf"])
        .len()
        .sort(["len", "conf"], descending=[True, False])
        .group_by(["season", "team_id"])
        .first()
    )
    conf_of: dict[int, np.ndarray] = {}
    for s in sorted(g["season"].unique().to_list()):
        arr: np.ndarray = np.full(len(team_ids), cix["OTHER"], dtype=np.int32)
        sub = modal.filter(pl.col("season") == s)
        for tm, c in zip(sub["team_id"].to_list(), sub["conf"].to_list(), strict=True):
            arr[tix[int(tm)]] = cix[c]
        conf_of[int(s)] = arr
    g = g.with_columns(
        pl.col("home_id").replace_strict(tix, return_dtype=pl.Int32).alias("home_ix"),
        pl.col("away_id").replace_strict(tix, return_dtype=pl.Int32).alias("away_ix"),
        pl.col("neutral_site").cast(pl.Float64).alias("neutral"),
    )
    return g, team_ids, len(conf_names), conf_of


def _fit_eb(
    h: np.ndarray,
    a: np.ndarray,
    nz: np.ndarray,
    y: np.ndarray,
    conf_ix: np.ndarray,
    prev: np.ndarray,
    n_teams: int,
    n_conf: int,
) -> tuple[np.ndarray, float, float, float]:
    """EM on the variance components of the two-level model (the stage 34 ``fit_eb``)."""
    T, C = n_teams, n_conf
    n = len(y)
    X = np.zeros((n, T + 1))
    X[np.arange(n), h] = 1.0
    X[np.arange(n), a] -= 1.0
    X[:, T] = 1.0 - nz
    XtX = X.T @ X
    Xty = X.T @ y
    sigma2, tau2, sconf2, rho = _START_SIGMA**2, _START_TAU2, _START_SCONF2, _START_RHO
    conf = np.zeros(C)
    theta = np.zeros(T + 1)
    onehot = np.zeros((T, C))
    onehot[np.arange(T), conf_ix] = 1.0
    has_prev = prev != 0.0
    for _ in range(_EM_ITERS):
        m = np.concatenate([conf[conf_ix] + rho * prev, [PRIOR_HFA_LOC]])
        d = np.concatenate([np.full(T, 1.0 / tau2), [1.0 / PRIOR_HFA_SCALE**2]])
        V = np.linalg.inv(XtX / sigma2 + np.diag(d))
        theta = V @ (Xty / sigma2 + d * m)
        th = theta[:T]
        tau2 = max(float(np.mean((th - m[:T]) ** 2 + np.diag(V)[:T])), 1e-3)
        r = y - X @ theta
        sigma2 = max(float(np.mean(r**2)) + float(np.trace(X @ V @ X.T)) / n, 1e-3)
        cnt = onehot.sum(0)
        gm = (onehot * (th - rho * prev)[:, None]).sum(0) / np.maximum(cnt, 1)
        conf = gm * (sconf2 / (sconf2 + tau2 / np.maximum(cnt, 1)))
        sconf2 = max(float(np.mean(conf**2)), 1e-3)
        if has_prev.sum() > 10:
            num = float(np.dot(prev[has_prev], (th - conf[conf_ix])[has_prev]))
            den = float(np.dot(prev[has_prev], prev[has_prev])) + 1e-9
            rho = float(np.clip(num / den, 0.0, 1.0))
    return theta[:T], float(theta[T]), float(np.sqrt(tau2)), float(np.sqrt(sigma2))


def _fit_window(
    g: pl.DataFrame,
    season: int,
    t: int,
    prev: np.ndarray,
    conf_of: dict[int, np.ndarray],
    n_teams: int,
    n_conf: int,
) -> tuple[np.ndarray, float, float, float]:
    sub = g.filter((pl.col("season") == season) & (pl.col("t") < t)).sort("game_id")
    if sub.height == 0:
        raise ValueError(
            f"cfb_hierarchical_ratings: no completed {season} games before slot t={t}; the model has no "
            "in-season evidence there (the first slot of a season), and it does not fall back to the closed form"
        )
    return _fit_eb(
        sub["home_ix"].to_numpy(),
        sub["away_ix"].to_numpy(),
        sub["neutral"].to_numpy(),
        sub["margin"].to_numpy(),
        conf_of[season],
        prev,
        n_teams,
        n_conf,
    )


@overload
def cfb_hierarchical_ratings(
    schedule: pl.DataFrame, season: int, t: int, *, first_season: int = ..., return_as_pandas: Literal[True]
) -> pd.DataFrame: ...
@overload
def cfb_hierarchical_ratings(
    schedule: pl.DataFrame,
    season: int,
    t: int,
    *,
    first_season: int = ...,
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...
def cfb_hierarchical_ratings(
    schedule: pl.DataFrame,
    season: int,
    t: int,
    *,
    first_season: int = FIRST_SEASON,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """As-of hierarchical team strengths for slot ``t`` of ``season``, in points.

    Feed the result to :func:`~sportsdataverse.cfb.cfb_game_predict.cfb_predict_games` with
    ``model="hierarchical"``. Expected home margin is ``theta_home - theta_away + hfa`` off
    neutral sites.

    Args:
        schedule: A :func:`~sportsdataverse.cfb.load_cfb_schedule` frame covering
            ``first_season`` through ``season``. It needs ``game_id``, ``season``, ``week``,
            ``season_type``, ``fbs_participant``, ``home_id``, ``away_id``, ``neutral_site``,
            ``home_conference``, ``away_conference``, ``home_points`` and ``away_points``. Only
            completed games before the slot are read, so a whole season's schedule can be passed.
        season: Season of the slot.
        t: Slot to rate: the regular-season week, or ``100 + week`` for the postseason (bowls
            are postseason week 1, so ``t=101``).
        first_season: Start of the carryover chain, seeded with zero strengths. The gated model
            starts at 2014; another start gives a different (ungated) model.
        return_as_pandas: If True, return a pandas DataFrame; otherwise polars.

    Returns:
        One row per team in the corpus, with columns:

        | col_name | type | description |
        |---|---|---|
        | season | Int64 | Season of the slot. |
        | asof_t | Int64 | The slot rated; only games before it were used. |
        | team_id | String | ESPN team id (the `cfb_ratings` key dtype). |
        | theta | Float64 | Team strength in points, relative to the corpus average. |
        | hfa | Float64 | Home-field advantage in points (one value per fit). |
        | tau_team | Float64 | Fitted between-team spread of strength. |
        | sigma_obs | Float64 | Fitted per-game residual spread. |

    Raises:
        ValueError: If ``schedule`` has no completed ``first_season`` games, or ``season`` has no
            completed games before slot ``t``. It never falls back to the closed form.

    Example:
        Quick start::

            from sportsdataverse.cfb import load_cfb_schedule, cfb_hierarchical_ratings
            sched = load_cfb_schedule(seasons=list(range(2014, 2027)))
            hier = cfb_hierarchical_ratings(sched, 2026, 13)

    See Also:
        * :func:`~sportsdataverse.cfb.cfb_game_predict.cfb_predict_games` -- the served pregame path.
    """
    sched = schedule if isinstance(schedule, pl.DataFrame) else pl.from_pandas(schedule)
    g, team_ids, n_conf, conf_of = _corpus(sched, season, t, first_season)
    if first_season not in conf_of:
        raise ValueError(f"cfb_hierarchical_ratings: no completed {first_season} games to start the carryover chain")
    n_teams = len(team_ids)
    prev = np.zeros(n_teams)
    for s in range(first_season, season):
        if s not in conf_of:
            raise ValueError(f"cfb_hierarchical_ratings: no completed {s} games; the carryover chain is broken")
        last = int(g.filter(pl.col("season") == s)["t"].max())
        prev = _fit_window(g, s, last + 1, prev, conf_of, n_teams, n_conf)[0]
    if season not in conf_of:
        raise ValueError(f"cfb_hierarchical_ratings: no completed {season} games before slot t={t}")
    theta, hfa, tau, sigma = _fit_window(g, season, t, prev, conf_of, n_teams, n_conf)
    out = pl.DataFrame(
        {
            "season": np.full(n_teams, season, dtype=np.int64),
            "asof_t": np.full(n_teams, t, dtype=np.int64),
            "team_id": [str(int(x)) for x in team_ids],
            "theta": theta.astype(np.float64),
            "hfa": np.full(n_teams, hfa),
            "tau_team": np.full(n_teams, tau),
            "sigma_obs": np.full(n_teams, sigma),
        }
    ).select(_HIER_COLUMNS)
    return out.to_pandas() if return_as_pandas else out
