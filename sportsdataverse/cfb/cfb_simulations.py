"""College football season simulations (nflseedR-style engine).

Engine design adapted from nflseedR (MIT, Sebastian Carl & Lee Sharpe):
https://nflseedr.com. Mirrors nflseedR's week-loop simulation with a
pluggable ``compute_results`` (``R/simulations.R``,
``nflseedR_compute_results`` in ``R/simulations_utils.R``) with the CFB
adaptations from the shared seedr-port spec.

CFB simplifications (documented deliberately — the R ``cfbseedR`` port uses
the SAME semantics so outputs cross-validate):

* **Sequential loop, no chunking.** nflseedR chunks simulations for
  parallelism; this port stacks all sims in one frame and loops weeks once.
* **Default results model** is nflseedR's ELO variant (init
  ``N(1500, 150)``, x1.2 postseason multiplier, estimate ``elo_diff / 25``,
  K=20 with log-MOV multiplier) with the home bump and the margin noise
  fitted to FBS results instead of nflseedR's NFL values: +59.4 ELO for a
  non-neutral home team (2.38 points; NFL 20) and margins drawn from
  ``N(estimate, 16.7)`` (NFL 13), rounded away from zero. Both are
  :func:`cfb_compute_results` arguments, the same defaults as cfbseedR's
  ``cfbseedR_compute_results()``, fitted by cfbseedR's
  ``data-raw/cfb_elo_constants.R``. The NFL rest-day adjustment is dropped
  (CFB plays weekly).
  Ties remain possible in REG games (engine parity with nflseedR; real CFB
  has had overtime since 1996) and are re-broken by win probability in
  postseason games.
* **Simulated postseason** = conference championship games + the CFP
  bracket from :func:`cfb_playoff_seeds`. CONF_CHAMP rows present in the
  input are simulated as given (their matchups may not be each sim's true
  rank-1/rank-2 pair — realistic projection input); conferences without a
  CONF_CHAMP row get a generated rank-1 vs rank-2 game (rank 1 home,
  neutral flag set).
* **Fixed CFP bracket, no reseeding.** Standard bracket order with the top
  seeds on byes (byes = next power of two minus field size). For the
  12-team CFP: 12@5, 11@6, 10@7 and 9@8 on campus; quarterfinals 1 v 8/9,
  2 v 7/10, 3 v 6/11, 4 v 5/12; semifinals the 1/4 side v the 2/3 side.
  First round at the better seed's home, later rounds neutral.
* **Season rules.** A ``season``-keyed input season picks the tiebreaker
  registry epochs and CFP selection rule (see
  :func:`sportsdataverse.cfb.cfb_playoff_seeds`); ``sim``-keyed input uses
  the current rules.
* **No draft order** (CFB has none).
"""

from __future__ import annotations

import math
from typing import Any, cast
from collections.abc import Callable

import numpy as np
import polars as pl

from sportsdataverse.cfb.cfb_standings import (
    FrameLike,
    _is_independent,
    _validate_games,
    _team_id,
    _to_polars,
    _validate_teams,
    cfb_standings,
)

__all__ = ["cfb_simulations", "cfb_compute_results", "cfb_ratings_compute_results"]

_SIM_INCLUDES = ("REG", "CONF", "POST")

ComputeResultsFn = Callable[..., dict[str, pl.DataFrame]]


def _round_out(expr: pl.Expr) -> pl.Expr:
    """Round away from zero (nflseedR ``round_out``)."""
    return pl.when(expr > 0).then(expr.ceil()).when(expr < 0).then(expr.floor()).otherwise(0.0).cast(pl.Float64)


def cfb_compute_results(
    teams: pl.DataFrame,
    games: pl.DataFrame,
    week_num: int,
    *,
    rng: np.random.Generator | None = None,
    elo: dict[str, float] | None = None,
    home_field: float = 59.4,
    margin_sd: float = 16.7,
    **kwargs: Any,
) -> dict[str, pl.DataFrame]:
    """Default results generator — nflseedR's dynamic ELO model for CFB.

    Fills ``result`` for week ``week_num`` games that are still unplayed and
    updates each team's ELO rating from that week's results (real results
    included). The ELO machinery is nflseedR's ``nflseedR_compute_results``
    minus the NFL rest-day adjustment (CFB plays weekly — documented
    simplification); the home-field bonus and the margin noise are fitted to
    FBS results (``home_field``, ``margin_sd``).

    The defaults come from cfbseedR's ``data-raw/cfb_elo_constants.R``, fitted
    on 8,551 completed FBS-vs-FBS games from 2014-2025 (2020 left out):
    ``home_field = 25 * 2.376`` points of within-season home edge (SE 0.168),
    and ``margin_sd`` = 16.734 points, the spread of real margins around the
    pregame-ELO estimate this function computes (SE 0.128). They replace
    nflseedR's NFL values (20 ELO = 0.8 points, sd 13). cfbseedR uses the
    same numbers; refit both together.

    Args:
        teams: Per-sim team table (``sim``, ``team``, ``conference``,
            optionally ``elo`` carried over from the previous week).
        games: Per-sim games table (engine schema; see
            :func:`sportsdataverse.cfb.cfb_standings`).
        week_num: The week to fill.
        rng: numpy Generator (seeded by :func:`cfb_simulations`). A fresh
            default generator is created when omitted.
        elo: Optional initial ratings ``{team: elo}`` applied to every sim.
            Teams missing from the dict start at 1500. When neither ``elo``
            nor a ``teams.elo`` column exists, ratings initialize randomly
            at ``N(1500, 150)`` per (sim, team) — nflseedR behavior.
        home_field: Home-field advantage in ELO points, added for games that
            are not neutral (default 59.4, i.e. 2.38 points of margin at 25
            ELO per point). ``20.0`` restores the pre-fit NFL value.
        margin_sd: Standard deviation, in points, of a simulated margin around
            its ELO estimate (default 16.7). ``13.0`` restores the NFL value.
        **kwargs: Ignored (forward-compatibility for custom callers).

    Returns:
        ``{"teams": ..., "games": ...}`` — updated frames, mirroring
        nflseedR's returned list.

    Example:
        Fill week 5 of a stacked sims frame::

            from sportsdataverse.cfb.cfb_simulations import cfb_compute_results
            out = cfb_compute_results(teams, games, 5, rng=rng)
            teams, games = out["teams"], out["games"]

        Simulate with the NFL constants instead (``cfb_simulations`` passes
        only ``rng``, so bind overrides with ``functools.partial``)::

            import functools
            from sportsdataverse.cfb.cfb_simulations import cfb_compute_results, cfb_simulations
            nfl_elo = functools.partial(cfb_compute_results, home_field=20.0, margin_sd=13.0)
            sims = cfb_simulations(games, teams, compute_results=nfl_elo, seed=1)

    See Also:
        * `nflseedR <https://nflseedr.com>`_ -- ``nflseedR_compute_results``.
        * `cfbfastR <https://cfbfastR.sportsdataverse.org>`_ -- CFB data in R.
    """
    if rng is None:
        rng = np.random.default_rng()
    if "elo" not in teams.columns:
        if elo is not None:
            ratings = pl.DataFrame({"team": list(elo.keys()), "elo": [float(v) for v in elo.values()]})
            teams = teams.join(ratings, on="team", how="left").with_columns(pl.col("elo").fill_null(1500.0))
        else:
            teams = teams.with_columns(pl.Series("elo", rng.normal(1500.0, 150.0, teams.height), dtype=pl.Float64))

    g = (
        games.join(
            teams.select("sim", pl.col("team").alias("home_team"), pl.col("elo").alias("_home_elo")),
            on=["sim", "home_team"],
            how="left",
        )
        .join(
            teams.select("sim", pl.col("team").alias("away_team"), pl.col("elo").alias("_away_elo")),
            on=["sim", "away_team"],
            how="left",
        )
        .with_columns(
            (
                pl.col("_home_elo")
                - pl.col("_away_elo")
                + pl.when(pl.col("neutral").fill_null(0) == 1).then(0.0).otherwise(home_field)
            ).alias("_elo_diff")
        )
        .with_columns(
            pl.when(pl.col("game_type") != "REG")
            .then(pl.col("_elo_diff") * 1.2)
            .otherwise(pl.col("_elo_diff"))
            .alias("_elo_diff")
        )
        .with_columns(
            (1.0 / ((10.0 ** (-pl.col("_elo_diff") / 400.0)) + 1.0)).alias("_wp"),
            (pl.col("_elo_diff") / 25.0).alias("_estimate"),
        )
    )
    n = g.height
    draws = pl.Series("_draw", rng.normal(0.0, margin_sd, n), dtype=pl.Float64)
    unifs = pl.Series("_unif", rng.uniform(0.0, 1.0, n), dtype=pl.Float64)
    is_week = pl.col("week") == week_num
    g = (
        g.with_columns(draws, unifs)
        .with_columns(_round_out(pl.col("_estimate") + pl.col("_draw")).alias("_sim_result"))
        .with_columns(
            # postseason games cannot tie — re-break by win probability
            pl.when((pl.col("game_type") != "REG") & (pl.col("_sim_result") == 0))
            .then(pl.when(pl.col("_unif") < pl.col("_wp")).then(1.0).otherwise(-1.0))
            .otherwise(pl.col("_sim_result"))
            .alias("_sim_result")
        )
        .with_columns(
            pl.when(is_week & pl.col("result").is_null())
            .then(pl.col("_sim_result"))
            .otherwise(pl.col("result"))
            .cast(pl.Float64)
            .alias("result")
        )
    )

    # ELO shift from this week's results (K=20, log-MOV multiplier)
    wk = g.filter(is_week & pl.col("result").is_not_null()).with_columns(
        pl.when(pl.col("result") > 0).then(1.0).when(pl.col("result") < 0).then(0.0).otherwise(0.5).alias("_outcome"),
        pl.when(pl.col("result") > 0)
        .then(pl.col("_elo_diff") * 0.001 + 2.2)
        .when(pl.col("result") < 0)
        .then(-pl.col("_elo_diff") * 0.001 + 2.2)
        .otherwise(1.0)
        .alias("_elo_input"),
    )
    wk = wk.with_columns(
        ((pl.max_horizontal(pl.col("result").abs(), pl.lit(1.0)) + 1.0).log() * 2.2 / pl.col("_elo_input")).alias(
            "_elo_mult"
        )
    ).with_columns((20.0 * pl.col("_elo_mult") * (pl.col("_outcome") - pl.col("_wp"))).alias("_shift"))
    shifts = (
        pl.concat(
            [
                wk.select("sim", pl.col("home_team").alias("team"), pl.col("_shift").alias("_delta")),
                wk.select("sim", pl.col("away_team").alias("team"), (-pl.col("_shift")).alias("_delta")),
            ]
        )
        .group_by("sim", "team")
        .agg(pl.col("_delta").sum())
    )
    teams = (
        teams.join(shifts, on=["sim", "team"], how="left")
        .with_columns((pl.col("elo") + pl.col("_delta").fill_null(0.0)).alias("elo"))
        .drop("_delta")
    )

    games_out = g.select(games.columns)
    return {"teams": teams, "games": games_out}


# Defaults of cfb_ratings_compute_results by rating_type, the same values as cfbseedR's
# cfbseedR_ratings_compute_results(). points / raw_epa: cfbseedR data-raw/cfb_generator_constants.R
# (walk-forward FBS-vs-FBS 2014-2025, 2020 out; home fixed at the ELO generator's 2.376). adj_epa is
# read live from cfb_prediction_constants (modern era) in _ratings_defaults, so a refit there moves
# both; cfbseedR copies the numbers, and the shared fixture test turns red until it does.
_RATINGS_DEFAULTS: dict[str, dict[str, Any]] = {
    "points": {
        "scale": {"0-3": 1.0929, "4-5": 0.9234, "6-7": 1.0305, "8-20": 1.0628},
        "margin_sd": {"0-3": 16.9680, "4-5": 16.6383, "6-7": 16.2311, "8-20": 16.4509},
        "home_field": 2.376,
    },
    "raw_epa": {
        "scale": {"0-3": 28.5848, "4-5": 34.3212, "6-7": 45.0972, "8-20": 52.0247},
        "margin_sd": {"0-3": 17.9308, "4-5": 17.2400, "6-7": 16.9808, "8-20": 17.3931},
        "home_field": 2.376,
    },
}
_RATING_TYPES = ("points", "adj_epa", "raw_epa")


def _ratings_defaults(rating_type: str) -> dict[str, Any]:
    if rating_type not in _RATING_TYPES:
        raise ValueError(f"rating_type must be one of {_RATING_TYPES}, not {rating_type!r}")
    if rating_type == "adj_epa":
        from sportsdataverse.cfb.cfb_prediction_constants import get_constants

        c = get_constants("modern")
        return {"scale": dict(c.slope_by_games), "margin_sd": c.margin_sd, "home_field": c.hfa_points}
    return _RATINGS_DEFAULTS[rating_type]


def _bucket_values(x: float | dict[str, float], n_games: np.ndarray, arg: str) -> np.ndarray:
    """Per-game value from a scalar or ``"lo-hi"`` buckets (cfbseedR ``ratings_bucket_value``).

    A count takes the bucket with the largest lower bound at or below it (R ``findInterval``); a
    count below every bucket takes the bottom one; NaN (unknown) takes the top one.
    """
    if not isinstance(x, dict):
        if isinstance(x, (int, float)) and not math.isnan(float(x)):
            return np.full(n_games.shape, float(x))
        raise ValueError(f"{arg} must be a number or a dict of 'lo-hi' buckets")
    try:
        parts = [k.split("-") for k in x]
        ok = all(len(b) == 2 for b in parts)
        lo = [float(b[0]) for b in parts]
        hi = [float(b[1]) for b in parts]
        ok = ok and not any(math.isnan(v) for v in lo + hi)
    except (ValueError, IndexError):
        ok, lo = False, []
    if not x or not ok or len(set(lo)) != len(lo):
        raise ValueError(f"{arg} buckets must be distinct games-played ranges written 'lo-hi', e.g. '0-3'")
    order = np.argsort(lo, kind="stable")
    lo_sorted = np.asarray(lo)[order]
    vals = np.asarray([float(v) for v in x.values()])[order]
    if np.isnan(vals).any():
        raise ValueError(f"{arg} buckets must not hold NaN")
    pick = np.maximum(np.searchsorted(lo_sorted, n_games, side="right") - 1, 0)
    pick[np.isnan(n_games)] = len(vals) - 1
    return vals[pick]


def _ratings_expected_margin(
    home_team: list[str],
    away_team: list[str],
    neutral: np.ndarray,
    ratings: FrameLike,
    *,
    rating_type: str = "points",
    scale: float | dict[str, float] | None = None,
    home_field: float | None = None,
    margin_sd: float | dict[str, float] | None = None,
) -> tuple[np.ndarray, np.ndarray]:
    """Expected home margin and margin SD per game (cfbseedR ``ratings_expected_margin``)."""
    r = _to_polars(ratings)
    if not {"team", "rating"} <= set(r.columns):
        raise ValueError("ratings must have columns 'team' and 'rating'")
    if not r.schema["rating"].is_numeric() or r["rating"].null_count() > 0 or r["rating"].is_nan().any():
        raise ValueError("ratings 'rating' must be numeric with no missing values")
    if r["team"].n_unique() != r.height:
        raise ValueError("ratings has more than one row for a team")
    has_games = "games" in r.columns
    if has_games and (not r.schema["games"].is_numeric() or (r["games"].drop_nulls() < 0).any()):
        raise ValueError("ratings 'games' must be a non-negative number")
    d = _ratings_defaults(rating_type)
    scale = d["scale"] if scale is None else scale
    home_field = d["home_field"] if home_field is None else home_field
    if not isinstance(home_field, (int, float)) or not math.isfinite(float(home_field)):
        raise ValueError("home_field must be a single finite number")
    margin_sd = d["margin_sd"] if margin_sd is None else margin_sd

    teams = r.select(_team_id(r, "team"))["team"].to_list()  # float ids join as "87", never "87.0"
    rating = dict(zip(teams, r["rating"].cast(pl.Float64).to_list()))
    r_home = np.asarray([rating.get(t, 0.0) for t in home_team], dtype=float)
    r_away = np.asarray([rating.get(t, 0.0) for t in away_team], dtype=float)
    n_games = np.full(len(home_team), np.nan)
    if has_games:
        g = dict(zip(teams, r["games"].cast(pl.Float64).fill_null(float("nan")).to_list()))
        g_home = np.asarray([g.get(t, np.nan) for t in home_team], dtype=float)
        g_away = np.asarray([g.get(t, np.nan) for t in away_team], dtype=float)
        n_games = np.fmin(g_home, g_away)  # NaN-ignoring, like R pmin(na.rm = TRUE)
    s = _bucket_values(scale, n_games, "scale")
    sd = _bucket_values(margin_sd, n_games, "margin_sd")
    if (sd < 0).any():
        raise ValueError("margin_sd must be at least 0")
    expected = s * (r_home - r_away) + np.where(neutral == 1, 0.0, float(home_field))
    return expected, sd


def cfb_ratings_compute_results(
    teams: pl.DataFrame,
    games: pl.DataFrame,
    week_num: int,
    *,
    ratings: FrameLike,
    rating_type: str = "points",
    scale: float | dict[str, float] | None = None,
    home_field: float | None = None,
    margin_sd: float | dict[str, float] | None = None,
    rng: np.random.Generator | None = None,
    **kwargs: Any,
) -> dict[str, pl.DataFrame]:
    """Results generator driven by fixed team ratings (points, adjusted EPA or raw EPA).

    An alternative to the default :func:`cfb_compute_results` (dynamic ELO): each unplayed
    ``week == week_num`` game gets a home margin drawn from
    ``Normal(scale * (rating_home - rating_away) + home_field, margin_sd)`` (no ``home_field`` at a
    neutral site), rounded away from zero. A postseason draw of exactly 0 (in practice only with
    ``margin_sd = 0``) is re-decided by the win probability ``Phi(expected / margin_sd)`` with a
    1-point margin. Ratings stay fixed, so ``teams`` passes through unchanged. Same semantics and
    defaults as cfbseedR's ``cfbseedR_ratings_compute_results()``.

    ``scale`` and ``margin_sd`` are a scalar or ``"lo-hi"`` games-played buckets. A game takes the
    bucket of its less-observed team (the smaller ``games`` of the two; a team missing from
    ``ratings`` or with a null count does not count), then the bucket with the largest lower bound
    at or below that count; below every bucket takes the bottom one, and a game with no count (no
    ``games`` column, say) takes the top one, treating the ratings as full-season ratings.

    Defaults (buckets 0-3 / 4-5 / 6-7 / 8-20 games):

    * ``"points"`` (points per game vs an average team): scale 1.0929 / 0.9234 / 1.0305 /
      1.0628, margin SD 16.968 / 16.638 / 16.231 / 16.451, home field 2.376.
    * ``"adj_epa"`` (opponent-adjusted net EPA/play, the scale of :func:`cfb_ratings` ``adj_net``):
      the modern-era ``slope_by_games``, ``margin_sd`` and ``hfa_points`` of
      :data:`sportsdataverse.cfb.cfb_prediction_constants.CFB_CONSTANTS`.
    * ``"raw_epa"`` (net EPA/play, no adjustment): scale 28.585 / 34.321 / 45.097 / 52.025,
      margin SD 17.931 / 17.240 / 16.981 / 17.393, home field 2.376.

    The points and raw-EPA values are cfbseedR's walk-forward fit
    (``data-raw/cfb_generator_constants.R``, FBS-vs-FBS 2014-2025, 2020 out). In that test the ELO
    margin formula on CollegeFootballData's pregame ELO had the smallest error overall (MAE 12.94
    vs 13.10 for a least-squares points rating); from about six games on the points rating is
    level with it, which makes it a reasonable input for a late-season or postseason simulation.

    Args:
        teams: Per-sim team table (``sim``, ``team``, ``conference``); returned unchanged.
        games: Per-sim games table (engine schema; see :func:`sportsdataverse.cfb.cfb_standings`).
        week_num: The week to fill.
        ratings: One row per team: ``team``, ``rating`` (numeric, no nulls) and optionally
            ``games`` (non-negative games behind the rating; null = unknown). Polars or pandas.
            Teams missing from it rate 0 (league average).
        rating_type: ``"points"`` (default), ``"adj_epa"`` or ``"raw_epa"``; picks the defaults.
        scale: Points of margin per rating unit: a number or ``{"lo-hi": value}`` buckets.
            ``None`` uses the default for ``rating_type``.
        home_field: Home-field advantage in points for non-neutral games. ``None`` = default.
        margin_sd: Margin SD in points (at least 0): a number or buckets. ``None`` = default.
        rng: numpy Generator (seeded by :func:`cfb_simulations`). A fresh default generator is
            created when omitted.
        **kwargs: Ignored (forward-compatibility for custom callers).

    Returns:
        ``{"teams": teams, "games": games}`` with ``result`` (Float64 home margin) filled for this
        week's previously unplayed games.

    Raises:
        ValueError: On an unknown ``rating_type``, a ``ratings`` frame without ``team`` /
            ``rating``, a missing or non-numeric rating, a duplicated team, a negative or
            non-numeric ``games``, malformed buckets, or a negative ``margin_sd``.

    Example:
        Fill week 3 from points ratings::

            import polars as pl
            from sportsdataverse.cfb.cfb_simulations import cfb_ratings_compute_results
            ratings = pl.DataFrame({"team": ["A", "B"], "rating": [7.0, -7.0], "games": [8, 8]})
            out = cfb_ratings_compute_results(teams, games, 3, ratings=ratings, rating_type="points")

        Simulate a season with it (``cfb_simulations`` passes only ``rng``, so bind the ratings
        with ``functools.partial``)::

            import functools
            from sportsdataverse.cfb.cfb_simulations import cfb_ratings_compute_results, cfb_simulations
            cr = functools.partial(cfb_ratings_compute_results, ratings=ratings, rating_type="points")
            sims = cfb_simulations(games, teams, compute_results=cr, seed=1)

    See Also:
        * `cfbseedR <https://cfbseedR.sportsdataverse.org>`_ -- ``cfbseedR_ratings_compute_results()``.
        * `cfbfastR <https://cfbfastR.sportsdataverse.org>`_ -- CFB data in R.
    """
    if rng is None:
        rng = np.random.default_rng()
    g = games.with_columns(pl.col("result").cast(pl.Float64))
    g = g.with_columns(pl.col("result").fill_nan(None))
    idx = np.flatnonzero(((g["week"] == week_num) & g["result"].is_null()).to_numpy())
    if idx.size == 0:
        return {"teams": teams, "games": games}
    sub = g[idx]
    neutral = (
        sub["neutral"].cast(pl.Float64).fill_null(0.0).to_numpy() if "neutral" in sub.columns else np.zeros(idx.size)
    )
    expected, sd = _ratings_expected_margin(
        sub.select(_team_id(sub, "home_team"))["home_team"].to_list(),
        sub.select(_team_id(sub, "away_team"))["away_team"].to_list(),
        neutral,
        ratings,
        rating_type=rating_type,
        scale=scale,
        home_field=home_field,
        margin_sd=margin_sd,
    )
    raw = rng.normal(expected, sd)
    res = np.where(raw > 0, np.ceil(raw), np.where(raw < 0, np.floor(raw), 0.0))
    post_tie = (sub["game_type"].fill_null("REG").to_numpy() != "REG") & (res == 0.0)
    if post_tie.any():
        with np.errstate(invalid="ignore", divide="ignore"):
            z = expected[post_tie] / sd[post_tie]
        wp = np.asarray([0.5 if np.isnan(v) else 0.5 * (1.0 + math.erf(v / math.sqrt(2.0))) for v in z])
        res[post_tie] = np.where(rng.uniform(0.0, 1.0, int(post_tie.sum())) < wp, 1.0, -1.0)
    filled = g["result"].to_numpy(allow_copy=True).astype(float, copy=True)
    filled[idx] = res
    games_out = g.with_columns(pl.Series("result", filled, dtype=pl.Float64).fill_nan(None))
    return {"teams": teams, "games": games_out.select(games.columns)}


def _apply_compute_results(
    compute_results: ComputeResultsFn,
    teams: pl.DataFrame,
    games: pl.DataFrame,
    week_num: int,
    rng: np.random.Generator,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    out = compute_results(teams, games, week_num, rng=rng)
    return out["teams"], out["games"]


def _generate_conf_champ_games(standings: pl.DataFrame, games: pl.DataFrame, week: int) -> pl.DataFrame:
    """Rank-1 vs rank-2 CONF_CHAMP games for conferences lacking one in the input."""
    have = (
        games.filter(pl.col("game_type") == "CONF_CHAMP")
        .join(
            standings.select("sim", pl.col("team").alias("home_team"), "conference"),
            on=["sim", "home_team"],
            how="left",
        )
        .select("sim", "conference")
        .unique()
    )
    top2 = standings.filter((_is_independent() == False) & (pl.col("conf_rank") <= 2)).join(  # noqa: E712
        have, on=["sim", "conference"], how="anti"
    )
    r1 = top2.filter(pl.col("conf_rank") == 1).select("sim", "conference", pl.col("team").alias("home_team"))
    r2 = top2.filter(pl.col("conf_rank") == 2).select("sim", "conference", pl.col("team").alias("away_team"))
    return r1.join(r2, on=["sim", "conference"], how="inner").select(
        "sim",
        pl.lit(week, dtype=pl.Int64).alias("week"),
        pl.lit("CONF_CHAMP").alias("game_type"),
        "home_team",
        "away_team",
        pl.lit(None, dtype=pl.Float64).alias("result"),
        pl.lit(1, dtype=pl.Int64).alias("neutral"),
    )


def _n_byes(n: int) -> int:
    """First-round byes in a fixed bracket of ``n`` seeds (4 for the 12-team CFP)."""
    return (1 << (n - 1).bit_length()) - n if n > 1 else 0


def _bye_cutoffs(standings: pl.DataFrame) -> pl.DataFrame:
    """(sim, _bye_cut): seeds 1.._bye_cut drew a first-round bye in that sim's actual field."""
    n_field = standings.group_by("sim").agg(pl.col("seed").max().fill_null(0).alias("_n"))
    cut = {n: _n_byes(n) for n in n_field["_n"].unique().to_list()}
    return n_field.select("sim", pl.col("_n").replace_strict(cut, return_dtype=pl.Int64).alias("_bye_cut"))


def _bracket_slots(n: int) -> dict[int, int]:
    """seed -> bracket slot of a fixed single-elimination bracket for ``n`` seeds.

    Standard bracket order (1 v P, P/2 v P/2+1, ...; P = next power of two),
    so the top ``P - n`` seeds draw byes. For 12 this is the CFP bracket:
    1 v 8/9 and 4 v 5/12 on one side, 2 v 7/10 and 3 v 6/11 on the other.
    """
    order = [1]
    while len(order) < n:
        m = 2 * len(order) + 1
        order = [s for x in order for s in (x, m - x)]
    return {seed: slot for slot, seed in enumerate(order) if seed <= n}


def _simulate_bracket(
    field: pl.DataFrame,
    teams: pl.DataFrame,
    games: pl.DataFrame,
    compute_results: ComputeResultsFn,
    week_start: int,
    rng: np.random.Generator,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Fixed single-elimination CFP bracket (no reseeding), see :func:`_bracket_slots`.

    First round at the better seed's home, later rounds neutral (better seed
    listed home). Returns (champions frame [sim, team], teams, games).
    """
    if field.height == 0:
        return pl.DataFrame(schema={"sim": pl.Int64, "team": pl.Utf8}), teams, games
    n = int(cast(int, field.get_column("seed").max()))
    slots = _bracket_slots(n)
    slot_df = pl.DataFrame(
        {"seed": list(slots), "_slot": list(slots.values())}, schema={"seed": pl.Int64, "_slot": pl.Int64}
    )
    alive = field.select("sim", "team", pl.col("seed").cast(pl.Int64)).join(slot_df, on="seed")
    week = week_start
    for r in range(1, (n - 1).bit_length() + 1):
        # teams sharing slot // 2**r meet this round; a lone team has a bye
        a = (
            alive.with_columns((pl.col("_slot") // (1 << r)).alias("_m"))
            .sort(["sim", "_m", "seed"])
            .with_columns(pl.int_range(pl.len()).over("sim", "_m").alias("_i"))
        )
        hi, lo = a.filter(pl.col("_i") == 0), a.filter(pl.col("_i") == 1)
        byes = hi.join(lo, on=["sim", "_m"], how="anti").select(alive.columns)
        rnd = hi.join(lo, on=["sim", "_m"], suffix="_low").select(
            "sim",
            pl.lit(week, dtype=pl.Int64).alias("week"),
            pl.lit("POST").alias("game_type"),
            pl.col("team").alias("home_team"),
            pl.col("team_low").alias("away_team"),
            pl.lit(None, dtype=pl.Float64).alias("result"),
            pl.lit(0 if r == 1 else 1, dtype=pl.Int64).alias("neutral"),
        )
        games = pl.concat([games, rnd])
        teams, games = _apply_compute_results(compute_results, teams, games, week, rng)
        played = games.filter((pl.col("week") == week) & (pl.col("game_type") == "POST"))
        winners = played.select(
            "sim",
            pl.when(pl.col("result") > 0).then(pl.col("home_team")).otherwise(pl.col("away_team")).alias("team"),
        ).join(alive, on=["sim", "team"], how="inner")
        alive = pl.concat([byes, winners.select(alive.columns)])
        week += 1
    return alive.select("sim", "team"), teams, games


def _game_summary(games: pl.DataFrame) -> pl.DataFrame:
    """Per unique matchup: games played, home win / tie rates and mean margin."""
    return (
        games.filter(pl.col("result").is_not_null())
        .group_by("game_type", "week", "home_team", "away_team")
        .agg(
            pl.len().cast(pl.Int64).alias("games_played"),
            (pl.col("result") > 0).cast(pl.Float64).mean().alias("home_win"),
            (pl.col("result") == 0).cast(pl.Float64).mean().alias("tie"),
            pl.col("result").mean().cast(pl.Float64).alias("result_mean"),
        )
        .sort("game_type", "week", "home_team")
    )


def cfb_simulations(
    games: FrameLike,
    teams: FrameLike,
    compute_results: ComputeResultsFn | None = None,
    *,
    simulations: int = 10000,
    playoff_seeds: int = 12,
    tiebreaker_depth: str = "SOS",
    sim_include: str = "POST",
    rankings: FrameLike | None = None,
    tiebreaker_data: dict[str, FrameLike] | None = None,
    seed: int | None = None,
    return_as_pandas: bool = False,
) -> dict[str, pl.DataFrame | Any]:
    """Simulate college football seasons (nflseedR-style week loop).

    Replicates the input season ``simulations`` times, fills unplayed games
    week by week through the pluggable ``compute_results``, then simulates
    the postseason (conference championships + CFP bracket) and aggregates
    per-team probabilities. See the module docstring for every documented
    CFB simplification.

    Args:
        games: One season of games in the engine schema (``season`` or
            ``sim``, ``week``, ``game_type``, ``home_team``, ``away_team``,
            ``result`` — null = unplayed, ``neutral``). Played results are
            kept as-is. A ``season`` column picks that season's tiebreakers
            and CFP rule; ``sim``-keyed input uses the current ones.
        teams: Team table (``team``, ``conference``, plus the optional
            :func:`sportsdataverse.cfb.cfb_standings` columns).
        compute_results: Results generator with the signature
            ``fn(teams, games, week_num, **kwargs) -> {"teams": ..., "games": ...}``
            filling ``result`` for that week's unplayed games only. Defaults
            to :func:`cfb_compute_results` (dynamic ELO).
        simulations: Number of simulated seasons (sequential, no chunking).
        playoff_seeds: CFP field size passed to :func:`cfb_playoff_seeds`.
        tiebreaker_depth: nflseedR depth ladder (``RANDOM`` < ``PRE-SOV`` <
            ``SOS`` < ``POINTS``) used by every standings computation.
        sim_include: How deep to simulate: ``"REG"`` (regular season only),
            ``"CONF"`` (+ conference championships) or ``"POST"``
            (+ CFP bracket, default).
        rankings: Optional committee rankings (``team``, ``rank``) forwarded
            to :func:`cfb_playoff_seeds`. When None, seeding falls back to
            the per-sim standings ordering (documented in
            :func:`cfb_playoff_seeds`).
        tiebreaker_data: Optional tiebreaker inputs (``"analytics_ratings"``,
            ``"cfp_rankings"``, ``"apr"`` frames) forwarded unchanged to every
            :func:`sportsdataverse.cfb.cfb_standings` call, so the official rungs run.
        seed: Seed for the numpy RNG (deterministic runs).
        return_as_pandas: Return pandas DataFrames instead of polars.

    Returns:
        Dict of frames mirroring the nflseedR summary list:

        * ``"standings"`` — per (sim, team) standings incl. ``conf_rank``,
          ``conf_champ`` and (``sim_include="POST"``) ``seed``.
        * ``"games"`` — all games incl. simulated results and generated
          postseason rows.
        * ``"overall"`` — per-team probabilities (``won_conf``,
          ``made_playoff``, ``first_round_bye``, ``won_cfp``) and mean
          record columns.
        * ``"game_summary"`` — per unique matchup: games played, home win /
          tie rates and mean margin.

    Raises:
        ValueError: On invalid ``sim_include`` / ``tiebreaker_depth``,
            multi-season input, missing columns, or duplicate team rows.

    Example:
        Simulate a season 100 times, deterministically::

            from sportsdataverse.cfb import cfb_simulations
            out = cfb_simulations(games, teams, simulations=100, seed=42,
                                  playoff_seeds=12)
            print(out["overall"].sort("won_cfp", descending=True).head())

        Regular season only::

            out = cfb_simulations(games, teams, simulations=100,
                                  sim_include="REG", seed=1)

    See Also:
        * `nflseedR <https://nflseedr.com>`_ -- the engine this adapts.
        * `cfbfastR <https://cfbfastR.sportsdataverse.org>`_ -- CFB data in R.
    """
    if sim_include not in _SIM_INCLUDES:
        raise ValueError(f"`sim_include` must be one of {_SIM_INCLUDES}; got {sim_include!r}")
    cr: ComputeResultsFn = compute_results if compute_results is not None else cfb_compute_results
    rng = np.random.default_rng(seed)
    g = _validate_games(games)
    t = _validate_teams(teams)
    if g["sim"].n_unique() != 1:
        raise ValueError("`games` must contain exactly one season")
    # a `season`-keyed input names the season whose rules apply; plain `sim` = current rules
    season = int(g["sim"][0]) if "sim" not in list(games.columns) and "season" in list(games.columns) else None
    st_kwargs: dict[str, Any] = {"tiebreaker_depth": tiebreaker_depth, "tiebreaker_data": tiebreaker_data}
    st_kwargs.update(season=season, rng=rng)

    base = g.drop("sim")
    sims_idx = pl.DataFrame({"sim": pl.int_range(1, simulations + 1, eager=True).cast(pl.Int64)})
    games_all = sims_idx.join(base, how="cross")
    teams_all = sims_idx.join(t, how="cross")

    # Regular season week loop -------------------------------------------------
    reg_weeks = (
        games_all.filter((pl.col("game_type") == "REG") & pl.col("result").is_null())
        .get_column("week")
        .unique()
        .sort()
        .to_list()
    )
    for w in reg_weeks:
        teams_all, games_all = _apply_compute_results(cr, teams_all, games_all, int(w), rng)

    # Conference championships -------------------------------------------------
    if sim_include in ("CONF", "POST"):
        interim = cfb_standings(games_all, t, **st_kwargs)
        assert isinstance(interim, pl.DataFrame)
        max_week = int(games_all.get_column("week").max())
        generated = _generate_conf_champ_games(interim, games_all, max_week + 1)
        if generated.height > 0:
            games_all = pl.concat([games_all, generated])
        cc_weeks = (
            games_all.filter((pl.col("game_type") == "CONF_CHAMP") & pl.col("result").is_null())
            .get_column("week")
            .unique()
            .sort()
            .to_list()
        )
        for w in cc_weeks:
            teams_all, games_all = _apply_compute_results(cr, teams_all, games_all, int(w), rng)

    # Final standings (before CFP games so playoff results don't pollute records)
    standings = cfb_standings(
        games_all,
        t,
        playoff_seeds=playoff_seeds if sim_include == "POST" else None,
        rankings=rankings,
        **st_kwargs,
    )
    assert isinstance(standings, pl.DataFrame)

    # CFP bracket ---------------------------------------------------------------
    champions = pl.DataFrame(schema={"sim": pl.Int64, "team": pl.Utf8})
    if sim_include == "POST":
        field = standings.filter(pl.col("seed").is_not_null())
        week_start = int(games_all.get_column("week").max()) + 1
        champions, teams_all, games_all = _simulate_bracket(field, teams_all, games_all, cr, week_start, rng)

    # Aggregation ---------------------------------------------------------------
    champ_probs = champions.group_by("team").agg((pl.len() / simulations).cast(pl.Float64).alias("won_cfp"))
    agg_cols = [
        pl.col("wins").mean().cast(pl.Float64).alias("wins"),
        pl.col("losses").mean().cast(pl.Float64).alias("losses"),
        pl.col("ties").mean().cast(pl.Float64).alias("ties"),
        pl.col("win_pct").mean().cast(pl.Float64).alias("win_pct"),
        pl.col("conf_champ").cast(pl.Float64).mean().alias("won_conf"),
    ]
    if "seed" in standings.columns:
        agg_cols += [
            pl.col("seed").is_not_null().cast(pl.Float64).mean().alias("made_playoff"),
            (pl.col("seed") <= pl.col("_bye_cut")).fill_null(False).cast(pl.Float64).mean().alias("first_round_bye"),
        ]
    overall = (
        (standings.join(_bye_cutoffs(standings), on="sim") if "seed" in standings.columns else standings)
        .group_by("team", "conference")
        .agg(agg_cols)
        .join(champ_probs, on="team", how="left")
        .with_columns(pl.col("won_cfp").fill_null(0.0))
        .sort("won_cfp", "won_conf", "win_pct", descending=[True, True, True])
    )
    if "made_playoff" not in overall.columns:
        overall = overall.with_columns(
            pl.lit(0.0).alias("made_playoff"),
            pl.lit(0.0).alias("first_round_bye"),
        )
    game_summary = _game_summary(games_all)
    games_out = games_all.sort(["sim", "week", "game_type", "home_team"])

    out: dict[str, pl.DataFrame | Any] = {
        "standings": standings,
        "games": games_out,
        "overall": overall,
        "game_summary": game_summary,
    }
    if return_as_pandas:
        out = {k: v.to_pandas(use_pyarrow_extension_array=True) for k, v in out.items()}
    return out
