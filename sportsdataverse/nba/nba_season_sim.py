"""NBA / WNBA remaining-schedule season sim (BB-D18): projected W-L, seeds, playoffs, lottery.

A seeded Monte Carlo over the games not played yet. Each simulated season is ranked with the
league's tiebreakers, runs the NBA play-in, and draws the draft lottery
(:mod:`sportsdataverse.nba.nba_season_rules`).

**The game model** (:func:`as_of_game_model`). BB-D19's possession ridge
(:func:`sportsdataverse._common.team_strength.possession_ridge_ratings`) fit on the games played
before the as-of date, at this module's own penalty. The expected home margin of a game is
``(net_home - net_away + 2 * hfa) * poss / 100``: the ridge's adjusted net ratings, its home term
(per 100, applied to both offenses), and the as-of mean possessions per team-game. The home
team wins when ``margin + noise > 0``; the noise is normal with the as-of probit scale
``margin_sd``. Both constants are in :data:`SIM_CONSTANTS`, chosen on an as-of-date holdout.

**The preseason prior** (:func:`preseason_prior`). The ridge shrinks each team toward last
season's final rating times a fitted regression factor (:data:`PRIOR_CONSTANTS`) instead of
toward average, with the penalty as the prior's weight, so the prior fades as possessions come
in. Before the first game the ratings are the prior and the home term, possessions and variance
are last season's. An expansion team starts at last season's bottom-quartile net rating.

**Rating uncertainty.** A team's rating error is shared by all its remaining games, so the sim
draws one shift per team per simulated season from the ridge posterior,
``N(0, s2 / (n_off + lam) + s2 / (n_def + lam))`` (``s2`` the variance of points per 100 per
possession, ``n`` the team's possessions so far), and shrinks the per-game noise so the variance
of each game's margin stays ``margin_sd ** 2``. The marginal game probabilities are unchanged;
the season totals get the spread a fixed rating would understate.

**Monte Carlo error.** ``n_sims`` simulated seasons (default 10,000) from ``numpy``'s PCG64
seeded by ``seed``, so a call is reproducible. A probability's standard error is
``sqrt(p (1 - p) / n_sims)``, at most 0.005 at 10,000; ``p_playoffs_se`` and ``proj_wins_se``
report it per team.

**Not modelled:** rest, travel, injuries and roster moves (BB-D23); NBA Cup knockout games not
yet on the schedule (each team's two are added in December, so a sim before then projects 80
games); the playoffs past the play-in; offseason roster moves beyond what last season's rating
and the regression factor carry.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, overload

import numpy as np
import polars as pl

from sportsdataverse._common.team_strength import possession_ridge_ratings
from sportsdataverse.nba.nba_season_rules import (
    NBA_LOTTERY_DRAWS,
    WNBA_LOTTERY_DRAWS,
    WNBA_PLAYOFF_TEAMS,
    Season,
    draw_picks,
    lottery_chances,
    lottery_slots,
    results_arrays,
    team_labels,
)
from sportsdataverse.nba.nba_team_season_rates import _league_loaders, nba_possession_rate_rows

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["nba_season_sim"]

#: league_id -> (ridge penalty, probit margin sd in points). Chosen by the as-of-date holdout in
#: ``tools/fit_pro_season_ratings.py`` (2026-11-03): weekly cutoffs from day 14, the ratings
#: through each cutoff scoring every later regular-season game; the penalty and sd minimize
#: pooled log loss on the TRAIN seasons, the HOLDOUT seasons are scored once.
#:
#: * NBA, train 2018 2019 2022 2023, holdout 2024-2026 (41,042 game-cutoffs, 3,376 games): the
#:   ridge at 800 with sd 13.3 scores log loss 0.61673 / Brier 0.21434, against the existing
#:   ``nba_team_ratings`` box fixed point through ``nba_predict_games`` at 0.61891 / 0.21508
#:   (its sd refit to 18.7 on train) and 0.61927 / 0.21514 as shipped (sd 14.504). Paired:
#:   -0.00218 +- 0.00102 log loss (SE clustered by game). The BB-D19 descriptive penalty, 1600,
#:   scores 0.61895.
#: * WNBA, train 2018 2019 2021, holdout 2024 2025 (3,920 game-cutoffs, 461 games; 2022, 2023
#:   and 2026 stay reserved by the 2026-11-02 sim-vs-market spec): the ridge at 200 with sd 13.3
#:   scores 0.63241 / 0.21993, the box fixed point 0.63369 / 0.22015 refit and 0.64671 / 0.22318
#:   as shipped. Paired: -0.00129 +- 0.00252 against the refit box (a tie), -0.0143 +- 0.0055
#:   against it as shipped. BB-D19's 800 scores 0.63204 on holdout but 0.61162 vs 0.60698 on train.
SIM_CONSTANTS: dict[str, tuple[float, float]] = {"00": (800.0, 13.3), "10": (200.0, 13.3)}

#: league_id -> (regression factor, new-team quantile) of the preseason prior
#: (:func:`preseason_prior`): the ridge shrinks toward last season's final ratings times the
#: factor, with the :data:`SIM_CONSTANTS` penalty as the prior's weight (800 / 200 possessions on
#: each side, about 8 / 2.5 games). Fit by ``tools/fit_preseason_prior.py`` (2026-11-03): weekly
#: cutoffs from the first game (week 0 = preseason), the ratings through each cutoff scoring every
#: later regular-season game; the factor minimizes TRAIN log loss on a 0.1 grid at the shipped
#: penalty and sd, the HOLDOUT seasons are scored once (paired SE clustered by game).
#:
#: * NBA, train 2018 2019 2022 2023, holdout 2024-2026: factor 0.8 (train 0.64915 -> 0.64184).
#:   Holdout weeks 0-6 log loss 0.64468 -> 0.63434 (-0.01034 +- 0.00247; week 0 0.69315 ->
#:   0.65791), the whole season 0.62559 -> 0.62081 (-0.00478 +- 0.00158).
#: * WNBA, train 2018 2019 2021, holdout 2024 2025: factor 0.6 (train 0.62904 -> 0.62283).
#:   Holdout weeks 0-6 0.65745 -> 0.65512 (-0.00233 +- 0.00400, a tie on 526 games), the season
#:   0.64141 -> 0.63920 (-0.00221 +- 0.00279).
#: * Also scanned: the penalty and sd chosen jointly with the factor on train (NBA 0.7 / 3200 /
#:   11.7, WNBA 0.8 / 200 / 15.5). The NBA joint pick loses on the holdout after week 1 (whole
#:   season +0.00218 +- 0.00242), so the shipped fit keeps the BB-D18 penalty and sd, which that
#:   holdout validated mid-season. This was decided after seeing the holdout.
#: * The new-team quantile is not fit: no train season has an expansion team. 0.25 (last
#:   season's bottom-quartile net rating) is a stated default; the 2025 Valkyries are its only
#:   holdout case.
PRIOR_CONSTANTS: dict[str, tuple[float, float]] = {"00": (0.8, 0.25), "10": (0.6, 0.25)}

_SEEDS = 15

#: :func:`simulate_pro_season` / :func:`nba_season_sim` output, one row per team.
SIM_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64(),
    "as_of_date": pl.Date(),
    "team_id": pl.Int64(),
    "conference": pl.Utf8(),
    "division": pl.Utf8(),
    "wins": pl.Int64(),
    "losses": pl.Int64(),
    "games_remaining": pl.Int64(),
    "adj_net_rtg": pl.Float64(),
    "adj_net_rtg_sd": pl.Float64(),
    "proj_wins": pl.Float64(),
    "proj_losses": pl.Float64(),
    "proj_wins_sd": pl.Float64(),
    "proj_wins_p10": pl.Float64(),
    "proj_wins_p90": pl.Float64(),
    "proj_wins_se": pl.Float64(),
    "p_division": pl.Float64(),
    **{f"p_seed_{k}": pl.Float64() for k in range(1, _SEEDS + 1)},
    "mean_seed": pl.Float64(),
    "p_top4": pl.Float64(),
    "p_top6": pl.Float64(),
    "p_play_in": pl.Float64(),
    "p_playoffs": pl.Float64(),
    "p_playoffs_se": pl.Float64(),
    "p_lottery": pl.Float64(),
    "p_pick_1": pl.Float64(),
    "p_drawn_pick": pl.Float64(),
    "mean_pick": pl.Float64(),
    "n_sims": pl.Int64(),
}

_GAME_COLS = ("season", "game_id", "home_team_id", "away_team_id", "home_pts", "away_pts")


@dataclass(frozen=True)
class GameModel:
    """As-of ratings and the constants that turn them into game margins.

    Attributes:
        ratings: One row per team: ``team_id`` (Int64), ``adj_net_rtg`` and ``adj_net_rtg_sd``
            (the posterior sd of the net rating), points per 100.
        hfa: The ridge's home term, points per 100 for the home offense over a neutral court.
        poss: Mean possessions per team per game.
        margin_sd: The probit scale of a game's margin, points.
        prior_sd: The posterior sd of a team with no possessions (the prior), points per 100.
    """

    ratings: pl.DataFrame
    hfa: float
    poss: float
    margin_sd: float
    prior_sd: float

    def margin(self, net_home: np.ndarray, net_away: np.ndarray) -> np.ndarray:
        """Expected home margin in points for home / away net ratings."""
        return (net_home - net_away + 2.0 * self.hfa) * self.poss / 100.0


@dataclass(frozen=True)
class PreseasonPrior:
    """Last season's final ratings, regressed to the mean, as the prior of the as-of ridge.

    Attributes:
        ratings: One row per team: ``team_id`` (Int64), ``off_dev`` / ``def_dev`` (points per 100
            over / under the league average, already regressed) and ``new_team`` (the
            expansion default, :func:`preseason_prior`).
        hfa: Last season's home term, per 100; used until this season has a game.
        poss: Last season's possessions per team-game; likewise.
        s2: Last season's variance of points per 100 per possession; likewise.
    """

    ratings: pl.DataFrame
    hfa: float
    poss: float
    s2: float


def _ridge_s2(rows: pl.DataFrame) -> float:
    return float((100.0 * rows.get_column("points").cast(pl.Float64)).var() or 0.0)


def preseason_prior(
    prev_rows: pl.DataFrame,
    teams: list[int],
    *,
    league_id: str = "00",
    regression: float | None = None,
    new_team_quantile: float | None = None,
) -> PreseasonPrior | None:
    """The preseason prior from last season's possessions.

    Last season's final ratings are BB-D19's published ridge
    (:func:`~sportsdataverse._common.team_strength.possession_ridge_ratings` at
    :data:`~sportsdataverse._common.team_strength.RIDGE_LAMBDA`). Each team's offense and defense
    coefficient is multiplied by the fitted regression factor (:data:`PRIOR_CONSTANTS`). A team
    that did not play last season (an expansion team) starts at the ``new_team_quantile`` of
    last season's net ratings, unregressed, split evenly between offense and defense.

    Args:
        prev_rows: Last season's regular-season possession rows
            (:data:`~sportsdataverse._common.team_season_rates.RATE_ROWS_SCHEMA`).
        teams: This season's team ids (Int64).
        league_id: ``"00"`` NBA or ``"10"`` WNBA.
        regression: Override the fitted factor (the fitting script scans it).
        new_team_quantile: Override the fitted new-team quantile.

    Returns:
        A :class:`PreseasonPrior`, or None when ``prev_rows`` is empty.
    """
    from sportsdataverse._common.team_strength import RIDGE_LAMBDA

    if prev_rows.height == 0:
        return None
    reg, new_q = PRIOR_CONSTANTS[league_id]
    reg = reg if regression is None else regression
    new_q = new_q if new_team_quantile is None else new_team_quantile
    final = possession_ridge_ratings(prev_rows, lam=RIDGE_LAMBDA[league_id])
    new_net = float(final.get_column("adj_net_rtg").quantile(new_q, interpolation="linear"))
    devs = final.select(
        "team_id",
        (pl.col("adj_off_rtg") - pl.col("adj_off_rtg").mean()).alias("o"),
        (pl.col("adj_def_rtg") - pl.col("adj_def_rtg").mean()).alias("d"),
    )
    ids = pl.DataFrame({"team_id": sorted(set(teams))}, schema={"team_id": pl.Int64()})
    assert ids.schema["team_id"] == devs.schema["team_id"], "team_id dtype differs across the join"
    ratings = ids.join(devs, on="team_id", how="left", validate="1:1").select(
        "team_id",
        pl.when(pl.col("o").is_null()).then(pl.lit(new_net / 2.0)).otherwise(reg * pl.col("o")).alias("off_dev"),
        pl.when(pl.col("d").is_null()).then(pl.lit(-new_net / 2.0)).otherwise(reg * pl.col("d")).alias("def_dev"),
        pl.col("o").is_null().alias("new_team"),
    )
    poss = prev_rows.height / (2.0 * prev_rows.get_column("game_id").n_unique())
    return PreseasonPrior(ratings, float(final.get_column("hfa")[0]), poss, _ridge_s2(prev_rows))


def as_of_game_model(rows: pl.DataFrame, *, league_id: str = "00", prior: PreseasonPrior | None = None) -> GameModel:
    """The season sim's game model from the possessions played so far.

    Args:
        rows: Possession rows (:data:`~sportsdataverse._common.team_season_rates.RATE_ROWS_SCHEMA`)
            of the games before the as-of date only.
        league_id: ``"00"`` NBA or ``"10"`` WNBA; selects :data:`SIM_CONSTANTS`.
        prior: The preseason prior (:func:`preseason_prior`). The ridge then shrinks each team
            toward its prior instead of toward average, and a team with no possession yet is
            rated at its prior. None: every team starts average.

    Returns:
        A :class:`GameModel`. With no rows and no prior, every team is average (no ratings) and
        the home term is 0; with a prior and no rows, the ratings are the prior and the home
        term, possessions and per-possession variance are last season's.

    Raises:
        ValueError: If ``league_id`` has no :data:`SIM_CONSTANTS` entry.
    """
    if league_id not in SIM_CONSTANTS:
        raise ValueError(f"league_id must be one of {sorted(SIM_CONSTANTS)}, got {league_id!r}")
    lam, sd = SIM_CONSTANTS[league_id]
    schema = {"team_id": pl.Int64(), "adj_net_rtg": pl.Float64(), "adj_net_rtg_sd": pl.Float64()}
    if rows.height == 0:
        if prior is None:
            return GameModel(pl.DataFrame(schema=schema), 0.0, 100.0, sd, float("nan"))
        prior_sd = float(np.sqrt(2.0 * prior.s2 / lam))
        ratings = prior.ratings.select(
            "team_id",
            (pl.col("off_dev") - pl.col("def_dev")).alias("adj_net_rtg"),
            pl.lit(prior_sd).alias("adj_net_rtg_sd"),
        ).cast(schema)
        return GameModel(ratings, prior.hfa, prior.poss, sd, prior_sd)
    rat = possession_ridge_ratings(rows, lam=lam, prior=None if prior is None else prior.ratings)
    if prior is not None:  # a team with no possession yet is rated at its prior
        unseen = prior.ratings.join(rat.select("team_id"), on="team_id", how="anti")
        rat = pl.concat(
            [
                rat.select("team_id", "adj_net_rtg", "hfa"),
                unseen.select(
                    "team_id",
                    (pl.col("off_dev") - pl.col("def_dev")).alias("adj_net_rtg"),
                    pl.lit(rat.get_column("hfa")[0], dtype=pl.Float64).alias("hfa"),
                ),
            ]
        )
    s2 = _ridge_s2(rows)
    n_off = rows.group_by(pl.col("offense_team_id").alias("team_id")).len("n_off")
    n_def = rows.group_by(pl.col("defense_team_id").alias("team_id")).len("n_def")
    for frame in (n_off, n_def):
        assert frame.schema["team_id"] == rat.schema["team_id"], "team_id dtype differs across the joins"
    ratings = (
        rat.join(n_off, on="team_id", how="left", validate="1:1")
        .join(n_def, on="team_id", how="left", validate="1:1")
        .select(
            "team_id",
            "adj_net_rtg",
            (s2 / (pl.col("n_off").fill_null(0) + lam) + s2 / (pl.col("n_def").fill_null(0) + lam))
            .sqrt()
            .alias("adj_net_rtg_sd"),
        )
        .cast(schema)
    )
    poss = rows.height / (2.0 * rows.get_column("game_id").n_unique())
    return GameModel(ratings, float(rat.get_column("hfa")[0]), poss, sd, float(np.sqrt(2.0 * s2 / lam)))


def _team_arrays(model: GameModel, teams: list[int]) -> tuple[np.ndarray, np.ndarray]:
    """(net, sd) per team in ``teams`` order; a team the model has not seen is average."""
    net = dict(zip(model.ratings["team_id"].to_list(), model.ratings["adj_net_rtg"].to_list(), strict=True))
    sd = dict(zip(model.ratings["team_id"].to_list(), model.ratings["adj_net_rtg_sd"].to_list(), strict=True))
    prior = model.prior_sd if np.isfinite(model.prior_sd) else 0.0
    return np.array([net.get(t, 0.0) for t in teams]), np.array([sd.get(t, prior) for t in teams])


def _noise_sd(model: GameModel, sd_home: np.ndarray, sd_away: np.ndarray, uncertain: bool) -> np.ndarray:
    """Per-game noise sd: the probit scale less the rating variance the season shift carries."""
    var = model.margin_sd**2 - (sd_home**2 + sd_away**2) * (model.poss / 100.0) ** 2 * float(uncertain)
    # the caller caps sd so var >= margin_sd**2 / 4; the floor only absorbs rounding
    return np.sqrt(np.maximum(var, 0.25 * model.margin_sd**2))


def _play_game(model: GameModel, net: np.ndarray, noise: float, home: int, away: int, rng: np.random.Generator) -> bool:
    """One simulated game between two teams (``net`` already shifted): True if home wins."""
    return bool(model.margin(net[home], net[away]) + noise * rng.standard_normal() > 0)


def _simulate_games(
    model: GameModel,
    net: np.ndarray,
    sd: np.ndarray,
    h: np.ndarray,
    a: np.ndarray,
    played: tuple[np.ndarray, np.ndarray, np.ndarray],
    *,
    n_sims: int,
    wnba: bool,
    uncertain: bool,
    rng: np.random.Generator,
) -> tuple[np.ndarray, np.ndarray | None, np.ndarray, np.ndarray]:
    """``(wins, pair_diff, point_diff, shift)`` per simulated season, the played games included."""
    w0, pd0, pair0 = played
    n_t = net.size
    shift = rng.standard_normal((n_sims, n_t)) * sd if uncertain else np.zeros((n_sims, n_t))
    scale = model.poss / 100.0
    margins = (
        model.margin(net[h], net[a])
        + (shift[:, h] - shift[:, a]) * scale
        + _noise_sd(model, sd[h], sd[a], uncertain) * rng.standard_normal((n_sims, h.size))
    )
    home_won = margins > 0
    wins = np.broadcast_to(w0.astype(np.int16), (n_sims, n_t, n_t)).copy()
    pair = np.broadcast_to(pair0, (n_sims, n_t, n_t)).copy() if wnba else None
    for u in range(h.size):
        wins[:, h[u], a[u]] += home_won[:, u]
        wins[:, a[u], h[u]] += ~home_won[:, u]
        if pair is not None:
            pair[:, h[u], a[u]] += margins[:, u]
            pair[:, a[u], h[u]] -= margins[:, u]
    signs = np.zeros((h.size, n_t))
    signs[np.arange(h.size), h] = 1.0
    signs[np.arange(h.size), a] = -1.0
    return wins, pair, pd0 + margins @ signs, shift


def _play_in(
    model: GameModel, net: np.ndarray, noise: np.ndarray, seeds: np.ndarray, conf: np.ndarray, rng: np.random.Generator
) -> np.ndarray:
    """``(T,)`` playoff berths for one NBA season: seeds 1-6, then the simulated play-in."""
    playoff: np.ndarray = np.zeros(seeds.size, dtype=bool)
    for c in np.unique(conf):
        by_seed = {int(seeds[t]): int(t) for t in np.flatnonzero(conf == c)}
        playoff[[by_seed[k] for k in range(1, 7)]] = True
        t7, t8, t9, t10 = (by_seed[k] for k in (7, 8, 9, 10))
        win78 = _play_game(model, net, float(noise[t7, t8]), t7, t8, rng)
        playoff[t7 if win78 else t8] = True
        loser78 = t8 if win78 else t7
        win910 = t9 if _play_game(model, net, float(noise[t9, t10]), t9, t10, rng) else t10
        last = _play_game(model, net, float(noise[loser78, win910]), loser78, win910, rng)
        playoff[loser78 if last else win910] = True
    return playoff


def _prior_arrays(prior_records: pl.DataFrame | None, index: dict[int, int]) -> tuple[np.ndarray, np.ndarray]:
    """(prior wins, prior games) per team index; zeros for a team with no prior season."""
    prior_w, prior_g = np.zeros(len(index)), np.zeros(len(index))
    if prior_records is not None:
        assert prior_records.schema["team_id"] == pl.Int64, "prior_records team_id must be Int64"
        for t, pw, plo in prior_records.select("team_id", "wins", "losses").iter_rows():
            if t in index:
                prior_w[index[t]], prior_g[index[t]] = pw, pw + plo
    return prior_w, prior_g


def _summary(
    sims: dict[str, np.ndarray], total_games: np.ndarray, *, league_id: str, draws: int, have_odds: bool
) -> dict[str, list]:
    """The per-team projection and odds columns from the simulated seasons."""
    seed_of, playoff, pick_of, total_wins = sims["seed_of"], sims["playoff"], sims["pick_of"], sims["total_wins"]
    n_sims, n_t = seed_of.shape
    nba = league_id == "00"
    none: list = [None] * n_t
    in_lottery = ~playoff
    with np.errstate(invalid="ignore"):
        mean_pick = np.where(in_lottery, pick_of, 0).sum(0) / in_lottery.sum(0)
    p_playoffs = playoff.mean(0)
    return {
        "proj_wins": total_wins.mean(0).tolist(),
        "proj_losses": (total_games - total_wins).mean(0).tolist(),
        "proj_wins_sd": total_wins.std(0).tolist(),
        "proj_wins_p10": np.quantile(total_wins, 0.1, axis=0).tolist(),
        "proj_wins_p90": np.quantile(total_wins, 0.9, axis=0).tolist(),
        "proj_wins_se": (total_wins.std(0) / np.sqrt(n_sims)).tolist(),
        "p_division": sims["leader"].mean(0).tolist() if nba else none,
        **{f"p_seed_{k}": (seed_of == k).mean(0).tolist() for k in range(1, _SEEDS + 1)},
        "mean_seed": seed_of.mean(0).tolist(),
        "p_top4": (seed_of <= 4).mean(0).tolist(),
        "p_top6": (seed_of <= 6).mean(0).tolist() if nba else none,
        "p_play_in": ((seed_of >= 7) & (seed_of <= 10)).mean(0).tolist() if nba else none,
        "p_playoffs": p_playoffs.tolist(),
        "p_playoffs_se": np.sqrt(p_playoffs * (1 - p_playoffs) / n_sims).tolist(),
        "p_lottery": in_lottery.mean(0).tolist(),
        "p_pick_1": (pick_of == 1).mean(0).tolist() if have_odds else none,
        "p_drawn_pick": ((pick_of >= 1) & (pick_of <= draws)).mean(0).tolist() if have_odds else none,
        "mean_pick": [None if not have_odds or np.isnan(v) else float(v) for v in mean_pick],
        "n_sims": [n_sims] * n_t,
    }


def _rank_seasons(
    sims: tuple[np.ndarray, np.ndarray | None, np.ndarray, np.ndarray],
    model: GameModel,
    net: np.ndarray,
    noise: np.ndarray,
    labels: tuple[np.ndarray, np.ndarray],
    *,
    league_id: str,
    lottery: tuple,
    rng: np.random.Generator,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Rank every simulated season, run the play-in, and order its lottery slots.

    Returns:
        ``(seed_of, leader, playoff, slot_team, slot_chance)``: per season and team the seed, the
        division-winner flag and the playoff berth; per season and lottery slot the team index
        and its chances (unused when the league has no published chances).
    """
    wins, pair, point_diff, shift = sims
    conf, div = labels
    lot_chances, n_lottery, prior_w, prior_g, total_wins, total_games = lottery
    n_sims, n_t = point_diff.shape
    seed_of: np.ndarray = np.zeros((n_sims, n_t), dtype=np.int64)
    leader: np.ndarray = np.zeros((n_sims, n_t), dtype=bool)
    playoff: np.ndarray = np.zeros((n_sims, n_t), dtype=bool)
    slot_team: np.ndarray = np.zeros((n_sims, max(n_lottery, 0)), dtype=np.int64)
    slot_chance = np.ones((n_sims, max(n_lottery, 0)))
    for s in range(n_sims):
        pair_s = None if pair is None else pair[s]
        season_s = Season(wins[s], point_diff[s], conf, div, league_id=league_id, rng=rng, pair_diff=pair_s)
        seed_of[s] = season_s.seeds()
        leader[s] = season_s.leader
        if league_id == "00":
            playoff[s] = _play_in(model, net + shift[s], noise, seed_of[s], conf, rng)
        else:
            playoff[s] = seed_of[s] <= WNBA_PLAYOFF_TEAMS
        if lot_chances is not None:
            out_ = np.flatnonzero(~playoff[s])
            pct = season_s.pct[out_]
            if league_id == "10":
                comb = (prior_w[out_] + total_wins[s, out_]) / (prior_g[out_] + total_games[s, out_])
                order, sc = lottery_slots(comb, pct, lot_chances, rng)
            else:
                order, sc = lottery_slots(pct, np.zeros(out_.size), lot_chances, rng)
            slot_team[s], slot_chance[s] = out_[order], sc
    return seed_of, leader, playoff, slot_team, slot_chance


def _check_inputs(games: pl.DataFrame, model: GameModel, *, league_id: str, n_sims: int) -> None:
    """The argument checks of :func:`simulate_pro_season` (see its ``Raises``)."""
    if league_id not in SIM_CONSTANTS:
        raise ValueError(f"league_id must be one of {sorted(SIM_CONSTANTS)}, got {league_id!r}")
    if games.get_column("season").n_unique() != 1:
        raise ValueError("games must hold exactly one season")
    for key in ("home_team_id", "away_team_id"):
        if games.schema[key] != model.ratings.schema["team_id"]:
            raise TypeError(f"{key} is {games.schema[key]}, model team_id is {model.ratings.schema['team_id']}")
    if n_sims < 1:
        raise ValueError(f"n_sims must be at least 1, got {n_sims}")


def _check_chances(chances: list[float] | None, n_lottery: int, league_id: str) -> None:
    """Caller-supplied lottery chances: one positive, finite value per lottery team, at least the draws."""
    if chances is not None and (
        len(chances) != n_lottery
        or len(chances) < (NBA_LOTTERY_DRAWS if league_id == "00" else WNBA_LOTTERY_DRAWS)
        or not all(np.isfinite(chances))
        or min(chances, default=0) <= 0
    ):
        raise ValueError(f"chances must be {n_lottery} positive, finite values (at least the draws), got {chances}")


def simulate_pro_season(
    games: pl.DataFrame,
    model: GameModel,
    *,
    league_id: str = "00",
    n_sims: int = 10_000,
    seed: int = 20261103,
    prior_records: pl.DataFrame | None = None,
    chances: list[float] | None = None,
    rating_uncertainty: bool = True,
    as_of_date: dt.date | None = None,
) -> pl.DataFrame:
    """Monte Carlo of one regular season's remaining games, with standings, play-in and lottery.

    Args:
        games: One season's regular-season games: ``season``, ``game_id``, ``home_team_id``,
            ``away_team_id`` (Int64), ``home_pts`` / ``away_pts`` (null for a game to simulate).
        model: The game model (:func:`as_of_game_model`).
        league_id: ``"00"`` NBA or ``"10"`` WNBA.
        n_sims: Simulated seasons.
        seed: Seeds the generator; the same inputs and seed give the same output.
        prior_records: WNBA only: the previous regular season's ``team_id``, ``wins``,
            ``losses``, added to the season's record for the lottery order. A team with no row
            (an expansion team) counts this season only.
        chances: Lottery chances per slot, worst first, overriding the published table; one
            positive, finite value per lottery team.
        rating_uncertainty: Draw one rating shift per team per season (see the module doc).
            False keeps the ratings fixed; the validation reports both.
        as_of_date: Carried into the output only.

    Returns:
        One row per team in :data:`SIM_SCHEMA`.

    Raises:
        ValueError: If ``games`` holds more than one season, ``league_id`` is unknown,
            ``n_sims < 1``, ``chances`` is malformed, or a conference (NBA) or the league (WNBA)
            has more than 15 teams.
        TypeError: If a team-id dtype differs from the model's ``team_id``.
    """
    _check_inputs(games, model, league_id=league_id, n_sims=n_sims)
    rng = np.random.default_rng(seed)
    teams = sorted(set(games["home_team_id"].to_list()) | set(games["away_team_id"].to_list()))
    index = {t: i for i, t in enumerate(teams)}
    n_t = len(teams)
    conf, div = team_labels(teams, league_id)
    if max(np.unique(conf, return_counts=True)[1]) > _SEEDS:
        raise ValueError(f"more than {_SEEDS} teams in a seeding table; SIM_SCHEMA has p_seed_1..{_SEEDS}")
    is_played = pl.col("home_pts").is_not_null() & pl.col("away_pts").is_not_null()
    played = results_arrays(games.filter(is_played), index)
    future = games.filter(~is_played)
    net, sd = _team_arrays(model, teams)
    # the shift of two teams may carry at most 3/4 of a game's variance, so the per-game noise
    # (``_noise_sd``) can always restore the margin_sd marginal; binds only near the prior
    sd = np.minimum(sd, np.sqrt(0.375) * model.margin_sd / (model.poss / 100.0))
    h = np.array([index[t] for t in future["home_team_id"].to_list()], dtype=np.int64)
    a = np.array([index[t] for t in future["away_team_id"].to_list()], dtype=np.int64)
    wins, pair, point_diff, shift = _simulate_games(
        model, net, sd, h, a, played, n_sims=n_sims, wnba=league_id == "10", uncertain=rating_uncertainty, rng=rng
    )
    total_wins = wins.sum(2)
    total_games = total_wins + wins.sum(1)
    n_lottery = n_t - (16 if league_id == "00" else WNBA_PLAYOFF_TEAMS)
    _check_chances(chances, n_lottery, league_id)
    lot_chances = chances if chances is not None else lottery_chances(league_id, n_lottery)
    draws = NBA_LOTTERY_DRAWS if league_id == "00" else WNBA_LOTTERY_DRAWS
    have_odds = lot_chances is not None and n_lottery >= draws
    prior_w, prior_g = _prior_arrays(prior_records, index)

    seed_of, leader, playoff, slot_team, slot_chance = _rank_seasons(
        (wins, pair, point_diff, shift),
        model,
        net,
        _noise_sd(model, sd[:, None], sd[None, :], rating_uncertainty),
        (conf, div),
        league_id=league_id,
        lottery=(lot_chances if have_odds else None, n_lottery, prior_w, prior_g, total_wins, total_games),
        rng=rng,
    )

    draws = draws if have_odds else 0
    pick_of: np.ndarray = np.zeros((n_sims, n_t), dtype=np.int64)
    if have_odds:
        np.put_along_axis(pick_of, slot_team, draw_picks(slot_chance, draws, rng), axis=1)
    sims = {"seed_of": seed_of, "leader": leader, "playoff": playoff, "pick_of": pick_of, "total_wins": total_wins}
    out = {
        "season": [int(games.get_column("season")[0])] * n_t,
        "as_of_date": [as_of_date] * n_t,
        "team_id": teams,
        "conference": conf.tolist(),
        "division": div.tolist(),
        "wins": played[0].sum(1).tolist(),
        "losses": played[0].sum(0).tolist(),
        "games_remaining": (np.bincount(h, minlength=n_t) + np.bincount(a, minlength=n_t)).tolist(),
        "adj_net_rtg": net.tolist(),
        "adj_net_rtg_sd": sd.tolist(),
        **_summary(sims, total_games, league_id=league_id, draws=draws, have_odds=have_odds),
    }
    return pl.DataFrame(out, schema=SIM_SCHEMA).sort("conference", "mean_seed")


def _regular_games(schedule: pl.DataFrame, as_of_date: dt.date | None) -> pl.DataFrame:
    """The regular-season games, scores blanked on and after ``as_of_date``.

    Games without both teams are dropped: until December the release lists the NBA Cup knockout
    games with TBD teams (``team_id`` 0), which are not on any team's schedule yet.
    """
    reg = (
        schedule.filter(pl.col("season_type") == "regular-season")
        .with_columns(
            pl.col("game_id").cast(pl.Utf8),
            pl.col("game_date").cast(pl.Utf8).str.slice(0, 10).str.to_date().alias("game_date"),
            pl.col("home_team_id").cast(pl.Int64),
            pl.col("away_team_id").cast(pl.Int64),
        )
        .filter((pl.col("home_team_id") > 0) & (pl.col("away_team_id") > 0))
    )
    if as_of_date is not None:
        future = pl.col("game_date") >= as_of_date
        reg = reg.with_columns(
            pl.when(future).then(None).otherwise(pl.col(c)).alias(c) for c in ("home_pts", "away_pts")
        )
    return reg


def _load_inputs(
    season: int, as_of_date: dt.date | None, league_id: str
) -> tuple[pl.DataFrame, GameModel, pl.DataFrame | None]:
    """(regular-season games, as-of game model, WNBA prior-season records) from the releases."""
    load_possessions, load_schedule, offset = _league_loaders(league_id)
    schedule = load_schedule([season - offset])
    games = _regular_games(schedule, as_of_date)
    done = games.filter(pl.col("home_pts").is_not_null() & pl.col("away_pts").is_not_null())
    rows = nba_possession_rate_rows(load_possessions([season - offset]), schedule, league_id=league_id)
    assert rows.schema["game_id"] == done.schema["game_id"], "game_id dtype differs between possessions and schedule"
    last_schedule = load_schedule([season - 1 - offset])
    prior = None
    if last_schedule.height:
        last_rows = nba_possession_rate_rows(
            load_possessions([season - 1 - offset]), last_schedule, league_id=league_id
        )
        teams = sorted(set(games["home_team_id"].to_list()) | set(games["away_team_id"].to_list()))
        prior = preseason_prior(last_rows, teams, league_id=league_id)
    model = as_of_game_model(
        rows.join(done.select("game_id"), on="game_id", how="semi"), league_id=league_id, prior=prior
    )
    records = None
    if league_id == "10":
        records = _records(_regular_games(last_schedule, None))
    return games, model, records


@overload
def nba_season_sim(
    season: int,
    *,
    as_of_date: dt.date | None = None,
    league_id: str = "00",
    n_sims: int = 10_000,
    seed: int = 20261103,
    chances: list[float] | None = None,
    return_as_pandas: Literal[False] = False,
) -> pl.DataFrame: ...
@overload
def nba_season_sim(
    season: int,
    *,
    as_of_date: dt.date | None = None,
    league_id: str = "00",
    n_sims: int = 10_000,
    seed: int = 20261103,
    chances: list[float] | None = None,
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...
def nba_season_sim(
    season: int,
    *,
    as_of_date: dt.date | None = None,
    league_id: str = "00",
    n_sims: int = 10_000,
    seed: int = 20261103,
    chances: list[float] | None = None,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Season sim as of a date: projected W-L, division, seed, play-in, playoff and lottery odds.

    Reads the ``{nba,wnba}_stats_schedules`` and ``_possessions`` releases for the season and the
    one before, fits the game model on the regular-season games played before ``as_of_date``,
    shrunk toward last season's final ratings (:func:`as_of_game_model`, :func:`preseason_prior`),
    and simulates the rest (:func:`simulate_pro_season`). Before the first game the ratings are
    the prior alone. The rules applied to each simulated season
    are in :mod:`sportsdataverse.nba.nba_season_rules`.

    Args:
        season: The season, named by the year it ends (``2027`` is 2026-27); the WNBA season is
            its calendar year.
        as_of_date: Games on or after this date are simulated even when the release has their
            score (a backtest). None: every game with a score counts as played.
        league_id: ``"00"`` NBA or ``"10"`` WNBA.
        n_sims: Simulated seasons (Monte Carlo error: see :data:`SIM_SCHEMA`'s ``_se`` columns).
        seed: Seeds the generator; the same release data and seed give the same output.
        chances: Lottery chances per slot, worst record first, one per lottery team; overrides the
            published table (needed for the 2027 WNBA lottery's seven teams, not published yet).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per team (stats.nba.com ``team_id``, Int64), sorted by conference and mean seed:

        * ``wins`` / ``losses`` so far, ``games_remaining``, and the as-of ``adj_net_rtg`` with its
          posterior sd ``adj_net_rtg_sd`` (capped so two teams' shift carries at most 3/4 of a
          game's variance; it binds only near the prior, before many games are played).
        * ``proj_wins`` / ``proj_losses`` (means), ``proj_wins_sd``, the 10th and 90th
          percentiles, and ``proj_wins_se`` (the Monte Carlo error of the mean).
        * ``p_division`` (NBA), ``p_seed_1`` .. ``p_seed_15`` (the regular-season finish in the
          conference, NBA, or the league, WNBA) and ``mean_seed``.
        * ``p_top4`` (first-round home court), ``p_top6`` (NBA, straight to the playoffs),
          ``p_play_in`` (NBA, seeds 7-10), ``p_playoffs`` (NBA: top 6 or through the play-in;
          WNBA: top 8) and its Monte Carlo ``p_playoffs_se``.
        * ``p_lottery``, ``p_pick_1``, ``p_drawn_pick`` (a pick from the drawing: the NBA's top
          four, the WNBA's top two) and ``mean_pick`` (given a lottery finish). The pick columns
          are null when the league has no published chances for that many lottery teams.
        * ``n_sims``.

    Raises:
        ValueError: If ``league_id`` is not ``"00"`` or ``"10"``.

    Example:
        The 2025-26 NBA as of the All-Star break::

            import datetime as dt
            from sportsdataverse.nba.nba_season_sim import nba_season_sim
            sim = nba_season_sim(2026, as_of_date=dt.date(2026, 2, 13))
            sim.select("team_id", "wins", "proj_wins", "p_playoffs", "p_pick_1").head()

        WNBA via ``league_id``::

            nba_season_sim(2025, as_of_date=dt.date(2025, 7, 1), league_id="10")

    See Also:
        * :func:`sportsdataverse.nba.nba_season_rules.nba_season_standings` -- the standings and
          tiebreakers each simulated season is ranked with
        * :func:`sportsdataverse.mbb.mbb_season_sim.mbb_season_sim` -- the college season sim
        * `hoopR <https://hoopR.sportsdataverse.org>`_ -- men's basketball (R)
        * `wehoop <https://wehoop.sportsdataverse.org>`_ -- women's basketball (R)
    """
    if league_id not in SIM_CONSTANTS:
        raise ValueError(f"league_id must be one of {sorted(SIM_CONSTANTS)}, got {league_id!r}")
    games, model, prior = _load_inputs(season, as_of_date, league_id)
    out = simulate_pro_season(
        games.select(*_GAME_COLS).with_columns(pl.lit(season, dtype=pl.Int64).alias("season")),
        model,
        league_id=league_id,
        n_sims=n_sims,
        seed=seed,
        prior_records=prior,
        chances=chances,
        as_of_date=as_of_date,
    )
    return out.to_pandas() if return_as_pandas else out


def _records(games: pl.DataFrame) -> pl.DataFrame:
    """``team_id``, ``wins``, ``losses`` over the completed games of ``games``."""
    done = games.filter(pl.col("home_pts").is_not_null() & pl.col("away_pts").is_not_null())
    home_won = pl.col("home_pts") > pl.col("away_pts")
    sides = pl.concat(
        [
            done.select(pl.col("home_team_id").alias("team_id"), home_won.alias("won")),
            done.select(pl.col("away_team_id").alias("team_id"), (~home_won).alias("won")),
        ]
    )
    return sides.group_by("team_id").agg(
        pl.col("won").sum().cast(pl.Int64).alias("wins"), (~pl.col("won")).sum().cast(pl.Int64).alias("losses")
    )
