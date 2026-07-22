"""Season-to-date data glue + walk-forward calibration backtest.

The full-season directive realized: one published-release parquet per
season feeds the node tree directly (the ``*_stats_pbp`` releases are
v3-schema — the classifiers' native input), with an as-of cutoff so
walk-forward refits never see past their date. The companion
``*_stats_schedules`` release (leaguegamelog shape, same game-id
namespace) supplies dates, matchups, and finals.

:func:`walk_forward_backtest` is deliberately a CALIBRATION harness: the
engine is team-symmetric today (league shelf; attribution only credits
players), so it cannot claim winner discrimination — what it CAN gate,
leakage-safe, is whether realized totals and margins fall where the
season-fitted tree says they should. Team-strength factors are the next
layer on top of this seam.

Loader imports are lazy so the module stays importable without the live
sdv-py data surface (the sdv-engine ``[live]`` extra provides it).
"""

from __future__ import annotations

import datetime as dt
from typing import Any, Dict, List, Optional

import numpy as np
import polars as pl

from sportsdataverse.nba.nba_possession_sim.engine import simulate_ensemble
from sportsdataverse.nba.nba_possession_sim.keygen import fit_learned_gamestate_keyer
from sportsdataverse.nba.nba_possession_sim.node_models import models_to_shelf
from sportsdataverse.nba.nba_possession_sim.rules import NBA_RULES, WNBA_RULES, SportRules
from sportsdataverse.nba.nba_possession_sim.shelf import (
    Shelf,
    player_game_logs_from_pbp,
    possessions_from_pbp,
)

_LEAGUES = ("wnba", "nba")


def _rules_for(league: str) -> SportRules:
    return WNBA_RULES if league == "wnba" else NBA_RULES


def _load_stats_pbp(league: str, seasons: List[int]) -> pl.DataFrame:
    if league == "wnba":
        from sportsdataverse.wnba.wnba_loaders import load_wnba_stats_pbp

        return load_wnba_stats_pbp(seasons)
    from sportsdataverse.nba.nba_loaders import load_nba_stats_pbp

    return load_nba_stats_pbp(seasons)


def _load_stats_schedule(league: str, seasons: List[int]) -> pl.DataFrame:
    if league == "wnba":
        from sportsdataverse.wnba.wnba_loaders import load_wnba_stats_schedules

        return load_wnba_stats_schedules(seasons)
    from sportsdataverse.nba.nba_loaders import load_nba_stats_schedules

    return load_nba_stats_schedules(seasons)


def games_from_leaguegamelog(log: pl.DataFrame) -> pl.DataFrame:
    """Pivot a leaguegamelog (two team-rows per game) into one-row games.

    Args:
        log: A ``*_stats_schedules`` frame (``GAME_ID`` / ``GAME_DATE`` /
            ``TEAM_ID`` / ``MATCHUP`` / ``PTS`` — home rows carry
            ``" vs. "`` in the matchup, away rows ``" @ "``).

    Returns:
        One row per game: ``game_id``, ``game_date`` (Date),
        ``home_team_id``, ``away_team_id``, ``home_pts``, ``away_pts``
        (null until final), ``completed``.

    Raises:
        ValueError: When required columns are absent.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_possession_sim.season import (
                games_from_leaguegamelog,
            )
            games = games_from_leaguegamelog(log)
            games.filter(pl.col("completed") == True).height  # noqa: E712
    """
    required = ["GAME_ID", "GAME_DATE", "TEAM_ID", "MATCHUP", "PTS"]
    missing = sorted(set(required) - set(log.columns))
    if missing:
        raise ValueError(f"leaguegamelog frame is missing columns: {missing}")
    base = log.select(
        pl.col("GAME_ID").cast(pl.Utf8).alias("game_id"),
        pl.col("GAME_DATE").cast(pl.Utf8).str.strptime(pl.Date, "%Y-%m-%d").alias("game_date"),
        pl.col("TEAM_ID").cast(pl.Int64).alias("team_id"),
        pl.col("MATCHUP").cast(pl.Utf8).alias("matchup"),
        pl.col("PTS").cast(pl.Int64, strict=False).alias("pts"),
    )
    home = base.filter(pl.col("matchup").str.contains(" vs. ")).select(
        "game_id",
        "game_date",
        pl.col("team_id").alias("home_team_id"),
        pl.col("pts").alias("home_pts"),
    )
    away = base.filter(pl.col("matchup").str.contains(" @ ")).select(
        "game_id",
        pl.col("team_id").alias("away_team_id"),
        pl.col("pts").alias("away_pts"),
    )
    return (
        home.join(away, on="game_id", how="inner")
        .with_columns((pl.col("home_pts").is_not_null() & pl.col("away_pts").is_not_null()).alias("completed"))
        .sort("game_date", "game_id")
    )


def season_data(
    league: str,
    seasons: List[int],
    *,
    through: Optional[dt.date] = None,
) -> Dict[str, Any]:
    """Season-to-date inputs for the node tree, leakage-cut at ``through``.

    Args:
        league: ``"wnba"`` or ``"nba"``.
        seasons: Seasons to load (release parquets, one download each).
        through: Optional INCLUSIVE cutoff — only games with
            ``game_date <= through`` contribute (pass the day before an
            evaluation date for as-of refits).

    Returns:
        ``{"schedule": games, "pbp": raw v3 rows of the included completed
        games, "events": classified possessions, "logs": per-player game
        logs (with ``ftm``)}``.

    Raises:
        ValueError: On an unknown league or when no completed games
            survive the cutoff.

    Example:
        Quick start::

            import datetime as dt
            from sportsdataverse.nba.nba_possession_sim.season import season_data
            data = season_data("wnba", [2026], through=dt.date(2026, 7, 21))
            data["events"].height, data["logs"].height
    """
    if league not in _LEAGUES:
        raise ValueError(f"league must be one of {_LEAGUES}")
    games = games_from_leaguegamelog(_load_stats_schedule(league, seasons))
    if through is not None:
        games = games.filter(pl.col("game_date") <= through)
    completed = games.filter(pl.col("completed") == True)  # noqa: E712
    if completed.height == 0:
        raise ValueError("no completed games survive the cutoff")
    pbp = _load_stats_pbp(league, seasons).filter(pl.col("game_id").is_in(completed["game_id"].implode()))
    return {
        "schedule": games,
        "pbp": pbp,
        "events": possessions_from_pbp(pbp),
        "logs": player_game_logs_from_pbp(pbp),
    }


def _calibrate_pace_to_total(
    shelf: Shelf,
    target_total: float,
    rules: SportRules,
    *,
    n_pilot: int = 400,
    seed: int = 11,
    passes: int = 2,
) -> Shelf:
    """Scale the pace node so simulated totals match a realized anchor.

    The clock-delta pace fit runs systematically fast (deltas between
    consecutive outcome events under-count dead time), which inflates
    possession counts and totals. This is the fitted-anchor version of the
    board's original hand calibration: two pilot passes scale the global
    burn AND every per-key pace rate by the simulated/realized total
    ratio, preserving the fitted per-state pace SHAPE while pinning the
    level to real finals.
    """
    for _ in range(passes):
        pilot = simulate_ensemble(shelf, n_sim=n_pilot, seed=seed, rules=rules)
        scale = float(pilot["mean_total"]) / target_total
        shelf.mean_possession_seconds *= scale
        if shelf.pace_rates is not None:
            shelf.pace_rates = {key: value * scale for key, value in shelf.pace_rates.items()}
    return shelf


def season_shelf(
    league: str,
    seasons: List[int],
    *,
    through: Optional[dt.date] = None,
    learned_keyer: bool = False,
    calibrate_total: bool = True,
) -> Shelf:
    """The fully fitted node tree from full season-to-date data.

    Args:
        league: ``"wnba"`` or ``"nba"``.
        seasons: Seasons to load.
        through: Optional inclusive as-of cutoff.
        learned_keyer: Compose with a fitted
            :class:`~sportsdataverse.nba.nba_possession_sim.keygen.LearnedGamestateKeyer`.
        calibrate_total: Anchor the pace level to the included games'
            REALIZED mean total (two pilot passes; leakage-consistent —
            the anchor honors ``through``). The clock-delta pace fit runs
            fast, so uncalibrated shelves sim hot.

    Returns:
        A ``models2shelf`` :class:`Shelf` with fitted outcome/rebound/
        pace/aux nodes.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_possession_sim.season import season_shelf
            shelf = season_shelf("wnba", [2026])
            ens = simulate_ensemble(shelf, n_sim=5000, seed=7, rules=WNBA_RULES)
    """
    data = season_data(league, seasons, through=through)
    keyer = fit_learned_gamestate_keyer(data["events"]) if learned_keyer else None
    shelf = models_to_shelf(data["events"], keyer=keyer, actions=data["pbp"])
    if calibrate_total:
        finals = data["schedule"].filter(pl.col("completed") == True)  # noqa: E712
        target = float((finals["home_pts"] + finals["away_pts"]).mean())
        shelf = _calibrate_pace_to_total(shelf, target, _rules_for(league))
    return shelf


def walk_forward_backtest(
    league: str,
    season: int,
    *,
    start: dt.date,
    end: dt.date,
    n_sim: int = 300,
    seed: int = 7,
    min_train_games: int = 20,
    learned_keyer: bool = False,
    team_factors: bool = True,
) -> Dict[str, Any]:
    """Walk-forward distributional calibration of the season-fitted tree.

    For every date in ``[start, end]`` with games: refit the FULL tree on
    games strictly before that date (the leakage boundary), simulate each
    game, and record where the realized total/margin fell in the simulated
    distribution. The engine is team-symmetric, so this gates CALIBRATION
    (are realized outcomes distributed as simulated?), not discrimination.

    Args:
        league: ``"wnba"`` or ``"nba"``.
        season: Season to walk.
        start: First evaluation date.
        end: Last evaluation date.
        n_sim: Simulations per game.
        seed: Base RNG seed (offset per game for independent streams).
        min_train_games: Skip evaluation dates with fewer completed games
            before them.
        learned_keyer: Compose each refit with a learned keyer.

    Returns:
        ``{"games": frame, "summary": dict, "n_refits": int}`` — the games
        frame carries one row per evaluated game (``game_date``,
        ``game_id``, realized ``total``/``margin``, simulated
        ``total_mean``/``total_p10``/``total_p90``/``margin_p10``/
        ``margin_p90``, and ``total_in_band``/``margin_in_band`` coverage
        flags); the summary carries coverage rates, the mean total bias,
        and the leakage assertion inputs (max train date per refit).

    Raises:
        ValueError: When no games fall in the window.

    Example:
        Two-week walk::

            import datetime as dt
            from sportsdataverse.nba.nba_possession_sim.season import (
                walk_forward_backtest,
            )
            res = walk_forward_backtest(
                "wnba", 2026, start=dt.date(2026, 7, 7), end=dt.date(2026, 7, 21))
            res["summary"]
    """
    data = season_data(league, [season])
    games = data["schedule"].filter(pl.col("completed") == True)  # noqa: E712
    window = games.filter((pl.col("game_date") >= start) & (pl.col("game_date") <= end))
    if window.height == 0:
        raise ValueError("no completed games in the evaluation window")
    rules = _rules_for(league)
    all_pbp = data["pbp"]

    rows: List[Dict[str, Any]] = []
    max_train_dates: List[dt.date] = []
    n_refits = 0
    for eval_date in sorted(window["game_date"].unique().to_list()):
        train_games = games.filter(pl.col("game_date") < eval_date)
        if train_games.height < min_train_games:
            continue
        train_pbp = all_pbp.filter(pl.col("game_id").is_in(train_games["game_id"].implode()))
        events = possessions_from_pbp(train_pbp)
        keyer = fit_learned_gamestate_keyer(events) if learned_keyer else None
        shelf = models_to_shelf(events, keyer=keyer, actions=train_pbp)
        # leakage-safe level anchor: the TRAIN games' realized mean total
        train_target = float((train_games["home_pts"] + train_games["away_pts"]).mean())
        shelf = _calibrate_pace_to_total(shelf, train_target, rules)
        ratings = fit_team_ratings(train_games) if team_factors else None
        n_refits += 1
        max_train_dates.append(train_games["game_date"].max())
        slate = window.filter(pl.col("game_date") == eval_date)
        for index, game in enumerate(slate.iter_rows(named=True)):
            home_factors = away_factors = None
            if ratings is not None:
                targets = matchup_targets(ratings, int(game["home_team_id"]), int(game["away_team_id"]))
                home_factors, away_factors = matchup_factors(shelf, rules, targets["home"], targets["away"])
            ens = simulate_ensemble(
                shelf,
                n_sim=n_sim,
                seed=seed + 1000 * n_refits + index,
                rules=rules,
                home_factors=home_factors,
                away_factors=away_factors,
            )
            total = ens["total"].astype(float)
            margin = ens["margin"].astype(float)
            realized_total = float(game["home_pts"] + game["away_pts"])
            realized_margin = float(game["home_pts"] - game["away_pts"])
            row = {
                "game_date": game["game_date"],
                "game_id": game["game_id"],
                "total": realized_total,
                "margin": realized_margin,
                "p_home": float((margin > 0).mean()),
                "home_win": bool(realized_margin > 0),
                "total_mean": float(total.mean()),
                "total_p10": float(np.quantile(total, 0.1)),
                "total_p90": float(np.quantile(total, 0.9)),
                "margin_p10": float(np.quantile(margin, 0.1)),
                "margin_p90": float(np.quantile(margin, 0.9)),
            }
            row["total_in_band"] = bool(row["total_p10"] <= realized_total <= row["total_p90"])
            row["margin_in_band"] = bool(row["margin_p10"] <= realized_margin <= row["margin_p90"])
            rows.append(row)

    if not rows:
        raise ValueError("window produced no evaluated games (min_train_games too high?)")
    frame = pl.DataFrame(rows)
    outcomes = frame["home_win"].cast(pl.Float64).to_numpy()
    predictions = frame["p_home"].to_numpy()
    summary = {
        "n_games": frame.height,
        "n_refits": n_refits,
        "winner_brier": float(np.mean((predictions - outcomes) ** 2)),
        "winner_baseline_brier": float(np.mean((0.5 - outcomes) ** 2)),
        "total_coverage_80": float(frame["total_in_band"].cast(pl.Float64).mean()),
        "margin_coverage_80": float(frame["margin_in_band"].cast(pl.Float64).mean()),
        "total_bias": float((frame["total_mean"] - frame["total"]).mean()),
        "max_train_date_lt_eval": all(
            d < e
            for d, e in zip(max_train_dates, sorted(frame["game_date"].unique().to_list())[-len(max_train_dates) :])
        ),
    }
    return {"games": frame, "summary": summary, "n_refits": n_refits}


#: Scoring outcomes the team-strength multipliers act on.
_SCORING_OUTCOMES = ("rim_make", "mid_make", "three_make", "ft_trip_1", "ft_trip_2", "ft_trip_3")


def fit_team_ratings(games: pl.DataFrame, *, shrinkage: float = 6.0) -> Dict[str, Any]:
    """Offense/defense strength + home edge from realized finals.

    Ratings are per-game points for/against, empirical-Bayes shrunk toward
    the league mean with ``n / (n + shrinkage)`` games and expressed as
    multiplicative factors (1.0 = league average).

    Args:
        games: :func:`games_from_leaguegamelog` output (completed rows).
        shrinkage: Pseudo-game weight of the league mean.

    Returns:
        ``{"off": {team_id: factor}, "def": {team_id: factor},
        "team_mean": league per-team points mean, "home_edge":
        mean(home points) / mean(away points)}``.

    Raises:
        ValueError: When no completed games exist.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_possession_sim.season import (
                fit_team_ratings, games_from_leaguegamelog,
            )
            ratings = fit_team_ratings(games_from_leaguegamelog(log))
            ratings["off"][home_id] * ratings["def"][away_id]
    """
    finals = games.filter(pl.col("completed") == True)  # noqa: E712
    if finals.height == 0:
        raise ValueError("no completed games to rate teams from")
    long = pl.concat(
        [
            finals.select(
                pl.col("home_team_id").alias("team_id"),
                pl.col("home_pts").alias("pf"),
                pl.col("away_pts").alias("pa"),
            ),
            finals.select(
                pl.col("away_team_id").alias("team_id"),
                pl.col("away_pts").alias("pf"),
                pl.col("home_pts").alias("pa"),
            ),
        ]
    )
    team_mean = float(long["pf"].mean())
    per_team = long.group_by("team_id").agg(
        pl.len().alias("n"), pl.col("pf").mean().alias("pf"), pl.col("pa").mean().alias("pa")
    )
    off: Dict[int, float] = {}
    defense: Dict[int, float] = {}
    for row in per_team.iter_rows(named=True):
        n = int(row["n"])
        off[int(row["team_id"])] = ((n * float(row["pf"]) + shrinkage * team_mean) / (n + shrinkage)) / team_mean
        defense[int(row["team_id"])] = ((n * float(row["pa"]) + shrinkage * team_mean) / (n + shrinkage)) / team_mean
    home_edge = float(finals["home_pts"].mean() / finals["away_pts"].mean())
    return {"off": off, "def": defense, "team_mean": team_mean, "home_edge": home_edge}


def matchup_targets(ratings: Dict[str, Any], home_team_id: int, away_team_id: int) -> Dict[str, float]:
    """Expected team points for one matchup from the fitted ratings.

    ``target = team_mean x own offense x opponent defense``, with the home
    edge split symmetrically across the two sides. Unrated teams (early
    season, expansion) fall back to league-average factors.

    Args:
        ratings: :func:`fit_team_ratings` output.
        home_team_id: Home team.
        away_team_id: Away team.

    Returns:
        ``{"home": expected home points, "away": expected away points}``.

    Example:
        Quick start::

            targets = matchup_targets(ratings, home_id, away_id)
    """
    edge = float(ratings["home_edge"]) ** 0.5
    home = ratings["team_mean"] * ratings["off"].get(home_team_id, 1.0) * ratings["def"].get(away_team_id, 1.0)
    away = ratings["team_mean"] * ratings["off"].get(away_team_id, 1.0) * ratings["def"].get(home_team_id, 1.0)
    return {"home": home * edge, "away": away / edge}


def matchup_factors(
    shelf: Shelf,
    rules: SportRules,
    home_target: float,
    away_target: float,
    *,
    n_pilot: int = 400,
    seed: int = 13,
    passes: int = 2,
) -> Any:
    """Pilot-calibrated per-side scoring factors hitting the matchup targets.

    Scales each side's make/trip outcome probabilities (renormalized by
    :class:`~sportsdataverse.nba.nba_possession_sim.factors.FactorAdjustment`)
    until the simulated team means match the targets — the PMF response to
    a multiplier is sublinear, so two pilot passes converge.

    Args:
        shelf: The (pace-calibrated) shelf.
        rules: League clock structure.
        home_target: Expected home points (:func:`matchup_targets`).
        away_target: Expected away points.
        n_pilot: Pilot simulations per pass.
        seed: Pilot RNG seed.
        passes: Calibration passes.

    Returns:
        ``(home_factors, away_factors)`` — auditable
        :class:`~sportsdataverse.nba.nba_possession_sim.factors.FactorAdjustment`
        pairs for the engine's per-side seam.

    Example:
        Quick start::

            hf, af = matchup_factors(shelf, WNBA_RULES, 84.2, 88.9)
            ens = simulate_ensemble(shelf, n_sim=5000, seed=7, rules=rules,
                                    home_factors=hf, away_factors=af)
    """
    from sportsdataverse.nba.nba_possession_sim.factors import FactorAdjustment

    home_mult = {outcome: 1.0 for outcome in _SCORING_OUTCOMES}
    away_mult = {outcome: 1.0 for outcome in _SCORING_OUTCOMES}
    for _ in range(passes):
        pilot = simulate_ensemble(
            shelf,
            n_sim=n_pilot,
            seed=seed,
            rules=rules,
            home_factors=FactorAdjustment(factors=dict(home_mult)),
            away_factors=FactorAdjustment(factors=dict(away_mult)),
        )
        home_scale = home_target / float(pilot["score_home"].mean())
        away_scale = away_target / float(pilot["score_away"].mean())
        home_mult = {outcome: value * home_scale for outcome, value in home_mult.items()}
        away_mult = {outcome: value * away_scale for outcome, value in away_mult.items()}
    return FactorAdjustment(factors=home_mult), FactorAdjustment(factors=away_mult)


def season_matchup(
    league: str,
    seasons: List[int],
    home_team_id: int,
    away_team_id: int,
    *,
    through: Optional[dt.date] = None,
    learned_keyer: bool = False,
) -> Dict[str, Any]:
    """The one-call pregame surface: fitted tree + matchup team factors.

    Args:
        league: ``"wnba"`` or ``"nba"``.
        seasons: Seasons to load.
        home_team_id: Home team (stats-namespace id).
        away_team_id: Away team.
        through: Optional inclusive as-of cutoff.
        learned_keyer: Compose the shelf with a learned keyer.

    Returns:
        ``{"shelf", "home_factors", "away_factors", "ratings", "targets",
        "data"}`` — feed shelf + factors straight to ``simulate_ensemble``
        / ``simulate_player_boxscores``.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_possession_sim.season import season_matchup
            game = season_matchup("wnba", [2026], home_id, away_id)
            ens = simulate_ensemble(game["shelf"], n_sim=5000, seed=7,
                                    rules=WNBA_RULES,
                                    home_factors=game["home_factors"],
                                    away_factors=game["away_factors"])
    """
    data = season_data(league, seasons, through=through)
    keyer = fit_learned_gamestate_keyer(data["events"]) if learned_keyer else None
    shelf = models_to_shelf(data["events"], keyer=keyer, actions=data["pbp"])
    finals = data["schedule"].filter(pl.col("completed") == True)  # noqa: E712
    rules = _rules_for(league)
    shelf = _calibrate_pace_to_total(shelf, float((finals["home_pts"] + finals["away_pts"]).mean()), rules)
    ratings = fit_team_ratings(finals)
    targets = matchup_targets(ratings, home_team_id, away_team_id)
    home_factors, away_factors = matchup_factors(shelf, rules, targets["home"], targets["away"])
    return {
        "shelf": shelf,
        "home_factors": home_factors,
        "away_factors": away_factors,
        "ratings": ratings,
        "targets": targets,
        "data": data,
    }
