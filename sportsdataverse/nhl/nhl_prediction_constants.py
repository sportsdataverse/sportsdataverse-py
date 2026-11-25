"""Prediction-spine shared constants + validation metrics (league-agnostic).

Single home for the NHL/PWHL prediction-and-market spine (T5.3): validation
metric functions (Brier, log-loss, Spearman, MAE, calibration table), the
as-of-date leakage split, and the per-league fitted-constants table
(``LEAGUE_CONSTANTS``). Every downstream module (:mod:`nhl_team_ratings`,
:mod:`nhl_market`, :mod:`nhl_player_props`) reads its HFA / sigma /
shrinkage / prop-prior numbers from :func:`get_constants` -- no NHL/PWHL
number is ever hard-coded inside an algorithm function.

Seeded constants (``margin_sd``, ``hfa``, ``shrink_k``, ``total_scale``,
``avg_*``) start from published references so the engine runs before
fitting, and are overwritten in-code by the committed fitting scripts in
``tools/models/nhl_prediction/`` (``fit_pregame.py`` for Task 2.3, ``fit_props.py``
for Task 4.2) once the 2023 backtest corpus is captured. Hockey is a
low-event-count, high-variance sport: ``margin_sd`` is deliberately wide and
``shrink_k`` (a games-played prior) is a first-class part of every rating,
not an afterthought -- see the design spec's hockey caveat
(``2026-07-07-nhl-prediction-market-design.md`` Sec 3.3/§9-4).

Example:
    Quick start::

        from sportsdataverse.nhl.nhl_prediction_constants import get_constants

        nhl = get_constants("nhl")
        print(nhl.margin_sd, nhl.hfa)

    Metric functions::

        from sportsdataverse.nhl.nhl_prediction_constants import brier_score
        import numpy as np
        brier_score(np.array([1, 0]), np.array([0.8, 0.3]))

See Also:
    * `nflfastR`_ -- the T7.2-shared Phi-margin power-rating core this spine mirrors.
    * `nhl-api-py`_ -- companion NHL Python client.

.. _nflfastR: https://www.nflfastr.com
.. _nhl-api-py: https://github.com/coreyjs/nhl-api-py
"""

from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass

import polars as pl
from sportsdataverse._common.metrics import (
    brier_score as brier_score,
    calibration_table as calibration_table,
    log_loss_score as log_loss_score,
    mae as mae,
    spearman_corr as spearman_corr,
)


def _settled_scores(pbp: pl.DataFrame) -> pl.DataFrame:
    """Per-game home / away goals as the game settled, from a ``load_nhl_pbp_full`` frame's GOAL events.

    The pbp logs every scoring shootout attempt as a GOAL (``period_type == "SHOOTOUT"``), but a shootout
    settles as one goal to its winner. Regulation and overtime goals count as they are.

    Returns:
        ``game_id: Int64, home_goals: Int64, away_goals: Int64``, one row per game with a GOAL event.
    """
    goals = pbp.filter((pl.col("event_type") == "GOAL") & pl.col("game_id").is_not_null()).with_columns(
        (pl.col("event_team_abbr") == pl.col("home_abbr")).alias("home"),
        (pl.col("period_type") == "SHOOTOUT").alias("shootout"),
    )
    per_game = goals.group_by("game_id").agg(
        (pl.col("home") & ~pl.col("shootout")).sum().cast(pl.Int64).alias("home_goals"),
        (~pl.col("home") & ~pl.col("shootout")).sum().cast(pl.Int64).alias("away_goals"),
        (pl.col("home") & pl.col("shootout")).sum().alias("so_home"),
        (~pl.col("home") & pl.col("shootout")).sum().alias("so_away"),
    )
    return per_game.select(
        pl.col("game_id").cast(pl.Int64),
        pl.col("home_goals") + (pl.col("so_home") > pl.col("so_away")).cast(pl.Int64),
        pl.col("away_goals") + (pl.col("so_away") > pl.col("so_home")).cast(pl.Int64),
    )


def as_of_ratings_split(df: pl.DataFrame, cutoff_date: _dt.date, *, date_col: str = "date") -> pl.DataFrame:
    """Filter a frame to rows strictly before ``cutoff_date`` (the leakage boundary).

    Args:
        df: a polars DataFrame with a date column.
        cutoff_date: the game date being predicted; only strictly-earlier rows are kept.
        date_col: name of the date column (default ``"date"``).

    Returns:
        The subset of ``df`` with ``df[date_col] < cutoff_date``.

    Example:
        Quick start::

            import datetime as dt
            import polars as pl
            from sportsdataverse.nhl.nhl_prediction_constants import as_of_ratings_split
            df = pl.DataFrame({"date": [dt.date(2023, 1, 1), dt.date(2023, 1, 2)]})
            as_of_ratings_split(df, dt.date(2023, 1, 2))
    """
    return df.filter(pl.col(date_col) < cutoff_date)


@dataclass(frozen=True)
class LeagueConstants:
    """Fitted, league-specific constants for the NHL/PWHL prediction spine.

    Attributes:
        hfa: home-ice edge on ``exp_margin``'s own (shrunk-rating) scale. NHL: fitted jointly with
            ``margin_sd``, so smaller than the real-goal home edge. PWHL: a seeded placeholder.
        margin_sd: standard deviation of the final goal margin (deliberately WIDE for hockey).
        avg_xgf: league mean even-strength xG-for, per game.
        avg_total_goals: league mean settled total goals per game (a shootout counts one goal), the level of
            every predicted total.
        total_scale: multiplier converting rating differential to total-goals deviation.
        shrink_k: games-played prior strength for rating shrinkage.
        prop_kappa: empirical-Bayes shrinkage strength per player-prop stat family.
        pos_priors: per-position (F/D) per-stat-family prior rates.
        prop_team_volume_slope: game-script tilt on a player-prop projection
            (favored team -> fewer late shots-for). SEEDED PLACEHOLDER (~0.04),
            not yet fitted -- a future prop-fit task should estimate it from the
            realized shots-vs-exp_margin slope, mirroring how fit_props.py fits
            prop_kappa/pos_priors.
        in_game_wp_artifact: filename of the bundled in-game win-probability model
            under ``sportsdataverse/nhl/models/``.
        min_season: earliest season this league's prediction spine supports.
    """

    hfa: float
    margin_sd: float
    avg_xgf: float
    avg_total_goals: float
    total_scale: float
    shrink_k: float
    prop_kappa: dict
    pos_priors: dict
    prop_team_volume_slope: float
    in_game_wp_artifact: str
    min_season: int


LEAGUE_CONSTANTS: dict[str, LeagueConstants] = {
    # Refitted 2026-11-13 on the retrained xG (nhl_xg_models 2026-11-13: 5v5 model fitted on 5v5
    # shots, rink coordinates fixed; published goals/sum-xG per season now 0.98-1.03, was ~0.77).
    # avg_xgf = 2.00: mean even-strength xG-for per team-game over 2022-23 .. 2025-26
    #   (2.099 / 1.962 / 1.923 / 2.012; `team_game_xg_rates`, even_strength_only=True; printed by
    #   fit_pregame.py over its AVG_XGF_SEASONS). It was
    #   2.55, from the old xG, which ran ~2.84 on 2022-23.
    # hfa/margin_sd/total_scale fitted by tools/models/nhl_prediction/fit_pregame.py against the
    # 2023 as-of-date backtest (1174 games, dates[20:] onward), converged on a second pass:
    #   hfa = 0.0298, margin_sd = 0.5109, fitted JOINTLY by Brier of
    #     Phi((exp_margin_without_hfa + hfa) / margin_sd). Both live on exp_margin's own
    #     compressed, shrunk-rating scale, so hfa is NOT the real-goal home edge (~0.13-0.16).
    #     The 2026-07-08 fit took hfa = 0.1630 from that real-goal residual but margin_sd = 0.9085
    #     from the compressed scale, which overstated home ice: mean home win prob 0.571 against
    #     0.519 actual. Now 0.520 against 0.520 on 2023; Brier 0.2376 -> 0.2321.
    #   total_scale = 2.5392, the OLS slope of the settled total on the raw total
    #     (shrinkage compresses the raw total's spread well below the real variance);
    #     applied in nhl_market.predict_total as
    #     avg_total_goals + total_scale * (raw_total - 2 * avg_xgf).
    #     The raw total is even-strength xG, so its mean is 2 * avg_xgf (3.999 observed);
    #     centring on avg_total_goals until 2026-11-11 put every total ~2 goals low.
    # Walk-forward holdout (fitted on 2022-23 only), 2023-24 / 2024-25 / 2025-26, before -> after:
    #   Brier 0.2390 / 0.2383 / 0.2451 -> 0.2362 / 0.2399 / 0.2417;
    #   mean home win prob 0.571 / 0.570 / 0.571 -> 0.522 / 0.521 / 0.522
    #     (actual 0.541 / 0.569 / 0.521);
    #   total bias -0.25 / -0.07 / -0.31 and MAE 1.87 / 1.89 / 1.87 unchanged (avg_xgf is both the
    #     rating shrinkage target and the total's centre, so its level cancels). Those totals were
    #     scored against the old label below, which counted every scoring shootout attempt.
    # Re-run 2026-11-18 on the nhl_pbp_full republished 2026-11-14: hfa / margin_sd / total_scale
    #   reproduce to 4 dp and avg_xgf to the shipped 2 dp (its seasons above average 1.999); the
    #   2025-26 slice after Jan 15 that flagged 0.571 vs 0.519 now reads 0.522 vs 0.519
    #   (Brier 0.2437 -> 0.2383, 574 games). hfa is one league constant, not an as-of estimate, so
    #   it has no early-season swing to shrink (unlike BB-D25's ridge home term, #1223). Per-season
    #   joint fits read 0.030 / 0.053 / 0.109 / 0.032 (2022-23 .. 2025-26); an expanding-window
    #   refit (0.030 / 0.042 / 0.062) scores Brier 0.2362 / 0.2392 / 0.2421 against the fixed
    #   value's 0.2362 / 0.2399 / 0.2417, a wash, so hfa stays fitted on 2022-23 alone.
    # Settled-score labels, 2026-11-25: fit_pregame.py's labels counted every scoring shootout attempt
    #   (the pbp logs each as a GOAL), adding 0.06-0.11 goals a game; _settled_scores now counts a shootout
    #   as one goal to its winner, matching the schedule's final score in 3936 / 3936 games of 2023-24 ..
    #   2025-26. hfa / margin_sd / avg_xgf reproduce unchanged (the winner was always right);
    #   total_scale refits 2.3189 -> 2.5392.
    # avg_total_goals = 6.19: the mean settled total of the three most recent complete regular seasons
    #   before the one predicted (2026-27): 6.226 / 6.081 / 6.254 for 2023-24 / 2024-25 / 2025-26, printed by
    #   fit_pregame.py (SEED_SEASONS) and pinned by tests/nhl/test_nhl_settled_totals.py. It was a 6.05
    #   published-reference seed. Refit once a season, after the regular season closes, never from an
    #   in-progress season. As-of holdout 2023-24 .. 2025-26 (3538 games) against settled totals:
    #   bias -0.125 +/- 0.037 (SE clustered by date) -> +0.015 +/- 0.037; per season -0.175 / +0.000 /
    #   -0.203 -> -0.035 / +0.140 / -0.063; MAE 1.842 -> 1.846, Brier over 5.5 0.2449 -> 0.2454, over 6.5
    #   0.2484 -> 0.2468 (Poisson around exp_total). A level fix, not an accuracy gain: the season-to-season
    #   swing is the size of the remaining bias, and the total_scale refit moves no prediction by more
    #   than 0.08 goals (predicted totals have sd 0.28), which shows in no holdout metric.
    #
    # prop_kappa/pos_priors fitted 2026-07-08 by tools/models/nhl_prediction/fit_props.py
    # against season 2024 skater boxscores (load_nhl_skater_boxscores only
    # publishes seasons >= 2024 -- season 2024 == the 2023-24 season; Phase 4
    # therefore uses 2024 while ratings/market Phases 1-3 stay on 2023, see the
    # fixtures README). Method-of-moments EB kappa (within-player Poisson
    # variance / between-player variance), computed separately per position
    # then averaged (unweighted) into the single per-stat kappa this table
    # carries: shots F kappa=2.10/D kappa=3.40 -> 2.75; points F kappa=3.91/D
    # kappa=5.86 -> 4.88. pos_priors are the raw per-position mean rates.
    "nhl": LeagueConstants(
        hfa=0.0298,
        margin_sd=0.5109,
        avg_xgf=2.00,
        avg_total_goals=6.19,
        total_scale=2.5392,
        shrink_k=15.0,
        prop_kappa={"shots": 2.7498, "points": 4.8837},
        pos_priors={"shots": {"F": 1.5746, "D": 1.1910}, "points": {"F": 0.4125, "D": 0.2858}},
        prop_team_volume_slope=0.04,  # SEEDED placeholder -- not yet fitted (see dataclass doc)
        in_game_wp_artifact="nhl_in_game_wp.json",
        min_season=2010,
    ),
    # PWHL: shorter league history + higher game-to-game variance -> stronger
    # shrinkage prior. hfa/avg_xgf/avg_total_goals/total_scale/prop_* remain
    # SEEDED placeholders (deferred per design spec Sec 9-7 -- the NHL-contract
    # pbp shape this row's consumers were built for has no PWHL adapter yet).
    #
    # margin_sd IS fit (T5.3 flesh-out 2026-07-11; re-fit for the T5.3b
    # coordinate xG 2026-07-12), OUT-OF-SAMPLE, as a byproduct of the PWHL xG
    # backtest (see sportsdataverse.pwhl.pwhl_xg_proxy + the held-out
    # fitter/report script tools/models/pwhl_prediction/build_pwhl_xg_fixture.py):
    # grid-search minimising Brier of Phi(exp_margin/margin_sd) on the 2025
    # season's 78 as-of games ONLY (xG model for that fit from 2024, a
    # strictly-prior season), with the held-out 2026 season kept entirely out
    # of the fit. Same method the NHL margin_sd comment above describes, but on
    # a held-out split (an earlier cut fit sd in-sample on all 244 games, which
    # made the beats-naive gate non-falsifiable -- Phi(m/sd)->0.5=naive as sd
    # grows, so the in-sample minimiser can never lose to naive). The seeded
    # 2.35 was a real-world-goal-margin-scale guess; the model's exp_margin is
    # heavily shrink-compressed (shrink_k=25 against ~10-20 games/team/season,
    # std ~0.065), so the correctly-scaled sigma is much smaller: 1.19 paired
    # with the DEFAULT coordinate xG (the categorical-quality proxy's own fit
    # is 1.21 -- the builder fits sd per method because the xG scale sets the
    # exp_margin scale). HONEST RESULT: on the held-out 2026 season (n=107,
    # 2026-07-12) coords gives Brier 0.2444 (quality 0.2449) vs naive 0.2500
    # -- a delta of only -0.0056 (-0.0051), WITHIN sampling noise (SE ~0.0055
    # on 107 games). The model is directionally correct but does NOT robustly
    # beat naive out-of-sample. It ships as a first-of-its-kind scaffold; the
    # gate (tests/pwhl/test_pwhl_xg_proxy_oracle.py) is held-out CALIBRATION +
    # no-worse-than-naive-within-noise, NOT a beats-naive magnitude claim --
    # a powered magnitude gate needs more PWHL seasons.
    "pwhl": LeagueConstants(
        hfa=0.15,
        margin_sd=1.19,
        avg_xgf=2.30,
        avg_total_goals=5.20,
        total_scale=1.0,
        shrink_k=25.0,
        prop_kappa={"shots": 8.0, "points": 10.0},
        pos_priors={"shots": {"F": 2.0, "D": 1.2}, "points": {"F": 0.50, "D": 0.28}},
        prop_team_volume_slope=0.04,  # SEEDED placeholder -- fitted when PWHL data lands
        in_game_wp_artifact="pwhl_in_game_wp.json",
        min_season=2024,
    ),
}


def get_constants(league: str) -> LeagueConstants:
    """Resolve the fitted-constants row for a league.

    Args:
        league: ``"nhl"`` or ``"pwhl"``.

    Returns:
        The :class:`LeagueConstants` row for ``league``.

    Raises:
        ValueError: if ``league`` is not a known key of ``LEAGUE_CONSTANTS``.

    Example:
        Quick start::

            from sportsdataverse.nhl.nhl_prediction_constants import get_constants
            get_constants("nhl").margin_sd
    """
    try:
        return LEAGUE_CONSTANTS[league]
    except KeyError as exc:
        raise ValueError(f"unknown league {league!r}; expected one of {sorted(LEAGUE_CONSTANTS)}") from exc
