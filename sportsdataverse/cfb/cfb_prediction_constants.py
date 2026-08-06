"""CFB prediction-spine constants + validation metrics (compute-on-demand)."""

from __future__ import annotations

from dataclasses import dataclass, field

from sportsdataverse._common.metrics import (
    as_of_ratings_split as as_of_ratings_split,
    brier_score as brier_score,
    calibration_table as calibration_table,
    log_loss_score as log_loss_score,
    mae as mae,
    spearman_corr as spearman_corr,
)


@dataclass
class RatingsConfig:
    """Tunable knobs for the CFB ratings engine (ridge regression + competitiveness filter).

    Args:
        ridge_lambda: L2 regularization strength for the ridge-regression rating
            fit. ``cfb_adjusted_epa._fit_opponent_ridge`` scales the sklearn
            penalty as ``alpha = ridge_lambda * n_plays``, so ``ridge_lambda`` is
            a per-observation penalty (scale-invariant in ``n``). The default
            ``0.02`` is tuned across five seasons (2021-2025) against ESPN FPI
            joined on ``team_id``, scored per season and ranked by the mean; it is
            a genuine interior maximum and also the best value on the worst
            season. It shares the default with
            ``cfb_adjusted_epa._RIDGE_LAMBDA`` so the two entry points cannot
            disagree -- they previously differed by 6,500x, and the 325 that
            ``cfb_adjusted_epa`` carried was a no-op adjustment.
        min_competitive_wp: Lower win-probability bound a game must clear to count as
            "competitive" (garbage-time / blowout filtering).
        max_competitive_wp: Upper win-probability bound a game must clear to count as
            "competitive".
        division: NCAA division slug the ratings are scoped to (e.g. ``"fbs"``).
    """

    ridge_lambda: float = 0.035
    min_competitive_wp: float = 0.1
    max_competitive_wp: float = 0.9
    division: str = "fbs"


@dataclass
class PredictConfig:
    """Era-specific coefficients for the CFB game-outcome prediction model.

    ``net_points_scale``, ``margin_sd``, ``total_intercept``, ``total_scale`` and
    ``total_pace_scale`` are **fitted (in-sample) on the 2023 backtest** by
    ``dev/cfb_prediction/fit_pregame.py``; ``hfa_epa`` is the ratings ridge's own
    home-field coefficient (also 2023). The ratings that feed the fit use a
    leakage-free week-by-week as-of boundary, but these coefficients are fit
    on the same 2023 games the backtest gate then scores -- so the gate is an
    in-sample regression guard, not an out-of-sample generalization result (a 2024
    holdout is a documented follow-up). See :mod:`sportsdataverse.cfb.cfb_game_predict`. ``adj_net`` from
    the ratings engine is on an EPA-per-play scale, so ``net_points_scale`` is the
    fitted EPA/play -> points conversion (without it the rating differential is
    negligible next to a points-scale HFA and the model is near-constant).

    Args:
        hfa_epa: COMPATIBILITY ONLY -- no longer reaches the margin. Home-field
            advantage on the EPA-per-play scale (the ratings ridge's native home
            coefficient, ~0.0185). It formerly entered the margin as
            ``net_points_scale * 2 * hfa_epa``, which implied ~1.65 pt against a
            measured ~3.0: routing an EPA-scale HFA through a points-scale slope
            tied the two together, so refitting either silently moved the other.
            :func:`cfb_game_predict.predict_margin` now adds ``hfa_points``
            directly and ignores this field. Retained because callers read it and
            because it remains the correct EPA-scale form for anything applying
            HFA component-wise to the ratings themselves (home_off += hfa_epa,
            home_def -= hfa_epa), where the offense/defense shifts cancel in the
            sum and so leave the *total* unchanged.
        hfa_points: Home-field advantage in POINTS, added directly to the
            predicted margin. This is the fitted HFA -- ``hfa_epa`` is not.
        slope_by_games: Points per unit of rating differential, keyed by
            ``"lo-hi"`` games-played buckets. A single slope is wrong because
            OLS slopes attenuate toward zero as the predictor gets noisier, and
            a two-game-old rating is far noisier than a twelve-game-old one.
            See :func:`cfb_game_predict.slope_for_games`.
        margin_sd: Standard deviation of the margin residuals, used to convert a
            predicted margin into a win probability via the Gaussian CDF (fitted).
        net_points_scale: Points per unit of net adjusted-EPA/play differential --
            the fitted slope mapping ``home_adj_net - away_adj_net`` to points.
        total_intercept: Fitted baseline point total (intercept of the totals fit).
        total_scale: Fitted slope on the summed four efficiency ratings for totals.
        total_pace_scale: Fitted slope on ``game_pace`` (``home_off_pace *
            away_off_pace / league_avg_pace``) for totals -- tempo scales a total
            (a sum) directly, unlike the margin (a differential, where pace
            cancels). Cuts total MAE ~6% vs the efficiency-only totals fit.
        avg_drives: Average number of offensive drives per team per game (reserved
            for the season Monte Carlo in Phase 4).
        points_per_epa: Conversion factor from expected-points-added to points
            (reserved for Phase 4).
        quality_win_threshold: Minimum rating differential for a win to count as a
            "quality win" in résumé-style summaries (Phase 3).
        bubble_adj_net: Net rating adjustment applied to bubble-team comparisons
            (Phase 3).
    """

    hfa_epa: float
    margin_sd: float
    net_points_scale: float
    total_intercept: float
    total_scale: float
    total_pace_scale: float
    avg_drives: float
    points_per_epa: float
    quality_win_threshold: float
    bubble_adj_net: float
    #: Home-field advantage in POINTS, added directly to the margin rather than
    #: routed through ``net_points_scale``. ``hfa_epa`` is retained for callers
    #: that read it, but the fit is on this one -- mixing an EPA-scale HFA with
    #: a points-scale slope is what let the two drift apart unnoticed.
    hfa_points: float = 3.0365
    #: Points per unit of rating differential, BY GAMES PLAYED. See
    #: :func:`cfb_game_predict.predict_margin`. A single slope is wrong because
    #: an as-of rating built on two games is a far noisier predictor than one
    #: built on twelve, and OLS slopes attenuate toward zero with predictor
    #: noise. Keys are "lo-hi" games-played buckets.
    slope_by_games: dict[str, float] = field(
        default_factory=lambda: {
            "0-3": 10.6201,
            "4-5": 26.0576,
            "6-7": 42.0032,
            "8-20": 54.4874,
        }
    )


CFB_CONSTANTS: dict[str, PredictConfig] = {
    # Refit 2026-08-03 by `cfb_higher_models.fit_pregame` in cfbfastR-cfb-data
    # (TRACKED code -- the previously cited `dev/cfb_prediction/fit_pregame.py`
    # existed nowhere, on disk or in git, so the old numbers could not be
    # reproduced or refreshed). Fitted walk-forward on 2014-2025, 6,790 games,
    # against the corrected corpus.
    #
    # WHY THE OLD VALUES WERE WRONG. net_points_scale=44.5367 was fit against
    # FULL-SEASON ratings and applied to AS-OF ratings. As-of ratings are the
    # same quantity measured with more noise, and OLS slopes attenuate toward
    # zero when the predictor is noisy -- so the correct multiplier is smaller,
    # and it is not one number. The old claim (brier 0.1416, spread MAE 3.23)
    # was an in-sample fit on full-season ratings; measured out-of-sample the
    # shipped constants delivered MAE 15.17 with a calibration slope of 0.55,
    # i.e. predictions stretched nearly 2x wider than reality.
    #
    # Measured on the corrected corpus, leakage-free (week W from ratings
    # through W-1), n=5,655:
    #     shipped                 MAE 15.17  slope 0.55  max_cal_err 0.309
    #     refit, flat slope       MAE 14.62  slope 0.94  max_cal_err 0.448
    #     refit + attenuation     MAE 14.01  slope 0.97  max_cal_err 0.198
    "modern": PredictConfig(
        hfa_epa=0.01848,  # retained for back-compat; the fit uses hfa_points
        hfa_points=3.0365,
        margin_sd=18.7894,
        net_points_scale=24.6578,
        # TOTALS ARE UNTOUCHED BY THIS REFIT, deliberately. `predict_total`
        # parameterises as `intercept + scale*sum4 + pace_scale*game_pace`
        # where sum4 is FOUR ratings (both offences AND both defences) and
        # game_pace is MULTIPLICATIVE and league-normalised
        # (home_pace*away_pace/avg). The refit in cfb_higher_models fits a
        # different model -- two offensive ratings, additive raw pace -- so its
        # coefficients are not interchangeable with these names.
        #
        # A first pass here dropped them in anyway (total_scale 19.08 -> 8.06)
        # and blew total MAE-vs-market from <=5.25 to 13.66. Identical field
        # names, different meanings: the same unit confusion that let hfa_epa
        # and net_points_scale drift apart. Refit these against THIS
        # parameterisation before changing them.
        total_intercept=26.8933,
        total_scale=19.0816,
        total_pace_scale=0.4267,
        avg_drives=12.0,
        points_per_epa=1.0,
        quality_win_threshold=0.0,
        bubble_adj_net=0.0,
        slope_by_games={
            "0-3": 10.6201,
            "4-5": 26.0576,
            "6-7": 42.0032,
            "8-20": 54.4874,
        },
    ),
}


def get_constants(era: str = "modern") -> PredictConfig:
    """Look up the :class:`PredictConfig` for a given era.

    Args:
        era: Era key into :data:`CFB_CONSTANTS` (e.g. ``"modern"``).

    Returns:
        The :class:`PredictConfig` registered for ``era``.

    Raises:
        ValueError: If ``era`` is not a registered key.

    Example:
        Quick start::

            from sportsdataverse.cfb.cfb_prediction_constants import get_constants
            cfg = get_constants("modern")
            cfg.hfa_epa
    """
    try:
        return CFB_CONSTANTS[era]
    except KeyError:
        valid = ", ".join(sorted(CFB_CONSTANTS))
        raise ValueError(f"Unknown era {era!r}; valid eras are: {valid}") from None
