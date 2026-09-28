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

    ``net_points_scale``, ``hfa_points``, ``margin_sd`` and ``slope_by_games`` are
    fitted on as-of ratings (week W from the ``through_week == W - 1`` snapshot)
    by ``cfb_higher_models.fit_pregame`` in cfbfastR-cfb-data, on 2014-2023 only,
    so the 2024 backtest gate scores a season the fit never saw. The totals trio
    is fitted separately (see ``CFB_CONSTANTS``); ``hfa_epa`` is the ratings
    ridge's own home-field coefficient. See
    :mod:`sportsdataverse.cfb.cfb_game_predict`. ``adj_net`` from
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
    #: Shrinkage constant for the tempo prior: a team's effective pace is
    #: ``(n/(n+k))*current + (k/(n+k))*prior_season``, n = games played.
    #:
    #: MEASURED, not chosen. Swept against walk-forward totals MAE on
    #: 2017-2024 using the ratings' own ``off_pace``:
    #:     raw 13.4695 | k=2 13.3994 | k=4 13.3814 | k=6 13.3808 | k=8 13.3847
    #: The 4-8 region is flat; k=6 is the walk-forward optimum. The 2024
    #: holdout independently prefers k=8 (12.9133 vs 12.9186), i.e. inside the
    #: same flat region -- k is selected on walk-forward, because picking it on
    #: the holdout would be selecting on the test set.
    #:
    #: WHY IT HELPS: raw single-season pace is noisy, and a noisy predictor's
    #: OLS coefficient attenuates toward zero. De-noising it un-attenuates the
    #: coefficient -- ``total_pace_scale`` rises 0.2246 -> 0.3785 across this
    #: change, the same errors-in-variables signature as the net_points_scale
    #: refit. A control confirms it is the BLEND doing the work: last season's
    #: pace ALONE ties raw (+0.002), so this is not "last year is better data".
    pace_blend_k: float = 6.0
    hfa_points: float = 2.7936
    #: Points per unit of rating differential, BY GAMES PLAYED. See
    #: :func:`cfb_game_predict.predict_margin`. A single slope is wrong because
    #: an as-of rating built on two games is a far noisier predictor than one
    #: built on twelve, and OLS slopes attenuate toward zero with predictor
    #: noise. Keys are "lo-hi" games-played buckets.
    slope_by_games: dict[str, float] = field(
        default_factory=lambda: {
            "0-3": 9.1185,
            "4-5": 29.7368,
            "6-7": 41.7733,
            "8-20": 55.1520,
        }
    )


CFB_CONSTANTS: dict[str, PredictConfig] = {
    # Refit 2026-09-27 by `cfb_higher_models.fit_pregame` in cfbfastR-cfb-data:
    #     python -m cfb_model_build.cfb_higher_models fit-pregame \
    #         --seasons 2014 ... 2025 --holdout 2024 2025
    # Fitted on 2014-2023 (5,673 as-of games); 2024-2025 are held out, so the
    # 2024 gate in tests/cfb/test_cfb_prediction_backtest.py is out-of-sample.
    # The fit's output, holdout partition and scores are committed there as
    # `models/pregame_fit.json`.
    #
    # WHY IT WAS REFIT. The weekly team summaries the fit selects rows and
    # games-played buckets from carried every bowl and CFP game in every
    # through-week snapshot (ESPN restarts postseason weeks at 1). Fixed in
    # cfbfastR-cfb-data #100, republished 2004-2026. Fit against
    # `cfb_ratings_weekly` as republished 2026-09-27 by cfbfastR-cfb-data #105
    # (snapshots now include each week's last kickoff day). The previous values
    # (24.6578 / 3.0365 / 18.7894, curve 10.62 / 26.06 / 42.00 / 54.49) were
    # fit on that leaked frame and on every season through 2025, 2024 included.
    #
    # `margin_sd` is the residual sd of the games-played CURVE's margin (the
    # formula served here), not the flat fit's 18.97, which priced WP ~5% wide.
    #
    # Same serving formula, scored on identical 2024-2025 games (n=1,202). A
    # NEAR-holdout: the fit never saw these seasons, but sdv-py #598's
    # adjusted-EPA shrinkage was tuned on 2023-2025.
    #     previous constants   MAE 13.29  brier 0.2040  slope 1.04  (had SEEN 2024-25)
    #     this refit           MAE 13.32  brier 0.2039  slope 1.02  max_cal_err 0.081
    #     paired dMAE +0.03 [-0.01, +0.06], dBrier -0.0001 [-0.0008, +0.0005]
    #     (2,000 game bootstraps): a statistical tie. What the refit buys is a
    #     leak-free, reproducible fit with 2024 genuinely out of its training.
    # Walk-forward 2016-2025 (each season fit only on earlier ones), n=5,724:
    #     refit + attenuation  MAE 13.87  brier 0.2090  slope 0.99  max_cal_err 0.163
    #
    # The 2026-08-03 refit's reasoning still holds: as-of ratings are noisier
    # than full-season ones, so the slope attenuates and grows with games
    # played (`slope_by_games`). `net_points_scale` is the flat slope over all
    # games, the fallback when games played is unknown.
    "modern": PredictConfig(
        hfa_epa=0.01848,  # retained for back-compat; the fit uses hfa_points
        hfa_points=2.7936,
        margin_sd=18.1043,
        net_points_scale=23.6945,
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
        # Refit 2026-08-06 on 2017-2023, validated on a 2024 holdout (n=616).
        # The previous trio was stale: it scored 13.3592 where a plain refit on
        # the same games scores 13.0891 (+0.270) and the pace-blend refit
        # scores 12.9186 (+0.441). These assume the BLENDED pace -- feeding
        # them a raw single-season pace mis-scales the tempo term.
        total_intercept=29.2191,
        total_scale=12.4216,
        total_pace_scale=0.3785,
        avg_drives=12.0,
        points_per_epa=1.0,
        quality_win_threshold=0.0,
        bubble_adj_net=0.0,
        slope_by_games={
            "0-3": 9.1185,
            "4-5": 29.7368,
            "6-7": 41.7733,
            "8-20": 55.1520,
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
