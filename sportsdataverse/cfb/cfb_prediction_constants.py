"""CFB prediction-spine constants + validation metrics (compute-on-demand)."""

from __future__ import annotations

from dataclasses import dataclass

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
        hfa_epa: Home-field advantage on the EPA-per-play scale -- the ratings
            ridge's native home coefficient (~0.0185). Applied component-wise
            (home_off += hfa_epa, home_def -= hfa_epa), so the home team's net
            rating gains ``2 * hfa_epa`` and the margin picks up
            ``net_points_scale * 2 * hfa_epa`` (~1.65 pt on the netted scale) while the *total* is
            unchanged (the offense/defense shifts cancel in the sum). This
            EPA-scale form is why an additive constant works where a multiplicative
            tilt cannot -- the ratings are per-play deviations near zero.
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


CFB_CONSTANTS: dict[str, PredictConfig] = {
    # net_points_scale / margin_sd / total_* fitted on the 2023 backtest by
    # dev/cfb_prediction/fit_pregame.py; hfa_epa is the ratings ridge's own home
    # coefficient (see that script for the exact procedure). Refit 2026-07-28
    # after `efficiency_ratings` switched to the R adjust_epa NETTED scale
    # (gameonpaper parity, ~1.8x smaller differentials at the top -> larger
    # points slope). Achieved on the refit: brier 0.1416 (beats ESPN FPI
    # 0.1436), spread MAE 3.23 (was 4.06), total MAE 4.88.
    "modern": PredictConfig(
        hfa_epa=0.01848,
        margin_sd=17.2493,
        net_points_scale=44.5367,
        total_intercept=26.8933,
        total_scale=19.0816,
        total_pace_scale=0.4267,
        avg_drives=12.0,
        points_per_epa=1.0,
        quality_win_threshold=0.0,
        bubble_adj_net=0.0,
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
