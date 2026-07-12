"""NBA/WNBA/G-League prediction-stack shared constants + validation metrics.

Home of the small, dependency-light pieces every module in the NBA prediction
& market stack (T3.3) shares:

* **Validation metrics** (:func:`brier_score`, :func:`log_loss_score`,
  :func:`spearman_corr`, :func:`mae`, :func:`calibration_table`) used by the
  phase oracle/backtest gates.
* **Per-``league_id`` fitted constants** (:data:`LEAGUE_CONSTANTS` /
  :func:`get_constants`) -- the algorithm/constants boundary described in the
  design spec: every league-specific number (home-court advantage, margin
  sigma, pace/efficiency baselines, game minutes) lives here, keyed by the
  stats.nba.com ``league_id`` (``"00"`` NBA, ``"10"`` WNBA, ``"20"``
  G-League) so a WNBA caller is a by-reference shim over this table -- the
  same pattern ``nba_possessions(league_id=...)`` / ``wnba_stats`` already
  use. No NBA-specific number may be hard-coded inside an algorithm function.
* **As-of-date leakage split** (:func:`as_of_ratings_split`) -- the boundary
  every predictive backtest uses so a game's own future never leaks into its
  own prediction.

*(T7.2-shared)*: the metrics + as-of-split pieces are byte-identical to the
MBB/WBB (``mbb_prediction_constants``) and CFB sibling modules and are a
prime candidate for the future cross-league infra factor-out; this module
implements them standalone per the T3.3 plan (mirroring MBB names).
"""

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

__all__ = [
    "LEAGUE_CONSTANTS",
    "LeagueConstants",
    "as_of_ratings_split",
    "brier_score",
    "calibration_table",
    "get_constants",
    "log_loss_score",
    "mae",
    "spearman_corr",
]


@dataclass(frozen=True)
class LeagueConstants:
    """Per-``league_id`` fitted constants for the NBA prediction & market stack.

    Attributes:
        hfa: Home-court advantage in points (fitted on the as-of-date
            backtest for the league; see ``dev/nba_prediction/fit_pregame.py``).
        margin_sd: Std. dev. of the game-margin residual (fitted jointly
            with ``hfa``; the Brier-minimizing sigma agrees to within a
            documented tolerance).
        avg_pace: League baseline possessions per team per game (adjusted-
            pace anchor for :func:`~sportsdataverse.nba.nba_team_ratings.adjust_pace`).
        avg_off_rtg: League baseline points per 100 possessions.
        game_minutes: Regulation game length in minutes (NBA/G-League 48,
            WNBA 40) -- structurally different, not a fitted number.
        in_game_wp_artifact: Filename of the bundled in-game-WP coefficients
            under ``sportsdataverse/nba/models`` (committed in Phase 3).
    """

    hfa: float
    margin_sd: float
    avg_pace: float
    avg_off_rtg: float
    game_minutes: int
    in_game_wp_artifact: str = "nba_in_game_wp.ubj"


# hfa/margin_sd are FITTED per league on their own as-of-date backtest
# (dev/nba_prediction/fit_pregame.py, 2026-07-08): hfa = mean(actual_margin -
# possession-scaled AdjNet diff) over non-neutral games, margin_sd = std of the residual
# around that fitted hfa (0 for neutral games).
#   NBA  ("00"): 1162 of 1320 2023-24 games -> hfa 2.184, margin_sd 14.504.
#   WNBA ("10"): 220 of 264 2024 games      -> hfa 1.184, margin_sd 10.944 (lower HFA +
#                variance than NBA, as expected).
# G-League ("20") has no committed fixture (ESPN has no G-League schedule/box loader and
# there is no gleague loader surface); its hfa/margin_sd remain seed values from league
# norms -- the algorithms accept league_id="20" and would fit identically given a data
# source. avg_pace/avg_off_rtg are league-norm anchors; game_minutes is structural (WNBA
# plays 40-minute games, NBA/G-League play 48), not fitted.
LEAGUE_CONSTANTS: dict[str, LeagueConstants] = {
    "00": LeagueConstants(
        hfa=2.184,
        margin_sd=14.504,
        avg_pace=99.5,
        avg_off_rtg=114.0,
        game_minutes=48,
        in_game_wp_artifact="nba_in_game_wp.ubj",
    ),
    "10": LeagueConstants(
        hfa=1.184,
        margin_sd=10.944,
        avg_pace=95.0,
        avg_off_rtg=101.0,
        game_minutes=40,
        in_game_wp_artifact="wnba_in_game_wp.ubj",
    ),
    "20": LeagueConstants(
        hfa=2.5,
        margin_sd=14.0,
        avg_pace=101.0,
        avg_off_rtg=110.0,
        game_minutes=48,
        in_game_wp_artifact="nbagl_in_game_wp.ubj",
    ),
}


def get_constants(league_id: str) -> LeagueConstants:
    """Return the :class:`LeagueConstants` for a ``league_id``.

    Args:
        league_id: stats.nba.com league id -- ``"00"`` NBA, ``"10"`` WNBA,
            ``"20"`` G-League.

    Returns:
        The league's :class:`LeagueConstants`.

    Raises:
        ValueError: If ``league_id`` is not a known key of
            :data:`LEAGUE_CONSTANTS`.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_prediction_constants import get_constants
            get_constants("00").hfa
    """
    try:
        return LEAGUE_CONSTANTS[league_id]
    except KeyError:
        raise ValueError(f"unknown league_id {league_id!r}; expected one of {sorted(LEAGUE_CONSTANTS)}") from None
