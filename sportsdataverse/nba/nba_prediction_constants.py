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

from collections.abc import Mapping
from dataclasses import dataclass, field

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
            backtest for the league; see ``tools/models/nba_prediction/fit_pregame.py``).
        margin_sd: Std. dev. of the game-margin residual (fitted jointly
            with ``hfa``; the Brier-minimizing sigma agrees to within a
            documented tolerance).
        avg_pace: League baseline possessions per team per game (adjusted-
            pace anchor for :func:`~sportsdataverse.nba.nba_team_ratings.adjust_pace`).
        avg_off_rtg: League baseline points per 100 possessions.
        game_minutes: Regulation game length in minutes (NBA/G-League 48,
            WNBA 40) -- structurally different, not a fitted number.
        pts_sd_a: Intercept of the player-prop points SD law
            ``sd = pts_sd_a + pts_sd_b * sqrt(mu)``.
        pts_sd_b: Slope of that law.
        count_dispersion: Player-prop count over-dispersion ``var/mean`` for
            reb/ast/fg3m (Negative-Binomial when > 1).
        stat_dispersion: ``var/mean`` per stat for the other prop counts
            (``stl, blk, tov, fgm, ftm``); a stat missing here has no fitted
            distribution in that league.
        rate_prior_minutes: Pseudo-minutes ``k`` per stat that
            :func:`~sportsdataverse.nba.nba_player_props.player_rates` shrinks a
            player's per-minute rate with, toward his minutes-and-role tier's
            pooled rate; empty means raw rates.
        in_game_wp_artifact: Filename of the bundled in-game-WP coefficients
            under ``sportsdataverse/nba/models`` (committed in Phase 3).
    """

    hfa: float
    margin_sd: float
    avg_pace: float
    avg_off_rtg: float
    game_minutes: int
    pts_sd_a: float
    pts_sd_b: float
    count_dispersion: float
    in_game_wp_artifact: str = "nba_in_game_wp.ubj"
    stat_dispersion: Mapping[str, float] = field(default_factory=dict)
    rate_prior_minutes: Mapping[str, float] = field(default_factory=dict)


# hfa/margin_sd are FITTED per league on their own as-of-date backtest
# (tools/models/nba_prediction/fit_pregame.py, 2026-07-08): hfa = mean(actual_margin -
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
#
# pts_sd_a/pts_sd_b/count_dispersion (player props) are FITTED per league by
# tools/models/nba_prediction/fit_player_props.py on that league's 2024 ESPN box logs, over
# rotation players (>= 15 games, >= 15 mpg scaled to game length):
#   NBA  ("00"): 320 players -> sd = 1.406 + 1.396*sqrt(mu), dispersion 1.24 (2026-07-08).
#   WNBA ("10"): 107 players -> sd = 1.190 + 1.363*sqrt(mu), dispersion 1.19 (2026-11-24); at
#                15 pts that is 6.46 vs NBA 6.81 (player-bootstrap 95% CIs 6.23-6.71 / 6.69-6.94).
# G-League ("20") has no box-log fixture, so it carries the NBA values as seeds, like its hfa.
#
# stat_dispersion / rate_prior_minutes (NBA only) are FITTED by
# tools/models/nba_prediction/fit_player_rate_prior.py on the 2024-25 and 2025-26 regular seasons
# (ESPN player box release; 2023-24 feeds only the 2024-25 tier prior), 2026-11-25:
#   stat_dispersion: median single-game var/mean over 704 rotation player-seasons.
#   rate_prior_minutes: the k minimizing mean CRPS of each stat's predictive over 51,752 played
#     player-games (players with >= 1 earlier game), grid then x1.25 refinement.
#   Held-out check on 2021-22, the keep rule written before it ran (25,373 player-games, 164 days;
#   no props constant had used that season; reproduce with the script's `--holdout 2022`): CRPS gain
#   against raw rates on every one of the nine stats, z 3.0 (pts) to 5.9 (stl) with game-day
#   clustered bootstrap SEs (7.1 to 20.3 clustered by player); non-inferior on pra; 80% coverage rises
#   on every stat and moves no more than .01 away from .80 on any (pts .799 -> .807). The gain sits in
#   players' first 10 games (pts +0.106) and is about zero after 30.
# WNBA and G-League are not fitted (no held-out season has been set aside to check a fit there), so
# they keep raw rates and have no distribution for the five extra stats.
LEAGUE_CONSTANTS: dict[str, LeagueConstants] = {
    "00": LeagueConstants(
        hfa=2.184,
        margin_sd=14.504,
        avg_pace=99.5,
        avg_off_rtg=114.0,
        game_minutes=48,
        pts_sd_a=1.406,
        pts_sd_b=1.396,
        count_dispersion=1.24,
        in_game_wp_artifact="nba_in_game_wp.ubj",
        stat_dispersion={"stl": 1.057, "blk": 1.039, "tov": 1.037, "fgm": 1.306, "ftm": 1.928},
        rate_prior_minutes={
            "pts": 50.0,
            "reb": 40.0,
            "ast": 40.0,
            "fg3m": 40.0,
            "stl": 320.0,
            "blk": 100.0,
            "tov": 160.0,
            "fgm": 64.0,
            "ftm": 100.0,
        },
    ),
    "10": LeagueConstants(
        hfa=1.184,
        margin_sd=10.944,
        avg_pace=95.0,
        avg_off_rtg=101.0,
        game_minutes=40,
        pts_sd_a=1.19,
        pts_sd_b=1.363,
        count_dispersion=1.19,
        in_game_wp_artifact="wnba_in_game_wp.ubj",
    ),
    "20": LeagueConstants(
        hfa=2.5,
        margin_sd=14.0,
        avg_pace=101.0,
        avg_off_rtg=110.0,
        game_minutes=48,
        pts_sd_a=1.406,
        pts_sd_b=1.396,
        count_dispersion=1.24,
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
