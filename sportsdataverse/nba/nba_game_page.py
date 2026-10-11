# sportsdataverse/nba/nba_game_page.py
"""Game-page derivations for one NBA game (shared by the GOP processor and the sdv-db stage).

Every function takes the hoopR-schema frames (``load_nba_pbp`` / ``load_nba_team_boxscore`` /
``load_nba_player_boxscore`` column names) so one implementation serves both callers: the Game on
Paper Flask processor builds those frames live from ESPN through
:func:`~sportsdataverse.nba.nba_pbp.espn_nba_pbp` + the ``helper_nba_*`` converters (see
:func:`game_page_frames`), and the sdv-db nightly stage reads the same shapes from the warehouse.
"""

from __future__ import annotations

from typing import Optional

import polars as pl

from sportsdataverse.nba.nba_game_predict import nba_in_game_win_prob, win_prob_from_margin
from sportsdataverse.nba.nba_prediction_constants import get_constants

__all__ = [
    "FT_WEIGHT",
    "OT_PERIOD_SECONDS",
    "REGULATION_PERIODS",
    "COORD_SENTINEL",
    "home_prior",
    "ot_adjusted_seconds",
    "score_win_prob",
]

#: Oliver's free-throw possession weight, used by every rate below.
FT_WEIGHT: float = 0.44
#: An NBA overtime period is five minutes.
OT_PERIOD_SECONDS: float = 300.0
REGULATION_PERIODS: int = 4
#: ESPN marks a missing play coordinate with ``-214748340``; anything below this is "no location".
COORD_SENTINEL: float = -1000.0


def home_prior(
    home_team_spread: Optional[float],
    game_spread_available: Optional[bool],
    *,
    league_id: str = "00",
) -> tuple[float, str]:
    """Pregame home win probability for the in-game model, and where it came from.

    ``home_team_spread`` is the signed expected home margin that ``load_nba_pbp`` /
    ``helper_nba_pbp_features`` carry (positive when the home side is favoured). Both hoopR and
    :func:`~sportsdataverse.nba.nba_pbp.helper_nba_pickcenter` substitute ``+2.5`` when no line
    exists, so the value is only trusted when ``game_spread_available`` is true; otherwise the
    prior is home-court advantage alone.

    Args:
        home_team_spread: Signed expected home margin, or ``None``.
        game_spread_available: Whether a real line backed that spread.
        league_id: ``"00"`` NBA / ``"10"`` WNBA / ``"20"`` G League (selects the constants).

    Returns:
        ``(probability, source)`` with ``source`` ``"spread"`` or ``"hfa"``.

    Raises:
        ValueError: If ``league_id`` is not ``"00"``, ``"10"`` or ``"20"``.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_game_page import home_prior
            home_prior(3.0, True)      # (0.58, 'spread')
            home_prior(2.5, False)     # (0.56, 'hfa')  the default line is ignored
    """
    if game_spread_available and home_team_spread is not None:
        return win_prob_from_margin(float(home_team_spread), league_id=league_id), "spread"
    return win_prob_from_margin(get_constants(league_id).hfa, league_id=league_id), "hfa"


def ot_adjusted_seconds(pbp: pl.DataFrame) -> pl.DataFrame:
    """Tag overtime rows and give them the OT period clock as their seconds remaining.

    The in-game model clips ``start_game_seconds_remaining`` at zero, so every overtime play
    (negative in the hoopR frame) would score as "game over at the current margin". Each OT period
    is scored as its own five-minute endgame instead: ``start_game_seconds_remaining`` becomes
    ``start_quarter_seconds_remaining`` on rows with ``period_number > 4``.

    Args:
        pbp: One game in the ``load_nba_pbp`` schema.

    Returns:
        The frame with ``wp_regime`` (``"reg"`` / ``"ot"``) and the adjusted seconds column.

    Raises:
        polars.exceptions.ColumnNotFoundError: If ``period_number``, ``start_game_seconds_remaining`` or
            ``start_quarter_seconds_remaining`` is missing.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_game_page import ot_adjusted_seconds
            tagged = ot_adjusted_seconds(pbp)
            tagged.filter(pl.col("wp_regime") == "ot").select("start_game_seconds_remaining").head()
    """
    is_ot = pl.col("period_number") > REGULATION_PERIODS
    return pbp.with_columns(
        pl.when(is_ot).then(pl.lit("ot")).otherwise(pl.lit("reg")).alias("wp_regime"),
        pl.when(is_ot)
        .then(pl.col("start_quarter_seconds_remaining").cast(pl.Float64))
        .otherwise(pl.col("start_game_seconds_remaining").cast(pl.Float64))
        .alias("start_game_seconds_remaining"),
    )


def score_win_prob(pbp: pl.DataFrame, *, league_id: str = "00") -> pl.DataFrame:
    """Per-play home win probability, its play-to-play delta, and the prior, for one game.

    Args:
        pbp: One game in the ``load_nba_pbp`` schema (needs ``home_team_spread``,
            ``game_spread_available``, ``period_number``, ``start_game_seconds_remaining``,
            ``start_quarter_seconds_remaining``, ``home_score``, ``away_score``, ``team_id``,
            ``home_team_id``), in play order.
        league_id: ``"00"`` / ``"10"`` / ``"20"``.

    Returns:
        The input plus ``pregame_home_prob``, ``prior_source``, ``wp_regime``, ``home_win_prob``
        and ``wp_delta`` (the first play's delta is measured from the prior). An empty input
        returns an empty frame with those columns.

    Raises:
        polars.exceptions.ColumnNotFoundError: If a required column is missing (``home_team_spread``,
            ``game_spread_available``, ``period_number``, ``start_game_seconds_remaining``,
            ``start_quarter_seconds_remaining``, ``home_score``, ``away_score``, ``team_id``,
            ``home_team_id``).
        ValueError: If ``league_id`` is unknown.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_game_page import score_win_prob
            from sportsdataverse.nba.nba_loaders import load_nba_pbp
            one = load_nba_pbp([2024]).filter(pl.col("game_id") == 401585828)
            wp = score_win_prob(one)

        Biggest swings::

            wp.sort(pl.col("wp_delta").abs(), descending=True).select("text", "wp_delta").head(10)
    """
    if pbp.height == 0:
        return pbp.with_columns(
            pl.lit(None, dtype=pl.Float64).alias("pregame_home_prob"),
            pl.lit(None, dtype=pl.String).alias("prior_source"),
            pl.lit(None, dtype=pl.String).alias("wp_regime"),
            pl.lit(None, dtype=pl.Float64).alias("home_win_prob"),
            pl.lit(None, dtype=pl.Float64).alias("wp_delta"),
        )
    first = pbp.row(0, named=True)
    prior, source = home_prior(first.get("home_team_spread"), first.get("game_spread_available"), league_id=league_id)
    tagged = ot_adjusted_seconds(pbp)
    scored = nba_in_game_win_prob(tagged, prior, league_id=league_id)
    wp = scored["home_win_prob"].cast(pl.Float64)
    return pbp.with_columns(
        pl.lit(prior, dtype=pl.Float64).alias("pregame_home_prob"),
        pl.lit(source, dtype=pl.String).alias("prior_source"),
        tagged["wp_regime"],
        wp.alias("home_win_prob"),
        (wp - wp.shift(n=1, fill_value=prior)).alias("wp_delta"),
    )
