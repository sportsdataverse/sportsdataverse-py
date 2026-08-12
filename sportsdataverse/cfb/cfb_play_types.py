"""Era-stable canonical play types for CFB play-by-play.

ESPN's ``type.text`` is **not one vocabulary** -- it is three, and they do not
map 1:1. Measured over ``play_by_play_2004..2025`` (3,145,840 plays, 53 distinct
``type.text`` values):

============================  =========================  =========================
concept                       2004                       2005-2013 / 2014+
============================  =========================  =========================
completed pass                ``Pass Completion``        ``Pass Completion`` (->2013)
                                                         ``Pass Reception`` (2014+)
punt                          ``Punt Return``            ``Punt``
                              (``Punt`` absent in 2004)
kickoff                       ``Kickoff Return           ``Kickoff``
                              (Offense)``
============================  =========================  =========================

The practical consequence: ``type.text == "Pass Reception"`` silently returns
**nothing before 2014**, and ``== "Punt"`` returns **nothing for 2004**. Every
consumer either reimplements this mapping or gets it quietly wrong, and a
season-spanning query mixes populations without any error being raised.

This module is the single mapping. ``PLAY_TYPE_CANONICAL`` collapses the
era-synonyms and leaves every genuine distinction intact (a rushing touchdown
stays distinct from a rush). Types ESPN has never shipped map to ``None`` rather
than passing through, so a new upstream value surfaces as a null instead of
silently becoming its own category.

Example:
    Filter every completed pass across all eras::

        import polars as pl
        from sportsdataverse.cfb import add_play_type_canonical

        pbp = add_play_type_canonical(pbp)
        passes = pbp.filter(pl.col("play_type_canonical") == "pass_completion")

    Coarse phase grouping (offense / special teams / administrative)::

        from sportsdataverse.cfb import PLAY_TYPE_FAMILY

        pbp.filter(pl.col("play_type_family") == "special_teams")

    Check for upstream vocabulary drift -- unmapped types become null::

        unmapped = pbp.filter(
            pl.col("type.text").is_not_null() & pl.col("play_type_canonical").is_null()
        )
"""

from __future__ import annotations

import polars as pl

__all__ = [
    "PLAY_TYPE_CANONICAL",
    "PLAY_TYPE_FAMILY",
    "add_play_type_canonical",
    "canonical_play_type_expr",
    "play_type_family_expr",
]

#: ESPN ``type.text`` -> era-stable canonical play type.
#:
#: The three entries marked ERA-SYNONYM are the whole point of this table: they
#: are the same football event written differently by era, verified against the
#: mechanics flags (``Punt Return`` carries ``punt_play=True`` on 100% of its
#: 5,755 rows across both 2004 and 2025; ``Kickoff Return (Offense)`` carries
#: ``kickoff_play=True`` on 100% of its 21,952 rows).
PLAY_TYPE_CANONICAL: dict[str, str] = {
    # --- rushing ---
    "Rush": "rush",
    "Rushing Touchdown": "rush_touchdown",
    # --- passing ---
    "Pass Completion": "pass_completion",  # ERA-SYNONYM (2004-2013)
    "Pass Reception": "pass_completion",  # ERA-SYNONYM (2014+)
    "Pass": "pass_completion",  # sparse 2005-2022 generic
    "Pass Incompletion": "pass_incompletion",
    "Passing Touchdown": "pass_touchdown",
    "Sack": "sack",
    # --- interceptions (three spellings of one event) ---
    "Interception Return": "interception",  # ERA-SYNONYM
    "Pass Interception": "interception",  # ERA-SYNONYM (2006-2011)
    "Pass Interception Return": "interception",  # ERA-SYNONYM (2015)
    "Interception Return Touchdown": "interception_touchdown",
    # --- fumbles ---
    "Fumble": "fumble",
    "Fumble Recovery (Own)": "fumble_recovery_own",
    "Fumble Recovery (Opponent)": "fumble_recovery_opponent",
    "Fumble Recovery (Own) Touchdown": "fumble_recovery_own_touchdown",
    "Fumble Recovery (Opponent) Touchdown": "fumble_recovery_opponent_touchdown",
    "Fumble Return Touchdown": "fumble_return_touchdown",
    # --- punts ---
    "Punt": "punt",  # ERA-SYNONYM (2005+)
    "Punt Return": "punt",  # ERA-SYNONYM (2004; punt_play=True 100%)
    "Punt Return Touchdown": "punt_return_touchdown",
    "Blocked Punt": "punt_blocked",
    "Blocked Punt Touchdown": "punt_blocked_touchdown",
    "Punt (Safety)": "punt_safety",
    "Punt Team Fumble Recovery": "punt_team_fumble_recovery",
    "Punt Team Fumble Recovery Touchdown": "punt_team_fumble_recovery_touchdown",
    # --- kickoffs ---
    "Kickoff": "kickoff",  # ERA-SYNONYM (2005+)
    "Kickoff Return (Offense)": "kickoff",  # ERA-SYNONYM (2004; kickoff_play=True 100%)
    "Kickoff Return Touchdown": "kickoff_return_touchdown",
    "Kickoff Team Fumble Recovery": "kickoff_team_fumble_recovery",
    "Kickoff (Safety)": "kickoff_safety",
    # --- field goals ---
    "Field Goal Good": "field_goal_made",
    "Field Goal Missed": "field_goal_missed",
    "Blocked Field Goal": "field_goal_blocked",
    "Blocked Field Goal Touchdown": "field_goal_blocked_touchdown",
    "Missed Field Goal Return": "missed_field_goal_return",
    "Missed Field Goal Return Touchdown": "missed_field_goal_return_touchdown",
    # --- extra points (rows vanish after 2013 -- see module note below) ---
    "Extra Point Good": "extra_point_made",
    "Extra Point Missed": "extra_point_missed",
    # --- two-point conversions ---
    "Two-Point Conversion Good": "two_point_made",
    "2pt Conversion": "two_point_made",
    "Two-Point Conversion Missed": "two_point_missed",
    "Two Point Pass": "two_point_pass",
    "Two Point Rush": "two_point_rush",
    "Defensive 2pt Conversion": "defensive_two_point",
    # --- other scoring / stoppages ---
    "Safety": "safety",
    "Penalty": "penalty",
    "Penalty (Safety)": "penalty_safety",
    "Timeout": "timeout",
    "End Period": "end_period",
    "End of Half": "end_period",
    "End of Game": "end_period",
    "Unknown": "unknown",
}

#: canonical play type -> coarse phase family.
#:
#: ``administrative`` rows are not football plays (timeouts, period markers) and
#: should be excluded from per-play rate denominators.
PLAY_TYPE_FAMILY: dict[str, str] = {
    "rush": "offense",
    "rush_touchdown": "offense",
    "pass_completion": "offense",
    "pass_incompletion": "offense",
    "pass_touchdown": "offense",
    "sack": "offense",
    "two_point_pass": "offense",
    "two_point_rush": "offense",
    "two_point_made": "offense",
    "two_point_missed": "offense",
    "interception": "turnover",
    "interception_touchdown": "turnover",
    "fumble": "turnover",
    "fumble_recovery_own": "turnover",
    "fumble_recovery_opponent": "turnover",
    "fumble_recovery_own_touchdown": "turnover",
    "fumble_recovery_opponent_touchdown": "turnover",
    "fumble_return_touchdown": "turnover",
    "defensive_two_point": "turnover",
    "punt": "special_teams",
    "punt_return_touchdown": "special_teams",
    "punt_blocked": "special_teams",
    "punt_blocked_touchdown": "special_teams",
    "punt_safety": "special_teams",
    "punt_team_fumble_recovery": "special_teams",
    "punt_team_fumble_recovery_touchdown": "special_teams",
    "kickoff": "special_teams",
    "kickoff_return_touchdown": "special_teams",
    "kickoff_team_fumble_recovery": "special_teams",
    "kickoff_safety": "special_teams",
    "field_goal_made": "special_teams",
    "field_goal_missed": "special_teams",
    "field_goal_blocked": "special_teams",
    "field_goal_blocked_touchdown": "special_teams",
    "missed_field_goal_return": "special_teams",
    "missed_field_goal_return_touchdown": "special_teams",
    "extra_point_made": "special_teams",
    "extra_point_missed": "special_teams",
    "safety": "other",
    "penalty": "other",
    "penalty_safety": "other",
    "unknown": "other",
    "timeout": "administrative",
    "end_period": "administrative",
}


def canonical_play_type_expr(source: str = "type.text") -> pl.Expr:
    """Build the polars expression mapping raw ``type.text`` to a canonical type.

    Args:
        source: Name of the raw play-type column.

    Returns:
        A ``pl.Expr`` aliased ``play_type_canonical``. Values absent from
        :data:`PLAY_TYPE_CANONICAL` (and nulls) yield null, so upstream
        vocabulary drift surfaces rather than silently creating a category.

    Example:
        Add just the canonical column to an existing frame::

            from sportsdataverse.cfb import canonical_play_type_expr

            pbp = pbp.with_columns(canonical_play_type_expr())
    """
    return pl.col(source).replace_strict(PLAY_TYPE_CANONICAL, default=None).alias("play_type_canonical")


def play_type_family_expr(source: str = "play_type_canonical") -> pl.Expr:
    """Build the polars expression mapping a canonical type to its phase family.

    Args:
        source: Name of the canonical play-type column.

    Returns:
        A ``pl.Expr`` aliased ``play_type_family``; unmapped values yield null.

    Example:
        Drop administrative rows before computing per-play rates::

            import polars as pl
            from sportsdataverse.cfb import add_play_type_canonical

            plays = add_play_type_canonical(pbp).filter(
                pl.col("play_type_family") != "administrative"
            )
    """
    return pl.col(source).replace_strict(PLAY_TYPE_FAMILY, default=None).alias("play_type_family")


def add_play_type_canonical(
    df: pl.DataFrame,
    *,
    source: str = "type.text",
    with_family: bool = True,
) -> pl.DataFrame:
    """Append ``play_type_canonical`` (and optionally ``play_type_family``).

    Args:
        df: A play-by-play frame.
        source: Name of the raw play-type column.
        with_family: Also append the coarse ``play_type_family`` column.

    Returns:
        The frame with the canonical column(s) appended. Returned unchanged when
        ``source`` is absent, so the helper is safe to apply to frames that have
        already been projected down.

    Example:
        Count plays per family for a season, era-agnostically::

            import polars as pl
            from sportsdataverse.cfb import add_play_type_canonical

            out = add_play_type_canonical(pbp)
            out.group_by("play_type_family").agg(pl.len())
    """
    if source not in df.columns:
        return df
    out = df.with_columns(canonical_play_type_expr(source))
    if with_family:
        out = out.with_columns(play_type_family_expr())
    return out
