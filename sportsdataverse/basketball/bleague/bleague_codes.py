"""Code tables for the B.LEAGUE play-by-play (``ActionCD1`` / ``ActionCD2`` / ``ActionCD3`` / ``AreaCD``).

The feed sends integer codes with no table. These were decoded from the Japanese ``PlayText`` of every sampled
game (sdv-internal-refs ``bleague/bleague-returns.md``, verified 2026-10-12). Codes the site's JavaScript knows
but no sample contains (20, 21, 26, 44, 46, 90 as an action) are left out, so :func:`action_name` returns ``None``
for them rather than guessing.

**The two feeds disagree on team rebounds.** On the ``v2`` feed (B1/B2 from 2020-21) code 18 is the team
DEFENSIVE rebound and 19 the OFFENSIVE one; on the ``v1`` feed (B3, B1/B2 2018-20) and the 2016-17 embed it is the
other way round. :func:`action_name` takes the feed for that reason. Substitutions also differ: ``v1`` has one
``substitution`` row (89, ``PlayerID1`` out, ``PlayerID2`` in), ``v2`` two (``player_out`` 87 and ``player_in`` 86).
"""

from __future__ import annotations

from typing import Dict, Optional

__all__ = ["ACTIONS", "AREAS", "QUALIFIERS", "SHOT_ACTIONS", "action_name", "area_name", "qualifier_name"]

#: Feed-independent ``ActionCD1`` codes -> English slug.
ACTIONS: Dict[int, str] = {
    1: "fg3_made",
    2: "fg3_missed",
    3: "fg2_made_outside_paint",
    4: "fg2_made_inside_paint",
    5: "fg2_missed_outside_paint",
    6: "fg2_missed_inside_paint",
    7: "ft_made",
    8: "ft_missed",
    9: "rebound_def",
    10: "rebound_off",
    11: "block",
    12: "assist",
    13: "turnover",
    14: "steal",
    15: "foul_drawn",
    16: "and_one",
    17: "team_turnover",
    22: "foul_personal",
    23: "foul_offensive",
    24: "foul_technical",
    25: "foul_unsportsmanlike",
    80: "game_start",
    81: "game_end",
    82: "period_start",
    83: "period_end",
    84: "clock_start",
    85: "clock_stop",
    86: "player_in",
    87: "player_out",
    88: "timeout",
    89: "substitution",
}

#: Field-goal attempts: code -> (shot type, made, inside the paint or None when the code does not say).
SHOT_ACTIONS: Dict[int, tuple] = {
    1: ("3pt", True, None),
    2: ("3pt", False, None),
    3: ("2pt", True, False),
    4: ("2pt", True, True),
    5: ("2pt", False, False),
    6: ("2pt", False, True),
}

#: ``ActionCD2`` / ``ActionCD3`` qualifier codes (one code space) -> English slug.
QUALIFIERS: Dict[int, str] = {
    27: "jump_shot",
    28: "layup",
    29: "dunk",
    30: "free_throws_awarded_0",
    31: "free_throws_awarded_1",
    32: "free_throws_awarded_2",
    33: "free_throws_awarded_3",
    34: "violation_24s",
    35: "fast_break",
    36: "points_off_turnover",
    37: "second_chance",
    38: "fast_break_points_off_turnover",
    39: "shooting_foul",
    40: "foul_out",
    47: "fast_break_second_chance",
    48: "points_off_turnover_second_chance",
    90: "timeout",
}

#: ``AreaCD`` shot zones -> zone slug. Of each mirrored pair the first code is the side where the distance along
#: the baseline is large, the second the side where it is small (see the ``area_code`` column description).
AREAS: Dict[int, str] = {
    1: "restricted_area",
    2: "paint",
    3: "baseline_mid_range",
    4: "wing_mid_range",
    5: "top_of_key_mid_range",
    6: "wing_mid_range",
    7: "baseline_mid_range",
    8: "corner_3",
    9: "wing_3",
    10: "top_3",
    11: "wing_3",
    12: "corner_3",
    13: "deep_3",
}


def action_name(code: Optional[int], feed: Optional[str] = None) -> Optional[str]:
    """English slug of an ``ActionCD1`` code, or ``None`` for a code no sample contains.

    Args:
        code: the integer ``ActionCD1``.
        feed: ``"v1"`` or ``"v2"``; decides the team-rebound codes 18 and 19 (``None`` leaves them unnamed).

    Returns:
        The slug, e.g. ``"fg3_made"``, or ``None``.

    Raises:
        None: unknown input gives ``None``.

    Example:
        Quick start::

            from sportsdataverse.basketball.bleague.bleague_codes import action_name

            action_name(18, "v2")   # "team_rebound_def"
            action_name(18, "v1")   # "team_rebound_off"
    """
    if code in (18, 19):
        if feed not in ("v1", "v2"):
            return None
        defensive = (code == 18) == (feed == "v2")
        return "team_rebound_def" if defensive else "team_rebound_off"
    return ACTIONS.get(code) if isinstance(code, int) and not isinstance(code, bool) else None


def qualifier_name(code: Optional[int]) -> Optional[str]:
    """English slug of an ``ActionCD2`` / ``ActionCD3`` code, or ``None`` when unknown.

    Args:
        code: the integer qualifier code.

    Returns:
        The slug (``"layup"``), or ``None``.

    Raises:
        None: unknown input gives ``None``.

    Example:
        Quick start::

            from sportsdataverse.basketball.bleague.bleague_codes import qualifier_name

            qualifier_name(28)   # "layup"
    """
    return QUALIFIERS.get(code) if isinstance(code, int) and not isinstance(code, bool) else None


def area_name(code: Optional[int]) -> Optional[str]:
    """Zone slug of an ``AreaCD`` code (the pair of mirrored codes share one slug), or ``None``.

    Args:
        code: the integer ``AreaCD`` (1-13).

    Returns:
        The slug (``"corner_3"``), or ``None``.

    Raises:
        None: unknown input gives ``None``.

    Example:
        Quick start::

            from sportsdataverse.basketball.bleague.bleague_codes import area_name

            area_name(12)   # "corner_3"
    """
    return AREAS.get(code) if isinstance(code, int) and not isinstance(code, bool) else None
