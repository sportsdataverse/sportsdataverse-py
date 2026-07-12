"""League constants for the college baseball/softball RE24/WPA port (T7.3, model 5).

Mirrors :mod:`sportsdataverse.mlb.mlb_run_expectancy` / ``mlb_win_expectancy``
(T6.4) for two ESPN-sourced college leagues that differ from MLB only in
regulation game length: college baseball is a 9-inning game (same as MLB),
college softball is 7 innings. Everything else (24-state base-out space, RE24
methodology, empirical win-expectancy bucketing) is reused by reference from
the MLB module -- see :mod:`sportsdataverse.baseball.college_run_expectancy`.

See Also:
    * `baseballr`_ -- R sibling package for MLB/college sabermetrics.

    .. _baseballr: https://baseballr.sportsdataverse.org
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict


@dataclass(frozen=True)
class CollegeBaseballConstants:
    """Per-league constants for the college baseball/softball RE24/WPA port.

    Attributes:
        innings: Regulation game length (9 for baseball, 7 for softball) --
            the RE24-exclusion / win-expectancy inning cap for this league.
        sport_slug: ESPN sport path segment (``"baseball"`` for both leagues).
        league_slug: ESPN league path segment (``"college-baseball"`` /
            ``"college-softball"``).
    """

    innings: int
    sport_slug: str
    league_slug: str


COLLEGE_BASEBALL_CONSTANTS: Dict[str, CollegeBaseballConstants] = {
    "college_baseball": CollegeBaseballConstants(innings=9, sport_slug="baseball", league_slug="college-baseball"),
    "college_softball": CollegeBaseballConstants(innings=7, sport_slug="softball", league_slug="college-softball"),
}

#: The 24-state base-out space (8 base-occupancy codes x 3 out-counts),
#: encoded identically to :mod:`sportsdataverse.mlb.mlb_run_expectancy`
#: (``"_"`` = empty, ``"1"``/``"2"``/``"3"`` = occupied): e.g. ``"1_3"`` is
#: runners on first and third.
BASE_STATES = ["___", "1__", "_2_", "__3", "12_", "1_3", "_23", "123"]


def get_college_baseball_constants(league: str) -> CollegeBaseballConstants:
    """Look up the :class:`CollegeBaseballConstants` for a league.

    Args:
        league: ``"college_baseball"`` or ``"college_softball"``.

    Returns:
        CollegeBaseballConstants: the matching constants row.

    Raises:
        ValueError: ``league`` is not one of the two supported keys.

    Example:
        Quick start::

            from sportsdataverse.baseball.college_baseball_constants import get_college_baseball_constants
            c = get_college_baseball_constants("college_softball")
            print(c.innings)  # 7
    """
    try:
        return COLLEGE_BASEBALL_CONSTANTS[league]
    except KeyError:
        raise ValueError(f"Unknown league {league!r}; expected one of {sorted(COLLEGE_BASEBALL_CONSTANTS)}") from None
