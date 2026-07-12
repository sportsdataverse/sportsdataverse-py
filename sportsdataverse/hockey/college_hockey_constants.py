"""NCAA college-hockey (MCH/WCH) league constants + the T7.3 capture contract.

**Phase-0 feasibility finding (2026-07-12, real ESPN captures, see
``tests/fixtures/league_ports/README.md``):** ESPN's college-hockey
``summary``/``game_plays`` payloads carry **only ``Goal`` and ``Penalty``
play types** -- no shot-attempt events, no ``x``/``y`` shot coordinates, no
structured strength-state field (it is embedded in free text, e.g. "Powerplay
Goal Scored by ..."), and there is no shift-chart endpoint in the wrapper
surface at all. This holds for both a zero-play regular-season game
(``mch`` event 401711791) and a 12-play national-championship game (``mch``
401717648, ``wch`` 401762970) -- the richest capture available.

Consequently the NHL xG/RAPM/GSAx port (T5.1's ``nhl_xg``/``nhl_rapm``/
``nhl_gsax``) has **no shot corpus to refit against** and is not ported here
-- there is nothing to build a shot model from, and RAPM/GSAx additionally
need shift-level on-ice personnel and shots-against, neither of which ESPN
exposes for NCAA hockey. This is a hard ESPN capture-contract gap, not a
downscoped/simplified model.

What the payload *does* support: team-game goals-for/against (from the
scoreboard/boxscore), which is enough for an opponent-adjusted goal-margin
rating (:mod:`sportsdataverse.hockey.college_hockey_ratings`), reusing the
league-agnostic :func:`sportsdataverse._common.ratings.iterative_opponent_adjust`
fixed point (the MBB/NBA KenPom-style solver).

``has_shot_coordinates`` / ``has_shift_data`` / ``has_full_pbp`` are locked
``False`` for both leagues today and are asserted by
``tests/hockey/test_college_hockey_constants.py`` -- if a future ESPN
recapture ships shot events, that test breaks and the xG port becomes
reachable.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CollegeHockeyConstants:
    """Per-league NCAA hockey constants + ESPN capture-contract flags."""

    league: str
    espn_slug: str
    has_shot_coordinates: bool
    has_shift_data: bool
    has_full_pbp: bool
    hfa_goals: float


COLLEGE_HOCKEY_CONSTANTS: dict[str, CollegeHockeyConstants] = {
    "mch": CollegeHockeyConstants(
        league="mch",
        espn_slug="mens-college-hockey",
        has_shot_coordinates=False,
        has_shift_data=False,
        has_full_pbp=False,
        # Empirically estimated: mean (home_goals - away_goals) = 0.3194 over the
        # 191 non-neutral games in the captured 2024-25 MCH sample. Reproduce
        # offline with dev/league_ports/fit_mch_hfa.py (reads the committed
        # fixture); not a published league constant.
        hfa_goals=0.32,
    ),
    "wch": CollegeHockeyConstants(
        league="wch",
        espn_slug="womens-college-hockey",
        has_shot_coordinates=False,
        has_shift_data=False,
        has_full_pbp=False,
        # ESPN's WCH scoreboard only populated the 2025 NCAA Tournament bracket
        # (8 teams, 7 games -- no regular-season games found across a full-season
        # date sweep); too thin to fit its own HFA, so it borrows the MCH estimate.
        hfa_goals=0.32,
    ),
}


def get_college_hockey_constants(league: str) -> CollegeHockeyConstants:
    """Look up NCAA hockey league constants.

    Args:
        league: ``"mch"`` or ``"wch"``.

    Returns:
        The league's :class:`CollegeHockeyConstants`.

    Raises:
        ValueError: ``league`` is not a known NCAA hockey slug.

    Example:
        Quick start::

            from sportsdataverse.hockey.college_hockey_constants import get_college_hockey_constants
            c = get_college_hockey_constants("mch")
            print(c.has_shot_coordinates)
    """
    try:
        return COLLEGE_HOCKEY_CONSTANTS[league]
    except KeyError:
        raise ValueError(f"Unknown NCAA hockey league: {league!r}") from None
