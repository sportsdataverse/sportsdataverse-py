"""League constants for the spring-football EP/WP port (UFL/XFL/CFL).

Ports the already-shipped, parity-validated NFL EP/WP suite
(``sportsdataverse.nfl.ep_wp``) onto ESPN spring-football data by calling
``enrich_nfl_pbp`` directly on a spring-football play frame (see
``spring_football_ep_wp.py``) rather than re-implementing any model or
derivation logic. This module documents the per-league RULE DELTAS from the
NFL that a from-scratch port would need to account for.

**Downscope note (Task 1.3):** ``touchback_yardline``, ``kickoff_spot``, and
``conversion_point_values`` below are cited, rule-derived metadata but are
NOT YET wired into scoring -- ``enrich_nfl_pbp``'s EP/WP/CP model loaders
(``calculate_expected_points`` / ``calculate_win_probability`` /
``calculate_completion_probability``) hard-code the NFL touchback spot and
the NFL 7-class point-value vector with no override kwarg today, and adding
one would mean threading new parameters through the parity-validated core
NFL pipeline for what is, on the actual captured data (see
``tests/fixtures/league_ports/FEASIBILITY.md``), a single fixture's worth of
plays -- not a safe trade against the core pipeline's parity guarantee. The
real down/distance/yardline/clock/score STATE fed into the models is genuine
league-native ESPN data; only the kickoff-touchback spot and the
point-value collapse use the NFL's numbers. UFL/XFL conversions do not
appear as separate plays in the captured data (the result is folded into the
touchdown play's text), so this does not bias the real score/state columns.

UFL/XFL kickoff + conversion rules change by season; the base entries hold
the CURRENT ruleset and :data:`SPRING_FOOTBALL_SEASON_OVERRIDES` the earlier
ones (``get_sf_constants(league, season=)`` resolves them):

* XFL 2020 and 2023: kickoffs from the 30, in-the-air touchback at the 35
  (``yardline_100`` = 65) rather than the NFL's 25 (``yardline_100`` = 75,
  2016+ rule); no PAT kick, conversions from the 2 (1 pt), 5 (2) or 10 (3).
* UFL 2024 (the USFL+XFL merger): kickoffs from the 20, every touchback at
  the 25, the XFL conversion tiers. UFL 2025: kickoffs from the 30 with two
  touchback spots (35 in the air, 20 after a landing-zone bounce). UFL 2026:
  a 33-yard one-point kick returns, the 3-point try moves to the 8, a field
  goal of 60+ yards is worth four, the in-the-air touchback moves to the 40.
* The 0.80 / 0.50 / 0.30 success rates are SEEDED PLACEHOLDERS (unused in
  scoring -- see the downscope note); refresh from observed league data when
  conversion modeling lands.

CFL differs structurally (3 downs, 110-yard field, rouge scoring) and is
handled separately (Phase 6, conditional refit-or-defer) -- its entry here
is descriptive metadata only; no CFL model/pipeline ships from this module.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any

import numpy as np

from sportsdataverse.nfl.model_vars import _EP_POINT_VALUES as _NFL_EP_POINT_VALUES


@dataclass(frozen=True)
class SpringFootballConstants:
    """Per-league rule deltas from the NFL for the spring-football EP/WP port.

    Attributes:
        league: League slug (``"ufl"``, ``"xfl"``, ``"cfl"``, or the internal
            ``"nfl_parity"`` test league).
        downs: Downs per set (4 for ufl/xfl, 3 for cfl).
        field_length: Field length in yards (100 for ufl/xfl, 110 for cfl).
        touchback_yardline: Kickoff-touchback yards-to-endzone. Documented
            rule metadata; see the module downscope note -- not yet wired
            into scoring.
        pat_kick: Whether the league kicks a traditional PAT (False for
            ufl/xfl -- they run 1/2/3-pt conversions instead).
        conversion_point_values: ``{points: success_rate}`` for the
            conversion-distance choices. Seeded placeholder rates, unused
            in scoring; refresh when conversion modeling lands.
        ep_point_values: 7-class EP point-value vector consumed by
            ``calculate_expected_points`` to collapse class probabilities
            into a scalar ``ep``. Currently the NFL vector verbatim (see
            downscope note).
        kickoff_spot: Standard kickoff line of scrimmage (yards from own
            goal line). Documented metadata only.
        model_dir: Import path of the league's model-bundle package.
            Reserved metadata -- nothing passes it today (the port calls
            ``enrich_nfl_pbp`` with its default NFL bundle); the cfl entry
            names a package that does not exist yet.
        conversion_spots: ``{points: yards_from_goal}`` line of scrimmage for
            each conversion value (a kicked PAT is the snap spot, not the kick
            distance). Documented metadata only.
        touchback_yardline_landing_zone: Yards-to-endzone after a kickoff that
            bounces in the landing zone before the end zone (UFL 2025+ has two
            touchback spots); ``None`` when the league has one spot.
    """

    league: str
    downs: int
    field_length: int
    touchback_yardline: int
    pat_kick: bool
    conversion_point_values: dict[int, float]
    ep_point_values: np.ndarray
    kickoff_spot: int
    model_dir: str
    conversion_spots: "dict[int, int] | None" = None
    touchback_yardline_landing_zone: "int | None" = None


SPRING_FOOTBALL_CONSTANTS: dict[str, SpringFootballConstants] = {
    "ufl": SpringFootballConstants(
        league="ufl",
        downs=4,
        field_length=100,
        touchback_yardline=60,
        pat_kick=True,
        conversion_point_values={1: 0.80, 2: 0.50, 3: 0.30},
        ep_point_values=_NFL_EP_POINT_VALUES.copy(),
        kickoff_spot=30,
        model_dir="sportsdataverse.nfl.models",
        conversion_spots={1: 15, 2: 2, 3: 8},
        touchback_yardline_landing_zone=80,
    ),
    "xfl": SpringFootballConstants(
        league="xfl",
        downs=4,
        field_length=100,
        touchback_yardline=65,
        pat_kick=False,
        conversion_point_values={1: 0.80, 2: 0.50, 3: 0.30},
        ep_point_values=_NFL_EP_POINT_VALUES.copy(),
        kickoff_spot=30,
        model_dir="sportsdataverse.nfl.models",
        conversion_spots={1: 2, 2: 5, 3: 10},
    ),
    "cfl": SpringFootballConstants(
        league="cfl",
        downs=3,
        field_length=110,
        touchback_yardline=75,
        pat_kick=True,
        conversion_point_values={1: 1.0, 2: 1.0},
        ep_point_values=_NFL_EP_POINT_VALUES.copy(),
        kickoff_spot=35,
        model_dir="sportsdataverse.football.models",
        conversion_spots={1: 25, 2: 3},
    ),
    # Mirrors the NFL's real values exactly. Used only by the NFL-parity gate
    # test to prove `enrich_spring_football_pbp` doesn't diverge from
    # `enrich_nfl_pbp` -- not a real spring-football league.
    "nfl_parity": SpringFootballConstants(
        league="nfl_parity",
        downs=4,
        field_length=100,
        touchback_yardline=75,
        pat_kick=True,
        conversion_point_values={1: 1.0, 2: 1.0},
        ep_point_values=_NFL_EP_POINT_VALUES.copy(),
        kickoff_spot=35,
        model_dir="sportsdataverse.nfl.models",
        conversion_spots={1: 15, 2: 2},
    ),
}

# The base entries above describe the CURRENT season. Earlier seasons differ;
# get_sf_constants(league, season=) applies the latest override at or before
# the season. Keyed by the first season each ruleset applied to. Sources:
# sdv-internal-refs rules/ufl.yaml (ufl-2024-inaugural-ruleset,
# ufl-2025-kickoff-from-30-two-touchback-spots, ufl-2026-kick-pat-four-point-fg),
# rules/xfl.yaml (xfl-2020-inaugural-ruleset, xfl-2023-relaunch-ruleset),
# rules/cfl.yaml (cfl-2015-convert-distances, cfl-2027-field-shortened-to-100-yards).
SPRING_FOOTBALL_SEASON_OVERRIDES: dict[str, dict[int, dict[str, Any]]] = {
    "ufl": {
        # kickoffs from the 20, every touchback at the 25, no kicked PAT, 1/2/3 from the 2/5/10
        2024: {
            "kickoff_spot": 20,
            "touchback_yardline": 75,
            "touchback_yardline_landing_zone": None,
            "pat_kick": False,
            "conversion_spots": {1: 2, 2: 5, 3: 10},
        },
        # kickoffs from the 30; in-the-air touchback at the 35, landing-zone bounce at the 20
        2025: {
            "kickoff_spot": 30,
            "touchback_yardline": 65,
            "touchback_yardline_landing_zone": 80,
            "pat_kick": False,
            "conversion_spots": {1: 2, 2: 5, 3: 10},
        },
        # 33-yard one-point kick returns (snap at the 15), 3-pt try from the 8, 4-point FG from
        # 60+ yards (no class in the NFL 7-class EP vector), in-the-air touchback at the 40
        2026: {
            "kickoff_spot": 30,
            "touchback_yardline": 60,
            "touchback_yardline_landing_zone": 80,
            "pat_kick": True,
            "conversion_spots": {1: 15, 2: 2, 3: 8},
        },
    },
    "xfl": {
        2020: {
            "kickoff_spot": 30,
            "touchback_yardline": 65,
            "pat_kick": False,
            "conversion_spots": {1: 2, 2: 5, 3: 10},
        },
        2023: {
            "kickoff_spot": 30,
            "touchback_yardline": 65,
            "pat_kick": False,
            "conversion_spots": {1: 2, 2: 5, 3: 10},
        },
    },
    "cfl": {
        # converts from the 5 (12-yard kick) and the 5; 110-yard field
        1986: {"conversion_spots": {1: 5, 2: 5}, "field_length": 110},
        # one-point kick from the 25 (32-yard kick), two-point try from the 3
        2015: {"conversion_spots": {1: 25, 2: 3}, "field_length": 110},
        # field shortened to 100 yards (announced 2025-09-22 for 2027)
        2027: {"conversion_spots": {1: 25, 2: 3}, "field_length": 100},
    },
}


def get_sf_constants(league: str, season: "int | None" = None) -> SpringFootballConstants:
    """Look up the :class:`SpringFootballConstants` for a spring-football league.

    Args:
        league: One of ``"ufl"``, ``"xfl"``, ``"cfl"``, or the internal
            ``"nfl_parity"`` test league.
        season: Calendar year of the season. ``None`` returns the current
            ruleset; otherwise the latest :data:`SPRING_FOOTBALL_SEASON_OVERRIDES`
            entry at or before ``season`` is applied (a season before the
            first override gets the first one).

    Returns:
        The matching :class:`SpringFootballConstants`.

    Raises:
        ValueError: ``league`` is not a recognized spring-football league
            (e.g. ``"nfl"`` -- use :mod:`sportsdataverse.nfl.ep_wp` directly).

    Example:
        Quick start::

            from sportsdataverse.football.spring_football_constants import get_sf_constants

            c = get_sf_constants("ufl")
            print(c.downs, c.pat_kick)
    """
    try:
        base = SPRING_FOOTBALL_CONSTANTS[league]
    except KeyError:
        raise ValueError(
            f"Unknown spring-football league {league!r}. Expected one of {sorted(SPRING_FOOTBALL_CONSTANTS)}."
        ) from None
    overrides = SPRING_FOOTBALL_SEASON_OVERRIDES.get(league)
    if season is None or not overrides:
        return base
    starts = sorted(overrides)
    applicable = [yr for yr in starts if yr <= season]
    return replace(base, **overrides[applicable[-1] if applicable else starts[0]])
