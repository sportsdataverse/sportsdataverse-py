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

UFL/XFL kickoff + conversion rules (both leagues share the post-2020 XFL
ruleset; UFL is the 2024 USFL+XFL merger):

* Kickoffs use the "shotgun"/spot-and-choose format; a touchback is spotted
  at the 35-yard line (``yardline_100`` = 65) rather than the NFL's 25
  (``yardline_100`` = 75, 2016+ rule).
* No PAT kick. After a touchdown the offense attempts a conversion from the
  2 (worth 1), 5 (worth 2), or 10-yard line (worth 3). The 0.80 / 0.50 /
  0.30 success rates below are SEEDED PLACEHOLDERS (unused in scoring --
  see the downscope note); refresh from observed league data when
  conversion modeling lands.

CFL differs structurally (3 downs, 110-yard field, rouge scoring) and is
handled separately (Phase 6, conditional refit-or-defer) -- its entry here
is descriptive metadata only; no CFL model/pipeline ships from this module.
"""

from __future__ import annotations

from dataclasses import dataclass

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


SPRING_FOOTBALL_CONSTANTS: dict[str, SpringFootballConstants] = {
    "ufl": SpringFootballConstants(
        league="ufl",
        downs=4,
        field_length=100,
        touchback_yardline=65,
        pat_kick=False,
        conversion_point_values={1: 0.80, 2: 0.50, 3: 0.30},
        ep_point_values=_NFL_EP_POINT_VALUES.copy(),
        kickoff_spot=30,
        model_dir="sportsdataverse.nfl.models",
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
    ),
}


def get_sf_constants(league: str) -> SpringFootballConstants:
    """Look up the :class:`SpringFootballConstants` for a spring-football league.

    Args:
        league: One of ``"ufl"``, ``"xfl"``, ``"cfl"``, or the internal
            ``"nfl_parity"`` test league.

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
        return SPRING_FOOTBALL_CONSTANTS[league]
    except KeyError:
        raise ValueError(
            f"Unknown spring-football league {league!r}. Expected one of {sorted(SPRING_FOOTBALL_CONSTANTS)}."
        ) from None
