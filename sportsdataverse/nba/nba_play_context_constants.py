"""Constants for the NBA/WNBA play-context engine (Cleaning the Glass recreation).

League-agnostic algorithms, league-specific constants: nothing in
:mod:`sportsdataverse.nba.nba_play_context` hard-codes a number from this module.

Definitions are recreated from Cleaning the Glass's own published methodology
(``sdv-internal-refs/cleaningtheglass/METHODOLOGY.md``, quoting CTG's
``/stats/guide/<slug>`` pages verbatim). Where CTG does NOT publish a threshold
(it says only "until the defense is set" / "within a few seconds of the
rebound"), we adopt the closest documented public convention and expose it as a
knob — see :data:`DEFAULT_TRANSITION_SECONDS` and :data:`DEFAULT_PUTBACK_SECONDS`.
"""

from __future__ import annotations

from typing import Final

# ---------------------------------------------------------------------------
# Shot zones (CTG / pbpstats taxonomy)
# ---------------------------------------------------------------------------

#: CTG's shot-location taxonomy. Distinct from :data:`nba_shot_zones.SHOT_ZONES`
#: (the *official NBA* zones: restricted area / paint-non-RA / mid-range / ...).
#: CTG splits the midrange at the free-throw-line distance instead:
#: rim <=4 ft, short mid 4-13.75 ft, long mid >13.75 ft (METHODOLOGY.md 5b).
CTG_SHOT_ZONES: Final[list[str]] = ["at_rim", "short_mid", "long_mid", "corner_3", "arc_3"]

#: Rim boundary in feet -- CTG: "Shots within 4 feet of the basket".
RIM_DISTANCE_FT: Final[float] = 4.0

#: Short/long midrange boundary in feet -- CTG: "outside of 4 feet, but inside of
#: ~14 feet (the free throw line distance)".
SHORT_MID_DISTANCE_FT: Final[float] = 14.0

#: Corner-three geometry in stats.nba.com legacy coordinates (tenths of a foot).
#: A three is a corner three iff ``|x_legacy| >= 220`` and ``y_legacy <= 87.5``
#: (same test the official-zone classifier uses; CTG calls these "below the break").
CORNER_THREE_ABS_X: Final[float] = 220.0
CORNER_THREE_MAX_Y: Final[float] = 87.5

# ---------------------------------------------------------------------------
# Possession start types (pbpstats taxonomy, possession.py:206-242)
# ---------------------------------------------------------------------------

#: Zone-qualified start-type stems. The full fine-grained vocabulary is the
#: cross product ``Off{Zone}{Make|Miss|Block}`` plus the unqualified members below.
START_TYPE_ZONE_STEMS: Final[dict[str, str]] = {
    "at_rim": "AtRim",
    "short_mid": "ShortMidRange",
    "long_mid": "LongMidRange",
    "corner_3": "Corner3",
    "arc_3": "Arc3",
}

#: Start types that carry no shot zone.
START_TYPE_DEADBALL: Final[str] = "OffDeadball"
START_TYPE_TIMEOUT: Final[str] = "OffTimeout"
START_TYPE_LIVE_BALL_TURNOVER: Final[str] = "OffLiveBallTurnover"
START_TYPE_FT_MAKE: Final[str] = "OffFTMake"
START_TYPE_FT_MISS: Final[str] = "OffFTMiss"

# ---------------------------------------------------------------------------
# CTG coarse start buckets (what CTG actually reports on)
# ---------------------------------------------------------------------------

#: CTG's Play-Context tables split transition by source into "Off Steals" and
#: "Off Live Rebounds"; the remaining starts roll up to made-shot / dead-ball /
#: timeout. These are the coarse buckets every CTG table groups by.
CTG_START_BUCKETS: Final[list[str]] = [
    "off_made",  # previous possession ended in a made FG or made final FT
    "off_live_rebound",  # defensive rebound of a live missed/blocked shot
    "off_steal",  # live-ball turnover (steal)
    "off_deadball",  # period start, dead-ball TO, team rebound, fallback
    "off_timeout",  # timeout at the possession boundary (wins over everything but period start)
]

# ---------------------------------------------------------------------------
# Play contexts (CTG's four)
# ---------------------------------------------------------------------------

#: CTG: "Everything that happens in the game of basketball can be divided into
#: four main contexts" -- halfcourt, transition, putback, miscellaneous.
PLAY_CONTEXTS: Final[list[str]] = ["transition", "putback", "halfcourt", "misc"]

# ---------------------------------------------------------------------------
# The four under-determined knobs (CTG does not publish these numbers)
# ---------------------------------------------------------------------------

#: Seconds from possession start within which the possession's INITIAL play must
#: occur for the possession to count as transition.
#:
#: CTG defines transition as "starting at the beginning of a possession and only
#: ending once the defense is set" -- it publishes NO seconds value, so this is a
#: calibrated knob, not a quoted constant.
#:
#: **Calibrated to 6.0s for the NBA** (fitting scan: ``dev/ctg_transition_calibration.py``).
#: Observed league-mean transition frequency on the three committed engine fixtures:
#:
#: ===========  ==========================
#: seconds      mean transition frequency
#: ===========  ==========================
#: 4.0          0.095
#: 5.0          0.126
#: **6.0**      **0.163**  <- adopted
#: 8.0          0.250
#: 10.0         0.350
#: ===========  ==========================
#:
#: Oracle band: CTG's own published Play-Context table shows transition
#: frequencies around **0.14** (e.g. Denver 14.3%), and Synergy's transition
#: play-type frequency runs **~0.15-0.16** league-wide. 6.0s lands in that band.
#:
#: NOTE: hoop-math's widely-cited **10-second** rule is a *college* convention with
#: a different denominator ("% of initial shots", 35s-shot-clock era). Applied
#: verbatim to NBA possessions it yields ~0.35 transition frequency -- ~2.4x CTG's
#: published rate -- so it is NOT the right default here. It remains selectable by
#: passing ``transition_seconds=10.0``.
DEFAULT_TRANSITION_SECONDS: Final[float] = 6.0

#: Seconds after an offensive rebound within which a shot by the SAME player
#: counts as a putback. CTG says only "within a few seconds of the rebound
#: before the defense gets set"; pbpstats (``field_goal.py:112-144``) uses 2s.
DEFAULT_PUTBACK_SECONDS: Final[float] = 2.0

#: A possession starting with <= this many seconds left in a NON-final period is
#: a "projected heave possession" and is excluded by default.
#: CTG (garbage_time guide, exact): "possessions that start with 4 or fewer
#: seconds on the game clock at the end of one of the first three quarters."
HEAVE_POSSESSION_SECONDS: Final[float] = 4.0

#: Periods for which the heave filter applies (CTG: "the first three quarters").
HEAVE_PERIODS: Final[tuple[int, ...]] = (1, 2, 3)

# ---------------------------------------------------------------------------
# Garbage time (CTG publishes this one exactly)
# ---------------------------------------------------------------------------

#: CTG (garbage_time guide, exact): "the game has to be in the 4th quarter, the
#: score differential has to be >= 25 for minutes 12-9, >= 20 for minutes 9-6,
#: and >= 10 for the remainder of the quarter."
#:
#: Encoded as ``(seconds_remaining_high, seconds_remaining_low, min_margin)``
#: half-open on the low end: the band applies when
#: ``low < seconds_remaining <= high``. Period-4 clock: 12:00 = 720s.
GARBAGE_TIME_PERIOD: Final[int] = 4
GARBAGE_TIME_BANDS: Final[tuple[tuple[float, float, int], ...]] = (
    (720.0, 540.0, 25),  # minutes 12-9
    (540.0, 360.0, 20),  # minutes 9-6
    (360.0, 0.0, 10),  # remainder of the quarter
)

#: CTG: "there have to be two or fewer starters on the floor **combined between
#: the two teams**." Applied only when starter-on-court data is supplied; see
#: :func:`~sportsdataverse.nba.nba_play_context.flag_garbage_time`.
GARBAGE_TIME_MAX_STARTERS: Final[int] = 2

# ---------------------------------------------------------------------------
# Transition variants
# ---------------------------------------------------------------------------

#: Selectable transition rules. ``hoop_math`` is the default (closest documented
#: analogue to CTG). See RECREATION.md 2c for the full comparison.
#:
#: * ``hoop_math`` -- initial play within N seconds; any non-timeout start type.
#: * ``haslametrics`` -- steal starts only, within N seconds (conservative).
#: * ``bigballr`` -- initial play within N seconds AND the previous possession
#:   ended live (a dead-ball start can never be transition).
TRANSITION_VARIANTS: Final[tuple[str, ...]] = ("hoop_math", "haslametrics", "bigballr")
DEFAULT_TRANSITION_VARIANT: Final[str] = "hoop_math"

#: Start buckets that end a possession "live" (ball in play, defense scrambling).
#: Used by the ``bigballr`` variant.
LIVE_START_BUCKETS: Final[frozenset[str]] = frozenset({"off_live_rebound", "off_steal", "off_made"})

#: Start buckets that can never be transition under ANY variant: after a timeout
#: the defense is set by construction.
NON_TRANSITION_START_BUCKETS: Final[frozenset[str]] = frozenset({"off_timeout"})

#: Event types that constitute a possession's "initial play" (the thing whose
#: timing decides transition): a shot attempt, a trip to the line, or a turnover.
#: Mirrors CTG's own definition of a *play* ("a play ends when the team attempts
#: a shot, goes to the foul line, or turns the ball over").
PLAY_ENDING_EVENT_TYPES: Final[frozenset[str]] = frozenset({"made_shot", "missed_shot", "free_throw", "turnover"})
