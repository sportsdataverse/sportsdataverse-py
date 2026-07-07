"""NCAA possession-core data models (cbb-explorer port).

Faithful port of hoop-explorer's ``cbb-explorer`` (Scala 2.12, ``utest``,
package ``org.piggottfamily.cbb_explorer``) data-model layer -- the first of
four Phase-5a modules (``mbb_ncaa_models.py`` / ``mbb_ncaa_events.py`` /
``mbb_ncaa_possessions.py`` / wbb shims). **Task 5a.1 ports the models**
consumed by the PBP-line extractors (5a.2) and the possession calculator
(5a.3): the identity/value types (:class:`TeamId`, :class:`PlayerId`,
:class:`Year`, :class:`TeamSeasonId`, :class:`Score`, :class:`LocationType`),
:class:`~LineupEvent.RawGameEvent` (``LineupEvent.scala:65-116``) and its
:class:`Direction` / :class:`PossessionEvent` possession-accessor companions,
:class:`ScoreInfo`, :class:`LineupId`, :class:`PlayerCodeId`,
:class:`LineupEventStats` (``LineupEventStats.scala``, all nested types), the
full :class:`LineupEvent` record, and :class:`PossCalcFragment`
(``PossessionUtils.scala:124-154``) with :func:`poss_calc_fragment_sum` and
:func:`score_to_tuple` (``ExtractorUtils.scala:107-113``).

**Only ``num_events``/``num_possessions``/``pts``/``plus_minus`` on
``LineupEventStats`` are exercised by this task** -- the shot/rebound/assist/
foul sub-stat tree is ported now (full shape, exact defaults) because 5a.3's
``calculate_stats`` builds a :class:`PossCalcFragment`, not a
``LineupEventStats``; the sub-stat tree is filled by a later phase (5c per
the roadmap). Porting the full shape now avoids a second migration.

**Scala idiom decisions (documented per project convention):**

* **``RawGameEvent.team`` / ``.opponent`` companion factories are renamed
  ``RawGameEvent.for_team`` / ``.for_opponent``.** The Scala companion
  object defines factory methods with the *same* names as the case class's
  own ``team`` / ``opponent`` fields (``LineupEvent.scala:107-110``) --
  legal in Scala because methods and case-class fields live in disjoint
  namespaces there. In Python a ``@dataclass``, a class-body
  ``team: Optional[str] = None`` annotation *is* the class attribute that
  supplies ``__init__``'s default; defining a same-named ``@classmethod``
  afterward in the same class body silently overwrites that class attribute
  with the classmethod object before the ``@dataclass`` decorator reads it,
  corrupting every future default-constructed instance's ``.team`` (verified
  with a throwaway repro: a fresh ``Foo()`` returns the bound classmethod,
  not ``None``). Monkey-patching the classmethod onto the class *after*
  decoration avoids the corruption at runtime but statically confuses mypy
  (``Foo.team`` is typed as the field's ``Optional[str]``, so the call site
  ``Foo.team(s, min)`` fails as "not callable" -- would need a type-ignore
  pragma at every call site, cascading into 5a.2/5a.3). Renaming is the
  lazy, fully-typed fix; behavior is identical to the Scala factories.
* **Scala's ``val empty`` singletons (``ScoreInfo.empty``,
  ``LineupEventStats.empty``) become Python ``@classmethod`` factories that
  build a fresh instance per call**, not a shared module-level constant.
  Scala case classes are immutable, so sharing one ``val`` instance is safe;
  this port's dataclasses are deliberately *mutable* (the possession
  calculator mutates ``num_possessions`` / etc. in place downstream), so a
  shared singleton default would be a classic aliased-mutable-default bug
  the first time two callers mutate "their own" empty stats object.
  ``LineupId.unknown`` stays a plain module-level constant because
  ``LineupId`` itself is frozen (an ``AnyVal`` wrapper) and never mutated.
* **``PlayerId`` / ``TeamId`` / ``Year`` / ``LineupId`` are frozen
  (hashable) dataclasses**, mirroring Scala's ``extends AnyVal`` immutable
  value-class semantics -- they are identifiers, plausibly used as dict/set
  keys by downstream phases. ``TeamSeasonId``, ``Score``, ``ScoreInfo``,
  ``PlayerCodeId``, ``PossCalcFragment``, ``LineupEventStats``, and
  ``LineupEvent`` are plain (mutable) dataclasses, matching the Scala case
  classes' field shape without over-constraining Python mutability.
* **``Game.Score.by_winner`` / ``.by_location`` are NOT ported.** Both
  operate on a full upstream ``Game`` case class (``won``, ``score``,
  ``location_type``, plus ``pace``/``rank``/``opp_rank``/``tier``,
  ``Game.scala:10-19``) that is out of scope for 5a -- neither
  ``LineupEvent`` nor ``PossessionUtils`` reference ``Game`` itself, only
  ``Game.Score`` (renamed :class:`Score` here) and ``Game.LocationType``
  (renamed :class:`LocationType`). Without a ``Game``-shaped argument
  these two functions have nothing to operate on; if a later phase ports
  ``Game`` this note flags where to add them back.
* **``Year.until`` and the ``Year`` companion's ``y2002_``.. named-constant
  table (``Year.scala:11-45``) are NOT ported** -- unused by
  ``LineupEvent``/``PossessionUtils``; only the bare ``Year(value)`` wrapper
  is needed for 5a.

**License / provenance (Apache License, Version 2.0).** This module is a
derivative work of ``LineupEvent.scala``, ``LineupEventStats.scala``,
``Game.scala``, ``TeamSeasonId.scala``, ``PlayerId.scala``, ``TeamId.scala``,
``Year.scala``, and (for :class:`PossCalcFragment` /
:func:`poss_calc_fragment_sum` / :func:`score_to_tuple`)
``PossessionUtils.scala`` / ``ExtractorUtils.scala`` from
`Alex-At-Home/cbb-explorer <https://github.com/Alex-At-Home/cbb-explorer>`_
(package ``org.piggottfamily.cbb_explorer``), which is licensed under the
Apache License, Version 2.0 (the upstream repo's ``LICENSE`` file; full text
at `<http://www.apache.org/licenses/LICENSE-2.0>`_). Per Apache-2.0 Section
4's redistribution-of-derivative-works obligations, sportsdataverse-py
(itself MIT-licensed) retains the upstream copyright notice for this
derivative::

    Copyright (c) Alex-At-Home (https://github.com/Alex-At-Home) and
    contributors. Licensed under the Apache License, Version 2.0.

See ``THIRD_PARTY_NOTICES.md`` at the repository root for the full
third-party attribution entry (upstream URL, license, and exactly what was
derived) -- Task 5a.4 adds cbb-explorer's first entry there (distinct from
the existing ``cbb-on-off-analyzer`` (TypeScript) entry that backs
``mbb_lineup_stats.py`` / ``mbb_ratings.py`` / ``mbb_luck.py`` /
``mbb_rapm.py`` / ``mbb_positions.py``).

**Landmine index (reachable scalar division).** None. Every computation in
this module's scope is integer addition/subtraction (:attr:`PossCalcFragment
.total_poss`, :func:`poss_calc_fragment_sum`) or pure string
split/regex-match (:class:`RawGameEvent` accessors, :func:`score_to_tuple`).
No division site exists to enumerate.

Example::

    from sportsdataverse.mbb.mbb_ncaa_models import PossCalcFragment

    frag = PossCalcFragment(1, 2, 3, 4, 5, 6, 7, 8)
    frag.total_poss  # 2
    frag.summary
    # 'total=[2] = shots=[1] - (orbs=[2] + db_orbs=[3]) + (ft_sets=[4] - techs=[6]) + to=[8] { +1s=[5] offset_techs=[7] }'

See Also:
    * `cbb-on-off-analyzer <https://github.com/Alex-At-Home/cbb-on-off-analyzer>`_ -- the TypeScript sibling this Scala core feeds
    * `hoopR <https://hoopR.sportsdataverse.org>`_ -- R men's basketball companion package
"""

from __future__ import annotations

import re
from dataclasses import astuple, dataclass, field
from datetime import datetime
from enum import Enum
from typing import ClassVar, Optional

__all__ = [
    "LocationType",
    "Score",
    "TeamId",
    "PlayerId",
    "Year",
    "TeamSeasonId",
    "Direction",
    "RawGameEvent",
    "PossessionEvent",
    "ScoreInfo",
    "LineupId",
    "PlayerCodeId",
    "ShotClockStats",
    "FieldGoalStats",
    "AssistEvent",
    "AssistInfo",
    "PlayerShotInfo",
    "LineupEventStats",
    "LineupEvent",
    "PossCalcFragment",
    "poss_calc_fragment_sum",
    "score_to_tuple",
    "PlayerEvent",
    "RosterEntry",
    "ConferenceId",
    "ShotLocation",
    "ShotGeo",
    "ShotEvent",
    "CutdownShotEvent",
]


class LocationType(Enum):
    """Game location (``Game.LocationType``, ``Game.scala:36-38``)."""

    HOME = "Home"
    AWAY = "Away"
    NEUTRAL = "Neutral"
    SEMI_HOME = "SemiHome"
    SEMI_AWAY = "SemiAway"


@dataclass
class Score:
    """Points scored / allowed (``Game.Score``, ``Game.scala:22``).

    Args:
        scored: Points scored by the team under analysis.
        allowed: Points scored by the opponent.
    """

    scored: int
    allowed: int


@dataclass(frozen=True)
class TeamId:
    """CBB team identifier (``TeamId``, ``TeamId.scala``, ``AnyVal``).

    Args:
        name: The unique team name.
    """

    name: str


@dataclass(frozen=True)
class PlayerId:
    """CBB player identifier (``PlayerId``, ``PlayerId.scala``, ``AnyVal``).

    Args:
        name: The unique player name.
    """

    name: str


@dataclass(frozen=True)
class Year:
    """CBB season, named by the year it ends (``Year``, ``Year.scala``).

    Args:
        value: The ending year of the season.
    """

    value: int


@dataclass
class TeamSeasonId:
    """A team's season identifier (``TeamSeasonId``, ``TeamSeasonId.scala``).

    Args:
        team: The team playing the season.
        year: The year the season ends.
    """

    team: TeamId
    year: Year


class Direction(Enum):
    """Which team is in possession (``RawGameEvent.Direction``, ``:119-121``)."""

    INIT = "Init"
    TEAM = "Team"
    OPPONENT = "Opponent"


@dataclass
class RawGameEvent:
    """A single NCAA play-by-play event line (``LineupEvent.RawGameEvent``,
    ``LineupEvent.scala:65-105``).

    Exactly one of ``team`` / ``opponent`` is populated per event -- the raw
    string is the literal ``"date,time,event"`` line from the NCAA website.

    Args:
        min: The game-clock minute (fractional) this event occurred at.
        team: The raw event string, if this event belongs to the team under
            analysis.
        opponent: The raw event string, if this event belongs to the
            opponent.
    """

    min: float
    team: Optional[str] = None
    opponent: Optional[str] = None

    @property
    def get_info(self) -> Optional[str]:
        """Event string from whichever of ``team``/``opponent`` is set."""
        return self.team if self.team is not None else self.opponent

    @property
    def info(self) -> str:
        """:attr:`get_info`, defaulting to ``""``."""
        info = self.get_info
        return info if info is not None else ""

    @property
    def show_dir(self) -> str:
        """Display arrow: ``">"`` for team events, ``"<"`` for opponent."""
        return ">" if self.team is not None else "<"

    @property
    def get_date_str(self) -> Optional[str]:
        """The leading ``date`` segment of :attr:`get_info`, comma-split."""
        info = self.get_info
        return info.split(",")[0] if info is not None else None

    @property
    def date_str(self) -> str:
        """:attr:`get_date_str`, defaulting to ``""``."""
        date_str = self.get_date_str
        return date_str if date_str is not None else ""

    @property
    def get_score_str(self) -> Optional[str]:
        """The second (``score``) comma-segment of :attr:`get_info`, if present."""
        info = self.get_info
        if info is None:
            return None
        parts = info.split(",")
        return parts[1] if len(parts) > 1 else None

    @property
    def score_str(self) -> str:
        """:attr:`get_score_str`, defaulting to ``"0-0"``."""
        score_str = self.get_score_str
        return score_str if score_str is not None else "0-0"

    @classmethod
    def for_team(cls, s: str, min: float) -> "RawGameEvent":
        """Build a team-side event (Scala ``RawGameEvent.team(s, min)``,
        ``LineupEvent.scala:107-108`` -- renamed per the "Scala idiom
        decisions" module note to avoid colliding with the ``team`` field).
        """
        return cls(min=min, team=s, opponent=None)

    @classmethod
    def for_opponent(cls, s: str, min: float) -> "RawGameEvent":
        """Build an opponent-side event (Scala ``RawGameEvent.opponent(s,
        min)``, ``LineupEvent.scala:109-110`` -- renamed per the "Scala
        idiom decisions" module note to avoid colliding with the
        ``opponent`` field).
        """
        return cls(min=min, team=None, opponent=s)


@dataclass(frozen=True)
class PossessionEvent:
    """Decomposes :class:`RawGameEvent`\\ s into attacking/defending sides
    (``RawGameEvent.PossessionEvent``, ``LineupEvent.scala:126-149``).

    Args:
        dir: Which team (``Direction.TEAM`` / ``Direction.OPPONENT``) is
            currently in possession.
    """

    dir: Direction

    def attacking_team(self, ev: RawGameEvent) -> Optional[str]:
        """The event string for the team in possession, or ``None``.

        Args:
            ev: The raw game event to inspect.

        Returns:
            ``ev.team`` if :attr:`dir` is ``Direction.TEAM``, ``ev.opponent``
            if ``Direction.OPPONENT``, else ``None``.
        """
        if self.dir == Direction.TEAM:
            return ev.team
        if self.dir == Direction.OPPONENT:
            return ev.opponent
        return None

    def defending_team(self, ev: RawGameEvent) -> Optional[str]:
        """The event string for the team NOT in possession, or ``None``.

        Args:
            ev: The raw game event to inspect.

        Returns:
            ``ev.team`` if :attr:`dir` is ``Direction.OPPONENT``,
            ``ev.opponent`` if ``Direction.TEAM``, else ``None``.
        """
        if self.dir == Direction.OPPONENT:
            return ev.team
        if self.dir == Direction.TEAM:
            return ev.opponent
        return None


@dataclass
class ScoreInfo:
    """Score context at the start/end of a lineup event
    (``LineupEvent.ScoreInfo``, ``LineupEvent.scala:153-158``).

    Args:
        start: Score at the start of the event.
        end: Score at the end of the event.
        start_diff: Score differential (team - opponent) at the start.
        end_diff: Score differential (team - opponent) at the end.
    """

    start: Score
    end: Score
    start_diff: int
    end_diff: int

    @classmethod
    def empty(cls) -> "ScoreInfo":
        """A fresh zeroed :class:`ScoreInfo` (``ScoreInfo.empty``, ``:161-166``)."""
        return cls(Score(0, 0), Score(0, 0), 0, 0)


@dataclass(frozen=True)
class LineupId:
    """The set of players on the floor, as an opaque id string
    (``LineupEvent.LineupId``, ``LineupEvent.scala:172``, ``AnyVal``).

    Args:
        value: The opaque lineup identifier.
    """

    value: str

    #: Placeholder lineup id used before lineup ids are calculated (``:179``).
    #: Declared as a ``ClassVar`` so cross-module use sites type-check under
    #: the whole-ratchet mypy run; assigned after the class body (a frozen
    #: dataclass cannot self-reference during class creation).
    unknown: ClassVar["LineupId"]


LineupId.unknown = LineupId("")


@dataclass
class PlayerCodeId:
    """A player's within-team-season code paired with their full identity
    (``LineupEvent.PlayerCodeId``, ``LineupEvent.scala:185-189``).

    Args:
        code: The player code, unique within the team/season only.
        id: The player's globally-unique identity.
        ncaa_id: The player's NCAA-issued id, if known.
    """

    code: str
    id: PlayerId
    ncaa_id: Optional[str] = None


@dataclass
class ShotClockStats:
    """Counting stats broken down by shot-clock segment
    (``LineupEventStats.ShotClockStats``, ``LineupEventStats.scala:51-57``).

    Args:
        total: Count across the entire shot clock.
        early: Count in the first 10s, if tracked.
        mid: Count in the middle 10s, if tracked.
        late: Count in the last 10s, if tracked.
        orb: Count in the first 10s following an offensive rebound, if
            tracked (else folded into ``mid``/``late`` as normal).
    """

    total: int = 0
    early: Optional[int] = None
    mid: Optional[int] = None
    late: Optional[int] = None
    orb: Optional[int] = None


@dataclass
class FieldGoalStats:
    """Field-goal counting stats (``LineupEventStats.FieldGoalStats``,
    ``LineupEventStats.scala:75-79``).

    Args:
        attempts: Shot attempts, successful or not.
        made: Successful shot attempts.
        ast: Successful shot attempts that were assisted, if tracked.
    """

    attempts: ShotClockStats = field(default_factory=ShotClockStats)
    made: ShotClockStats = field(default_factory=ShotClockStats)
    ast: Optional[ShotClockStats] = None


@dataclass
class AssistEvent:
    """One assist relationship's counts (``LineupEventStats.AssistEvent``,
    ``LineupEventStats.scala:64-67``).

    Args:
        player_code: The other player in the assist event (by code).
        count: The assist counts, by shot-clock segment.
    """

    player_code: str
    count: ShotClockStats = field(default_factory=ShotClockStats)


@dataclass
class AssistInfo:
    """Detailed assist info, split into given/received
    (``LineupEventStats.AssistInfo``, ``LineupEventStats.scala:87-91``).

    Args:
        counts: Raw assist statistics.
        target: Players "I" assisted, if tracked.
        source: Players who assisted "me", if tracked.
    """

    counts: ShotClockStats = field(default_factory=ShotClockStats)
    target: Optional[list[AssistEvent]] = None
    source: Optional[list[AssistEvent]] = None


@dataclass
class PlayerShotInfo:
    """Per-player shot-quality info, keyed by lineup slot
    (``LineupEventStats.PlayerShotInfo``, ``LineupEventStats.scala:98-103``).
    Each tuple is a fixed-arity 5-slot (one per lineup spot), mirroring the
    Scala ``PlayerTuple[Int] = Tuple5[Int, Int, Int, Int, Int]`` alias.

    Args:
        unknown_3pm: 3pt makes of unknown assist status, per slot.
        early_3pa: Early-shot-clock 3pt attempts, per slot.
        unast_3pm: Unassisted 3pt makes, per slot.
        ast_3pm: Assisted 3pt makes, per slot.
    """

    unknown_3pm: Optional[tuple[int, int, int, int, int]] = None
    early_3pa: Optional[tuple[int, int, int, int, int]] = None
    unast_3pm: Optional[tuple[int, int, int, int, int]] = None
    ast_3pm: Optional[tuple[int, int, int, int, int]] = None


@dataclass
class LineupEventStats:
    """A lineup event's full counting-stat tree (``LineupEventStats``,
    ``LineupEventStats.scala:7-38``).

    Only ``num_events``/``num_possessions``/``pts``/``plus_minus`` are
    exercised by Phase 5a -- see the module docstring's scope note.

    Args:
        num_events: Number of raw events folded into this lineup event.
        num_possessions: Number of possessions attributed to this lineup
            event.
        fg: Overall field-goal stats.
        fg_rim: Rim field-goal stats.
        fg_mid: Mid-range field-goal stats.
        fg_2p: 2pt field-goal stats.
        fg_3p: 3pt field-goal stats.
        ft: Free-throw stats.
        orb: Offensive-rebound stats, if tracked.
        drb: Defensive-rebound stats, if tracked.
        to: Turnover stats.
        stl: Steal stats, if tracked.
        blk: Block stats, if tracked.
        assist: Assist stats, if tracked.
        ast_rim: Rim-shot assist info, if tracked.
        ast_mid: Mid-range-shot assist info, if tracked.
        ast_3p: 3pt-shot assist info, if tracked.
        foul: Foul stats, if tracked.
        player_shot_info: Per-player shot-quality info, if tracked.
        pts: Points scored.
        plus_minus: Point differential while this lineup was on the floor.
    """

    num_events: int = 0
    num_possessions: int = 0

    fg: FieldGoalStats = field(default_factory=FieldGoalStats)
    fg_rim: FieldGoalStats = field(default_factory=FieldGoalStats)
    fg_mid: FieldGoalStats = field(default_factory=FieldGoalStats)
    fg_2p: FieldGoalStats = field(default_factory=FieldGoalStats)
    fg_3p: FieldGoalStats = field(default_factory=FieldGoalStats)
    ft: FieldGoalStats = field(default_factory=FieldGoalStats)

    orb: Optional[ShotClockStats] = None
    drb: Optional[ShotClockStats] = None

    to: ShotClockStats = field(default_factory=ShotClockStats)
    stl: Optional[ShotClockStats] = None
    blk: Optional[ShotClockStats] = None

    assist: Optional[ShotClockStats] = None
    ast_rim: Optional[AssistInfo] = None
    ast_mid: Optional[AssistInfo] = None
    ast_3p: Optional[AssistInfo] = None

    foul: Optional[ShotClockStats] = None

    player_shot_info: Optional[PlayerShotInfo] = None

    pts: int = 0
    plus_minus: int = 0

    @classmethod
    def empty(cls) -> "LineupEventStats":
        """A fresh all-defaults :class:`LineupEventStats` (``:41``)."""
        return cls()


@dataclass
class LineupEvent:
    """A portion of a game during which a given lineup was on the floor
    (``LineupEvent``, ``LineupEvent.scala:41-58``).

    Args:
        date: The date of the game.
        location_type: Home/away/neutral (etc.) for this game.
        start_min: The point in the game at which the lineup entered.
        end_min: The point in the game at which the lineup changed.
        duration_mins: The duration of the lineup.
        score_info: The score differential context for this event.
        team: The team under analysis.
        opponent: The opposing team.
        lineup_id: A string that defines the set of players on the floor.
        players: Mapping from player code to full identity, for this lineup.
        players_in: Players who subbed in for this event.
        players_out: Players who subbed out for this event.
        raw_game_events: The raw NCAA event strings for both teams.
        team_stats: Numerical stats extracted for the lineup (team side).
        opponent_stats: Numerical stats extracted for the lineup (opponent
            side).
        player_count_error: If the lineup is "impossible", the number of
            players actually seen (for analysis purposes).
    """

    date: datetime
    location_type: LocationType
    start_min: float
    end_min: float
    duration_mins: float
    score_info: ScoreInfo
    team: TeamSeasonId
    opponent: TeamSeasonId
    lineup_id: LineupId
    players: list[PlayerCodeId]
    players_in: list[PlayerCodeId]
    players_out: list[PlayerCodeId]
    raw_game_events: list[RawGameEvent]
    team_stats: LineupEventStats
    opponent_stats: LineupEventStats
    player_count_error: Optional[int] = None


@dataclass
class PossCalcFragment:
    """Running stats needed to calculate possessions for one lineup event,
    one direction at a time (``PossessionUtils.PossCalcFragment``,
    ``PossessionUtils.scala:124-144``).

    Args:
        shots_made_or_missed: Count of shot attempts (made or missed).
        liveball_orbs: Count of live-ball offensive rebounds.
        actual_deadball_orbs: Count of dead-ball offensive rebounds.
        ft_events: Count of free-throw *sets* (capped-at-1 flag per set).
        ignored_and_ones: Count of and-one free throws ignored for
            possession purposes (capped-at-1 flag).
        bad_fouls: Count of technical/flagrant fouls counted against the
            defending side (capped-at-1 flag).
        offsetting_bad_fouls: Count of technical/flagrant fouls that offset
            (net zero) rather than counting against either side (capped-at-1
            flag).
        turnovers: Count of turnovers.
    """

    shots_made_or_missed: int = 0
    liveball_orbs: int = 0
    actual_deadball_orbs: int = 0
    ft_events: int = 0
    ignored_and_ones: int = 0
    bad_fouls: int = 0
    offsetting_bad_fouls: int = 0
    turnovers: int = 0

    @property
    def total_poss(self) -> int:
        """Estimated possessions for this fragment (``:134-137``)."""
        return (
            self.shots_made_or_missed
            - (self.liveball_orbs + self.actual_deadball_orbs)
            + (self.ft_events - self.bad_fouls)
            + self.turnovers
        )

    @property
    def summary(self) -> str:
        """Debug string breaking down :attr:`total_poss`'s components
        (``:138-143``, exact format -- oracle-pinned)."""
        return (
            f"total=[{self.total_poss}] = "
            f"shots=[{self.shots_made_or_missed}] - (orbs=[{self.liveball_orbs}] + "
            f"db_orbs=[{self.actual_deadball_orbs}]) + "
            f"(ft_sets=[{self.ft_events}] - techs=[{self.bad_fouls}]) + to=[{self.turnovers}]"
            f" {{ +1s=[{self.ignored_and_ones}] offset_techs=[{self.offsetting_bad_fouls}] }}"
        )


def poss_calc_fragment_sum(a: PossCalcFragment, b: PossCalcFragment) -> PossCalcFragment:
    """Field-wise add two :class:`PossCalcFragment`\\ s
    (``PossCalcFragment.sum``, ``PossessionUtils.scala:146-153``).

    The Scala original uses ``shapeless.Generic`` to zip the two case
    classes' fields and sum pairwise; since every field is a plain ``Int``,
    a plain :func:`zip` over :func:`dataclasses.astuple` reproduces the same
    behavior without the generic-programming machinery.

    Args:
        a: The left-hand fragment.
        b: The right-hand fragment.

    Returns:
        A new :class:`PossCalcFragment` with each field summed.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_models import (
                PossCalcFragment,
                poss_calc_fragment_sum,
            )

            frag1 = PossCalcFragment(1, 2, 3, 4, 5, 6, 7, 8)
            frag2 = PossCalcFragment(1, 3, 5, 7, 9, 11, 13, 15)
            poss_calc_fragment_sum(frag1, frag2)
            # PossCalcFragment(2, 5, 8, 11, 14, 17, 20, 23)
    """
    return PossCalcFragment(*(x + y for x, y in zip(astuple(a), astuple(b))))


_SCORE_PATTERN = re.compile(r"([0-9]+)-([0-9]+)")


def score_to_tuple(s: str) -> tuple[int, int]:
    """Parse a ``"scored-allowed"`` score string (``ExtractorUtils.score_to_tuple``,
    ``ExtractorUtils.scala:107-113``).

    Scala's ``str match { case regex(s1, s2) => ... }`` on a compiled
    ``Regex`` requires the ENTIRE string to match (``Regex.unapplySeq`` calls
    ``Matcher.matches()``, not ``find()``) -- ported here as
    :func:`re.fullmatch`, not :func:`re.match`/:func:`re.search`.

    Args:
        s: The raw score string, e.g. ``"55-68"``.

    Returns:
        ``(scored, allowed)`` as a tuple of ints, or ``(0, 0)`` if ``s``
        doesn't fully match ``([0-9]+)-([0-9]+)``.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_models import score_to_tuple

            score_to_tuple("55-68")   # (55, 68)
            score_to_tuple("garbage")  # (0, 0)
    """
    m = _SCORE_PATTERN.fullmatch(s)
    if m is None:
        return (0, 0)
    return (int(m.group(1)), int(m.group(2)))


@dataclass
class PlayerEvent:
    """A lineup event's stats, narrowed to one player (``PlayerEvent``,
    ``models/ncaa/PlayerEvent.scala:48-70``). **Scope addition, Task 5c.4**
    -- deferred by 5a since only :func:`~sportsdataverse.mbb
    .mbb_ncaa_lineup_enrich.create_player_events` (5c.4) returns it. Appended
    here (not inserted among the 5a-reviewed classes above) to keep this an
    additive-only change.

    Same field shape as :class:`LineupEvent` with two fields prepended
    (``player``, ``player_stats``) -- the Scala builds this via a
    ``shapeless.LabelledGeneric`` HList splice of ``PlayerEvent``'s own
    ``player``/``player_stats`` onto every field of a ``LineupEvent``
    instance; this port has no generic-programming machinery, so
    :func:`~sportsdataverse.mbb.mbb_ncaa_lineup_enrich.create_player_events`
    constructs the dataclass directly instead.

    **``SingleEventMeta`` / ``event_meta`` / ``game_id`` are NOT ported.**
    ``PlayerEvent.scala``'s companion object nests a ``SingleEventMeta`` case
    class, but the two fields that would carry it (``event_meta``,
    ``game_id``) are commented out in the Scala source itself
    (``PlayerEvent.scala:67-69``) -- never part of the live case class, and
    ``create_player_events`` never constructs a ``SingleEventMeta``. Nothing
    to defer; there is no live field to port.

    Args:
        player: The player this narrowed event describes.
        player_stats: The player's own numerical stats for this lineup event.
        date: The date of the game.
        location_type: Home/away/neutral (etc.) for this game.
        start_min: The point in the game at which the lineup entered.
        end_min: The point in the game at which the lineup changed.
        duration_mins: The duration of the lineup.
        score_info: The score differential context for this event.
        team: The team under analysis.
        opponent: The opposing team.
        lineup_id: A string that defines the set of players on the floor.
        players: Mapping from player code to full identity, for this lineup.
        players_in: Players who subbed in for this event.
        players_out: Players who subbed out for this event.
        raw_game_events: The raw NCAA event strings for both teams.
        team_stats: Numerical stats extracted for the lineup (team side).
        opponent_stats: Numerical stats extracted for the lineup (opponent
            side).
        player_count_error: If the lineup is "impossible", the number of
            players actually seen (for analysis purposes).
    """

    player: PlayerCodeId
    player_stats: LineupEventStats
    date: datetime
    location_type: LocationType
    start_min: float
    end_min: float
    duration_mins: float
    score_info: ScoreInfo
    team: TeamSeasonId
    opponent: TeamSeasonId
    lineup_id: LineupId
    players: list[PlayerCodeId]
    players_in: list[PlayerCodeId]
    players_out: list[PlayerCodeId]
    raw_game_events: list[RawGameEvent]
    team_stats: LineupEventStats
    opponent_stats: LineupEventStats
    player_count_error: Optional[int] = None


@dataclass
class RosterEntry:
    """An entry in an NCAA team roster (``RosterEntry``, ``models/ncaa
    /RosterEntry.scala:11-21``). **Scope addition, Task 5e.1** -- the first
    model consumed by the HTML-parser layer (``mbb_ncaa_roster_parser.py``).
    Appended here (not inserted among the 5a-reviewed classes above) to keep
    this an additive-only change, matching :class:`PlayerEvent`'s precedent.

    The trailing ``role`` field (present in the Scala case class shape but
    never populated by ``RosterParser.parse_roster`` -- every construction
    site there, both the real-player and ``__coach__`` branches, passes the
    literal ``None`` for it) is presumably set by a later phase's box-score
    parser (``BoxscoreParser``, out of this task's scope); it is carried
    here for shape fidelity even though Task 5e.1's only producer never
    populates it.

    Args:
        player_code_id: The player's code + full identity (the roster-row
            equivalent of a box-score/PbP player reference).
        number: The jersey number, as printed (may be non-numeric text).
        pos: The listed position.
        height: The listed height, in ``"FT-IN"`` text form (e.g. ``"6-3"``).
        height_in: :attr:`height` parsed to total inches, if it matched
            :data:`height_regex`.
        year_class: The listed academic year (``"Fr"``/``"So"``/``"Jr"``/
            ``"Sr"``/etc.).
        gp: Games played.
        origin: The player's hometown/prior-school text, if the source
            table has that column (v1 rosters only).
        role: Reserved for a later phase; always ``None`` from
            ``parse_roster`` (see above).
    """

    player_code_id: PlayerCodeId
    number: str
    pos: str
    height: str
    height_in: Optional[int]
    year_class: str
    gp: int
    origin: Optional[str]
    role: Optional[str]

    #: Feet-hyphen-inches height matcher (``RosterEntry.height_regex``,
    #: ``models/ncaa/RosterEntry.scala:24``).
    height_regex: ClassVar[re.Pattern[str]] = re.compile(r"([0-9]+)[-]([0-9]+)")


@dataclass(frozen=True)
class ConferenceId:
    """CBB conference identifier (``ConferenceId``, ``models/ConferenceId
    .scala:7``, ``AnyVal``). **Scope addition, Task 5e.4** -- the first model
    consumed by ``mbb_ncaa_team_parsers.py`` (``TeamIdParser.get_team_triples``
    / ``build_lineup_cli_array`` / ``build_available_team_list``). Appended
    here (not inserted among the 5a-reviewed classes above) to keep this an
    additive-only change, matching :class:`RosterEntry`'s precedent.

    **``ConferenceId.is_high_major`` (the companion object's other member,
    ``models/ConferenceId.scala:11-16``) is NOT ported.** It has no call site
    anywhere in ``TeamIdParser``/``TeamScheduleParser`` (verified: the only
    other ``ConferenceId`` construction sites in the upstream tree are
    ``kenpom/TeamParser.scala`` and ``BuildIngestPipeline.scala``, neither of
    which is in this port's scope, and neither calls ``is_high_major``
    either) -- nothing in Phase 5e would exercise it. Noted here rather than
    silently dropped, matching this module's precedent for other
    unreferenced companion-object members (see the module docstring's
    ``Year.until`` / ``Game.Score.by_winner`` notes).

    Args:
        name: The unique name of the conference.
    """

    name: str


@dataclass
class ShotLocation:
    """A shot's court-relative coordinates, in feet (``ShotEvent.ShotLocation``,
    ``models/ncaa/ShotEvent.scala:48``). **Scope addition, Task 5e.5** --
    flattened out of the Scala ``ShotEvent`` companion object per this
    module's established nested-object-flattening precedent (see the module
    docstring's "Scala idiom decisions": ``ScoreInfo``/``PlayerCodeId`` were
    already flattened out of ``LineupEvent``'s companion the same way).

    Args:
        x: Feet from the basket; positive is to the right of the basket
            (facing the goal), negative is to the left.
        y: Feet from the basket along the baseline-perpendicular axis.
    """

    x: float
    y: float


@dataclass
class ShotGeo:
    """A shot's synthetic lat/lon, for geo-aware visualization tooling
    (``ShotEvent.ShotGeo``, ``models/ncaa/ShotEvent.scala:45``). **Scope
    addition, Task 5e.5** -- flattened per :class:`ShotLocation`'s note.

    Args:
        lat: Synthetic latitude (feet-to-meters converted, offset from an
            arbitrary base point -- not a real-world location).
        lon: Synthetic longitude, same convention as :attr:`lat`.
    """

    lat: float
    lon: float


@dataclass
class ShotEvent:
    """Info about one shot taken during a game, all distances in feet
    (``ShotEvent``, ``models/ncaa/ShotEvent.scala:9-29``). **Scope addition,
    Task 5e.5** -- the model produced by
    :func:`~sportsdataverse.mbb.mbb_ncaa_shot_parser.create_shot_event_data`.

    **Fields ``mbb_ncaa_shot_parser.create_shot_event_data`` (this task)
    actually populates:** ``player`` (best-effort -- tidy-resolved + coded
    for the team under analysis, name-coded only for the opponent),
    ``date``/``location_type``/``team``/``opponent`` (copied from the
    box-score lineup), ``is_off``, ``score`` (re-oriented for home/away/
    neutral perspective), ``min`` (ascending game-clock time, after
    ``phase1_shot_event_enrichment``), ``loc``/``dist`` (transformed court
    coordinates + Euclidean distance from the basket, after the
    self-correcting flip pass), ``geo`` (synthetic lat/lon), ``raw_event``
    (the SVG ``<title>`` text, for debugging).

    **Fields left as placeholders for a LATER phase (Task 5e.6,
    ``PlayByPlayUtils``/``ShotEnrichmentUtils``):** ``lineup_id`` (always
    ``None`` here -- "discard if bad lineup" per the Scala comment, filled in
    once the shot is matched against an actual on-floor lineup), ``players``
    (always ``[]`` here -- filled in from the matched lineup), ``pts``
    (**not the real point value** -- this task only sets it to ``1``/``0``
    for made/missed, matching the Scala's own ``// (enrich in final phase)``
    comment; the real 2pt/3pt value comes from ``PlayByPlayUtils.shot_value``
    in Task 5e.6), ``value`` (always ``0`` here, same "final phase" note),
    ``ast_by``/``is_ast``/``is_trans`` (always ``None`` here -- assist/
    transition attribution needs the play-by-play cross-reference Task 5e.6
    builds).

    Args:
        player: The shooting player's code + identity, if resolved (``None``
            is never actually produced by this task's parser, but the type
            allows for it per the Scala ``Option``).
        date: The date of the game.
        location_type: Home/away/neutral (etc.) for this game.
        team: The team under analysis.
        opponent: The opposing team.
        is_off: Whether the team under analysis is the one shooting.
        lineup_id: The on-floor lineup id, if/when matched (see above).
        players: The on-floor lineup's players, if/when matched (see above).
        score: The score at the time of the shot, team-oriented.
        min: The ascending game-clock time (minutes) of the shot.
        loc: The shot's transformed court location, in feet.
        geo: The shot's synthetic lat/lon.
        dist: The shot's distance from the basket, in feet.
        pts: Made(``1``)/missed(``0``) flag from this task -- NOT the real
            point value (see above).
        value: Always ``0`` from this task (see above).
        ast_by: The assisting player, if/when matched (see above).
        is_ast: Whether the shot was assisted, if/when matched (see above).
        is_trans: Whether the shot was in transition, if/when matched (see
            above).
        raw_event: The raw SVG ``<title>`` text this shot was parsed from,
            for debugging (discarded before writing to disk upstream).
    """

    player: Optional[PlayerCodeId]
    date: datetime
    location_type: LocationType
    team: TeamSeasonId
    opponent: TeamSeasonId
    is_off: bool
    lineup_id: Optional[LineupId]
    players: list[PlayerCodeId]
    score: Score
    min: float
    loc: ShotLocation
    geo: ShotGeo
    dist: float
    pts: int
    value: int
    ast_by: Optional[PlayerCodeId]
    is_ast: Optional[bool]
    is_trans: Optional[bool]
    raw_event: Optional[str]


@dataclass
class CutdownShotEvent:
    """A narrowed :class:`ShotEvent`, keeping only the fields needed once a
    shot has been matched to a player/lineup event (``CutdownShotEvent``,
    ``models/ncaa/ShotEvent.scala:31-40``). **Scope addition, Task 5e.5** --
    ported for shape fidelity even though **it is dead code in the ENTIRE
    upstream tree**: grepping shows it appears only in its own definition
    (``ShotEvent.scala``) and as the never-populated ``shot_info:
    Option[CutdownShotEvent]`` field on ``PlayerEvent.scala`` -- it is never
    constructed anywhere. (``PlayByPlayUtils.shot_value`` is an UNRELATED
    ``event_str -> int`` point-value classifier that merely shares a similar
    name -- Task 5e.6 will NOT produce this type either.) Appended here (not
    inserted among the 5a-reviewed classes above) to keep this an
    additive-only change, matching :class:`RosterEntry`/:class:`ConferenceId`'s
    precedent.

    Args:
        loc: The shot's court location, in feet, if known.
        geo: The shot's synthetic lat/lon, if known.
        dist: The shot's distance from the basket, in feet, if known.
        pts: The point value if made (``2``/``3``), else ``0``.
        value: The shot's attempt value (``2``/``3``), regardless of make/miss.
        is_ast: Whether the shot was assisted, if known.
        is_trans: Whether the shot was in transition, if known.
        is_orb: Whether the shot followed an offensive rebound, if known.
    """

    loc: Optional[ShotLocation]
    geo: Optional[ShotGeo]
    dist: Optional[float]
    pts: int
    value: int
    is_ast: Optional[bool]
    is_trans: Optional[bool]
    is_orb: Optional[bool]
