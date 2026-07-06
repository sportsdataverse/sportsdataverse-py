"""NCAA stint-builder core: player codes + team-name parsing (men's basketball).

Faithful Python port of the player-code generator, team-name parser, the
play-by-play event ADT, and event reordering from ``ExtractorUtils.scala`` in
Alex-At-Home/cbb-explorer (the Scala NCAA play-by-play ingestion pipeline
behind hoop-explorer.com). This module is extended by Task 5b.5 with the
substitution-tracking stint builder itself (``build_partial_lineup_list``).

Ported functions/types (Scala anchors in each docstring):

* :func:`remove_diacritics` -- NFD-normalize + strip combining marks
  (``ExtractorUtils.scala:38-43``).
* :func:`build_player_code` -- turn a box-score / play-by-play player name
  into a short unique-within-team code (``ExtractorUtils.scala:290-391``).
* :func:`parse_team_name` -- match the two team-title strings against the
  target team (``ExtractorUtils.scala:240-267``).
* :data:`SUB_SAFETY_DELTA_MINS` + :class:`SubInEvent` / :class:`SubOutEvent`
  / :class:`OtherTeamEvent` / :class:`OtherOpponentEvent` /
  :class:`GameBreakEvent` / :class:`GameEndEvent` -- the ``PlayByPlayEvent``
  ADT (``ExtractorUtils.scala:822-882``, nested inside ``object Model``).
* :class:`LineupBuildingState` -- the substitution-tracking fold state
  (``ExtractorUtils.scala:735-819``, also nested inside ``object Model``).
* :func:`reorder_and_reverse` -- reorders a same-minute block of events so
  subs never enclose the plays they logically precede/follow
  (``ExtractorUtils.scala:435-599``).

**Scala idiom decision: the ``Model`` companion object is flattened.**
Scala nests the ADT + ``LineupBuildingState`` inside ``private[ExtractorUtils]
object Model``; this project already flattens every Scala
companion/nested object into plain module-level members (see
``mbb_ncaa_models.py``, ``mbb_ncaa_names.py``'s ``NameFixer`` note), so the
same is done here -- no unrequested ``Model`` class wrapper.

**``PlayByPlayEvent`` traits become ``Union`` aliases + ``isinstance``
checks, not a class hierarchy.** Scala's ``sealed trait PlayByPlayEvent``
(with sub-traits ``MiscGameEvent`` / ``SubEvent`` / ``MiscGameBreak``) lets
each case class implement a shared interface; giving every dataclass a
common base class would work too, but the Scala guards that dispatch on
these traits (``case ev: Model.MiscGameEvent if ...``) are themselves
*disjunctions over concrete case classes* (``OtherTeamEvent |
OtherOpponentEvent``, etc.) -- an ``isinstance(ev, (OtherTeamEvent,
OtherOpponentEvent))`` check against a plain ``Union`` type alias reproduces
that exactly, with no inheritance machinery to invent. ``is_team_dir`` on
``OtherTeamEvent``/``OtherOpponentEvent`` is a Scala ``def`` returning a
fixed literal (``true``/``false``), not a constructor field -- ported as a
Python ``@property`` for the same reason (so the dataclass's positional
constructor arity stays 3, matching the Scala case class).

**``LineupBuildingState.with_*`` methods return NEW instances (mirroring
Scala's ``case class.copy``), not in-place mutation.** ``LineupEvent`` /
``LineupEventStats`` elsewhere in this port are deliberately mutable
dataclasses (see the "Scala idiom decisions" note in ``mbb_ncaa_models.py``)
because a later phase accumulates stats into them in place. But
``LineupBuildingState`` is the accumulator for a Scala ``foldLeft`` -- the
Scala state machine is copy-on-write by construction, and Task 5b.5's
``build_partial_lineup_list`` fold will read as ``state =
state.with_player_in(...)`` at every step. If ``with_*`` mutated ``curr`` in
place instead, every entry appended to ``prev`` earlier in the same fold
would need its own independent ``LineupEvent`` object anyway (``prev``
accumulates *completed*, frozen-in-time lineups) -- returning a fresh
``LineupBuildingState`` (via :func:`dataclasses.replace`, with a freshly
``replace``'d ``curr``) is both the faithful port of ``copy`` and the
simpler, alias-safe choice. Every ``with_*`` method below returns a new
``LineupBuildingState``; none mutate ``self``.

Attribution: derived from `cbb-explorer
<https://github.com/Alex-At-Home/cbb-explorer>`_ (Apache License 2.0,
Copyright Alex-At-Home / org.piggottfamily). This is a source-language
translation (Scala -> Python), not a copy; upstream file:
``src/main/scala/org/piggottfamily/cbb_explorer/utils/parsers/ncaa/ExtractorUtils.scala``.
See ``THIRD_PARTY_NOTICES.md`` for the full notice.

Landmine index (reachable error sites, numbered across the module):
    1. ``build_player_code`` indexes ``fragment[0]`` inside its transform
       helpers, but every call site is guarded (``if not fragment`` returns
       ``""``; empty fragments are dropped by ``_name_filter`` before code
       assembly), so no ``IndexError`` is reachable through the public
       surface. The Scala wraps the whole body in a print-and-rethrow
       ``catch``; the Python simply lets any unexpected exception propagate.
    2. ``reorder_and_reverse``'s ``inner_sort`` reads ``ordered_block[...]``
       only through comprehensions/``sorted()``, never a bare index; no
       reachable ``IndexError``. ``LineupBuildingState.is_active`` divides
       nothing (subtraction only) -- no reachable ``ZeroDivisionError``.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Iterable, Optional, Sequence, Union

from sportsdataverse.mbb.mbb_ncaa_data_quality import (
    ParseError,
    build_sub_error,
    misspellings,
    players_with_duplicate_names,
    team_aliases,
)
from sportsdataverse.mbb.mbb_ncaa_events import (
    parse_foul_info,
    parse_free_throw_attempt,
    parse_free_throw_event,
    parse_free_throw_made,
    parse_personal_foul,
    parse_shot_made,
    parse_technical_foul,
)
from sportsdataverse.mbb.mbb_ncaa_models import (
    LineupEvent,
    PlayerCodeId,
    PlayerId,
    RawGameEvent,
    Score,
    TeamId,
    Year,
)

if TYPE_CHECKING:
    # Avoids a circular import: mbb_ncaa_names imports build_player_code FROM
    # this module. `from __future__ import annotations` (above) means the
    # `tidy_ctx: TidyPlayerContext` field annotation below is never evaluated
    # at runtime, so this import is safe as a type-checking-only forward ref.
    from sportsdataverse.mbb.mbb_ncaa_names import TidyPlayerContext

__all__ = [
    "PLAYER_CODE_MAX_LENGTH",
    "PLAYER_CODE_MAX_FRAGMENT_LENGTH",
    "remove_diacritics",
    "build_player_code",
    "parse_team_name",
    "SUB_SAFETY_DELTA_MINS",
    "SubInEvent",
    "SubOutEvent",
    "OtherTeamEvent",
    "OtherOpponentEvent",
    "GameBreakEvent",
    "GameEndEvent",
    "SubEvent",
    "MiscGameEvent",
    "MiscGameBreak",
    "PlayByPlayEvent",
    "LineupBuildingState",
    "reorder_and_reverse",
]

#: Max total length of a player code (``ExtractorUtils.scala:12``).
PLAYER_CODE_MAX_LENGTH = 16

#: Max length of any one name fragment, e.g. ``"MAMUKELASHVILI"`` is
#: truncated to ``"MAMUKELASH"`` (``ExtractorUtils.scala:17``).
PLAYER_CODE_MAX_FRAGMENT_LENGTH = 10

#: Team-title matcher: optional ``#N `` seed prefix, the team name, optional
#: trailing `` (W-L)`` record (``ExtractorUtils.scala:234``). Scala's
#: ``Regex.unapplySeq`` requires a FULL match -> ``re.fullmatch``.
_EXTRACT_TEAM_REGEX = re.compile(r"([#][0-9]+ +)?([^ ].*?)( *[(][0-9]+-[0-9]+[)])?")

_COMMA_SPLIT_RE = re.compile(r"\s*,\s*")
_WHITESPACE_RE = re.compile(r"\s+")


def remove_diacritics(fragment: str) -> str:
    """Strip diacritical marks, e.g. ``"Juhász"`` -> ``"Juhasz"``
    (``ExtractorUtils.scala:38-43``: NFD normalization then removal of the
    combining-diacritical-marks block).

    Args:
        fragment: Any string (a full player name or a name fragment).

    Returns:
        The string with combining marks removed.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_stints import remove_diacritics
            print(remove_diacritics("Dorka Juhász"))  # "Dorka Juhasz"
    """
    return "".join(ch for ch in unicodedata.normalize("NFD", fragment) if not unicodedata.combining(ch))


def _first_last(fragment: str) -> str:
    """First char upper + last char lower (``ExtractorUtils.scala:299-305``)."""
    if not fragment:
        return ""
    return f"{fragment[0].upper()}{fragment[-1].lower()}"


def _transform(fragment: str, max_len: int) -> str:
    """Capitalize the first char, lowercase the rest, capped at ``max_len``
    (``ExtractorUtils.scala:306-312``: ``fragment(0).toUpper +
    fragment.take(max_len).tail.toLowerCase``)."""
    if not fragment:
        return ""
    return f"{fragment[0].upper()}{fragment[:max_len][1:].lower()}"


def _transform_first_name(fragment: str, full_name_lower: str) -> str:
    """First-name fragment transform with duplicate-name special-casing
    (``ExtractorUtils.scala:313-321``). Scala's ``Map.get`` yields
    ``Option[Option[String]]`` -- the 3-way absent / present-with-``None`` /
    present-with-code split needs ``key in table`` in Python, NOT ``.get``.
    """
    if full_name_lower in players_with_duplicate_names:
        special_case = players_with_duplicate_names[full_name_lower]
        if special_case is not None:
            return special_case
        return _first_last(fragment)
    return _transform(fragment, 2)


def _name_filter(candidate: str) -> bool:
    """True if a fragment is junk to drop (``ExtractorUtils.scala:344-351``):
    empty, digit-leading, an ordinal word, or a jr/sr/roman-numeral suffix."""
    return (
        not candidate
        or candidate[0].isdigit()
        or candidate == "the"
        or candidate == "first"
        or candidate == "second"
        or candidate == "third"
        or candidate == "jr"
        or candidate == "sr"
        or candidate == "iv"
        or candidate == "vi"
        or (candidate.startswith("ii") and candidate.endswith("ii"))
    )


def build_player_code(in_name: str, team: Optional[TeamId]) -> PlayerCodeId:
    """Build a short player code from a name, in any of the NCAA formats
    (``ExtractorUtils.scala:290-391``).

    The code is a compact ``FirstInitials + [Middle] + Lastname`` string
    (e.g. ``"Mitchell, Makhi"`` -> ``"MiMitchell"``) unique within a team +
    season, used to join play-by-play name fragments to box-score rosters.
    Supported input shapes: ``"First [Middle...] Last"`` (no comma),
    ``"Last, First [Middle...]"``, and ``"Last, Suffix, First"``.

    The full name is first corrected via the team-scoped misspelling table
    and diacritic-stripped -- that corrected string becomes the returned
    :class:`~sportsdataverse.mbb.mbb_ncaa_models.PlayerId`. Each fragment is
    lowercased, de-dotted, individually misspelling-corrected, and truncated
    to :data:`PLAYER_CODE_MAX_FRAGMENT_LENGTH`. Junk fragments (jr/sr/roman
    numerals/ordinals/digit-leading) are dropped -- except the first-name
    fragment, which is never dropped for being short.

    Args:
        in_name: The raw player name as it appears in the source HTML.
        team: The team, for team-scoped misspelling corrections; ``None``
            uses only the generic corrections.

    Returns:
        A ``PlayerCodeId`` with the derived ``code`` and the corrected full
        name as ``id`` (``ncaa_id`` is always ``None`` here).

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_stints import build_player_code
            pc = build_player_code("Mitchell, Makhi", None)
            print(pc.code)  # "MiMitchell"

        Play-by-play (all-caps, truncated) form::

            build_player_code("BIGBY-WILLIAM,KAVELL", None).code  # "KaBigby-will"

    See Also:
        * `hoopR <https://hoopR.sportsdataverse.org>`_ -- men's college
          basketball data in R.
    """
    # Full-name misspelling correction, then diacritic strip
    # (ExtractorUtils.scala:296-298). This corrected name is the PlayerId.
    corrected = misspellings(team).get(in_name, in_name)
    name = remove_diacritics(corrected)

    # Split into fragments in first..last(+suffix) order
    # (ExtractorUtils.scala:322-333). Scala split(regex, 3) == maxsplit=2.
    comma_parts = _COMMA_SPLIT_RE.split(name, maxsplit=2)
    if len(comma_parts) == 1:
        parts = _WHITESPACE_RE.split(comma_parts[0])
    elif len(comma_parts) == 2:
        last_name_set, first_name_set = comma_parts
        parts = _WHITESPACE_RE.split(first_name_set) + _WHITESPACE_RE.split(last_name_set)
    else:
        last_name_set, suffix, first_name_set = comma_parts
        parts = _WHITESPACE_RE.split(first_name_set) + _WHITESPACE_RE.split(last_name_set) + [suffix]

    # Per-fragment: lowercase, strip dots, misspelling-correct, truncate
    # (ExtractorUtils.scala:334-341).
    frag_misspellings = misspellings(team)
    fragments = []
    for name_part in parts:
        lower = name_part.lower().replace(".", "")
        fragments.append(frag_misspellings.get(lower, lower)[:PLAYER_CODE_MAX_FRAGMENT_LENGTH])

    # Junk filtering -- the head (first name) is never dropped for size
    # (ExtractorUtils.scala:343-357).
    if fragments:
        head, tail = fragments[0], fragments[1:]
        kept = ([] if _name_filter(head) else [head]) + [c for c in tail if len(c) >= 2 and not _name_filter(c)]
    else:
        kept = []

    # Code assembly (ExtractorUtils.scala:358-382).
    if not kept:
        code = ""
    elif len(kept) == 1:
        code = _transform(kept[0], PLAYER_CODE_MAX_LENGTH)
    else:
        head, tail = kept[0], kept[1:]
        last_size = len(tail[-1])
        leftover = PLAYER_CODE_MAX_LENGTH - last_size - 2
        if leftover >= 2:
            # Short last name -> spend the leftover on the middle fragment;
            # long last name -> treat the middle like a 2-char head.
            leftover_to_use = leftover if last_size < 6 else 2
            middle_frag = tail[-2] if len(tail) >= 2 else ""
            middle = _transform(middle_frag, leftover_to_use)
        else:
            middle = ""
        code = _transform_first_name(head, name.lower()) + middle + _transform(tail[-1], PLAYER_CODE_MAX_LENGTH)

    return PlayerCodeId(code=code, id=PlayerId(name))


def parse_team_name(teams: list[str], target_team: TeamId, year: Year) -> Union[tuple[str, str, bool], ParseError]:
    """Match the two team-title strings against the target team
    (``ExtractorUtils.scala:240-267``).

    Strips an optional ``#N `` seed prefix and trailing `` (W-L)`` record
    from each title (e.g. ``"#10 Iowa (3-3)"`` -> ``"Iowa"``), applies the
    season-scoped team-alias table, then requires the target to be exactly
    one of the two entries.

    Args:
        teams: The two team-title strings from the game page.
        target_team: The team we are building lineups for.
        year: Season, for the ``team_aliases`` data-quality overrides.

    Returns:
        ``(target_name, opponent_name, target_is_first)`` on success, or a
        :class:`~sportsdataverse.mbb.mbb_ncaa_data_quality.ParseError` when
        the titles don't contain the target (Scala returns
        ``Either[ParseError, ...]``; Python has no ``Either``, so the error
        value is returned directly -- check ``isinstance(result, ParseError)``).

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_models import TeamId, Year
            from sportsdataverse.mbb.mbb_ncaa_stints import parse_team_name
            parse_team_name(["#1 TeamA (1-1)", "TeamB (4-1)"], TeamId("TeamA"), Year(2018))
            # ("TeamA", "TeamB", True)
    """
    target_team_str = target_team.name
    aliases = team_aliases.get(year, {})
    cleaned = []
    for title in teams:
        m = _EXTRACT_TEAM_REGEX.fullmatch(title)
        if m is None:
            continue  # Scala `collect` drops non-matching entries
        just_team = m.group(2)
        cleaned.append(aliases.get(TeamId(just_team), TeamId(just_team)).name.strip())

    if len(cleaned) == 2 and cleaned[0] == target_team_str:
        return (target_team_str, cleaned[1], True)
    if len(cleaned) == 2 and cleaned[1] == target_team_str:
        return (target_team_str, cleaned[0], False)
    return build_sub_error(
        "team",
        error=(f"Could not find/match team names (target=[TeamId({target_team.name})]): " + "/".join(teams)),
    )


# ---------------------------------------------------------------------------
# PlayByPlayEvent ADT (``object Model``, ExtractorUtils.scala:822-882)
# ---------------------------------------------------------------------------

#: Minimum inactivity window (4 seconds, expressed in game-clock minutes)
#: -- see :meth:`LineupBuildingState.is_active`
#: (``Model.SUB_SAFETY_DELTA_MINS``, ``ExtractorUtils.scala:732``).
SUB_SAFETY_DELTA_MINS = 4.0 / 60


@dataclass
class SubInEvent:
    """A player subs into the game (``Model.SubInEvent``, ``ExtractorUtils.scala:850-853``).

    Args:
        min: The ascending game-clock minute of the event.
        score: The score at the time of the event.
        player_name: The raw or processed name of the player subbing in.
    """

    min: float
    score: Score
    player_name: str

    def with_min(self, new_min: float) -> "SubInEvent":
        """Return a copy with :attr:`min` replaced (``:852``)."""
        return replace(self, min=new_min)


@dataclass
class SubOutEvent:
    """A player subs out of the game (``Model.SubOutEvent``, ``ExtractorUtils.scala:854-857``).

    Args:
        min: The ascending game-clock minute of the event.
        score: The score at the time of the event.
        player_name: The raw or processed name of the player subbing out.
    """

    min: float
    score: Score
    player_name: str

    def with_min(self, new_min: float) -> "SubOutEvent":
        """Return a copy with :attr:`min` replaced (``:856``)."""
        return replace(self, min=new_min)


@dataclass
class OtherTeamEvent:
    """A non-sub event belonging to the team under analysis
    (``Model.OtherTeamEvent``, ``ExtractorUtils.scala:858-865``).

    Args:
        min: The ascending game-clock minute of the event.
        score: The score at the time of the event.
        event_string: The raw play-by-play event string.
    """

    min: float
    score: Score
    event_string: str

    @property
    def is_team_dir(self) -> bool:
        """Always ``True`` -- a fixed literal in the Scala, not a
        constructor field (``:864``)."""
        return True

    def with_min(self, new_min: float) -> "OtherTeamEvent":
        """Return a copy with :attr:`min` replaced (``:863``)."""
        return replace(self, min=new_min)


@dataclass
class OtherOpponentEvent:
    """A non-sub event belonging to the opponent (``Model.OtherOpponentEvent``,
    ``ExtractorUtils.scala:866-873``).

    Args:
        min: The ascending game-clock minute of the event.
        score: The score at the time of the event.
        event_string: The raw play-by-play event string.
    """

    min: float
    score: Score
    event_string: str

    @property
    def is_team_dir(self) -> bool:
        """Always ``False`` -- a fixed literal in the Scala, not a
        constructor field (``:872``)."""
        return False

    def with_min(self, new_min: float) -> "OtherOpponentEvent":
        """Return a copy with :attr:`min` replaced (``:871``)."""
        return replace(self, min=new_min)


@dataclass
class GameBreakEvent:
    """A break in play (timeout, end of period, etc.) short of the end of
    the game (``Model.GameBreakEvent``, ``ExtractorUtils.scala:874-877``).

    Args:
        min: The ascending game-clock minute of the event.
        score: The score at the time of the event.
    """

    min: float
    score: Score

    def with_min(self, new_min: float) -> "GameBreakEvent":
        """Return a copy with :attr:`min` replaced (``:876``)."""
        return replace(self, min=new_min)


@dataclass
class GameEndEvent:
    """The end of the game (``Model.GameEndEvent``, ``ExtractorUtils.scala:878-881``).

    Args:
        min: The ascending game-clock minute of the event.
        score: The score at the time of the event.
    """

    min: float
    score: Score

    def with_min(self, new_min: float) -> "GameEndEvent":
        """Return a copy with :attr:`min` replaced (``:880``)."""
        return replace(self, min=new_min)


#: Events referencing a substituting player (``Model.SubEvent``,
#: ``ExtractorUtils.scala:843-847``).
SubEvent = Union[SubInEvent, SubOutEvent]

#: Events carrying a raw event string + team/opponent direction
#: (``Model.MiscGameEvent``, ``ExtractorUtils.scala:835-842``).
MiscGameEvent = Union[OtherTeamEvent, OtherOpponentEvent]

#: Game-break events (``Model.MiscGameBreak``, ``ExtractorUtils.scala:848``).
MiscGameBreak = Union[GameBreakEvent, GameEndEvent]

#: The full play-by-play event ADT (``Model.PlayByPlayEvent``,
#: ``ExtractorUtils.scala:822-833``).
PlayByPlayEvent = Union[SubInEvent, SubOutEvent, OtherTeamEvent, OtherOpponentEvent, GameBreakEvent, GameEndEvent]


@dataclass
class LineupBuildingState:
    """State for building raw lineup data across a fold over play-by-play
    events (``Model.LineupBuildingState``, ``ExtractorUtils.scala:735-819``).

    See the module docstring's "``with_*`` methods return NEW instances"
    note -- every mutator below returns a fresh :class:`LineupBuildingState`
    rather than mutating ``self``.

    Args:
        curr: The lineup event currently being built.
        tidy_ctx: Name-resolution context for the current game (see
            :class:`~sportsdataverse.mbb.mbb_ncaa_names.TidyPlayerContext`);
            threaded through, unused by this task's methods -- Task 5b.5's
            ``build_partial_lineup_list`` calls
            :func:`~sportsdataverse.mbb.mbb_ncaa_names.tidy_player` with it
            on every sub event.
        prev: Completed lineup events, most-recently-completed first (i.e.
            the reverse of :meth:`build`'s output order).
        old_format: ``True`` once latched onto the legacy (pre-2018-ish)
            NCAA play-by-play format, ``None`` until the first sub is seen.
    """

    curr: LineupEvent
    tidy_ctx: "TidyPlayerContext"
    prev: list[LineupEvent] = field(default_factory=list)
    old_format: Optional[bool] = None

    def build(self) -> list[LineupEvent]:
        """The full chronological lineup-event list (``:742-744``:
        ``(curr :: prev).reverse``)."""
        return list(reversed(self.prev)) + [self.curr]

    def is_sub(self, raw: RawGameEvent) -> bool:
        """Whether ``raw`` is an *opponent*-side substitution line
        (``:749-758``).

        Only :attr:`~sportsdataverse.mbb.mbb_ncaa_models.RawGameEvent.opponent`
        is inspected -- per the Scala scaladoc, "opposition subs are
        currently treated as game events but shouldn't result in new
        lineups"; the team's own subs never reach here as raw events in the
        first place (they route through the fold's dedicated ``Sub*Event``
        branches, not :meth:`with_team_event`), so this check only ever
        needs to look at the opponent side. Ported verbatim, quirk and all.

        Args:
            raw: The raw event to classify.

        Returns:
            ``True`` if ``raw.opponent`` ends with one of the four
            substitution phrases (case/whitespace-insensitive), else
            ``False`` (including when ``raw.opponent`` is ``None``).
        """
        if raw.opponent is None:
            return False
        s_lower = raw.opponent.lower().strip()
        return (
            s_lower.endswith("leaves game")
            or s_lower.endswith("enters game")
            or s_lower.endswith("substitution in")
            or s_lower.endswith("substitution out")
        )

    def is_active(self, min: float) -> bool:
        """Whether the current lineup has non-sub activity, or has simply
        been on the floor long enough to trust (``:760-766``).

        Args:
            min: The ascending game-clock minute to check against.

        Returns:
            ``True`` if any raw event on :attr:`curr` isn't an opponent sub,
            or if ``min`` is more than :data:`SUB_SAFETY_DELTA_MINS` past
            :attr:`curr`'s ``end_min``.
        """
        return any(not self.is_sub(raw) for raw in self.curr.raw_game_events) or (
            min - self.curr.end_min > SUB_SAFETY_DELTA_MINS
        )

    def with_player_in(self, player_name: str) -> "LineupBuildingState":
        """Prepend a new "subbed in" player code onto :attr:`curr` (``:770-778``)."""
        return replace(
            self,
            curr=replace(
                self.curr,
                players_in=[build_player_code(player_name, self.curr.team.team)] + self.curr.players_in,
            ),
        )

    def with_player_out(self, player_name: str) -> "LineupBuildingState":
        """Prepend a new "subbed out" player code onto :attr:`curr` (``:779-787``)."""
        return replace(
            self,
            curr=replace(
                self.curr,
                players_out=[build_player_code(player_name, self.curr.team.team)] + self.curr.players_out,
            ),
        )

    def with_latest_score(self, score: Score) -> "LineupBuildingState":
        """Update :attr:`curr`'s running end-of-event score (``:788-796``)."""
        return replace(
            self,
            curr=replace(self.curr, score_info=replace(self.curr.score_info, end=score)),
        )

    def with_team_event(self, min: float, event_string: str) -> "LineupBuildingState":
        """Append a team-side raw event and bump ``end_min`` (``:797-807``)."""
        return replace(
            self,
            curr=replace(
                self.curr,
                end_min=min,
                raw_game_events=[RawGameEvent.for_team(event_string, min)] + self.curr.raw_game_events,
            ),
        )

    def with_opponent_event(self, min: float, event_string: str) -> "LineupBuildingState":
        """Append an opponent-side raw event and bump ``end_min`` (``:808-818``)."""
        return replace(
            self,
            curr=replace(
                self.curr,
                end_min=min,
                raw_game_events=[RawGameEvent.for_opponent(event_string, min)] + self.curr.raw_game_events,
            ),
        )


def reorder_and_reverse(reversed_partial_events: Iterable[PlayByPlayEvent]) -> list[PlayByPlayEvent]:
    """Orders same-minute play-by-play events so subs never enclose the plays
    they logically precede/follow (``ExtractorUtils.scala:435-599``).

    Groups consecutive events sharing the same :attr:`min` into a block (the
    input arrives in descending/reverse-chronological order, so blocks are
    discovered and internally accumulated in reverse too), then -- for any
    block containing a sub -- reorders it via ``inner_sort``: events
    referencing a subbed-OUT player (or scoring no higher than the sub) land
    in a pre-sub group, the subs themselves come next (in ascending-score
    order), and events referencing a subbed-IN player (or scoring higher
    than the sub) land in a trailing post-sub group. Free-throw attempts
    sharing the sub's inferred "direction" (team vs. opponent, inferred from
    the nearest preceding shot/FT/foul) are pulled into the pre-sub group
    unless the shooter is one of the players being subbed in. Blocks with no
    sub are returned unchanged apart from the initial score-based sort.

    Args:
        reversed_partial_events: Events for one lineup event, in
            reverse-chronological (descending-time) order -- the natural
            order encountered walking play-by-play text bottom-up.

    Returns:
        The same events, forward-chronological (ascending time), with each
        same-minute block internally reordered so no sub encloses a play it
        logically shouldn't.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_models import Score
            from sportsdataverse.mbb.mbb_ncaa_stints import (
                OtherTeamEvent,
                SubInEvent,
                reorder_and_reverse,
            )
            events = [
                SubInEvent(0.4, Score(0, 0), "player1"),
                OtherTeamEvent(0.4, Score(0, 0), "rebound"),
            ]
            reorder_and_reverse(events)
            # [OtherTeamEvent(...), SubInEvent(...)]
    """

    def event_refs_player(ev: MiscGameEvent, in_or_out: Sequence[SubEvent]) -> bool:
        """Does ``ev``'s raw string mention one of ``in_or_out``'s player
        names? (``:484-492``)."""
        return any(candidate.player_name in ev.event_string for candidate in in_or_out)

    def score_gt(s1: Score, s2: Score) -> bool:
        """Lexicographic ``(scored, allowed)`` comparison (``:499-502``)."""
        return (s1.scored, s1.allowed) > (s2.scored, s2.allowed)

    def inner_sort(pre_ordered_block: list[PlayByPlayEvent]) -> list[PlayByPlayEvent]:
        """Ensures subs don't enclose plays within one same-minute block
        (``:440-574``)."""

        def sort_key(ev: PlayByPlayEvent) -> tuple[int, int, int]:
            # (rank scoring shots earliest, subs latest, everything else in
            # between -- ties broken by stable sort, matching Scala sortBy)
            if isinstance(ev, (OtherTeamEvent, OtherOpponentEvent)) and (
                parse_free_throw_made(ev.event_string) is not None or parse_shot_made(ev.event_string) is not None
            ):
                return (ev.score.scored, ev.score.allowed, 0)
            if isinstance(ev, (SubOutEvent, SubInEvent)):
                return (ev.score.scored, ev.score.allowed, 10)
            return (ev.score.scored, ev.score.allowed, 1)

        ordered_block = sorted(pre_ordered_block, key=sort_key)

        subs: list[SubEvent] = [ev for ev in ordered_block if isinstance(ev, (SubInEvent, SubOutEvent))]
        if not subs:  # nothing to do
            return ordered_block

        # work to do to match up subs and events, see the scaladoc above
        sub_ins = [ev for ev in subs if isinstance(ev, SubInEvent)]
        sub_outs = [ev for ev in subs if isinstance(ev, SubOutEvent)]

        @dataclass
        class _State:
            sub_score: Optional[Score]
            direction_team: Optional[bool]
            group_1: list[PlayByPlayEvent]
            group_2: list[PlayByPlayEvent]

            def build(self) -> list[PlayByPlayEvent]:
                # (the loop below prepends onto ordered_block's order, i.e.
                # reverses it, so both groups need reversing back here.)
                return list(reversed(self.group_1)) + subs + list(reversed(self.group_2))

        def add_to_state(state: _State, ev: PlayByPlayEvent) -> _State:
            """Adds ``ev`` to the pre- or post-sub group based on ``state``
            (``:498-534``)."""
            if state.sub_score is not None and score_gt(ev.score, state.sub_score):
                return replace(state, group_2=[ev] + state.group_2)

            # (log direction of pre-sub action, so we can later pull FTs
            # from after the sub back into the same direction's group)
            direction_team = state.direction_team
            if isinstance(ev, (OtherTeamEvent, OtherOpponentEvent)):
                es = ev.event_string
                if (
                    parse_free_throw_event(es) is not None
                    or parse_shot_made(es) is not None
                    or parse_foul_info(es) is not None
                ):
                    direction_team = ev.is_team_dir
                elif parse_technical_foul(es) is not None or parse_personal_foul(es) is not None:
                    direction_team = not ev.is_team_dir
            return replace(state, direction_team=direction_team, group_1=[ev] + state.group_1)

        state = _State(sub_score=None, direction_team=None, group_1=[], group_2=[])
        for ev in ordered_block:
            if isinstance(ev, (SubInEvent, SubOutEvent)):
                # (already collected into `subs` above)
                state = replace(state, sub_score=ev.score)
            elif (
                isinstance(ev, (OtherTeamEvent, OtherOpponentEvent))
                and parse_free_throw_attempt(ev.event_string) is not None
                and state.direction_team is not None
                and state.direction_team == ev.is_team_dir
                # (unless a player being subbed-in is taking the FT! It happens...)
                and not event_refs_player(ev, sub_ins)
            ):
                # Always put FTs tied to pre-sub events in the first group
                state = replace(state, group_1=[ev] + state.group_1)
            elif isinstance(ev, OtherTeamEvent):
                if event_refs_player(ev, sub_ins) and not event_refs_player(ev, sub_outs):
                    # (if the player appears in both in and out, assume the first block)
                    state = replace(state, group_2=[ev] + state.group_2)
                elif event_refs_player(ev, sub_outs):
                    state = replace(state, group_1=[ev] + state.group_1)
                else:
                    state = add_to_state(state, ev)
            else:
                # (game breaks and opponent events, leave well alone)
                # TODO: also need to handle opponent subs and re-ordering else
                # the advanced stats will be wrong
                state = add_to_state(state, ev)

        return state.build()

    blocks: list[list[PlayByPlayEvent]] = []
    for event in reversed_partial_events:
        if not blocks:
            # Create a new block of play-by-play events
            blocks.append([event])
        elif not blocks[0]:
            # Create a new block (in practice this won't happen)
            blocks[0] = [event]
        elif blocks[0][0].min == event.min:
            # Add the new play-by-play event to the existing block
            blocks[0] = [event] + blocks[0]
        else:
            # Reorder the existing block, then start a new one
            head = blocks[0]
            blocks = [[event], inner_sort(head)] + blocks[1:]

    if not blocks:
        return []
    last, *tail = blocks
    result = inner_sort(last)
    for block in tail:
        result.extend(block)
    return result
