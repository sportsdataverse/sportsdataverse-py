"""NCAA stint-builder core: player codes + team-name parsing (men's basketball).

Faithful Python port of the player-code generator and team-name parser from
``ExtractorUtils.scala`` in Alex-At-Home/cbb-explorer (the Scala NCAA
play-by-play ingestion pipeline behind hoop-explorer.com). This module is
extended by later Phase-5b tasks with the play-by-play event ADT, event
reordering, and the substitution-tracking stint builder
(``build_partial_lineup_list``).

Ported functions (Scala anchors in each docstring):

* :func:`remove_diacritics` -- NFD-normalize + strip combining marks
  (``ExtractorUtils.scala:38-43``).
* :func:`build_player_code` -- turn a box-score / play-by-play player name
  into a short unique-within-team code (``ExtractorUtils.scala:290-391``).
* :func:`parse_team_name` -- match the two team-title strings against the
  target team (``ExtractorUtils.scala:240-267``).

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
"""

from __future__ import annotations

import re
import unicodedata
from typing import Optional, Union

from sportsdataverse.mbb.mbb_ncaa_data_quality import (
    ParseError,
    build_sub_error,
    misspellings,
    players_with_duplicate_names,
    team_aliases,
)
from sportsdataverse.mbb.mbb_ncaa_models import PlayerCodeId, PlayerId, TeamId, Year

__all__ = [
    "PLAYER_CODE_MAX_LENGTH",
    "PLAYER_CODE_MAX_FRAGMENT_LENGTH",
    "remove_diacritics",
    "build_player_code",
    "parse_team_name",
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
