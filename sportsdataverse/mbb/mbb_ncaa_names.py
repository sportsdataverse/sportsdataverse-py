"""NCAA name-resolution chain (men's basketball) -- ``tidy_player`` + ``NameFixer``.

Faithful Python port of the **name-resolution half** of
``LineupErrorAnalysisUtils.scala`` in Alex-At-Home/cbb-explorer (the Scala
NCAA play-by-play ingestion pipeline behind hoop-explorer.com): the
substitution-event name -> box-score-player reconciliation chain that
``ExtractorUtils.build_partial_lineup_list`` (Phase 5b.5) calls on every
``SubIn``/``SubOut`` event. The **stint-VALIDATION half** of the same Scala
object (``validate_lineup`` / ``clump_bad_lineups`` / ``handle_common_sub_bug``
/ ``add_missing_players`` / ``find_missing_subs`` / ``analyze_and_fix_clumps``
/ ``categorize_bad_lineups``, plus their ``BadLineupClump`` /
``ValidationError`` supporting types) is **out of scope for this module** --
it is Phase 5d's job, once the stint builder itself (5b.4/5b.5) exists for it
to validate.

Ported members (Scala anchors in each docstring):

* :class:`TidyPlayerContext` / :func:`build_tidy_player_context` --
  precomputed box-score-code lookup tables (``LineupErrorAnalysisUtils.scala:31-73``).
* :func:`tidy_player` -- the ordered name-resolution fallback chain
  (``:76-144``).
* :func:`convert_from_initials` / :func:`convert_from_digits` -- two of the
  chain's fallback strategies (``:147-175``).
* :class:`NoSurnameMatch` / :class:`WeakSurnameMatch` / :class:`StrongSurnameMatch`
  / :func:`box_aware_compare` -- fuzzy single-candidate scoring
  (``NameFixer``, ``:658-766``).
* :class:`FuzzyMatchError` / :func:`fuzzy_box_match` -- the top-level fuzzy
  resolver that picks among box-score names (``:774-905``).

**FUZZY-MATCH PARITY (rapidfuzz vs Java fuzzywuzzy) -- read before touching
the scoring code.** The Scala calls Java ``me.xdrop.fuzzywuzzy.FuzzySearch
.weightedRatio`` everywhere :func:`box_aware_compare` needs a similarity
score; this port uses ``rapidfuzz.fuzz.WRatio`` (:func:`_weighted_ratio`).
**Critical gotcha: rapidfuzz's ``WRatio`` does NOT preprocess by default**
(``processor=None`` unless passed explicitly), whereas fuzzywuzzy/Java's
``weightedRatio`` always runs its own ``full_process`` (lowercase + strip
non-alphanumeric-to-space + collapse whitespace) before scoring, regardless
of caller preprocessing. Calling ``fuzz.WRatio`` with no processor
reproduced every *single-fragment* Java score exactly (candidate/box-name
words with no internal punctuation), but silently mis-bucketed several
*multi-token, comma-joined* pairs -- e.g. ``"sarion,mcgee"`` vs
``"mcgee,sarion"`` (a plain token transposition) scored only 50 unprocessed
(landing ``WeakSurnameMatch`` when Java's real score is a trivial
``StrongSurnameMatch`` >= 70, since Java's ``full_process`` turns the comma
into a space before token-sorting). Passing
``processor=rapidfuzz.utils.default_process`` (rapidfuzz's own
lowercase+de-punctuate+collapse-whitespace preprocessor) closed every one of
these gaps: a throwaway probe over the 7 pairs that flipped bucket without
it, plus the 18 pairs whose exact Java integer is mined directly out of
``LineupErrorAnalysisUtilsTests.scala``'s expected message strings (e.g.
``"candidates=(finklea,17);(amaya,20)"`` literally encodes Java's computed
integers), showed every pair landing on the correct side of every threshold
(80 / 75 / 70) with ``default_process``, and several landed on the *exact*
Java integer (e.g. ``"dee dee pryor"`` vs ``"pryor, dearica"``: both 69;
``"jonathan fanard"`` vs ``"fanord, donalson"``: both 38) where the
unprocessed call did not. Per the task protocol (threshold-crossing is the
pass/fail bar, not byte-identical scores) this is a clear pass -- see the
Task 5b.3 report for the full probe table. :func:`_weighted_ratio` therefore
always passes ``processor=rapidfuzz.utils.default_process`` explicitly.

**Behavioral quirk ported verbatim: ``tidy_player``'s resolution cache is
keyed asymmetrically.** The cache is *read* by the raw, un-normalized
``p_in`` argument but *written* under the misspelling-corrected /
diacritic-stripped ``p`` (``player_id.id.name``) -- see
``LineupErrorAnalysisUtils.scala:80-143``. When ``p_in`` needed no
correction (``p == p_in``) the cache behaves normally; when ``p_in`` *was*
corrected, the write goes under a different key than any future read of
that same misspelled ``p_in`` will ever hit, so the cache never actually
short-circuits repeat lookups of a misspelled name (only repeat lookups of
an already-correct name benefit). This is upstream behavior, not a bug
introduced here -- ported byte-for-byte per this project's faithful-port
discipline.

**Recursive self-call ignores its own returned context.** The
double-barrel-strip fallback (dashes stripped, e.g. ``"Smith-Jones"`` ->
``"Jones"``) recurses into :func:`tidy_player` with the *original* ``ctx``
and keeps only the resolved name (``tidy_player(new_p, ctx)[0]``) -- any
cache population from that inner call is discarded; only the outer call's
final :func:`_with_updated_cache` survives. Ported as-is (``:95-100``).

**``fixes_for_debug`` dropped.** The Scala ``NameFixer.fixes_for_debug``
mutable ``Map`` (``:769``) exists purely to de-duplicate ``println``
diagnostics across repeated ``fuzzy_box_match`` calls with the same
``(team_context, candidate)`` key -- it has no effect on any return value.
This port has no logging surface to de-duplicate, so it is dropped entirely
(not even an inert dict) rather than added as unused scaffolding.

**``NameFixer`` flattened to module level.** Scala nests ``box_aware_compare``
/ ``fuzzy_box_match`` / the ``MatchResult`` hierarchy inside an ``object
NameFixer``; this project already flattens every Scala companion
object/nested-object into plain module-level functions and dataclasses
(see ``mbb_ncaa_models.py`` / ``mbb_ncaa_stints.py``), so the same is done
here -- no unrequested ``NameFixer`` class wrapper.

Attribution: derived from `cbb-explorer
<https://github.com/Alex-At-Home/cbb-explorer>`_ (Apache License 2.0,
Copyright Alex-At-Home / org.piggottfamily). This is a source-language
translation (Scala -> Python), not a copy; upstream file:
``src/main/scala/org/piggottfamily/cbb_explorer/utils/parsers/ncaa/LineupErrorAnalysisUtils.scala``
(name-resolution half only; the ``ExtractorUtils.name_is_initials`` helper
consumed by :func:`convert_from_initials` is also ported here, since this
task's file scope did not include modifying ``mbb_ncaa_stints.py``. **Task
5e.1 promoted it from private (``_name_is_initials``) to public
(:func:`name_is_initials`)** -- ``mbb_ncaa_roster_parser.py``'s
``parse_roster`` needs the same initials-shorthand check to reject
initials-only roster rows, making this module a second consumer). See
``NOTICE`` for the full notice.

Landmine index (reachable error sites, numbered across the module):
    1. ``_truncate_code_1`` / ``_truncate_code_2`` fall back to the input
       code unchanged when their regex doesn't fullmatch -- no exception
       reachable.
    2. :func:`box_aware_compare` splits ``candidate`` / ``box_name`` with
       ``re.split`` on ``"[, ]+"`` / ``"\\s*,\\s*"``. Python's ``re.split``
       (unlike Scala/Java's ``String.split`` with the default ``limit=0``,
       which trims *trailing* empty strings but keeps a *leading* one) can
       leave a leading ``""`` fragment if a name starts with the delimiter
       (e.g. a stray leading comma). None of the oracle's tested inputs
       start with a delimiter, so this divergence is unreached by the
       tested surface; a future caller feeding a leading-comma name should
       verify behavior explicitly.

Example::

    from sportsdataverse.mbb.mbb_ncaa_names import (
        build_tidy_player_context,
        tidy_player,
    )

    ctx = build_tidy_player_context(box_lineup)
    resolved_name, ctx = tidy_player("MITCHELL,M", ctx)

See Also:
    * `hoopR <https://hoopR.sportsdataverse.org>`_ -- men's college
      basketball data in R.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from typing import Optional, Union

from rapidfuzz import fuzz, utils

from sportsdataverse.mbb.mbb_ncaa_models import LineupEvent, PlayerCodeId, TeamId
from sportsdataverse.mbb.mbb_ncaa_stints import build_player_code

__all__ = [
    "MIN_SURNAME_SCORE",
    "MIN_FIRST_NAME_SCORE",
    "MIN_OVERALL_SCORE",
    "MIN_USEFUL_SURNAME_LEN",
    "MIN_USEFUL_FIRST_NAME_LEN",
    "TidyPlayerContext",
    "build_tidy_player_context",
    "tidy_player",
    "code_from_box",
    "display_name_to_roster_key",
    "name_is_initials",
    "convert_from_initials",
    "convert_from_digits",
    "NoSurnameMatch",
    "WeakSurnameMatch",
    "StrongSurnameMatch",
    "MatchResult",
    "FuzzyMatchError",
    "box_aware_compare",
    "fuzzy_box_match",
]

#: Surname-fragment score threshold (``NameFixer.min_surname_score``, ``:649``).
MIN_SURNAME_SCORE = 80

#: First-name-fragment score threshold (``NameFixer.min_first_name_score``, ``:650``).
MIN_FIRST_NAME_SCORE = 75

#: Whole-name score threshold for a "strong" match (``NameFixer.min_overall_score``, ``:651``).
MIN_OVERALL_SCORE = 70

#: Minimum surname-fragment length to trust a fuzzy surname match (``:652``).
MIN_USEFUL_SURNAME_LEN = 4

#: Minimum first-name-fragment length to trust a fuzzy first-name match (``:653``).
MIN_USEFUL_FIRST_NAME_LEN = 3

_TRUNCATE_CODE_1_RE = re.compile(r"([A-Z]).*?([A-Z][a-z.-]*)")
_TRUNCATE_CODE_2_RE = re.compile(r"([A-Z][a-z]).*?([A-Z][a-z.-]*)")

_DOUBLE_BARREL_RE = re.compile(r"[a-zA-Z]+-([a-zA-Z]+)")

_TEAM_TOKENS = ("Team", "TEAM", "TEAM DEF", "TEAM FULL")


_ROSTER_KEY_SUFFIX = re.compile(r"(?i)^(jr|sr|ii|iii|iv|v)\.?$")
_ROSTER_KEY_NICKNAME = re.compile(r'["“”].*?["“”]')


def _roster_key_token(tok: str) -> str:
    tok = unicodedata.normalize("NFKD", tok)
    tok = "".join(c for c in tok if not unicodedata.combining(c))
    return re.sub(r"[^A-Za-z]", "", tok).upper()


def display_name_to_roster_key(name: Optional[str]) -> str:
    """``"Ballisager Webb, Jermaine"`` -> ``"JERMAINE.BALLISAGER.WEBB"``.

    Box-score and shot-chart pages render a player as ``"Surname, First"``,
    while ``team_rosters`` renders the same person as ``FIRST.MIDDLE.LAST``
    uppercase -- whitespace becomes dots, hyphens collapse, diacritics fold.
    Joining the two needs one canonical direction, and this is it.

    Args:
        name: The display name (``"Surname, First"``), or ``None``.

    Returns:
        The roster-style key, or ``""`` when the name cannot be split into at
        least a surname and a first name. An empty key never matches, which is
        the intended outcome -- an unresolved row beats a wrong join.

    Each normalization below was measured against real 2024 MBB data, and the
    match rate is the reason each one exists:

    ==========================================  ==========
    step                                        match rate
    ==========================================  ==========
    naive comma split                              93.04%
    + suffix / quoted-nickname strip               98.07%
    + whitespace -> dots (multi-token surnames)     99.08%
    ==========================================  ==========

    The multi-token step is the subtle one: the roster keeps INTERIOR dots as
    token separators (``JERMAINE.BALLISAGER.WEBB``), so collapsing a two-word
    surname into ``BALLISAGERWEBB`` silently misses.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_names import display_name_to_roster_key

            display_name_to_roster_key("Clark, Garry")            # "GARRY.CLARK"
            display_name_to_roster_key("Wrightsell Jr., Latrell") # "LATRELL.WRIGHTSELL"
            display_name_to_roster_key('"TJ" Madlock, Antonio')   # "ANTONIO.MADLOCK"
    """
    if not name:
        return ""
    name = _ROSTER_KEY_NICKNAME.sub(" ", name)
    parts = [p.strip() for p in name.split(",")]
    parts = [p for p in parts if p and not _ROSTER_KEY_SUFFIX.fullmatch(p)]
    if len(parts) < 2:
        return ""
    first, last = parts[-1], " ".join(parts[:-1])

    def _tokens(field: str) -> "list[str]":
        out = [_roster_key_token(t) for t in field.split()]
        return [t for t in out if t and not _ROSTER_KEY_SUFFIX.fullmatch(t)]

    # BOTH components must survive normalization. Filtering the combined token
    # list instead would happily emit a surname-only key when the given-name
    # field held nothing usable -- "Smith, Jr. III" and "Smith, 123" both
    # collapse to "SMITH", a partial key that JOINS, which is worse than no
    # key at all. Fail closed: an empty key never matches.
    first_toks, last_toks = _tokens(first), _tokens(last)
    if not first_toks or not last_toks:
        return ""
    return ".".join(first_toks + last_toks)


def _truncate_code_1(code: str) -> str:
    """``AaBbCcc...`` -> ``ABbb...`` -- initial + surname only
    (``LineupErrorAnalysisUtils.truncate_code_1``, ``:39-46``)."""
    m = _TRUNCATE_CODE_1_RE.fullmatch(code)
    return code if m is None else m.group(1) + m.group(2)


def _truncate_code_2(code: str) -> str:
    """``AaBbCcc...`` -> ``AaCcc...`` -- first-name-initials + surname
    (``LineupErrorAnalysisUtils.truncate_code_2``, ``:48-56``)."""
    m = _TRUNCATE_CODE_2_RE.fullmatch(code)
    return code if m is None else m.group(1) + m.group(2)


def name_is_initials(name: str) -> Optional[tuple[str, str]]:
    """Detect a 2-initial name shorthand, e.g. ``"A B"`` or ``"B, A"``
    (``ExtractorUtils.name_is_initials``, ``ExtractorUtils.scala:94-102``).
    Ported here (rather than into ``mbb_ncaa_stints.py``) since
    :func:`convert_from_initials` was this module's original consumer;
    promoted from private to public in Task 5e.1 for
    ``mbb_ncaa_roster_parser.py``'s ``parse_roster`` (a second consumer,
    which only needs ``.nonEmpty`` -- whether a match exists at all -- to
    reject initials-only roster rows).

    Args:
        name: The candidate initials string.

    Returns:
        ``(p1, p2)`` -- ``p1`` is the leading initial in a ``"A B"``-style
        string, or the trailing initial in a ``"B, A"``-style string;
        ``None`` if ``name`` doesn't fit either 3- or 4-character shape.
    """
    chars = list(name)
    if len(chars) == 4 and chars[1] == "," and chars[2] == " ":
        return (chars[3], chars[0])
    if len(chars) == 3 and chars[1] == " ":
        return (chars[0], chars[2])
    return None


@dataclass
class TidyPlayerContext:
    """Precomputed box-score lookup tables + resolution cache for
    :func:`tidy_player` (``LineupErrorAnalysisUtils.TidyPlayerContext``,
    ``:31-36``).

    Args:
        box_lineup: The box-score lineup event this context resolves names
            against.
        all_players_map: Player code -> full name, for every player in
            ``box_lineup.players``.
        alt_all_players_map: Truncated player code (see
            :func:`_truncate_code_1` / :func:`_truncate_code_2`) -> the list
            of full names sharing that truncation -- used when the exact
            code doesn't match but a unique truncated one does.
        resolution_cache: Memoizes prior :func:`tidy_player` resolutions.
            See the module docstring's "Behavioral quirk" note -- this is
            read by the raw input name but written by the corrected name,
            faithfully reproducing the upstream asymmetry.
    """

    box_lineup: LineupEvent
    all_players_map: dict[str, str]
    alt_all_players_map: dict[str, list[str]]
    resolution_cache: dict[str, str] = field(default_factory=dict)


def build_tidy_player_context(box_lineup: LineupEvent) -> TidyPlayerContext:
    """Build the alternative player-code lookup maps for a box-score lineup
    (``LineupErrorAnalysisUtils.build_tidy_player_context``, ``:59-73``).

    Sometimes the play-by-play uses ``SURNAME,INITIAL`` instead of
    ``SURNAME,NAME``, or ``SURNAME,NAME1`` instead of ``SURNAME,NAME1
    NAME2`` -- both collapse to the same *truncated* code, so grouping by
    truncated code (and only keeping groups with exactly one distinct name)
    lets :func:`tidy_player` recover the box-score name unambiguously.

    Args:
        box_lineup: The box-score lineup event to index.

    Returns:
        A fresh :class:`TidyPlayerContext` (empty ``resolution_cache``).

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_names import build_tidy_player_context
            ctx = build_tidy_player_context(box_lineup)
    """
    all_players_map: dict[str, str] = {}
    for player_code_id in box_lineup.players:
        all_players_map[player_code_id.code] = player_code_id.id.name

    group_1: dict[str, list[str]] = {}
    for code, name in all_players_map.items():
        group_1.setdefault(_truncate_code_1(code), []).append(name)

    group_2: dict[str, list[str]] = {}
    for code, name in all_players_map.items():
        group_2.setdefault(_truncate_code_2(code), []).append(name)

    # Scala `++`: the right-hand map (group_2) wins outright on key
    # collision (whole value replaced, not merged).
    alt_all_players_map: dict[str, list[str]] = {**group_1, **group_2}

    return TidyPlayerContext(box_lineup, all_players_map, alt_all_players_map)


def convert_from_initials(name: str, codes_to_names: dict[str, str]) -> Optional[str]:
    """Resolve a 2-initial name (``"A B"`` / ``"B, A"``) to the single
    box-score player whose code starts with those initials
    (``LineupErrorAnalysisUtils.convert_from_initials``, ``:147-164``).

    Args:
        name: The candidate initials string.
        codes_to_names: Player code -> full name (e.g.
            :attr:`TidyPlayerContext.all_players_map`).

    Returns:
        The single matching full name, or ``None`` if ``name`` isn't an
        initials shorthand, or if zero or multiple codes match.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_names import convert_from_initials
            convert_from_initials("A B", {"AoBo": "name1"})  # "name1"
    """
    initials = name_is_initials(name)
    if initials is None:
        return None
    p1, p2 = initials
    candidates = [
        full_name for code, full_name in codes_to_names.items() if len(code) >= 3 and code[0] == p1 and code[2] == p2
    ]
    if len(candidates) == 1:
        return candidates[0]
    return None


def convert_from_digits(name: str, player_numbers: list[PlayerCodeId]) -> Optional[str]:
    """Resolve a jersey-number-only name to its box-score player
    (``LineupErrorAnalysisUtils.convert_from_digits``, ``:166-175``).

    Args:
        name: The candidate name; only matches if every character is a
            digit (an empty string is vacuously all-digit, matching Scala's
            ``forall`` on an empty ``String``).
        player_numbers: Candidate ``(code, id)`` pairs -- typically
            ``box_lineup.players_out``, since a number-only PbP mention
            almost always refers to a player who just left the game.

    Returns:
        The matching player's full name, or ``None`` if ``name`` isn't
        all-digit or no code matches.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_models import PlayerCodeId, PlayerId
            from sportsdataverse.mbb.mbb_ncaa_names import convert_from_digits
            codes = [PlayerCodeId(code="1000", id=PlayerId("name1"))]
            convert_from_digits("1000", codes)  # "name1"
    """
    if not all(ch.isdigit() for ch in name):
        return None
    for player_code_id in player_numbers:
        if player_code_id.code == name:
            return player_code_id.id.name
    return None


def tidy_player(p_in: str, ctx: TidyPlayerContext) -> tuple[str, TidyPlayerContext]:
    """Resolve a raw play-by-play name to its box-score full name, via an
    ordered fallback chain (``LineupErrorAnalysisUtils.tidy_player``,
    ``:76-144``). Order is semantic -- ported as ordered first-non-``None``:

    1. Cache hit (see the module docstring's asymmetric-cache note).
    2. Exact box-score code match.
    3. Unique truncated-code match (:attr:`TidyPlayerContext.alt_all_players_map`).
    4. Double-barrel-surname strip retry (``"Smith-Jones"`` -> ``"Jones"``),
       recursing into this same function.
    5. Initials (:func:`convert_from_initials`).
    6. Jersey number (:func:`convert_from_digits`, against
       ``ctx.box_lineup.players_out``).
    7. Truncated-code + inserted-"j"-for-"junior" retry.
    8. Fuzzy match (:func:`fuzzy_box_match`) -- skipped (and normalized to
       ``"Team"``) for the four team-stat-row sentinels.
    9. Identity fallthrough (the input is returned unresolved; a later,
       out-of-scope validation pass is expected to reject it).

    Args:
        p_in: The raw play-by-play name.
        ctx: The lookup context (see :func:`build_tidy_player_context`).

    Returns:
        ``(resolved_name, updated_ctx)`` -- ``updated_ctx`` carries the new
        cache entry.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_names import build_tidy_player_context, tidy_player
            ctx = build_tidy_player_context(box_lineup)
            resolved_name, ctx = tidy_player("MITCHELL,M", ctx)
    """
    cached = ctx.resolution_cache.get(p_in)
    if cached is not None:
        return (cached, ctx)

    player_id = build_player_code(p_in, ctx.box_lineup.team.team)
    p = player_id.id.name  # normalized name

    def with_updated_cache(resolved_p: str) -> TidyPlayerContext:
        return TidyPlayerContext(
            box_lineup=ctx.box_lineup,
            all_players_map=ctx.all_players_map,
            alt_all_players_map=ctx.alt_all_players_map,
            resolution_cache={**ctx.resolution_cache, p: resolved_p},
        )

    resolved: Optional[str] = ctx.all_players_map.get(player_id.code)

    if resolved is None:
        alt = ctx.alt_all_players_map.get(player_id.code)
        if alt is not None and len(alt) == 1:
            resolved = alt[0]

    if resolved is None:
        new_p = _DOUBLE_BARREL_RE.sub(r"\1", p)
        if new_p != p:
            resolved = tidy_player(new_p, ctx)[0]

    if resolved is None:
        resolved = convert_from_initials(p, ctx.all_players_map)

    if resolved is None:
        resolved = convert_from_digits(p, ctx.box_lineup.players_out)

    if resolved is None:
        truncated_code = _truncate_code_1(player_id.code)
        alt2 = ctx.alt_all_players_map.get(truncated_code)
        if alt2 is not None and len(alt2) == 1 and truncated_code:
            junior = truncated_code[0] + "j" + truncated_code[1:]
            resolved = ctx.all_players_map.get(junior)

    if resolved is None:
        if p in _TEAM_TOKENS:
            resolved = "Team"
        else:
            match = fuzzy_box_match(p, list(ctx.all_players_map.values()), str(ctx.box_lineup.team))
            if not isinstance(match, FuzzyMatchError):
                resolved = match

    if resolved is None:
        resolved = p  # rejected later on by the (out-of-scope) validation pass

    return (resolved, with_updated_cache(resolved))


@dataclass
class NoSurnameMatch:
    """No candidate surname fragment scored well enough
    (``NameFixer.NoSurnameMatch``, ``:638-643``).

    Args:
        box_name: The box-score name compared against.
        exact_first_name: A first-name fragment shared verbatim between
            candidate and box name, if any.
        near_first_name: A first-name fragment fuzzy-matching the box
            name's first name, if any (only computed when
            ``exact_first_name`` is absent).
        err: Human-readable diagnostic (debug-only; see the module
            docstring's fuzzy-match-parity note for why its embedded score
            may not byte-match the upstream Java oracle).
    """

    box_name: str
    exact_first_name: Optional[str]
    near_first_name: Optional[str]
    err: str


@dataclass
class WeakSurnameMatch:
    """A surname fragment matched, but the whole-name score fell short of
    :data:`MIN_OVERALL_SCORE` (``NameFixer.WeakSurnameMatch``, ``:644-645``).

    Args:
        box_name: The box-score name compared against.
        score: The whole-name similarity score.
        info: Human-readable diagnostic (debug-only; see the fuzzy-match-
            parity note).
    """

    box_name: str
    score: int
    info: str


@dataclass
class StrongSurnameMatch:
    """A surname fragment matched and the whole-name score cleared
    :data:`MIN_OVERALL_SCORE` (``NameFixer.StrongSurnameMatch``, ``:646-647``).

    Args:
        box_name: The box-score name compared against.
        score: The whole-name similarity score.
    """

    box_name: str
    score: int


#: Union of :func:`box_aware_compare`'s three possible outcomes
#: (``NameFixer.MatchResult``, ``:637``).
MatchResult = Union[NoSurnameMatch, WeakSurnameMatch, StrongSurnameMatch]


def _weighted_ratio(a: str, b: str) -> int:
    """``rapidfuzz.fuzz.WRatio``, rounded to the nearest int (Java's
    ``FuzzySearch.weightedRatio`` returns ``Int``). See the module
    docstring's fuzzy-match-parity note -- this is the Python analog of
    every ``FuzzySearch.weightedRatio(...)`` call in the Scala source.
    ``processor=utils.default_process`` is required: rapidfuzz's ``WRatio``
    does not preprocess by default, but Java's ``weightedRatio`` always
    does, and comma-joined multi-token names mis-bucket without it."""
    return int(round(fuzz.WRatio(a, b, processor=utils.default_process)))


def _remove_jr(fragment: str) -> bool:
    """``True`` unless ``fragment`` is the literal suffix ``"jr."``
    (``box_aware_compare``'s local ``remove_jr``, ``:665``)."""
    return fragment != "jr."


def box_aware_compare(candidate_in: str, box_name_in: str) -> MatchResult:
    """Score how well a single play-by-play candidate name fits a single
    box-score name (``NameFixer.box_aware_compare``, ``:658-766``).

    Args:
        candidate_in: The raw play-by-play name fragment.
        box_name_in: One box-score player's full name (``"Surname, First
            [Middle]"`` format).

    Returns:
        A :class:`StrongSurnameMatch` / :class:`WeakSurnameMatch` /
        :class:`NoSurnameMatch`, per the surname- and whole-name-score
        thresholds.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_names import box_aware_compare
            box_aware_compare("Tuitele, Peanut", "Tuitele, Peanut")
            # StrongSurnameMatch(box_name='Tuitele, Peanut', score=100)
    """
    candidate = candidate_in.lower()
    box_name = box_name_in.lower()

    box_name_decomp = re.split(r"\s*,\s*", box_name, maxsplit=1)
    surname_frags = [f for f in box_name_decomp[0].split(" ") if _remove_jr(f)]
    longest_surname_fragment = max(surname_frags, key=len) if surname_frags else "unknown"

    candidate_frags = [f for f in re.split(r"[, ]+", candidate) if _remove_jr(f)]

    def decompose_first_names(ignore: Optional[str] = None) -> tuple[Optional[str], Optional[str]]:
        filtered_frags = [f for f in candidate_frags if f != ignore]
        candidate_frag_set = set(filtered_frags)
        box_first_names: Optional[list[str]] = None
        if len(box_name_decomp) > 1:
            box_first_names = [f for f in re.split(r"[, ]+", box_name_decomp[1]) if _remove_jr(f)]

        exact_first_name: Optional[str] = None
        if box_first_names is not None and len(box_first_names) == 1:
            single = box_first_names[0]
            if len(single) >= MIN_USEFUL_FIRST_NAME_LEN and single in candidate_frag_set:
                exact_first_name = single

        near_first_name: Optional[str] = None
        if exact_first_name is None and box_first_names is not None and len(box_first_names) == 1:
            single = box_first_names[0]
            if len(single) >= MIN_USEFUL_FIRST_NAME_LEN:
                for frag in filtered_frags:
                    if _weighted_ratio(frag, single) >= MIN_FIRST_NAME_SCORE:
                        near_first_name = frag
                        break

        return (exact_first_name, near_first_name)

    candidate_frag_scores = [(frag, _weighted_ratio(frag, longest_surname_fragment)) for frag in candidate_frags]

    def passes(frag: str, score: int) -> bool:
        if frag == longest_surname_fragment and len(frag) >= (MIN_USEFUL_SURNAME_LEN - 1):
            return True
        if score > MIN_SURNAME_SCORE and len(frag) >= MIN_USEFUL_SURNAME_LEN:
            return True
        if score > MIN_SURNAME_SCORE and len(frag) >= MIN_USEFUL_SURNAME_LEN - 1:
            exact, near = decompose_first_names(frag)
            return exact is not None and near is None
        return False

    filtered = [(frag, score) for frag, score in candidate_frag_scores if passes(frag, score)]
    filtered.sort(key=lambda pair: pair[1], reverse=True)  # stable, matches Scala's sortWith
    best_frag_score = filtered[0] if filtered else None

    if best_frag_score is not None:
        frag, frag_score = best_frag_score
        overall_score = _weighted_ratio(candidate, box_name)
        if overall_score >= MIN_OVERALL_SCORE:
            return StrongSurnameMatch(box_name_in, overall_score)
        info = (
            f"[{candidate}] vs [{box_name}]: Matched [{longest_surname_fragment}] "
            f"with [Some(({frag},{frag_score}))], but overall score was [{overall_score}]"
        )
        return WeakSurnameMatch(box_name_in, overall_score, info)

    exact_first_name, near_first_name = decompose_first_names()
    candidates_str = ";".join(f"({frag},{score})" for frag, score in candidate_frag_scores)
    err = (
        f"[{candidate}] vs [{box_name}]: Failed to find a fragment matching "
        f"[{longest_surname_fragment}], candidates={candidates_str}"
    )
    return NoSurnameMatch(box_name_in, exact_first_name, near_first_name, err)


@dataclass
class FuzzyMatchError:
    """A failed :func:`fuzzy_box_match` resolution (Scala's ``Left[String]``
    half of ``Either[String, String]`` -- Python has no ``Either``, so the
    error is returned directly; check ``isinstance(result, FuzzyMatchError)``,
    matching the ``parse_team_name`` / :class:`~sportsdataverse.mbb.mbb_ncaa_data_quality.ParseError`
    convention already used in this port).

    Args:
        message: Human-readable description of why no name won.
    """

    message: str


def fuzzy_box_match(candidate: str, unassigned_box_names: list[str], team_context: str) -> Union[str, FuzzyMatchError]:
    """Pick the single unassigned box-score name a mis-spelled play-by-play
    name most likely refers to (``NameFixer.fuzzy_box_match``, ``:774-905``).

    Resolution order: a single strong match wins outright; multiple strong
    matches only resolve if there's a clear (>10-point) winner; failing
    that, a single weak match wins; failing that, a first-name-only match
    only wins if there are no other first-name matches (exact or fuzzy)
    among the un-matched box names.

    Args:
        candidate: The raw play-by-play name.
        unassigned_box_names: Box-score full names not yet claimed by
            another resolution.
        team_context: Debug-only context string (Scala used it to
            de-duplicate diagnostic prints; this port has no logging
            surface to de-duplicate, so the value is otherwise unused).

    Returns:
        The winning box-score name, or a :class:`FuzzyMatchError` describing
        why no single name won.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_names import fuzzy_box_match
            fuzzy_box_match(
                "sirena tuitele",
                ["Suitele, Sirena", "Tuitele, Peanut", "Guity, Amaya"],
                "team_context",
            )
            # "Suitele, Sirena"
    """
    matches = [box_aware_compare(candidate, box_name) for box_name in unassigned_box_names]

    strong: list[StrongSurnameMatch] = []
    weak: list[WeakSurnameMatch] = []
    first_name_only: list[NoSurnameMatch] = []
    no_match: list[NoSurnameMatch] = []

    for match in matches:
        if isinstance(match, StrongSurnameMatch):
            strong.append(match)
        elif isinstance(match, WeakSurnameMatch):
            weak.append(match)
        elif match.exact_first_name is not None:
            first_name_only.append(match)
        else:
            no_match.append(match)

    if strong:
        if len(strong) == 1:
            return strong[0].box_name
        sorted_strong = sorted(strong, key=lambda m: m.score, reverse=True)
        best = sorted_strong[0]
        threshold_score = best.score - 10
        new_candidates = [m for m in sorted_strong[1:] if m.score > threshold_score]
        if new_candidates:
            return FuzzyMatchError(f"ERROR.1A: multiple strong matches: [{candidate}] vs {sorted_strong}")
        return best.box_name

    if weak:
        if len(weak) == 1:
            return weak[0].box_name
        return FuzzyMatchError(f"ERROR.2A: multiple weak matches: [{candidate}] vs {weak}")

    if first_name_only:
        if len(first_name_only) > 1:
            return FuzzyMatchError(f"ERROR.3A: multiple first name matches: [{candidate}] vs {first_name_only}")
        if any(m.near_first_name is not None for m in no_match):
            bad_l2 = [m for m in no_match if m.near_first_name is not None]
            return FuzzyMatchError(f"ERROR.3B: multiple near first name matches: [{candidate}] vs {bad_l2}")
        # DELIBERATE DIVERGENCE from Scala ":890-897", which retired this
        # branch as a false-positive risk and always errored -- even though
        # it had already established a UNIQUE exact-first-name match with no
        # near-miss rivals. That rejection is what deletes name-changed
        # players: the NCAA retro-updates the roster/box to a player's
        # CURRENT surname while the play-by-play keeps the surname as of the
        # game, so `EATON, LEXI` (pbp) never matches `Rydalch, Lexi` (box).
        # The surname gate scores those at zero, so the first name is the
        # only signal left -- and the two guards above already require it to
        # be unique and unrivalled.
        #
        # Measured before flipping, over an 800-game sample (WBB 2015 + MBB
        # 2015, ~4.3k and ~4.0k successful matches respectively). The branch
        # fires on 13 unique (team, pbp_name, box_name) triples, ALL correct:
        #
        #   9  surname changes  EATON->Rydalch, BROADHEAD->Devashrayee,
        #                       FULLER->Nielson, MORRISON->Pulsipher,
        #                       OWENS->Mitchell, GORDON->Christensen,
        #                       SIMS->Harris, HERZBERG->Howell,
        #                       MCDOWELL->Michael
        #   3  misspellings     QEDAN->Qeden, ADEJINI->Adeniji, GRAY->Gary
        #   1  scorer typo      `KELLEY,RYAN Enters Game` (once) on a Siena
        #                       roster carrying `Oliver, Ryan` (31 pbp
        #                       mentions) and no Kelley on EITHER roster
        #
        # Zero false positives. The men's side is the control: far fewer
        # surname changes, and it fired only twice in 400 games.
        #
        # Rejecting these is not the safe option -- it is its own corruption.
        # An unmatched sub leaves the on-court set at 4 or 6, every stint is
        # flagged `player_count_error`, and the team yields ZERO good stints
        # for the game. BYU 2014-15 had three name-changed players at once
        # and produced lineups for only 9 of 33 games.
        #
        # ERROR.3A (ambiguous) and ERROR.3B (near-miss rival) still reject.
        return first_name_only[0].box_name

    return FuzzyMatchError("ERROR.4A: no good matches")


def _name_key(name: str) -> str:
    """Case- and spacing-insensitive key for roster/pbp name comparison.

    `Woods, Trevin` (roster) and `WOODS,TREVIN` (play-by-play) are the same
    person; only case and the space after the comma differ.
    """
    return re.sub(r"\s+", "", name).casefold()


def code_from_box(name: str, box_lineup: LineupEvent, team: Optional[TeamId] = None) -> PlayerCodeId:
    """Resolve a tidied player NAME to the box roster's own ``PlayerCodeId``.

    :func:`~sportsdataverse.mbb.mbb_ncaa_stints.build_player_code` is a
    faithful ``ExtractorUtils.scala`` port and keys a player as
    ``{first-two-letters}{Surname}``. When two teammates collide on that --
    siblings, overwhelmingly --
    :func:`~sportsdataverse.mbb.mbb_ncaa_boxscore_parser.validate_box_score`
    widens BOTH to full-name codes so the game is not thrown away.

    Re-deriving a code from the tidied name after that point silently undoes
    the widening: both Morris twins code back to ``MaMorris``, one of them
    wins the match, and the other DISAPPEARS from the lineup events. Kansas
    2010 parsed 110 events with ``MarcusMorris`` present 18 times and
    ``MarkieffMorris`` present ZERO times -- a game that looks healthy by
    every count while a starter is missing.

    So the roster is the authority. **Every PBP-side path that needs a code
    for a name must call this, never** ``build_player_code``.

    Args:
        name: The tidied player name, as produced by :func:`tidy_player`.
        box_lineup: The team's box-score
            :class:`~sportsdataverse.mbb.mbb_ncaa_models.LineupEvent`, whose
            ``players`` carry the (possibly widened) codes.
        team: Team context for the fallback ``build_player_code`` call, used
            only when ``name`` is not on the roster.

    Returns:
        The roster's
        :class:`~sportsdataverse.mbb.mbb_ncaa_models.PlayerCodeId` when
        ``name`` is on it, else a freshly built one.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_names import code_from_box
            code_from_box("Morris, Markieff", box_lineup, box_lineup.team.team)

        The distinction that matters::

            from sportsdataverse.mbb.mbb_ncaa_stints import build_player_code
            build_player_code("Morris, Markieff", team).code  # "MaMorris" -- collides
            code_from_box("Morris, Markieff", box_lineup, team).code  # "MarkieffMorris"

    See Also:
        * `bigballR`_ -- the R sibling whose NCAA lineup engine this ports.

    .. _bigballR: https://github.com/jflancer/bigballR
    """
    for player in box_lineup.players or []:
        if player.id.name == name:
            return player

    # Fall back to a case/space-insensitive comparison before deriving. The
    # roster spells a player `Woods, Trevin`; the play-by-play spells the same
    # person `WOODS,TREVIN`, and not every caller hands us a tidied name. An
    # exact-only match sends those straight to `build_player_code`, which is
    # precisely the re-derivation this function exists to prevent -- LIU
    # 2014-15's Woods brothers both landed back on `TrWoods` that way.
    # Case is not load-bearing in NCAA player names (see CLAUDE.md).
    key = _name_key(name)
    hits = [p for p in box_lineup.players or [] if _name_key(p.id.name) == key]
    if len(hits) == 1:
        return hits[0]
    return build_player_code(name, team)
