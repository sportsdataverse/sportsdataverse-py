"""NCAA data-quality curated tables + ``ParseError`` (cbb-explorer port).

Faithful port of hoop-explorer's ``cbb-explorer`` (Scala 2.12, ``utest``,
package ``org.piggottfamily.cbb_explorer``) data-quality layer -- the first
of six Phase-5b modules. **Task 5b.1 ports the curated data tables**
(``DataQualityIssues.scala``) and the minimal error-reporting scaffolding
(``ParseError.scala`` + ``ParseUtils.build_sub_error``) that
``build_player_code`` / ``parse_team_name`` (Task 5b.2) and the
``tidy_player`` name-resolution chain (Task 5b.3) consume on every
substitution event.

**Scope is mostly data, not logic** -- the module's job is to transcribe
``DataQualityIssues.scala`` verbatim: a wrong entry silently mis-codes a
real player. Every table below was checked line-by-line against the Scala
source and cross-verified against ``DataQualityIssuesTests.scala``'s inline
oracle literals (the ``combos``/``alias_combos`` shape assertions).

**``misspellings`` fallback semantics (merged, not two-level).** The Scala
value is::

    val misspellings: Map[Option[TeamId], Map[String, String]] =
      Map(<team-specific entries>).mapValues(_ ++ generic_misspellings)
        .withDefault(_ => generic_misspellings)

This is a **precomputed per-team merge**, not a runtime two-level lookup:
at construction time every team-specific map already has
``generic_misspellings`` folded in (Scala's ``++`` means the *right-hand*
map's entries win on key collision, so ``generic_misspellings`` would win
over a team-specific entry sharing the same key -- moot today since
``generic_misspellings`` is empty, but the merge direction is preserved
here for fidelity). A team **not** present in the outer map falls back to
the plain (unmerged) ``generic_misspellings`` via Scala's ``withDefault``.
:func:`misspellings` reproduces exactly this: team hit → the precomputed
merge; team miss (including ``team=None``, which is never a real key in
the Scala literal either) → a copy of :data:`generic_misspellings`.

**``players_missing_from_boxscore`` (Task 5e.2 addition).** Deferred by
Task 5b.1 (its only caller, ``BoxscoreParser.scala:245``'s
``inject_validated_players``, was out of scope then); ported now verbatim
alongside the ``BoxscoreParser`` port that consumes it
(``mbb_ncaa_boxscore_parser.py``).

**Landmine index (reachable scalar division).** None. Every operation in
this module is dict/list construction, string concatenation, or
``str.upper``/``str.lower`` -- no division site exists to enumerate.

Apache-2.0 third-party port — see the ``NOTICE`` file at the repository root for the upstream copyright and full attribution.

Example::

    from sportsdataverse.mbb.mbb_ncaa_data_quality import (
        ParseError,
        build_sub_error,
        misspellings,
    )

    err = build_sub_error("team", error="Could not match team names")
    err.id  # '[team]'

    ParseError.single("", "[value]", "Failed to locate a numeric field")

See Also:
    * `hoopR <https://hoopR.sportsdataverse.org>`_ -- R men's basketball companion package
    * `wehoop <https://wehoop.sportsdataverse.org>`_ -- R women's basketball companion package
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from sportsdataverse.mbb.mbb_ncaa_models import TeamId, Year

__all__ = [
    "ParseError",
    "build_sub_error",
    "enrich_sub_error",
    "enrich_sub_errors",
    "combos",
    "fix_combos",
    "alias_combos",
    "generic_misspellings",
    "misspellings",
    "players_with_duplicate_names",
    "players_missing_from_boxscore",
    "team_aliases",
    "team_name_equivalents",
    "same_school",
]


@dataclass
class ParseError:
    """A parse-time error (``ParseError``, ``ParseError.scala:9``).

    Args:
        location: The module in which the error occurred.
        id: The module-specific id for which the error occurred.
        messages: Human-readable description(s) of the error.
    """

    location: str
    id: str
    messages: list[str]

    @classmethod
    def single(cls, location: str, id: str, message: str) -> "ParseError":
        """Single-message convenience constructor (``ParseError.apply``,
        ``ParseError.scala:19-21`` -- the companion object's single-``message``
        overload). Python has no method overloading, so this is a
        ``classmethod`` rather than a second constructor signature.

        Args:
            location: The module in which the error occurred.
            id: The module-specific id for which the error occurred.
            message: A single human-readable description of the error.

        Returns:
            A :class:`ParseError` with ``messages=[message]``.
        """
        return cls(location, id, [message])


def _build_error_id(value: str) -> str:
    """Wrap a non-empty id fragment in brackets, else ``""``
    (``ParseUtils.build_error_id``, ``ParseUtils.scala:57``)."""
    return f"[{value}]" if value else ""


def build_sub_error(*subids: str, error: str) -> ParseError:
    """Build a location-less :class:`ParseError` from id fragments
    (``ParseUtils.build_sub_error``, ``ParseUtils.scala:83-85``, delegating
    through ``build_error``/``build_errors``/``build_error_id`` with
    ``location=""``/``base_id=""``; the ``shapeless``-based
    ``sequence_kv_results`` accumulation machinery in the same file is out
    of scope).

    Scala's call shape is curried -- ``build_sub_error("team")("message")``
    (two argument groups: varargs ``subids``, then a single ``error``
    string). Python has no currying sugar for that shape, so ``subids`` is
    a plain ``*args`` tuple and ``error`` is a required keyword-only
    argument at the same call site.

    Args:
        *subids: Zero or more id fragments; each non-empty fragment is
            wrapped in ``[...]`` and the results concatenated.
        error: The single human-readable error message.

    Returns:
        A :class:`ParseError` with ``location=""`` and
        ``messages=[error]``.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_data_quality import build_sub_error

            err = build_sub_error("team", error="Could not match team names")
            err.id  # '[team]'
    """
    return ParseError(location="", id="".join(_build_error_id(s) for s in subids), messages=[error])


def enrich_sub_errors(location: str, base_id: str, errors: list[ParseError]) -> list[ParseError]:
    """Adds top-level location information to a list of sub-errors generated
    by a child parser (``ParseUtils.enrich_sub_errors``, ``ParseUtils.scala:87-89``).

    Args:
        location: The module in which the (now top-level) error occurred.
        base_id: An id fragment prepended (bracket-wrapped, if non-empty) to
            each error's existing ``id``.
        errors: The child-parser errors to enrich (their ``location`` is
            **replaced**, not merged -- matching the Scala's ``ParseError(location,
            ..., error.messages)`` construction, which discards the child's
            own ``location``).

    Returns:
        A new list of :class:`ParseError`, one per input error, each with
        ``location`` set to ``location`` and ``id`` set to
        ``build_error_id(base_id) + error.id``.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_data_quality import build_sub_error, enrich_sub_errors

            child_err = build_sub_error("game_time", error="Could not find time")
            enrich_sub_errors("ncaa.parse_playbyplay", "", [child_err])
    """
    return [
        ParseError(location=location, id=_build_error_id(base_id) + error.id, messages=error.messages)
        for error in errors
    ]


def enrich_sub_error(location: str, base_id: str, error: ParseError) -> list[ParseError]:
    """Adds top-level location information to a single sub-error, returning a
    list for consistency (``ParseUtils.enrich_sub_error``, ``ParseUtils.scala:91-93``).

    Args:
        location: The module in which the (now top-level) error occurred.
        base_id: An id fragment prepended (bracket-wrapped, if non-empty) to
            ``error``'s existing ``id``.
        error: The child-parser error to enrich.

    Returns:
        :func:`enrich_sub_errors` applied to a single-element ``[error]``
        list.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_data_quality import build_sub_error, enrich_sub_error

            child_err = build_sub_error("game_score", error="Could not find score")
            enrich_sub_error("ncaa.parse_playbyplay", "", child_err)
    """
    return enrich_sub_errors(location, base_id, [error])


def combos(first: str, last: str) -> list[str]:
    """Generate the three name-string variants NCAA sources use for one
    player (``DataQualityIssues.combos``, ``DataQualityIssues.scala:330-337``).

    The Scala signature takes a single ``(String, String)`` tuple, but every
    call site (including the ``fix_combos``/``alias_combos`` helpers below
    and the upstream ``DataQualityIssuesTests`` oracle) invokes it with two
    positional arguments via Scala's tuple auto-conversion -- ported here as
    a plain two-argument function since Python has no such conversion.

    Args:
        first: The player's first name.
        last: The player's last name.

    Returns:
        ``[f"{last}, {first}", f"{first} {last}",
        f"{last.upper()},{first.upper()}"]`` -- new-box, new-PbP, and
        old-box/legacy-PbP formats respectively.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_data_quality import combos

            combos("Makhi", "Mitchell")
            # ['Mitchell, Makhi', 'Makhi Mitchell', 'MITCHELL,MAKHI']
    """
    return [f"{last}, {first}", f"{first} {last}", f"{last.upper()},{first.upper()}"]


def fix_combos(first: str, last: str, code_start: Optional[str] = None) -> list[tuple[str, Optional[str]]]:
    """Pair each of :func:`combos`' three name variants with a shared
    player-code override (``DataQualityIssues.fix_combos``,
    ``DataQualityIssues.scala:340-346``).

    Args:
        first: The player's first name.
        last: The player's last name.
        code_start: The forced player-code prefix for every variant, or
            ``None`` to leave the default ``build_player_code`` truncation
            behavior in place.

    Returns:
        Three ``(name_variant, code_start)`` pairs.
    """
    return [(name, code_start) for name in combos(first, last)]


def alias_combos(first: str, last: str, to_name: str) -> dict[str, str]:
    """Pair each of :func:`combos`' three name variants with a shared alias
    target (``DataQualityIssues.alias_combos``, ``DataQualityIssues.scala:351-356``).

    Args:
        first: The player's first (mis-recorded) first name.
        last: The player's (mis-recorded) last name.
        to_name: The canonical ``"Lastname, Firstname"`` this player should
            resolve to.

    Returns:
        A dict mapping each of the three name variants to ``to_name``.
    """
    return {name: to_name for name in combos(first, last)}


# ---------------------------------------------------------------------------
# team_aliases -- teams that changed names mid-season
# (DataQualityIssues.scala:9-13)
# ---------------------------------------------------------------------------

team_aliases: dict[Year, dict[TeamId, TeamId]] = {
    Year(2021): {TeamId("NIU"): TeamId("Northern Ill.")},
}
"""Season-scoped team renames (``DataQualityIssues.team_aliases``)."""


team_name_equivalents: tuple[frozenset[str], ...] = (
    frozenset({"New Orleans", "LSU New Orleans"}),
    frozenset({"NIU", "Northern Ill."}),
)
"""Names that denote the SAME school, as an equivalence -- not a rewrite.

Distinct from :data:`team_aliases`, which models a mid-season RENAME and
rewrites one name to another. These are two spellings that coexist: the
box-score page and the schedule disagree, and WHICH side uses WHICH varies by
season. A directional rewrite therefore fixes one era and breaks another --
mapping page `New Orleans` -> `LSU New Orleans` repaired 2015 (schedule says
`LSU New Orleans`) and immediately broke 2024, where the schedule itself says
`New Orleans`. Equivalence has no direction, so it holds in both eras without
a season key.

Every entry was MEASURED. Bucketing team-match failures across 2,800 sampled
games (MBB + WBB, seasons 2011/2015/2019/2023) found exactly one genuine name
mismatch in either league:

    MBB   0 distinct mismatches
    WBB   6 distinct, ALL `LSU New Orleans` -- the page says `New Orleans`
          against `McNeese`, `West Ala.`, `Centenary (LA)`, `ULM`,
          `Pittsburgh`, `UTEP`

The far more common failure (456 MBB / 333 WBB events) was NOT an alias at
all: the box page names only ONE team when the opponent is non-D-I, handled
in :func:`~sportsdataverse.mbb.mbb_ncaa_stints.parse_team_name` with the
caller's known sides.

The `NIU` / `Northern Ill.` class fixes the INHERITED :data:`team_aliases`
entry ``Year(2021): {NIU -> Northern Ill.}``, which has this exact
directional flaw. Measured on real games, that rewrite is a perfect trade --
it repairs one direction by breaking the other:

    season 2021-22 (alias active)  target `NIU` FAIL x3   `Northern Ill.` OK x3
    season 2015    (no alias)      target `NIU` OK        `Northern Ill.` FAIL

Both targets occur in the SAME season, so no directional rewrite can be
right. The class is ADDITIVE: the rewrite still fires and `same_school` then
matches the rewritten name against either spelling, so the inherited entry is
left untouched rather than diverging further from the Scala oracle.

Surfaced by the skip ledger during the corpus re-parse -- 6 events, all NIU,
all with BOTH titles present and one exactly equal to the target.

Every remaining `team_aliases` entry is a directional rewrite and can fail the
same way in the season its target uses the other spelling; auditing them all
is tracked separately.

Do NOT add a fuzzy team matcher here. `Miami (FL)` / `Miami (OH)`,
`New Orleans` / `Southern-N.O.` and `Loyola (IL)` / `Loyola (MD)` are
distinct schools whose names differ by less than a typo, and a silently wrong
team is far worse than a dropped game.
"""


def same_school(a: str, b: str) -> bool:
    """Whether two team-name spellings denote the same school.

    Exact match, or both names inside one :data:`team_name_equivalents` class.

    Args:
        a: One team-name spelling.
        b: The other team-name spelling.

    Returns:
        ``True`` if the two names refer to the same school.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_data_quality import same_school
            same_school("New Orleans", "LSU New Orleans")  # True
            same_school("Miami (FL)", "Miami (OH)")        # False
    """
    if a == b:
        return True
    return any(a in cls and b in cls for cls in team_name_equivalents)


# ---------------------------------------------------------------------------
# players_with_duplicate_names -- forced player-code overrides
# (DataQualityIssues.scala:36-160)
# ---------------------------------------------------------------------------

# Each tuple is (first, last, code_start) -- transcribed verbatim, in
# upstream source order, from the `fix_combos(...) ++ fix_combos(...) ++ ...`
# chain. `code_start=None` means "use build_player_code's default first/last
# initial truncation"; an explicit string forces that player-code prefix.
_DUPLICATE_NAME_SPECS: list[tuple[str, str, Optional[str]]] = [
    # Mitchell brothers (Maryland 19/20 / RI 20/21)
    ("Makhi", "Mitchell", None),
    ("Makhel", "Mitchell", None),
    # Hamilton brothers (BC 18-19/20)
    ("Jared", "Hamilton", None),
    ("Jairus", "Hamilton", None),
    # Wisconsin team-mates, give Jordan "Jn" and Jonathan/Johnny gets "Jo"
    ("Jordan", "Davis", None),
    # Cumberland relatives (Cinci) -- both major players, use misspellings
    # to truncate Jaev's name instead of picking a favorite
    ("Jaev", "Cumberland", None),
    ("Jarron", "Cumberland", None),
    # Men 20/21
    # Bama 20/21 - Quinerly bros(?) Jahvon and Jaden, leave Jahvon with Ja,
    # Jaden gets Jn
    ("Jaden", "Quinerly", None),
    # Wichita St 20/21 - Trey and Trevin Wade. Trevin gets Tn
    ("Trevin", "Wade", None),
    # Ohio 20/21 - Miles and Michael Brown. Michael gets Ml
    ("Michael", "Brown", None),
    # Men 22/23
    # Arizona St. 2022/23, 2x Cambridge transfers (change both)
    ("Devan", "Cambridge", None),
    ("Desmond", "Cambridge Jr.", None),
    # Kansas City, 2022/23, Precious and Promise Idiaru (change both)
    ("Precious", "Idiaru", None),
    ("Promise", "Idiaru", None),
    # CSU Bakersfield 2022/23, Kareem and Kaseem Watson -- Kareem->Ka,
    # Kaseem->Ks (see also under misspellings)
    ("Kas", "Watson", None),
    # Seton Hall 2022/23, new! JaQuan Harris, old Jamir Harris
    ("JaQuan", "Harris", None),
    # Southern U 2022/23, Jaronn+Jariyon Wilkens -- Jariyon->Jy, Jaronn keeps Ja
    ("Jariy", "Wilkens", None),
    # Men 23/24
    # Colorado St. 2023/24, Kyle and Kyan Evans. Kyan's a Fr, sorry kid
    ("Kyan", "Evans", None),
    # Troy 2023/24 Cooper and Cobi Campbell
    ("Cooper", "Campbell", None),
    # CSU Bakersfield 2023/24, "Marvin McGhee III" (Sr) and "Marvin McGhee IV" (So)
    ("Marvin", "McGhee IV", None),
    # Coppin St 2023/24 Cam'Ron and Car'Ron Brown, both Jrs with similar stats
    ("Car'Ron", "Brown", None),
    # Women 18/19
    # Women 2018 Wash St. Molina - Chanelle v Cherilyn (leave Chanelle else
    # will conflict with 3rd sister Celena!)
    ("Cherilyn", "Molina", None),
    # Women 19/20
    # Women 2019 Cinci - Scott, Jadyn / Jada
    ("Jadyn", "Scott", None),
    ("Jada", "Scott", None),
    # Women 2019 Memphis - Williams, Lanetta / Lanyce (->Le)
    ("Lanyce", "Williams", None),
    # Women 20/21
    # Gonzaga 2020/21 (W) "Truong, Kaylynne" and "Truong, Kayleigh" - Kayleigh
    # is the starter
    ("Kaylynne", "Truong", None),
    # Women 21/22
    # Florida 2021/22 Tatiana and Taliyah Wyche -- Taliyah be Th, else both Ta!
    ("Taliyah", "Wyche", None),
    # Syracuse 2021/22 Christianna and Chrislyn Carr .. both starting!
    ("Chrislyn", "Carr", None),
    # Women 22/23
    # Miami 22/23: Haley and Hannah Cavinder, both from Fresno St (change both)
    ("Haley", "Cavinder", None),
    ("Hannah", "Cavinder", None),
    # Women 23/24
    # ECU 23/24: as well as the Wyche sisters, Khia and Khloe Miller
    ("Khloe", "Miller", None),
    # From 25/26 on, calculated programmatically -- Women 25/26
    ("Stacy", "Utomi", None),
    ("Taylor", "Barbot", None),
    ("Kallie", "Peppler", None),
    ("Chloe", "Gannon", None),
    ("Macie", "Warren", None),
    ("Hana", "Abdel Aal", "Hn"),
    ("Haya", "Abdel Aal", "Hy"),
    ("Maliyah", "Johnson", None),
    ("Alivia", "Cox", "Av"),
    ("Alexis", "Cox", "Ax"),
    ("Makensie", "Charles", "Me"),
    ("Makayla", "Charles", "My"),
    # Men 25/26
    ("Jayden", "Ross", None),
    ("Cooper", "Bowser", None),
    ("Cameron", "Boozer", "Cm"),
    ("Cayden", "Boozer", "Cy"),
    ("Dominykas", "Butka", None),
]


def _build_duplicate_name_table() -> dict[str, Optional[str]]:
    """Flatten :data:`_DUPLICATE_NAME_SPECS` into the lower-cased lookup
    table (``.map { case (name, fix) => (name.toLowerCase, fix) }.toMap``,
    ``DataQualityIssues.scala:158-160``).

    A later spec's variant overwrites an earlier one on key collision --
    matching Scala's ``List[(String, T)].toMap`` (last-listed value wins),
    reproduced here by iterating the specs in source order and assigning
    into a plain dict.
    """
    table: dict[str, Optional[str]] = {}
    for first, last, code_start in _DUPLICATE_NAME_SPECS:
        for name, code in fix_combos(first, last, code_start):
            table[name.lower()] = code
    return table


players_with_duplicate_names: dict[str, Optional[str]] = _build_duplicate_name_table()
"""Lower-cased full-name -> forced player-code-prefix override, or ``None``
to use ``build_player_code``'s default first/last-initial truncation
(``DataQualityIssues.players_with_duplicate_names``)."""


# ---------------------------------------------------------------------------
# misspellings -- team-scoped (+ generic) name corrections
# (DataQualityIssues.scala:165-325)
# ---------------------------------------------------------------------------

generic_misspellings: dict[str, str] = {}
"""Common misspellings applying to every team -- currently none
(``DataQualityIssues.generic_misspellings``)."""

_MISSPELLINGS_BY_TEAM: dict[TeamId, dict[str, str]] = {
    # ---- PBP misspellings: too hard to resolve at the source ----
    TeamId("Ark.-Pine Bluff"): {  # SWAC -- wrong in the PBP, 2019/29 (sic, per upstream comment)
        "PATTERSON,OMAR": "Parchman, Omar",
    },
    TeamId("Wichita St."): {  # AAC -- wrong in the PBP, 2018/19
        "CHA,ISAIAH POOR": "Poor Bear-Chandler, Isaiah",
    },
    TeamId("LSU"): {  # SEC -- wrong in the PBP, W 2024/25
        "Johnson, Flau'Jae": "Johnson, Flau'jae",
    },
    # ---- Roster/box misspellings ----
    TeamId("BYU"): {  # MWC -- roster/box name difference (W) 2020/21
        **alias_combos("Babalu", "Ugwu", "Stewart, Babalu"),
    },
    TeamId("Morgan St."): {  # MEAC -- roster/box name difference 2020/21
        # (appears twice, byte-identical, in the upstream literal -- a
        # harmless copy/paste duplicate, transcribed once here)
        "Devonish, Sherwyn": "Devonish-Prince, Sherwyn",
    },
    TeamId("NJIT"): {  # America East -- box score misspelling 23/24
        "Lewal, Levi": "Lawal, Levi",
    },
    # ---- Both PBP and box ----
    TeamId("Fordham"): {  # A10 -- nickname (Josh "Colon" Navarro) used in box + PbP
        **alias_combos("Josh", "Colon", "Navarro, Josh"),
    },
    TeamId("Southern California"): {  # PAC-12 -- married/maiden-name confusion 2023/24
        "Darius, Dominique": "Onu, Dominique",
        "Dominique Darius": "Onu, Dominique",
    },
    TeamId("Florida"): {  # SEC -- married during the season, switch to married name
        **alias_combos("Alexia", "Mobley", "Gassett, Alexia"),
    },
    TeamId("Maryland"): {  # Big Ten -- confusion over name
        **alias_combos("Guillermo", "Del Pino Luque", "Del Pino, Guillermo"),
    },
    TeamId("Idaho"): {  # confusion over name
        **alias_combos("Lorena", "Vitoria Barbosa Anunciacao", "Barbosa, Lorena"),
    },
    TeamId("Dayton"): {  # confusion over name
        **alias_combos("Grace", "Talle", "Talle, MG"),
    },
    TeamId("California Baptist"): {  # confusion over name
        **alias_combos("Sofia", "Alonso", "Alonso Hidalgo, Sofia"),
    },
    TeamId("Portland"): {  # married just before season
        **alias_combos("Nicole", "Rodriguez", "Anderson, Nicole"),
    },
    # ---- Verbal commits ----
    TeamId("Oral Roberts"): {  # Summit -- nickname (Josh "Colon" Navarro comment reused upstream)
        **alias_combos("Max", "Abams", "Abmas, Max"),
    },
    # ---- Hack to workaround duplicate name ----
    TeamId("Cincinnati"): {  # the Cumberlands have caused quite a mess!
        "CUMBERLAND,J": "Cumberland, Jarron",  # legacy typo, just in case
        **alias_combos("Jaevin", "Cumberland", "Cumberland, Jaev"),
    },
    TeamId("CSU Bakersfield"): {  # the Watsons have caused quite a mess!
        **alias_combos("Kaseem", "Watson", "Watson, Kas"),
    },
    TeamId("Delaware St."): {  # transferred here 2023/24 -- same Watson issue
        **alias_combos("Kaseem", "Watson", "Watson, Kas"),
    },
    TeamId("Southern U."): {  # the Wilkens have caused quite a mess!
        **alias_combos("Jariyon", "Wilkens", "Wilkens, Jariy"),
    },
}

_MISSPELLINGS_MERGED: dict[TeamId, dict[str, str]] = {
    team: {**team_map, **generic_misspellings} for team, team_map in _MISSPELLINGS_BY_TEAM.items()
}
"""Each team's map with :data:`generic_misspellings` folded in --
``generic_misspellings`` wins on key collision, matching Scala's
``team_map ++ generic_misspellings`` merge direction (moot today since
``generic_misspellings`` is empty)."""


def misspellings(team: Optional[TeamId]) -> dict[str, str]:
    """Team-scoped misspelling map, falling back to the generic map
    (``DataQualityIssues.misspellings``, ``DataQualityIssues.scala:165-322``
    -- see the module docstring's "fallback semantics" note for why this is
    a precomputed merge, not a runtime two-level lookup).

    Args:
        team: The team to look up team-specific corrections for. ``None``
            (like any team absent from the table) falls back to
            :data:`generic_misspellings`.

    Returns:
        A fresh dict -- the team's misspelling map merged with
        :data:`generic_misspellings`, or a copy of
        :data:`generic_misspellings` if ``team`` has no specific entries.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_data_quality import misspellings
            from sportsdataverse.mbb.mbb_ncaa_models import TeamId

            misspellings(TeamId("NJIT"))["Lewal, Levi"]  # 'Lawal, Levi'
            misspellings(TeamId("Some Unlisted Team"))  # {} (generic fallback)
            misspellings(None)  # {} (generic fallback)
    """
    if team is None:
        return dict(generic_misspellings)
    return _MISSPELLINGS_MERGED.get(team, dict(generic_misspellings))


# ---------------------------------------------------------------------------
# players_missing_from_boxscore -- players whose box-score row is absent
# entirely (DataQualityIssues.scala:16-34). Task 5e.2 addition -- deferred by
# Task 5b.1 since its only consumer, BoxscoreParser.inject_validated_players,
# was out of scope then.
# ---------------------------------------------------------------------------

players_missing_from_boxscore: dict[TeamId, dict[Year, list[str]]] = {
    TeamId("La Salle"): {  # A10
        Year(2018): ["Cooney, Kyle", "Shuler, Johnnie", "Kuhar, Chris", "Joseph, Dajour"],
    },
    TeamId("Morgan St."): {  # MEAC
        Year(2020): ["McCray-Pace, Lapri"],
    },
    TeamId("Xavier"): {  # BE
        Year(2018): ["Vanderpohl, Nick"],
    },
    TeamId("St. Bonaventure"): {  # A10
        Year(2023): ["Essamvous, Assa"],
    },
}
"""Team/season-scoped players known to be entirely missing from a box-score
page's player table (``DataQualityIssues.players_missing_from_boxscore``) --
manually appended by ``BoxscoreParser.inject_validated_players`` as extra
lineup entries alongside the roster/other-source fallbacks."""
