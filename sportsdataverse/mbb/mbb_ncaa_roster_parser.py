"""NCAA roster HTML parser (cbb-explorer port).

Faithful port of hoop-explorer's ``cbb-explorer`` (Scala 2.12, ``utest``,
package ``org.piggottfamily.cbb_explorer``) ``RosterParser.scala`` -- the
second of six Phase-5e modules and the first to consume the shared
:mod:`sportsdataverse.mbb.mbb_ncaa_html` selector helpers. Ports
:func:`parse_roster` (turns a saved NCAA roster page into a list of
:class:`~sportsdataverse.mbb.mbb_ncaa_models.RosterEntry`) and
:func:`get_unified_ncaa_id` (extracts a player's lowest cross-season NCAA id
from a player page).

**v0/v1 selector tables.** Two eras of the NCAA stats site ship different
HTML shapes for the same roster data -- ``version_format=0`` (legacy,
``table#stat_grid``) and ``version_format=1`` (2018+, ``table.dataTable``).
Both eras are ported as a pair of finder-function bundles
(``_BUILDERS_V0`` / ``_BUILDERS_V1``, indexed by ``version_format`` exactly
like the Scala's ``builders_array = Array(builders_v0, builders_v1)``) so
:func:`parse_roster` itself is version-agnostic. Every ``td:eq(N)`` selector
(unsupported by soupsieve) becomes a :func:`~sportsdataverse.mbb.mbb_ncaa_html.td_at`
call -- see that module for the full JSoup -> bs4 translation table.

**Scala Map iteration-order emulation (the load-bearing subtlety here).**
``RosterParser.parse_roster`` deduplicates same-name roster rows via a
``foldLeft`` into a ``scala.collection.immutable.Map[PlayerCodeId,
RosterEntry]``, then reads back ``.values.toList`` before a final
``.sortWith(_.gp > _.gp)`` (**gp-only** -- ties are NOT broken by any
explicit secondary key). Because Scala's ``sortWith`` is a stable sort, the
relative order of same-``gp`` entries in the final result is exactly
whatever order the ``Map``'s ``.values.toList`` produced -- and for a
``Map`` beyond 4 entries, Scala silently upgrades from the small
insertion-ordered ``Map1``..``Map4`` representations to a genuine
``HashMap`` (a hash-array-mapped trie), whose iteration order is a
deterministic function of each key's (smeared) hash code, **not** insertion
order. ``sample_roster.html``'s 15-player roster hits this exactly (the
gp=20 tie group's expected order in ``RosterParserTests`` -- Eytle-Rock,
Spasojevic, Rogers, Kennedy -- matches neither jersey/HTML-row order nor
alphabetical/code order), so faithfully reproducing the oracle's exact
output order requires reproducing the JVM's ``HashMap`` iteration order,
not merely "a deterministic order."

This was derived and **empirically verified against the running JVM**
(`Alex-At-Home/cbb-explorer <https://github.com/Alex-At-Home/cbb-explorer>`_,
Scala 2.12.19, via a throwaway ``sbt run`` probe against a scratch copy of
the clone -- never against the read-only reference clone itself), not
guessed: :func:`_player_code_id_hash` reproduces
``PlayerCodeId(code, PlayerId(name), None).hashCode()`` (Scala's default
case-class hash, ``scala.util.hashing.MurmurHash3.productHash`` with seed
``0xcafebabe``, mixing the three fields' own hash codes -- ``PlayerId`` is
``extends AnyVal``, so its hash delegates to the wrapped ``String``'s
Java ``hashCode()``, and ``None.hashCode()`` equals ``"None".hashCode()``
since it's an arity-0 case object); :func:`_hashmap_improve` reproduces
``scala.collection.immutable.HashMap``'s internal ``improve`` hash-smear
(distinct from the unrelated ``scala.util.hashing.byteswap32`` -- an
initial wrong guess that matched neither the shape of the real
``HashMap.improve`` nor the observed order, confirmed by testing against
JVM ground truth before this final version); and
:func:`_hashmap_radix_key` reproduces the hash-trie's depth-first iteration
order (each level branches on the next 5 bits from the LSB up, so the
overall order is the improved hash with its 5-bit digit groups read
LSB-group-first). All three were validated to reproduce **15/15** exact
``hashCode``/``improve`` values and the exact 15-entry iteration order
against the live JVM's own ``scala.collection.immutable.HashMap`` before
being ported here -- this is not a coincidental match, it is the actual
algorithm. **For 4 or fewer deduplicated entries**, Scala's ``Map1``..
``Map4`` preserve insertion (first-seen) order instead, so
:func:`_scala_map_values_order` branches on size rather than always taking
the hash-trie path.

Apache-2.0 third-party port — see the ``NOTICE`` file at the repository root for the upstream copyright and full attribution.

**Either convention.** Scala's ``Either[List[ParseError], X]`` becomes a
plain ``X | list[ParseError]`` return-type union (the 5b ``parse_team_name``
precedent) -- callers ``isinstance``-check for ``list`` to detect the error
branch.

**``v0_box_name_to_first_last`` NOT ported.** See the "Scala idiom decision"
note in ``mbb_ncaa_stints.py`` -- it backs only a maintainer ``println``
diagnostic with no effect on ``parse_roster``'s return value, the same
situation as ``mbb_ncaa_names.py``'s documented ``fixes_for_debug`` drop.

**Landmine index (reachable scalar division).** None. The height-inches
computation (``ft * 12 + in``) and every hash-mixing helper below are
integer multiplication/addition/XOR/shift only -- no division site exists
to enumerate. All list indexing goes through
:func:`~sportsdataverse.mbb.mbb_ncaa_html.td_at` (already ``IndexError``-guarded)
or guarded ``.find()``/``.get()`` calls.

Example::

    from sportsdataverse.mbb.mbb_ncaa_models import TeamId
    from sportsdataverse.mbb.mbb_ncaa_roster_parser import parse_roster

    with open("tests/fixtures/ncaa/sample_roster.html", encoding="utf-8") as f:
        html = f.read()
    result = parse_roster("sample_roster.html", html, TeamId("TeamA"), version_format=0)
    if not isinstance(result, list) or (result and hasattr(result[0], "messages")):
        raise RuntimeError(result)  # list[ParseError]
    entries = result  # list[RosterEntry]
    entries[0].player_code_id.code

See Also:
    * `hoopR <https://hoopR.sportsdataverse.org>`_ -- R men's basketball companion package
    * `wehoop <https://wehoop.sportsdataverse.org>`_ -- R women's basketball companion package
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Callable, Optional, Union

from bs4 import BeautifulSoup
from bs4.element import Tag

from sportsdataverse.mbb.mbb_ncaa_data_quality import ParseError, build_sub_error
from sportsdataverse.mbb.mbb_ncaa_html import jsoup_text, parse_html, td_at
from sportsdataverse.mbb.mbb_ncaa_models import PlayerCodeId, PlayerId, RosterEntry, TeamId
from sportsdataverse.mbb.mbb_ncaa_names import name_is_initials
from sportsdataverse.mbb.mbb_ncaa_stints import build_player_code, name_in_v0_box_format

__all__ = [
    "parse_roster",
    "get_unified_ncaa_id",
]

#: Error-reporter location tags (``` `ncaa.parse_roster` ```/``` `ncaa.parse_player` ```,
#: ``RosterParser.scala:33-34``).
_LOCATION_PARSE_ROSTER = "ncaa.parse_roster"
_LOCATION_PARSE_PLAYER = "ncaa.parse_player"


@dataclass(frozen=True)
class _RosterBuilders:
    """One version-era's HTML finder functions (``RosterParser.base_builders``,
    ``RosterParser.scala:37-48``)."""

    coach_finder: Callable[[BeautifulSoup], Optional[str]]
    player_info_finder: Callable[[BeautifulSoup], list[Tag]]
    name_finder: Callable[[Tag], Optional[str]]
    number_finder: Callable[[Tag], Optional[str]]
    ncaa_id_finder: Callable[[Tag], Optional[str]]
    pos_finder: Callable[[Tag], Optional[str]]
    height_finder: Callable[[Tag], Optional[str]]
    class_finder: Callable[[Tag], Optional[str]]
    games_played_finder: Callable[[Tag], Optional[str]]
    origin_finder: Callable[[Tag], Optional[str]]


# ---------------------------------------------------------------------------
# v0 (legacy) selectors (``RosterParser.builders_v0``, ``:49-84``)
# ---------------------------------------------------------------------------


def _v0_coach_finder(doc: BeautifulSoup) -> Optional[str]:
    el = doc.select_one("div#head_coaches_div a[href]")
    return jsoup_text(el) if el is not None else None


def _v0_player_info_finder(doc: BeautifulSoup) -> list[Tag]:
    return doc.select("table#stat_grid tbody tr")


def _v0_name_finder(row: Tag) -> Optional[str]:
    td = td_at(row, 1)
    return jsoup_text(td) if td is not None else None


def _v0_ncaa_id_finder(row: Tag) -> Optional[str]:
    td = td_at(row, 1)
    if td is None:
        return None
    a = td.find("a", recursive=False)
    if a is None:
        return None
    href = a.get("href")
    if href is None:
        return None
    return str(href).split("stats_player_seq=")[-1]


def _v0_number_finder(row: Tag) -> Optional[str]:
    td = td_at(row, 0)
    return jsoup_text(td) if td is not None else None


def _v0_pos_finder(row: Tag) -> Optional[str]:
    td = td_at(row, 2)
    return jsoup_text(td) if td is not None else None


def _v0_height_finder(row: Tag) -> Optional[str]:
    td = td_at(row, 3)
    return jsoup_text(td) if td is not None else None


def _v0_class_finder(row: Tag) -> Optional[str]:
    td = td_at(row, 4)
    return jsoup_text(td) if td is not None else None


def _v0_games_played_finder(row: Tag) -> Optional[str]:
    td = td_at(row, 5)
    return jsoup_text(td) if td is not None else None


def _v0_origin_finder(row: Tag) -> Optional[str]:
    return None  # not supported in v0


_BUILDERS_V0 = _RosterBuilders(
    coach_finder=_v0_coach_finder,
    player_info_finder=_v0_player_info_finder,
    name_finder=_v0_name_finder,
    number_finder=_v0_number_finder,
    ncaa_id_finder=_v0_ncaa_id_finder,
    pos_finder=_v0_pos_finder,
    height_finder=_v0_height_finder,
    class_finder=_v0_class_finder,
    games_played_finder=_v0_games_played_finder,
    origin_finder=_v0_origin_finder,
)


# ---------------------------------------------------------------------------
# v1 (2018+) selectors (``RosterParser.builders_v1``, ``:85-132``)
# ---------------------------------------------------------------------------


def _v1_coach_finder(doc: BeautifulSoup) -> Optional[str]:
    # ":-soup-contains" is soupsieve's non-deprecated spelling of JSoup's
    # ":contains()" -- functionally identical, avoids a FutureWarning.
    el = doc.select_one("div.card-header:-soup-contains(Coach) + div.card-body a[href]")
    return jsoup_text(el) if el is not None else None


def _v1_player_info_finder(doc: BeautifulSoup) -> list[Tag]:
    return doc.select("table.dataTable tbody tr")


def _v1_name_finder(row: Tag) -> Optional[str]:
    td = td_at(row, 3)
    return jsoup_text(td) if td is not None else None


def _v1_ncaa_id_finder(row: Tag) -> Optional[str]:
    td = td_at(row, 3)
    if td is None:
        return None
    a = td.find("a", recursive=False)
    if a is None:
        return None
    href = a.get("href")
    if href is None:
        return None
    return str(href).split("/")[-1]


def _v1_number_finder(row: Tag) -> Optional[str]:
    td = td_at(row, 2)
    return jsoup_text(td) if td is not None else None


def _v1_pos_finder(row: Tag) -> Optional[str]:
    td = td_at(row, 5)
    return jsoup_text(td) if td is not None else None


def _v1_height_finder(row: Tag) -> Optional[str]:
    td = td_at(row, 6)
    return jsoup_text(td) if td is not None else None


def _v1_class_finder(row: Tag) -> Optional[str]:
    td = td_at(row, 4)
    return jsoup_text(td).replace(".", "") if td is not None else None  # strip trailing "."


def _v1_games_played_finder(row: Tag) -> Optional[str]:
    td = td_at(row, 0)
    return jsoup_text(td) if td is not None else None


def _v1_origin_finder(row: Tag) -> Optional[str]:
    td = td_at(row, 7)
    return jsoup_text(td) if td is not None else None


_BUILDERS_V1 = _RosterBuilders(
    coach_finder=_v1_coach_finder,
    player_info_finder=_v1_player_info_finder,
    name_finder=_v1_name_finder,
    number_finder=_v1_number_finder,
    ncaa_id_finder=_v1_ncaa_id_finder,
    pos_finder=_v1_pos_finder,
    height_finder=_v1_height_finder,
    class_finder=_v1_class_finder,
    games_played_finder=_v1_games_played_finder,
    origin_finder=_v1_origin_finder,
)

#: Indexed by ``version_format`` (``RosterParser.builders_array``, ``:133``).
_BUILDERS = (_BUILDERS_V0, _BUILDERS_V1)


def _player_unified_ncaa_id_finder(doc: BeautifulSoup) -> list[str]:
    """v1-only bonus finder (``builders_v1.player_unified_ncaa_id_finder``,
    ``RosterParser.scala:123-131``)."""
    ids = []
    for el in doc.select("tr[id^=player_season_] td:first-child a"):
        href = el.get("href")
        if href is None:
            continue
        ids.append(str(href).split("/")[-1])
    return ids


# ---------------------------------------------------------------------------
# Scala immutable.HashMap iteration-order emulation -- see the module
# docstring's dedicated note for the full derivation + JVM-verification story.
# ---------------------------------------------------------------------------


def _java_string_hash(s: str) -> int:
    """Java/Scala ``String.hashCode()``: ``s[0]*31**(n-1) + ... + s[n-1]``,
    32-bit wraparound."""
    h = 0
    for ch in s:
        h = (h * 31 + ord(ch)) & 0xFFFFFFFF
    return h


def _murmur3_mix(h: int, data: int) -> int:
    """``scala.util.hashing.MurmurHash3.mix`` (mix one 32-bit word into the
    running hash state)."""
    k = data & 0xFFFFFFFF
    k = (k * 0xCC9E2D51) & 0xFFFFFFFF
    k = ((k << 15) | (k >> 17)) & 0xFFFFFFFF  # rotl(k, 15)
    k = (k * 0x1B873593) & 0xFFFFFFFF
    h2 = (h ^ k) & 0xFFFFFFFF
    h2 = ((h2 << 13) | (h2 >> 19)) & 0xFFFFFFFF  # rotl(h2, 13)
    return (h2 * 5 + 0xE6546B64) & 0xFFFFFFFF


def _murmur3_finalize(h: int, length: int) -> int:
    """``scala.util.hashing.MurmurHash3.finalizeHash`` (avalanche mixing,
    folding in the product's field count)."""
    h = (h ^ length) & 0xFFFFFFFF
    h ^= h >> 16
    h = (h * 0x85EBCA6B) & 0xFFFFFFFF
    h ^= h >> 13
    h = (h * 0xC2B2AE35) & 0xFFFFFFFF
    h ^= h >> 16
    return h & 0xFFFFFFFF


#: Scala case-class default hash seed (``MurmurHash3.productSeed``).
_PRODUCT_SEED = 0xCAFEBABE

#: ``scala.None.hashCode()`` -- an arity-0 case object's hash equals its
#: ``productPrefix`` string's Java hash (verified: both equal ``2433880``).
_NONE_HASH_CODE = _java_string_hash("None")


def _player_code_id_hash(code: str, id_name: str) -> int:
    """Reproduces ``PlayerCodeId(code, PlayerId(id_name), None).hashCode()``
    -- the default Scala case-class hash (``MurmurHash3.productHash``) over
    the 3 fields' own hash codes, in declaration order.

    Args:
        code: The dedup key's ``PlayerCodeId.code``.
        id_name: The dedup key's ``PlayerCodeId.id.name`` (``PlayerId``
            is ``extends AnyVal``, so its hash delegates to this string's
            Java ``hashCode()``).

    Returns:
        The 32-bit (signed-range, but returned unmasked/non-negative here --
        callers only ever feed this into :func:`_hashmap_improve`, which
        re-masks) case-class hash code.
    """
    h = _PRODUCT_SEED
    h = _murmur3_mix(h, _java_string_hash(code))
    h = _murmur3_mix(h, _java_string_hash(id_name))
    h = _murmur3_mix(h, _NONE_HASH_CODE)
    return _murmur3_finalize(h, 3)


def _hashmap_improve(hcode: int) -> int:
    """``scala.collection.immutable.HashMap``'s internal hash-smear
    (``improve``, distinct from the unrelated ``scala.util.hashing
    .byteswap32``) -- spreads a case-class hash's entropy across all 32
    bits before hash-trie indexing."""
    h = hcode & 0xFFFFFFFF
    h = (h + (~(h << 9) & 0xFFFFFFFF)) & 0xFFFFFFFF
    h ^= h >> 14
    h = (h + ((h << 4) & 0xFFFFFFFF)) & 0xFFFFFFFF
    h ^= h >> 10
    return h & 0xFFFFFFFF


def _hashmap_radix_key(hcode: int) -> int:
    """Turns an improved hash into a plain sort key that reproduces the
    hash-array-mapped-trie's depth-first iteration order: each trie level
    branches on the next 5 bits starting from the LSB, so re-assembling the
    5-bit groups LSB-group-first (as the most-significant digits of the
    returned key) makes a plain ascending integer sort equivalent to the
    trie's traversal order."""
    h = hcode & 0xFFFFFFFF
    key = 0
    for i in range(7):  # 7 groups of 5 bits covers all 32 bits (ceil(32/5))
        key = (key << 5) | ((h >> (5 * i)) & 0x1F)
    return key


def _scala_map_values_order(entries: list[tuple[tuple[str, str], RosterEntry]]) -> list[RosterEntry]:
    """Reorders deduplicated roster entries to match
    ``Map[PlayerCodeId, RosterEntry].values.toList``'s iteration order.

    Args:
        entries: ``(dedup_key, entry)`` pairs in fold (first-seen) order,
            where ``dedup_key = (code, id_name)`` (``ncaa_id`` is always
            ``None`` in this dedup context, so it contributes a fixed
            constant to the hash and is omitted from the Python key tuple).

    Returns:
        The entries' values, reordered: insertion order when
        ``len(entries) <= 4`` (Scala's ``Map1``..``Map4`` preserve it),
        else sorted by :func:`_hashmap_radix_key` of each key's
        :func:`_player_code_id_hash` (post :func:`_hashmap_improve`) --
        reproducing a real ``HashMap``'s hash-trie order.
    """
    if len(entries) <= 4:
        return [v for _, v in entries]
    return [
        v
        for _, v in sorted(
            entries,
            key=lambda kv: _hashmap_radix_key(_hashmap_improve(_player_code_id_hash(*kv[0]))),
        )
    ]


def _new_roster_entry(
    player_code_id: PlayerCodeId,
    number: str,
    pos: str,
    height: str,
    year_class: str,
    gp: int,
    origin: Optional[str],
) -> RosterEntry:
    m = RosterEntry.height_regex.fullmatch(height)
    height_in = int(m.group(1)) * 12 + int(m.group(2)) if m is not None else None
    return RosterEntry(player_code_id, number, pos, height, height_in, year_class, gp, origin, None)


def parse_roster(
    filename: str,
    in_html: str,
    team_id: TeamId,
    version_format: int,
    include_coach: bool = False,
) -> Union[list[RosterEntry], list[ParseError]]:
    """Parses a saved NCAA team-roster page (``RosterParser.parse_roster``,
    ``RosterParser.scala:155-302``).

    Args:
        filename: The source file name, used only for error reporting.
        in_html: The raw roster-page HTML.
        team_id: The team this roster belongs to (feeds
            :func:`~sportsdataverse.mbb.mbb_ncaa_stints.build_player_code`'s
            team-scoped misspelling corrections).
        version_format: ``0`` for the legacy ``table#stat_grid`` layout,
            ``1`` for the 2018+ ``table.dataTable`` layout.
        include_coach: Whether to append a synthetic ``"__coach__"``
            :class:`~sportsdataverse.mbb.mbb_ncaa_models.RosterEntry` for the
            head coach, if the page has one.

    Returns:
        The roster entries (real players first, sorted by games-played
        descending, then the coach entry if requested and found), or a
        single-element ``list[ParseError]`` if two DIFFERENT players
        collide on the same player code (a genuine data-quality error --
        see the module docstring's dedup-order note for why same-name
        duplicate rows are silently merged instead).

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_models import TeamId
            from sportsdataverse.mbb.mbb_ncaa_roster_parser import parse_roster

            with open("tests/fixtures/ncaa/sample_roster.html", encoding="utf-8") as f:
                html = f.read()
            entries = parse_roster("sample_roster.html", html, TeamId("TeamA"), version_format=0)
    """
    try:
        soup = parse_html(in_html)
    except Exception as exc:  # pragma: no cover - bs4/lxml is lenient; mirrors Scala's Try(request)
        return [
            ParseError(
                location=_LOCATION_PARSE_ROSTER,
                id=f"[{filename}]" if filename else "",
                messages=[f"Exception=[{exc}]"],
            )
        ]

    builders = _BUILDERS[version_format]

    coach: Optional[RosterEntry] = None
    if include_coach:
        coach_name = builders.coach_finder(soup)
        if coach_name is not None:
            coach = RosterEntry(
                player_code_id=PlayerCodeId(code="__coach__", id=PlayerId(coach_name)),
                number="",
                pos="",
                height="",
                height_in=None,
                year_class="",
                gp=-1,
                origin=None,
                role=None,
            )

    raw_players: list[RosterEntry] = []
    for row in builders.player_info_finder(soup):
        name_raw = builders.name_finder(row)
        if name_raw is None:
            continue
        name = name_raw if version_format == 0 else name_in_v0_box_format(name_raw)

        if name_is_initials(name) is not None:
            continue  # no initials allowed in the roster

        ncaa_id = builders.ncaa_id_finder(row)
        player_code_id = replace(build_player_code(name, team_id), ncaa_id=ncaa_id)

        number = builders.number_finder(row)
        pos = builders.pos_finder(row)
        height = builders.height_finder(row)
        year_class = builders.class_finder(row)
        gp_raw = builders.games_played_finder(row)
        if number is None or pos is None or height is None or year_class is None or gp_raw is None:
            continue

        origin = builders.origin_finder(row)
        try:
            gp = int(gp_raw)
        except ValueError:
            gp = 0

        raw_players.append(_new_roster_entry(player_code_id, number, pos, height, year_class, gp, origin))

    # Primary sort: gp desc, ties by code asc -- so the fold below dedups by
    # keeping the higher-gp (or lexicographically-earlier-code) entry first.
    raw_players.sort(key=lambda p: (-p.gp, p.player_code_id.code))

    dedup: dict[tuple[str, str], RosterEntry] = {}
    for p in raw_players:
        key = (p.player_code_id.code, p.player_code_id.id.name)
        if key not in dedup:  # can get duplicate names, so just ignore them
            dedup[key] = p

    ordered_values = _scala_map_values_order(list(dedup.items()))
    players = sorted(ordered_values, key=lambda p: -p.gp)  # stable: ties keep map-iteration order

    # Validate duplicates (like in box score parsing logic): two DIFFERENT
    # players (different full names) landing on the same player code is a
    # genuine data-quality error, not a benign same-name repeat.
    codes = [p.player_code_id.code for p in players]
    if len(set(codes)) != len(players):
        duplicate_groups: dict[str, list[PlayerCodeId]] = {}
        for p in players:
            duplicate_groups.setdefault(p.player_code_id.code, []).append(p.player_code_id)
        dup_only = {code: ids for code, ids in duplicate_groups.items() if len(ids) > 1}
        return [build_sub_error(error=f"Duplicate players: [{dup_only}]")]

    return players + ([coach] if coach is not None else [])


def get_unified_ncaa_id(filename: str, in_html: str) -> Union[Optional[str], list[ParseError]]:
    """Gets a player's lowest cross-season NCAA id from a saved player page
    (``RosterParser.get_unified_ncaa_id``, ``RosterParser.scala:136-152``).

    Always uses the v1 selector table -- this bonus lookup only exists on
    2018+-era pages.

    Args:
        filename: The source file name, used only for error reporting.
        in_html: The raw player-page HTML.

    Returns:
        The numerically-lowest NCAA id found, ``None`` if the page has no
        ``tr[id^=player_season_]`` rows, or a single-element
        ``list[ParseError]`` if the HTML couldn't be parsed at all.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_roster_parser import get_unified_ncaa_id
            get_unified_ncaa_id("player.html", player_page_html)
    """
    try:
        soup = parse_html(in_html)
    except Exception as exc:  # pragma: no cover - bs4/lxml is lenient; mirrors Scala's Try(request)
        return [
            ParseError(
                location=_LOCATION_PARSE_PLAYER,
                id=f"[{filename}]" if filename else "",
                messages=[f"Exception=[{exc}]"],
            )
        ]

    ncaa_ids = _player_unified_ncaa_id_finder(soup)
    if not ncaa_ids:
        return None
    return min(ncaa_ids, key=int)  # pick the lowest numerical value
