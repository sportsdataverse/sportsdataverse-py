"""Shared engine for the basketball cross-source crosswalks (MBB/WBB/NBA/WNBA).

Faithful port of ``crosswalk_basketball.R`` from wehoop (WNBA/WBB) and hoopR
(NBA/MBB): the name/team normalizers, the Eastern-Time date reducer, and the
deterministic blocked greedy fuzzy matcher (Jaro-Winkler, ``p = 0.1``).

**This module decides whether the crosswalk port is correct.** Every function
mirrors its R sibling's semantics exactly — including greedy first-come
consumption order inside a block, the blank-name-key skip, and the R
``stringdist::stringsim(method = "jw", p = 0.1)`` scorer — so the Python
builders reproduce the R producers' committed outputs row-for-row.

The two R packages' engines differ in exactly one place: hoopR's exact-name
pass tie-breaks multiple exact hits by jersey/DOB before taking the first,
while wehoop's takes the first unused hit directly. The ``exact_tiebreak``
flag on :func:`fuzzy_match` selects the variant (``False`` = wehoop,
``True`` = hoopR).

Example:
    Quick start::

        import polars as pl
        from sportsdataverse._common_crosswalk_basketball import fuzzy_match

        left = pl.DataFrame({"block": ["1"], "id": ["a1"], "name_key": ["caitlin clark"]})
        right = pl.DataFrame({"block": ["1"], "id": ["b9"], "name_key": ["caitlin clark"]})
        print(fuzzy_match(left, right))

    Normalizers::

        from sportsdataverse._common_crosswalk_basketball import (
            normalize_college_team,
            normalize_name,
            normalize_team,
        )
        print(normalize_name("A'ja Wilson Jr."))          # "aja wilson"
        print(normalize_team("The UConn Huskies"))        # "uconn huskies"
        print(normalize_college_team("Missouri State"))   # "missouri st"

    See Also:
        * `wehoop`_ -- R source of the WBB/WNBA crosswalk engine
        * `hoopR`_ -- R source of the MBB/NBA crosswalk engine

    .. _wehoop: https://wehoop.sportsdataverse.org
    .. _hoopR: https://hoopR.sportsdataverse.org
"""

from __future__ import annotations

import re
import unicodedata
from datetime import date, datetime, timezone
from typing import List, Optional, Union

import polars as pl

try:  # Python 3.9+: stdlib zoneinfo; tzdata wheel backs it on Windows
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - py3.9 always has zoneinfo
    ZoneInfo = None  # type: ignore[assignment,misc]

__all__ = [
    "normalize_name",
    "normalize_team",
    "normalize_college_team",
    "to_eastern",
    "jaro_winkler",
    "fuzzy_match",
]

#: Output column order of the ESPN+Fox player crosswalk (wbb / mbb).
PLAYER_ESPN_FOX_COLUMNS = [
    "season",
    "espn_team_id",
    "team_abbreviation",
    "player_name",
    "espn_athlete_id",
    "espn_full_name",
    "espn_jersey",
    "espn_position",
    "fox_athlete_id",
    "fox_player",
    "fox_jersey",
    "fox_position_group",
    "yahoo_player_id",
    "yahoo_player_name",
    "match_method",
    "match_confidence",
    "match_keys",
]

_EASTERN = ZoneInfo("America/New_York") if ZoneInfo is not None else None


def _ascii_fold(text: str) -> str:
    """Transliterate to ASCII like ``stringi::stri_trans_general(x, "Latin-ASCII")``."""
    return unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")


def normalize_name(value: Optional[str]) -> str:
    """Normalize a person name for cross-source matching.

    Port of wehoop/hoopR ``.bb_normalize_name``: ASCII-fold, lowercase, strip
    apostrophes, map ``. _ -`` to spaces, drop generational suffixes
    (jr/sr/ii/iii/iv), drop every remaining non ``[a-z ]`` character, and
    squish whitespace. ``None`` becomes ``""``.

    Args:
        value: Raw person name (or ``None``).

    Returns:
        The normalized matching key (possibly ``""``).

    Example:
        Quick start::

            from sportsdataverse._common_crosswalk_basketball import normalize_name
            print(normalize_name("Ka'dence O'Neal-Smith Jr."))
    """
    if value is None:
        return ""
    x = _ascii_fold(str(value)).lower()
    x = re.sub(r"['`’]", "", x)
    x = re.sub(r"[._\-]", " ", x)
    x = re.sub(r"\b(jr|sr|ii|iii|iv)\b", "", x)
    x = re.sub(r"[^a-z ]", " ", x)
    return re.sub(r"\s+", " ", x).strip()


def normalize_team(value: Optional[str]) -> str:
    """Normalize a pro/full team name for cross-source matching.

    Port of ``.bb_normalize_team``: ASCII-fold, lowercase, map every non
    ``[a-z ]`` to a space, squish, then strip one leading ``"the "``.

    Args:
        value: Raw team name (or ``None``).

    Returns:
        The normalized matching key (possibly ``""``).

    Example:
        Quick start::

            from sportsdataverse._common_crosswalk_basketball import normalize_team
            print(normalize_team("The Indiana Fever"))
    """
    if value is None:
        return ""
    x = _ascii_fold(str(value)).lower()
    x = re.sub(r"[^a-z ]", " ", x)
    x = re.sub(r"\s+", " ", x).strip()
    return re.sub(r"^the ", "", x)


def normalize_college_team(value: Optional[str]) -> str:
    """Normalize a college school/location name (contracting form).

    Port of ``.bb_normalize_college_team``: ASCII-fold, lowercase, ``&`` to
    `` and ``, punctuation to spaces, collapse spelled-out ``state``/``saint``
    to ``st``, drop ``university``, squish. The canonical form is lossy but
    CONSISTENT across sources (``"Missouri St."`` == ``"Missouri State"``).

    Args:
        value: Raw school/location name (or ``None``).

    Returns:
        The normalized matching key (possibly ``""``).

    Example:
        Quick start::

            from sportsdataverse._common_crosswalk_basketball import normalize_college_team
            print(normalize_college_team("Saint Mary's"))  # "st marys" -> "st mary s"
    """
    if value is None:
        return ""
    x = _ascii_fold(str(value)).lower()
    x = x.replace("&", " and ")
    x = re.sub(r"[^a-z0-9 ]", " ", x)
    x = re.sub(r"\b(state|saint)\b", "st", x)
    x = re.sub(r"\buniversity\b", " ", x)
    return re.sub(r"\s+", " ", x).strip()


def to_eastern(value: Union[str, date, datetime, None]) -> Optional[date]:
    """Reduce a UTC timestamp (ISO string, datetime, or date) to the ET date.

    Port of ``.bb_to_eastern``: a ``date`` passes through; a naive datetime is
    assumed UTC; an ISO string is parsed as UTC. The instant is converted to
    ``America/New_York`` and its local calendar date returned.

    Args:
        value: UTC datetime, ISO-8601 string, ``date``, or ``None``.

    Returns:
        The Eastern-Time calendar date, or ``None`` for null/unparseable input.

    Example:
        Quick start::

            from sportsdataverse._common_crosswalk_basketball import to_eastern
            print(to_eastern("2026-01-15T01:30:00Z"))  # 2026-01-14 (ET)
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, date):
        return value
    else:
        text = str(value).strip()
        if not text:
            return None
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            try:
                dt = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    if _EASTERN is None:  # pragma: no cover - zoneinfo always importable py3.9+
        return dt.date()
    return dt.astimezone(_EASTERN).date()


def jaro_winkler(left: str, right: str, p: float = 0.1) -> float:
    """Jaro-Winkler similarity matching R ``stringdist::stringsim(method="jw")``.

    Standard Jaro similarity (match window ``floor(max(m, n) / 2) - 1``,
    half-transposition count) plus the Winkler common-prefix boost
    ``sim + l * p * (1 - sim)`` with prefix length ``l`` capped at 4. With
    stringdist's default boost threshold ``bt = 0`` the boost always applies,
    which is what the R crosswalk engine uses.

    Args:
        left: First string.
        right: Second string.
        p: Winkler prefix scaling factor (R crosswalks use ``0.1``).

    Returns:
        Similarity in ``[0, 1]``; two empty strings score ``1.0``.

    Example:
        Quick start::

            from sportsdataverse._common_crosswalk_basketball import jaro_winkler
            print(round(jaro_winkler("dwayne", "duane"), 2))  # 0.84
    """
    if left == right:
        return 1.0
    n1, n2 = len(left), len(right)
    if n1 == 0 or n2 == 0:
        return 0.0
    window = max(max(n1, n2) // 2 - 1, 0)
    match1 = [False] * n1
    match2 = [False] * n2
    matches = 0
    for i, ch in enumerate(left):
        lo = max(0, i - window)
        hi = min(n2, i + window + 1)
        for j in range(lo, hi):
            if not match2[j] and right[j] == ch:
                match1[i] = True
                match2[j] = True
                matches += 1
                break
    if matches == 0:
        return 0.0
    transpositions = 0
    j = 0
    for i in range(n1):
        if match1[i]:
            while not match2[j]:
                j += 1
            if left[i] != right[j]:
                transpositions += 1
            j += 1
    # Half the transposition count, as a REAL number. Integer division here is
    # a silent scorer defect: it only diverges when the count is odd, which the
    # textbook Jaro-Winkler examples never exercise.
    half_t = transpositions / 2.0
    jaro = (matches / n1 + matches / n2 + (matches - half_t) / matches) / 3.0
    prefix = 0
    for a, b in zip(left, right):
        if a != b or prefix == 4:
            break
        prefix += 1
    return jaro + prefix * p * (1.0 - jaro)


_MATCH_SCHEMA = {
    "block": pl.Utf8,
    "left_id": pl.Utf8,
    "right_id": pl.Utf8,
    "match_method": pl.Utf8,
    "match_confidence": pl.Float64,
}


def _column(df: pl.DataFrame, name: str) -> List[Optional[str]]:
    return [None if v is None else str(v) for v in df[name].to_list()]


def fuzzy_match(
    left: pl.DataFrame,
    right: pl.DataFrame,
    min_confidence: float = 0.92,
    *,
    exact_tiebreak: bool = False,
) -> pl.DataFrame:
    """Deterministic blocked fuzzy matcher (greedy within block).

    Port of ``.bb_fuzzy_match``: within each block (in left-frame order),
    every left row first tries an **exact** normalized-name match against the
    unused right rows; the leftovers then take the best **Jaro-Winkler** match
    at or above ``min_confidence``, tie-breaking equal-best candidates by
    jersey then DOB. Right rows are consumed greedily — the iteration order is
    part of the contract, so this loop is intentionally row-wise, not
    vectorized. Left rows with a blank ``name_key`` never match anything.

    Args:
        left: Frame with columns ``block``, ``id``, ``name_key`` and optional
            ``jersey`` / ``dob`` (all read as strings).
        right: Frame with the same columns.
        min_confidence: Jaro-Winkler floor for a fuzzy match (R default 0.92).
        exact_tiebreak: ``True`` applies the hoopR variant, which tie-breaks
            multiple exact-name hits by jersey/DOB; ``False`` (wehoop) takes
            the first unused exact hit.

    Returns:
        ``pl.DataFrame`` with columns ``block``, ``left_id``, ``right_id``,
        ``match_method`` (``exact_name`` / ``fuzzy_jw`` / ``unmatched``) and
        ``match_confidence`` (1.0 exact, the JW score for fuzzy, the best
        rejected score — or null — for unmatched). Empty left input returns a
        typed zero-row frame.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse._common_crosswalk_basketball import fuzzy_match

            left = pl.DataFrame({"block": ["1", "1"], "id": ["e1", "e2"],
                                 "name_key": ["paige bueckers", "azzi fudd"]})
            right = pl.DataFrame({"block": ["1", "1"], "id": ["f7", "f8"],
                                  "name_key": ["azzi fudd", "paige buecker"]})
            print(fuzzy_match(left, right).sort("left_id"))
    """
    required = ("block", "id", "name_key")
    for frame, side in ((left, "left"), (right, "right")):
        missing = [c for c in required if c not in frame.columns]
        if missing:
            raise ValueError(f"fuzzy_match: {side} frame missing columns {missing}")
    has_jersey = "jersey" in left.columns and "jersey" in right.columns
    has_dob = "dob" in left.columns and "dob" in right.columns

    if left.height == 0:
        return pl.DataFrame(schema=_MATCH_SCHEMA)

    l_block = _column(left, "block")
    l_id = _column(left, "id")
    l_key = _column(left, "name_key")
    l_jersey = _column(left, "jersey") if has_jersey else []
    l_dob = _column(left, "dob") if has_dob else []
    r_block = _column(right, "block")
    r_id = _column(right, "id")
    r_key = _column(right, "name_key")
    r_jersey = _column(right, "jersey") if has_jersey else []
    r_dob = _column(right, "dob") if has_dob else []

    rows: List[dict] = []

    # unique() in R preserves first-appearance order.
    seen: set = set()
    blocks: List[Optional[str]] = []
    for b in l_block:
        if b not in seen:
            seen.add(b)
            blocks.append(b)

    for b in blocks:
        l_idx = [i for i, v in enumerate(l_block) if v == b]
        r_idx = [j for j, v in enumerate(r_block) if v == b]
        r_used = [False] * len(r_idx)
        block_rows: List[Optional[dict]] = [None] * len(l_idx)
        pending: List[int] = []

        def unmatched_row(left_pos: int, conf: Optional[float] = None) -> dict:
            return {
                "block": b,
                "left_id": l_id[l_idx[left_pos]],
                "right_id": None,
                "match_method": "unmatched",
                "match_confidence": conf,
            }

        for pos, i in enumerate(l_idx):
            key = l_key[i] or ""
            if not key:
                block_rows[pos] = unmatched_row(pos)
                continue
            hits = [k for k, j in enumerate(r_idx) if not r_used[k] and (r_key[j] or "") == key]
            if hits:
                if exact_tiebreak and len(hits) > 1 and has_jersey and l_jersey[i] is not None:
                    jt = [k for k in hits if r_jersey[r_idx[k]] is not None and r_jersey[r_idx[k]] == l_jersey[i]]
                    if jt:
                        hits = jt
                if exact_tiebreak and len(hits) > 1 and has_dob and l_dob[i] is not None:
                    dt = [k for k in hits if r_dob[r_idx[k]] is not None and r_dob[r_idx[k]] == l_dob[i]]
                    if dt:
                        hits = dt
                k = hits[0]
                r_used[k] = True
                block_rows[pos] = {
                    "block": b,
                    "left_id": l_id[i],
                    "right_id": r_id[r_idx[k]],
                    "match_method": "exact_name",
                    "match_confidence": 1.0,
                }
            else:
                pending.append(pos)

        for pos in pending:
            i = l_idx[pos]
            avail = [k for k in range(len(r_idx)) if not r_used[k]]
            if not avail:
                block_rows[pos] = unmatched_row(pos)
                continue
            sims = [jaro_winkler(l_key[i] or "", r_key[r_idx[k]] or "") for k in avail]
            best = max(sims)
            if best >= min_confidence:
                cands = [avail[m] for m, s in enumerate(sims) if s >= best - 1e-9]
                if len(cands) > 1 and has_jersey:
                    jt = [
                        k
                        for k in cands
                        if r_jersey[r_idx[k]] is not None
                        and l_jersey[i] is not None
                        and r_jersey[r_idx[k]] == l_jersey[i]
                    ]
                    if jt:
                        cands = jt
                if len(cands) > 1 and has_dob:
                    dt = [
                        k
                        for k in cands
                        if r_dob[r_idx[k]] is not None and l_dob[i] is not None and r_dob[r_idx[k]] == l_dob[i]
                    ]
                    if dt:
                        cands = dt
                k = cands[0]
                r_used[k] = True
                block_rows[pos] = {
                    "block": b,
                    "left_id": l_id[i],
                    "right_id": r_id[r_idx[k]],
                    "match_method": "fuzzy_jw",
                    "match_confidence": best,
                }
            else:
                block_rows[pos] = unmatched_row(pos, best)

        rows.extend(r for r in block_rows if r is not None)

    return pl.DataFrame(rows, schema=_MATCH_SCHEMA)


# ---------------------------------------------------------------------------
# Shared assemblers
#
# The R player assemblers are byte-identical across the two leagues that share
# a source mix: ``.bb_assemble_player_crosswalk_{wbb,mbb}`` (ESPN + Fox) and
# ``.bb_assemble_player_crosswalk_{wnba,nba}`` (ESPN + Stats API + Fox, with
# only the ``wnba_``/``nba_`` column prefix differing). They live here rather
# than being duplicated into four league modules.
# ---------------------------------------------------------------------------


def as_str_id(expr: pl.Expr, dtype: pl.DataType) -> pl.Expr:
    """Cast an id column to ``Utf8`` without minting float-origin ``"123.0"``.

    A float-typed id stringifies as ``"123.0"``, which silently breaks every
    downstream join. Integer-like columns therefore route through ``Int64``
    first; genuinely non-numeric columns cast straight to ``Utf8``.

    Args:
        expr: The polars expression to cast.
        dtype: The column's current dtype (from ``df.schema``).

    Returns:
        A ``Utf8``-typed expression.
    """
    if dtype.is_float():
        return expr.cast(pl.Int64, strict=False).cast(pl.Utf8)
    if dtype.is_integer():
        return expr.cast(pl.Int64).cast(pl.Utf8)
    return expr.cast(pl.Utf8)


def str_id(df: pl.DataFrame, name: str) -> pl.Expr:
    """``as_str_id`` bound to a named column of ``df`` (null when absent)."""
    if name not in df.columns:
        return pl.lit(None, dtype=pl.Utf8).alias(name)
    return as_str_id(pl.col(name), df.schema[name]).alias(name)


def _name_keys(df: pl.DataFrame, column: str) -> List[str]:
    """Normalized person-name keys for ``column`` (``""`` for nulls, as in R)."""
    return [normalize_name(v) for v in df[column].to_list()]


def _match_frame(espn: pl.DataFrame, name_key: List[str], *, dob: bool) -> pl.DataFrame:
    cols = {
        "block": [None if v is None else str(v) for v in espn["espn_team_id"].to_list()],
        "id": espn.select(str_id(espn, "espn_athlete_id")).to_series().to_list(),
        "name_key": name_key,
        "jersey": espn.select(str_id(espn, "espn_jersey")).to_series().to_list(),
    }
    if dob:
        cols["dob"] = espn.select(str_id(espn, "espn_birth_date")).to_series().to_list()
    return pl.DataFrame(cols)


def _right_frame(
    src: pl.DataFrame, id_col: str, name_col: str, jersey_col: str, dob_col: Optional[str]
) -> pl.DataFrame:
    cols = {
        "block": [None if v is None else str(v) for v in src["espn_team_id"].to_list()],
        "id": src.select(str_id(src, id_col)).to_series().to_list(),
        "name_key": [normalize_name(v) for v in src[name_col].to_list()],
        "jersey": src.select(str_id(src, jersey_col)).to_series().to_list(),
    }
    if dob_col is not None:
        cols["dob"] = src.select(str_id(src, dob_col)).to_series().to_list()
    return pl.DataFrame(cols)


def _unmatched_like(ids: List[Optional[str]]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "block": [None] * len(ids),
            "left_id": ids,
            "right_id": [None] * len(ids),
            "match_method": ["unmatched"] * len(ids),
            "match_confidence": [None] * len(ids),
        },
        schema=_MATCH_SCHEMA,
    )


def _fox_detail(fox: pl.DataFrame) -> pl.DataFrame:
    return fox.select(
        str_id(fox, "fox_athlete_id"),
        pl.col("fox_player").cast(pl.Utf8),
        str_id(fox, "fox_jersey"),
        pl.col("fox_position_group").cast(pl.Utf8),
    )


def assemble_player_espn_fox(
    espn: pl.DataFrame,
    fox: pl.DataFrame,
    season: int,
    min_confidence: float = 0.92,
    *,
    exact_tiebreak: bool = False,
) -> pl.DataFrame:
    """ESPN-anchored player crosswalk against Fox (the WBB / MBB shape).

    Port of ``.bb_assemble_player_crosswalk_wbb`` / ``_mbb``: ESPN is the
    anchor, Fox is matched by normalized name within the team block (exact,
    then Jaro-Winkler with a jersey tiebreak), and Yahoo columns are null
    placeholders.

    Args:
        espn: One row per ESPN athlete with ``espn_team_id``,
            ``team_abbreviation``, ``espn_athlete_id``, ``espn_full_name``,
            ``espn_jersey``, ``espn_position``.
        fox: One row per Fox athlete with ``espn_team_id``,
            ``fox_athlete_id``, ``fox_player``, ``fox_jersey``,
            ``fox_position_group``. May be empty.
        season: Season stamp written to the ``season`` column.
        min_confidence: Jaro-Winkler floor for fuzzy matches.
        exact_tiebreak: hoopR (MBB) tie-breaks multiple exact hits by jersey;
            wehoop (WBB) does not.

    Returns:
        ``pl.DataFrame`` with :data:`PLAYER_ESPN_FOX_COLUMNS`.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse._common_crosswalk_basketball import assemble_player_espn_fox

            espn = pl.DataFrame({"espn_team_id": [41], "team_abbreviation": ["UCONN"],
                                 "espn_athlete_id": ["1"], "espn_full_name": ["Azzi Fudd"],
                                 "espn_jersey": ["35"], "espn_position": ["G"]})
            print(assemble_player_espn_fox(espn, pl.DataFrame(), 2026).columns)
    """
    name_key = _name_keys(espn, "espn_full_name")
    left = _match_frame(espn, name_key, dob=False)

    if fox.height:
        right = _right_frame(fox, "fox_athlete_id", "fox_player", "fox_jersey", None)
        matches = fuzzy_match(left, right, min_confidence, exact_tiebreak=exact_tiebreak)
    else:
        matches = _unmatched_like(left["id"].to_list())

    out = espn.select(
        pl.lit(season, dtype=pl.Int32).alias("season"),
        pl.col("espn_team_id").cast(pl.Int32),
        pl.col("team_abbreviation").cast(pl.Utf8),
        pl.Series("player_name", name_key, dtype=pl.Utf8),
        str_id(espn, "espn_athlete_id"),
        pl.col("espn_full_name").cast(pl.Utf8),
        str_id(espn, "espn_jersey"),
        pl.col("espn_position").cast(pl.Utf8),
    ).join(
        matches.select(
            pl.col("left_id").alias("espn_athlete_id"),
            pl.col("right_id").alias("fox_athlete_id"),
            "match_method",
            "match_confidence",
        ),
        on="espn_athlete_id",
        how="left",
        maintain_order="left",
    )

    if fox.height:
        out = out.join(_fox_detail(fox), on="fox_athlete_id", how="left", maintain_order="left")
    else:
        out = out.with_columns(
            pl.lit(None, dtype=pl.Utf8).alias("fox_player"),
            pl.lit(None, dtype=pl.Utf8).alias("fox_jersey"),
            pl.lit(None, dtype=pl.Utf8).alias("fox_position_group"),
        )

    return out.with_columns(
        pl.lit(None, dtype=pl.Utf8).alias("yahoo_player_id"),
        pl.lit(None, dtype=pl.Utf8).alias("yahoo_player_name"),
        pl.lit(None, dtype=pl.Utf8).alias("match_keys"),
    ).select(PLAYER_ESPN_FOX_COLUMNS)


def assemble_player_espn_stats_fox(
    espn: pl.DataFrame,
    stats: pl.DataFrame,
    fox: pl.DataFrame,
    season: int,
    prefix: str,
    min_confidence: float = 0.92,
    *,
    exact_tiebreak: bool = False,
) -> pl.DataFrame:
    """ESPN-anchored player crosswalk against the Stats API + Fox (NBA / WNBA).

    Port of ``.bb_assemble_player_crosswalk_nba`` / ``_wnba``. ``match_method``
    and ``match_confidence`` describe the **Stats API** match (jersey + DOB
    tiebreaks); Fox contributes ``fox_athlete_id`` only, exactly as in R.

    Args:
        espn: One row per ESPN athlete, with ``espn_birth_date`` in addition to
            the columns :func:`assemble_player_espn_fox` needs.
        stats: One row per Stats API player with ``espn_team_id``,
            ``{prefix}_player_id``, ``{prefix}_player_name``,
            ``{prefix}_jersey_num``, ``{prefix}_position``,
            ``{prefix}_birth_date``. May be empty.
        fox: One row per Fox athlete (see :func:`assemble_player_espn_fox`).
        season: Season stamp written to the ``season`` column.
        prefix: ``"nba"`` or ``"wnba"``.
        min_confidence: Jaro-Winkler floor for fuzzy matches.
        exact_tiebreak: hoopR (NBA) tie-breaks multiple exact hits by
            jersey/DOB; wehoop (WNBA) does not.

    Returns:
        ``pl.DataFrame``, one row per ESPN athlete, 21 columns.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse._common_crosswalk_basketball import (
                assemble_player_espn_stats_fox,
            )

            espn = pl.DataFrame({"espn_team_id": [5], "team_abbreviation": ["LV"],
                                 "espn_athlete_id": ["1"], "espn_full_name": ["A'ja Wilson"],
                                 "espn_jersey": ["22"], "espn_position": ["F"],
                                 "espn_birth_date": ["1996-08-08"]})
            df = assemble_player_espn_stats_fox(espn, pl.DataFrame(), pl.DataFrame(), 2026, "wnba")
            print(df["match_method"].to_list())
    """
    pid, pname = f"{prefix}_player_id", f"{prefix}_player_name"
    pjersey, ppos = f"{prefix}_jersey_num", f"{prefix}_position"

    name_key = _name_keys(espn, "espn_full_name")
    left = _match_frame(espn, name_key, dob=True)
    left_fox = _match_frame(espn, name_key, dob=False)

    if stats.height:
        right = _right_frame(stats, pid, pname, pjersey, f"{prefix}_birth_date")
        m_stats = fuzzy_match(left, right, min_confidence, exact_tiebreak=exact_tiebreak)
    else:
        m_stats = _unmatched_like(left["id"].to_list())

    if fox.height:
        right_fox = _right_frame(fox, "fox_athlete_id", "fox_player", "fox_jersey", None)
        m_fox = fuzzy_match(left_fox, right_fox, min_confidence, exact_tiebreak=exact_tiebreak)
    else:
        m_fox = _unmatched_like(left_fox["id"].to_list())

    out = (
        espn.select(
            pl.lit(season, dtype=pl.Int32).alias("season"),
            pl.col("espn_team_id").cast(pl.Int32),
            pl.col("team_abbreviation").cast(pl.Utf8),
            pl.Series("player_name", name_key, dtype=pl.Utf8),
            str_id(espn, "espn_athlete_id"),
            pl.col("espn_full_name").cast(pl.Utf8),
            str_id(espn, "espn_jersey"),
            pl.col("espn_position").cast(pl.Utf8),
        )
        .join(
            m_stats.select(
                pl.col("left_id").alias("espn_athlete_id"),
                pl.col("right_id").alias(pid),
                "match_method",
                "match_confidence",
            ),
            on="espn_athlete_id",
            how="left",
            maintain_order="left",
        )
        .join(
            (
                stats.select(
                    str_id(stats, pid), pl.col(pname).cast(pl.Utf8), str_id(stats, pjersey), pl.col(ppos).cast(pl.Utf8)
                )
                if stats.height
                else pl.DataFrame(schema={pid: pl.Utf8, pname: pl.Utf8, pjersey: pl.Utf8, ppos: pl.Utf8})
            ),
            on=pid,
            how="left",
            maintain_order="left",
        )
        .join(
            m_fox.select(pl.col("left_id").alias("espn_athlete_id"), pl.col("right_id").alias("fox_athlete_id")),
            on="espn_athlete_id",
            how="left",
            maintain_order="left",
        )
    )

    if fox.height:
        out = out.join(_fox_detail(fox), on="fox_athlete_id", how="left", maintain_order="left")
    else:
        out = out.with_columns(
            pl.lit(None, dtype=pl.Utf8).alias("fox_player"),
            pl.lit(None, dtype=pl.Utf8).alias("fox_jersey"),
            pl.lit(None, dtype=pl.Utf8).alias("fox_position_group"),
        )

    return out.with_columns(
        pl.lit(None, dtype=pl.Utf8).alias("yahoo_player_id"),
        pl.lit(None, dtype=pl.Utf8).alias("yahoo_player_name"),
        pl.lit(None, dtype=pl.Utf8).alias("match_keys"),
    ).select(
        [
            "season",
            "espn_team_id",
            "team_abbreviation",
            "player_name",
            "espn_athlete_id",
            "espn_full_name",
            "espn_jersey",
            "espn_position",
            pid,
            pname,
            pjersey,
            ppos,
            "fox_athlete_id",
            "fox_player",
            "fox_jersey",
            "fox_position_group",
            "yahoo_player_id",
            "yahoo_player_name",
            "match_method",
            "match_confidence",
            "match_keys",
        ]
    )


def pair_key(home: Optional[int], away: Optional[int]) -> Optional[str]:
    """Sorted ``"{lo}_{hi}"`` team-pair key; ``None`` when either id is missing.

    Port of the ``.pair_key`` closure the WBB/MBB schedule assemblers use so
    Torvik's unordered ``team1``/``team2`` join against ESPN's home/away.

    Args:
        home: One side's ESPN team id.
        away: The other side's ESPN team id.

    Returns:
        The order-independent pair key, or ``None``.

    Example:
        Quick start::

            from sportsdataverse._common_crosswalk_basketball import pair_key
            print(pair_key(41, 2))       # "2_41"
            print(pair_key(41, None))    # None
    """
    if home is None or away is None:
        return None
    lo, hi = sorted((int(home), int(away)))
    return f"{lo}_{hi}"
