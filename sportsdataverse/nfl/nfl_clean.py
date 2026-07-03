"""NFL play-by-play name/id/team canonicalization (nflfastR ``clean_pbp`` port).

Faithful Python port of nflfastR's ``helper_additional_functions.R::clean_pbp``
(lines 51-447) plus ``team_name_fn`` (lines 499-515) and
``fix_weird_pass_plays`` (lines 641-661, already ported standalone). Given a
play-by-play frame, :func:`clean_nfl_pbp` derives/overwrites the columns
nflfastR's cleaning stage is responsible for:

* ``aborted_play`` (0/1 from ``desc`` containing ``"Aborted"``).
* ``success`` (``epa > 0`` -> 1.0 / 0.0, null when ``epa`` is null).
* ``passer`` / ``rusher`` / ``receiver`` (+ ``*_jersey_number``) — extracted
  from ``desc`` via the four nflfastR parser regexes, then patched through the
  hardcoded name-fix tables (``"G.Minshew" -> "G.Minshew II"``, etc.) and the
  ``qb_scramble``/season<=2005 passer-from-rusher fallback.
* ``pass`` / ``rush`` (0/1 play-type flags) — see the **compute-if-absent**
  note below.
* ``first_down`` / ``special`` / ``play`` (0/1 gate flags).
* Team-abbreviation normalization (``SD -> LAC``, ``OAK -> LV``, ...) applied
  to every team-ish column nflfastR's ``mutate_at(..., team_name_fn)`` touches
  (27 columns per the verbatim R ``vars()`` list — see :data:`TEAM_COLUMNS`;
  only touches whichever of those are present on the input frame).
* ``passer_id`` / ``rusher_id`` / ``receiver_id`` and canonicalized
  ``passer`` / ``rusher`` / ``receiver`` names — a per-``(name, posteam,
  season)`` mode vote onto the id, then a per-id mode vote back onto the name
  string (irons out name-string variants that share one id). See
  :func:`_resolve_name_id`.
* ``name`` / ``jersey_number`` / ``id`` (passer-else-rusher).
* ``fantasy`` / ``fantasy_id`` (cleaned rusher-else-receiver, else passer on a
  scramble) and ``fantasy_player_name`` / ``fantasy_player_id`` (same
  fallback, but sourced from the **raw**, uncleaned ``rusher_player_name`` /
  ``receiver_player_name`` columns — nflfastR deliberately uses the raw names
  here, not the cleaned ``rusher``/``receiver``).
* ``out_of_bounds`` (0/1 from ``desc``).
* ``home_opening_kickoff`` (0/1, per ``game_id``: was the home team the first
  team with a non-null ``posteam``).

Scope decisions (read before using on non-nflverse frames)
-----------------------------------------------------------
* **``pass`` / ``rush`` are compute-if-absent, not always-recompute.** The R
  source pre-strips *every* drop.cols entry (including ``pass``/``rush``) and
  unconditionally recomputes them, making ``clean_pbp`` idempotent end to end.
  This port narrows that for exactly these two columns: nflverse-loaded
  frames (:func:`sportsdataverse.nfl.load_nfl_pbp`) already carry
  authoritative ``pass``/``rush`` from nflfastR itself, so re-deriving them
  from this port's simplified regex extraction would be redundant at best and
  a silent downgrade at worst. ESPN/native frames that lack ``pass``/``rush``
  still get them computed via the verbatim §6 formula. Every other drop.cols
  entry (``passer``, ``rusher``, ``receiver``, ``success``, ``first_down``,
  ``special``, ``play``, the three ``*_id`` columns, ``name``, ``id``, the
  jersey-number columns, ``aborted_play``, the four ``fantasy*`` columns,
  ``out_of_bounds``) is still pre-stripped-then-recomputed every call, exactly
  matching R's idempotent-rerun contract.
* **``maybe_valid`` / ``uniquify_ids`` (old GameCenter-id handling) are
  document-and-skip, verified absent from this function.** These names do
  not appear anywhere in ``helper_additional_functions.R`` (grep-verified
  against the nflfastR source) — they live in ``helper_scrape_gc.R``, the
  retired GameCenter-era scraper, and are never called from ``clean_pbp``.
  The only id-resolution logic ``clean_pbp`` performs is the ``custom_mode``
  two-pass mode-vote ported in :func:`_resolve_name_id`, which is fully
  ported above; there is no GC-id branch to skip.
* **Regex rewrite: lookaround -> capture group.** nflfastR's four parser
  regexes (``big_parser``, ``rush_finder``, ``pass_finder``,
  ``receiver_finder``, plus ``number_parser``/``receiver_number``) are
  PCRE lookbehind/lookahead-heavy; polars' Rust regex engine has no
  lookaround support at all. Since R's ``str_extract`` only ever returns the
  *matched text* (and every lookaround in these patterns is zero-width), each
  rewrite here keeps the semantically-equivalent behavior by turning the
  lookaround into a **consumed-but-non-captured** group and extracting only
  the one capture group that corresponds to the name (or jersey number) --
  see :data:`_NAME_CORE` / :data:`_PASS_CONTEXT` / :data:`_RUSH_CONTEXT` and
  the ``_PASSER_PATTERN`` / ``_RUSHER_PATTERN`` / ``_RECEIVER_PATTERN``
  family below. One deliberate simplification: R's ``rush_finder`` has a
  literal (almost certainly accidental) stray ``" | "`` alternative
  (``"(FUMBLES) | (left end)|..."``) that -- taken literally in a
  non-``(?x)`` R regex -- adds a bare space as a matching alternative, making
  the lookahead nearly vacuous. This port implements the *intended* semantics
  (the named FUMBLES/rush-direction phrases only) rather than replicating
  that stray-space quirk; see the task report for the parity discussion.
* **``fix_weird_pass_plays``** (§3, the 15-row hardcoded ``game_id_play_id``
  false-positive override) is ported as :data:`_FIX_WEIRD_PASS_PLAYS` and
  applied in the same post-hoc position as the R source (after ``pass`` has
  already been derived from ``desc``/backward-lateral/kickoff logic, only
  ever forcing ``1 -> 0``, never the reverse).

Required / optional input columns
----------------------------------
Required: ``desc``, ``epa``, ``game_id``, ``play_id``, ``season``,
``posteam``. Everything else nflfastR's derivation touches
(``qb_scramble``, ``kickoff_attempt``, ``qb_kneel``, ``first_down_rush``,
``first_down_pass``, ``first_down_penalty``, ``play_type``,
``passer_player_name``/``_id``, ``rusher_player_name``/``_id``,
``receiver_player_name``/``_id``, ``fumbled_1_player_name``/``_id``) is
filled with a safe default (``0`` for the int flags, null for the rest) when
absent from the input frame, so the function is usable on ESPN/native frames
that don't carry the full nflverse column set. The 27 :data:`TEAM_COLUMNS`
are normalized only for whichever of them are actually present.

Example:
    Quick start on a loaded nflverse frame::

        from sportsdataverse.nfl import load_nfl_pbp
        from sportsdataverse.nfl.nfl_clean import clean_nfl_pbp

        pbp = load_nfl_pbp([2023])
        cleaned = clean_nfl_pbp(pbp)
        print(cleaned.select("passer", "passer_id", "name", "id").head())

    Pandas output::

        cleaned_pd = clean_nfl_pbp(pbp, return_as_pandas=True)

    Pipeline next step (one line)::

        import polars as pl
        cleaned.filter(pl.col("play") == 1).group_by("passer").len()

    See Also:
        * `nflfastR`_ -- the R package this API mirrors
        * `nflreadpy`_ -- Python parity wrapper for nflverse loaders

    .. _nflfastR: https://www.nflfastr.com
    .. _nflreadpy: https://github.com/nflverse/nflreadpy
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Dict, List, Optional, Sequence, Union, overload

import polars as pl

from sportsdataverse.nfl.datasets import team_abbr_mapping

if TYPE_CHECKING:  # pragma: no cover -- annotation-only import (PEP 563 defers eval)
    import pandas as pd

__all__ = ["clean_nfl_pbp", "team_name_fn"]

# ---------------------------------------------------------------------------
# team_name_fn -- verbatim §6 team-abbreviation fixups (10 historical codes),
# sourced from datasets.py::team_abbr_mapping rather than re-hardcoded. Every
# one of the 10 R codes is present in team_abbr_mapping with an identical
# target, so no local additions were needed.
# ---------------------------------------------------------------------------
_TEAM_NAME_FIX_CODES: tuple[str, ...] = (
    "JAC",
    "STL",
    "SL",
    "LAR",
    "ARZ",
    "BLT",
    "CLV",
    "HST",
    "SD",
    "OAK",
)

#: The 27 team-ish columns nflfastR's `mutate_at(vars(...), team_name_fn)`
#: touches (verbatim R `vars()` list, `helper_additional_functions.R` lines
#: 2687-2714 of the reference doc). The port-contract prose there says "26
#: columns"; the actual transcribed `vars()` list has 27 entries -- the R
#: source (transcribed above it) wins per the porting reference's own rule.
TEAM_COLUMNS: List[str] = [
    "posteam",
    "defteam",
    "home_team",
    "away_team",
    "timeout_team",
    "td_team",
    "return_team",
    "penalty_team",
    "side_of_field",
    "forced_fumble_player_1_team",
    "forced_fumble_player_2_team",
    "solo_tackle_1_team",
    "solo_tackle_2_team",
    "assist_tackle_1_team",
    "assist_tackle_2_team",
    "assist_tackle_3_team",
    "assist_tackle_4_team",
    "tackle_with_assist_1_team",
    "tackle_with_assist_2_team",
    "fumbled_1_team",
    "fumbled_2_team",
    "fumble_recovery_1_team",
    "fumble_recovery_2_team",
    "yrdln",
    "end_yard_line",
    "drive_start_yard_line",
    "drive_end_yard_line",
]


def team_name_fn(expr: pl.Expr) -> pl.Expr:
    """Fold historical/relocated team codes onto their current abbreviation.

    Verbatim port of nflfastR's ``team_name_fn`` (a plain
    ``stringr::str_replace_all`` over a 10-entry named vector). Operates as a
    **substring** replace (not a full-value lookup) so it also fixes
    embedded codes like ``"SD 49" -> "LAC 49"`` on yard-line columns. The
    10 from-codes are disjoint from all of their to-values, so the order of
    the 10 sequential replacements does not matter (verified in
    :mod:`tests.nfl.test_nfl_clean`).

    Args:
        expr: A ``polars.Expr`` over a Utf8 column (e.g. ``pl.col("posteam")``).

    Returns:
        The same expression with every occurrence of the 10 historical codes
        replaced by their current-franchise code.
    """
    for code in _TEAM_NAME_FIX_CODES:
        expr = expr.str.replace_all(code, team_abbr_mapping[code], literal=True)
    return expr


# ---------------------------------------------------------------------------
# Regex constants -- rewritten from nflfastR's lookaround-heavy PCRE patterns
# into consumed-but-not-captured groups (polars/Rust regex has no lookaround
# support at all). See the module docstring's "Regex rewrite" note.
# ---------------------------------------------------------------------------

#: First.Last name core -- rewrite of nflfastR's `big_parser` (the leading
#: `(?<=)` in the R source is a zero-width empty lookbehind, i.e. a no-op, so
#: it is simply dropped here).
_NAME_CORE = r"[A-Z][A-Za-z]*(?:\.|\s)+[A-Z][A-Za-z']*-?[A-Za-z]*(?:\s(?:Jr\.|Sr\.|I{2,3})|IV)?"

#: Trailing context for a pass play -- rewrite of `pass_finder`'s lookahead
#: as a consumed (non-captured) suffix.
_PASS_CONTEXT = r"(?:\s*[a-z]*\s*(?: pass|sack|scramble))"

#: Trailing context for a rush play -- rewrite of `rush_finder`'s lookahead.
#: Intentionally implements the *named* rush-direction/FUMBLES alternatives
#: only (see the module docstring's stray-space note on the R source).
_RUSH_CONTEXT = (
    r"(?:\s*[a-z]*\s*(?:FUMBLES|left end|left tackle|left guard"
    r"|up the middle|right guard|right tackle|right end))"
)

#: `abnormal_play` -- plain alternation, no lookaround in the original, ported
#: as-is.
_ABNORMAL_PLAY = (
    r"(?:Lateral|lateral|pitches to|Direct snap to|New quarterback for"
    r"|Aborted|backwards pass|Pass back to|Flea-flicker)"
)

_PASSER_PATTERN = f"({_NAME_CORE}){_PASS_CONTEXT}"
_PASSER_JERSEY_PATTERN = rf"(\d{{1,2}})-(?:{_NAME_CORE}){_PASS_CONTEXT}"
_RUSHER_PATTERN = f"({_NAME_CORE}){_RUSH_CONTEXT}"
_RUSHER_JERSEY_PATTERN = rf"(\d{{1,2}})-(?:{_NAME_CORE}){_RUSH_CONTEXT}"
_RECEIVER_PATTERN = rf"(?:to|for)\s\d{{0,2}}-?({_NAME_CORE})"
_RECEIVER_JERSEY_PATTERN = rf"(?:to|for)\s(\d{{0,2}})-?(?:{_NAME_CORE})"

#: `desc` whitespace cleanup -- collapses a stray run of spaces right after a
#: "space-or-dash + Capital + period" token (e.g. ``"  T. Brady"`` ->
#: ``"  T.Brady"``). No lookaround in the R source; ported as a plain
#: capture-group + `${1}` replacement.
_DESC_CLEANUP_PATTERN = r"((?:\s|-)[A-Z]\.)\s+"

#: §3 `fix_weird_pass_plays` -- the 15 hardcoded `{game_id}_{play_id}` keys
#: where the desc-based `pass` derivation is a known false positive. Verbatim
#: from the reference doc (count verified: 15).
_FIX_WEIRD_PASS_PLAYS: List[str] = [
    "1999_01_ARI_PHI_1611",
    "1999_01_SF_JAX_1788",
    "1999_01_SF_JAX_2081",
    "1999_11_ATL_TB_1740",
    "2001_09_MIN_PHI_1307",
    "2001_14_NE_BUF_452",
    "2002_16_PIT_TB_527",
    "2003_02_HOU_NO_3924",
    "2003_15_PIT_NYJ_873",
    "2004_05_BUF_NYJ_2555",
    "2005_07_SD_PHI_321",
    "2011_02_STL_NYG_1369",
    "2016_05_NE_CLE_912",
    "2016_06_CAR_NO_2690",
    "2020_10_BAL_NE_2013",
]

#: drop.cols (§6), minus `pass`/`rush` -- see the module docstring's
#: compute-if-absent scope note for why those two are excluded here.
_DROP_COLS: List[str] = [
    "success",
    "passer",
    "rusher",
    "receiver",
    "special",
    "first_down",
    "play",
    "passer_id",
    "rusher_id",
    "receiver_id",
    "name",
    "id",
    "passer_jersey_number",
    "rusher_jersey_number",
    "receiver_jersey_number",
    "jersey_number",
    "aborted_play",
    "fantasy",
    "fantasy_id",
    "fantasy_player_name",
    "fantasy_player_id",
    "out_of_bounds",
]

#: Documented output schema (dtypes) for every column `clean_nfl_pbp` adds,
#: used to build a stable empty-frame result and for reference.
_ADDED_SCHEMA: Dict[str, pl.DataType] = {
    "aborted_play": pl.Int64,
    "success": pl.Float64,
    "passer": pl.Utf8,
    "passer_jersey_number": pl.Int64,
    "rusher": pl.Utf8,
    "rusher_jersey_number": pl.Int64,
    "receiver": pl.Utf8,
    "receiver_jersey_number": pl.Int64,
    "pass": pl.Int64,
    "rush": pl.Int64,
    "first_down": pl.Int64,
    "special": pl.Int64,
    "play": pl.Int64,
    "passer_id": pl.Utf8,
    "rusher_id": pl.Utf8,
    "receiver_id": pl.Utf8,
    "name": pl.Utf8,
    "jersey_number": pl.Int64,
    "id": pl.Utf8,
    "fantasy": pl.Utf8,
    "fantasy_id": pl.Utf8,
    "fantasy_player_name": pl.Utf8,
    "fantasy_player_id": pl.Utf8,
    "out_of_bounds": pl.Int64,
    "home_opening_kickoff": pl.Int64,
}

#: Optional input columns this port fills with a safe default when absent
#: (name -> (dtype, default)), so the function is usable on ESPN/native
#: frames that don't carry the full nflverse column set.
_OPTIONAL_DEFAULTS: Dict[str, "tuple[pl.DataType, object]"] = {
    "qb_scramble": (pl.Int64, 0),
    "kickoff_attempt": (pl.Int64, 0),
    "qb_kneel": (pl.Int64, 0),
    "first_down_rush": (pl.Int64, 0),
    "first_down_pass": (pl.Int64, 0),
    "first_down_penalty": (pl.Int64, 0),
    "play_type": (pl.Utf8, None),
    "passer_player_name": (pl.Utf8, None),
    "passer_player_id": (pl.Utf8, None),
    "rusher_player_name": (pl.Utf8, None),
    "rusher_player_id": (pl.Utf8, None),
    "receiver_player_name": (pl.Utf8, None),
    "receiver_player_id": (pl.Utf8, None),
    "fumbled_1_player_name": (pl.Utf8, None),
    "fumbled_1_player_id": (pl.Utf8, None),
}


def _first_seen_mode(values: Union[pl.Series, List[Optional[str]], None]) -> Optional[str]:
    """Most-frequent value, ties broken by first occurrence.

    ``values`` is whatever ``map_elements`` hands us for one group's list --
    in practice a ``polars.Series`` (iterable, yielding ``None`` for nulls),
    but a plain Python list works identically.

    Port of nflfastR's ``custom_mode`` (``utils.R``): ``unique(x)`` preserves
    first-occurrence order and ``which.max`` returns the *first* index
    achieving the max, so on a tie the earliest-seen value wins -- not
    ``pandas.Series.mode()`` (sorts, returns all ties) and not
    ``collections.Counter.most_common`` (arbitrary/insertion-order tie
    behavior that differs by Python version).
    """
    if values is None or len(values) == 0:
        return None
    counts: Dict[str, int] = {}
    first_pos: Dict[str, int] = {}
    for i, v in enumerate(values):
        if v is None:
            continue
        counts[v] = counts.get(v, 0) + 1
        if v not in first_pos:
            first_pos[v] = i
    if not counts:
        return None
    return min(counts, key=lambda k: (-counts[k], first_pos[k]))


def _resolve_name_id(
    df: pl.DataFrame,
    name_col: str,
    id_col: str,
    source_id_col: str,
    extra_group_cols: Sequence[str],
) -> pl.DataFrame:
    """Two-pass per-name-per-team-per-season id resolution ("Seb's stuff").

    Pass 1: group by ``(name_col, *extra_group_cols)`` and mode-vote
    ``source_id_col`` onto a new ``id_col`` (null when ``name_col`` is null).
    Pass 2: re-group by the just-computed ``id_col`` and mode-vote
    ``name_col`` back onto itself -- this irons out name-string variants that
    share one id, and (verbatim R semantics) nulls ``name_col`` out wherever
    ``id_col`` is null, even if a name string was present pre-resolution.

    Assumes ``df`` already carries the row-order-restoring index column (the
    caller re-sorts once, after all three name/id pairs are resolved, rather
    than after each one) and that any pre-existing ``id_col`` has already
    been dropped by the drop.cols pre-strip.
    """
    group_cols = [name_col, *extra_group_cols]

    pass1 = (
        df.filter(pl.col(name_col).is_not_null())
        .group_by(group_cols, maintain_order=True)
        .agg(pl.col(source_id_col).alias("_ids"))
        .with_columns(pl.col("_ids").map_elements(_first_seen_mode, return_dtype=pl.Utf8).alias(id_col))
        .select([*group_cols, id_col])
    )
    df = df.join(pass1, on=group_cols, how="left")

    pass2 = (
        df.filter(pl.col(id_col).is_not_null())
        .group_by(id_col, maintain_order=True)
        .agg(pl.col(name_col).alias("_names"))
        .with_columns(pl.col("_names").map_elements(_first_seen_mode, return_dtype=pl.Utf8).alias("_name_mode"))
        .select([id_col, "_name_mode"])
    )
    df = df.drop(name_col).join(pass2, on=id_col, how="left").rename({"_name_mode": name_col})
    return df


def _with_default(df: pl.DataFrame, name: str, dtype: "pl.DataType", default: object) -> pl.DataFrame:
    """Add ``name`` with a constant default when absent; no-op otherwise."""
    if name in df.columns:
        return df
    return df.with_columns(pl.lit(default, dtype=dtype).alias(name))


def _empty_result(df: pl.DataFrame, return_as_pandas: bool) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Zero-row result carrying the input schema plus the documented added
    columns (never raises, matches the project's empty-frame convention).
    """
    missing = {name: dtype for name, dtype in _ADDED_SCHEMA.items() if name not in df.columns}
    if missing:
        df = df.with_columns([pl.lit(None, dtype=dtype).alias(name) for name, dtype in missing.items()])
    return df.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else df


@overload
def clean_nfl_pbp(df: pl.DataFrame) -> pl.DataFrame: ...
@overload
def clean_nfl_pbp(df: pl.DataFrame, *, return_as_pandas: bool = ...) -> Union[pl.DataFrame, "pd.DataFrame"]: ...


def clean_nfl_pbp(
    df: pl.DataFrame,
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Canonicalize names/ids/teams on a play-by-play frame (nflfastR ``clean_pbp`` port).

    See the module docstring for the full column set added, the
    compute-if-absent scope note on ``pass``/``rush``, and the lookaround ->
    capture-group regex rewrites.

    Args:
        df: An nflverse-shape (or ESPN/native) play-by-play ``polars.DataFrame``.
            Required columns: ``desc``, ``epa``, ``game_id``, ``play_id``,
            ``season``, ``posteam``. See the module docstring for the full
            optional-column-with-default list.
        return_as_pandas: If ``True``, return a ``pandas.DataFrame``; otherwise
            a ``polars.DataFrame`` (default).

    Returns:
        The input frame with every §6 column added/overwritten (idempotent --
        pre-existing values of those columns, except ``pass``/``rush``, are
        dropped and recomputed). A zero-row input yields a zero-row frame
        carrying the full documented schema rather than raising.

    Example:
        Quick start::

            from sportsdataverse.nfl import load_nfl_pbp
            from sportsdataverse.nfl.nfl_clean import clean_nfl_pbp

            pbp = load_nfl_pbp([2023])
            cleaned = clean_nfl_pbp(pbp)
            print(cleaned.select("name", "id", "fantasy").head())

        Pandas output::

            cleaned_pd = clean_nfl_pbp(pbp, return_as_pandas=True)

        Pipeline next step (one line)::

            import polars as pl
            cleaned.filter(pl.col("play") == 1).group_by("passer").len()

        See Also:
            * `nflfastR`_ -- the R package this API mirrors
            * `nflreadpy`_ -- Python parity wrapper for nflverse loaders

        .. _nflfastR: https://www.nflfastr.com
        .. _nflreadpy: https://github.com/nflverse/nflreadpy
    """
    if df.height == 0:
        return _empty_result(df, return_as_pandas)

    # Idempotent pre-strip: drop any pre-existing values of the columns this
    # function is about to (re)compute, mirroring R's
    # `select(-any_of(drop.cols))`. `pass`/`rush` are deliberately excluded --
    # see the module docstring's compute-if-absent scope note.
    df = df.drop([c for c in _DROP_COLS if c in df.columns])

    had_pass = "pass" in df.columns
    had_rush = "rush" in df.columns

    for name, (dtype, default) in _OPTIONAL_DEFAULTS.items():
        df = _with_default(df, name, dtype, default)

    df = df.with_columns(
        aborted_play=pl.col("desc").str.contains("Aborted").cast(pl.Int64),
        desc=pl.col("desc").str.replace_all(_DESC_CLEANUP_PATTERN, "${1}"),
    )

    df = df.with_columns(
        success=pl.when(pl.col("epa").is_null())
        .then(None)
        .otherwise(pl.when(pl.col("epa") > 0).then(1.0).otherwise(0.0))
        .cast(pl.Float64),
        passer=pl.col("desc").str.extract(_PASSER_PATTERN, 1),
        passer_jersey_number=pl.col("desc").str.extract(_PASSER_JERSEY_PATTERN, 1).cast(pl.Int64, strict=False),
        rusher=pl.col("desc").str.extract(_RUSHER_PATTERN, 1),
        rusher_jersey_number=pl.col("desc").str.extract(_RUSHER_JERSEY_PATTERN, 1).cast(pl.Int64, strict=False),
        receiver=pl.col("desc").str.extract(_RECEIVER_PATTERN, 1),
        receiver_jersey_number=pl.col("desc").str.extract(_RECEIVER_JERSEY_PATTERN, 1).cast(pl.Int64, strict=False),
    )

    # rusher-as-last-resort: aborted snaps / plain "F.Last to NYG 44." plays
    # where neither the pass nor rush regex fired.
    df = df.with_columns(
        rusher=pl.when(
            pl.col("rusher").is_null() & pl.col("passer").is_null() & pl.col("rusher_player_name").is_not_null()
        )
        .then(pl.col("rusher_player_name"))
        .otherwise(pl.col("rusher"))
    )

    # abnormal-play overwrite (laterals, direct snaps, aborted snaps, ...):
    # prefer the ESPN/nflverse-supplied raw name over the regex extraction.
    abnormal = pl.col("desc").str.contains(_ABNORMAL_PLAY)
    df = df.with_columns(
        receiver=pl.when(abnormal & pl.col("receiver_player_name").is_not_null())
        .then(pl.col("receiver_player_name"))
        .otherwise(pl.col("receiver")),
        rusher=pl.when(abnormal & pl.col("rusher_player_name").is_not_null())
        .then(pl.col("rusher_player_name"))
        .otherwise(pl.col("rusher")),
        passer=pl.when(abnormal & pl.col("passer_player_name").is_not_null())
        .then(pl.col("passer_player_name"))
        .otherwise(pl.col("passer")),
    )

    # pre-2006 charting fix: scramble plays with no passer inherit the rusher
    # as the passer (season <= 2005 only).
    df = df.with_columns(
        passer=pl.when(
            pl.col("passer").is_null()
            & (pl.col("qb_scramble") == 1)
            & pl.col("rusher").is_not_null()
            & (pl.col("season") <= 2005)
        )
        .then(pl.col("rusher"))
        .otherwise(pl.col("passer"))
    )
    # once a passer is resolved (incl. the scramble fallback above), there is
    # no rusher on that play.
    df = df.with_columns(
        rusher=pl.when(pl.col("passer").is_not_null()).then(None).otherwise(pl.col("rusher")),
        receiver=pl.when(pl.col("desc").str.contains(" pass ", literal=True)).then(pl.col("receiver")).otherwise(None),
    )

    # pass / rush / first_down / special / play flags.
    if had_pass:
        pass_expr = pl.col("pass")
    else:
        pass_expr = (
            pl.col("desc").str.contains(r"(?: pass )|(?:sacked)|(?:scramble)") | (pl.col("qb_scramble") == 1)
        ).cast(pl.Int64)
        pass_expr = (
            pl.when(
                pl.col("desc").str.to_lowercase().str.contains(r"(?:backward pass)|(?:backwards pass)|(?:lateral pass)")
                & pl.col("rusher").is_not_null()
            )
            .then(0)
            .otherwise(pass_expr)
        )
        pass_expr = pl.when(pl.col("kickoff_attempt") == 1).then(0).otherwise(pass_expr)
        combined_id = pl.concat_str(["game_id", pl.col("play_id").cast(pl.Utf8)], separator="_")
        pass_expr = pl.when(combined_id.is_in(_FIX_WEIRD_PASS_PLAYS)).then(0).otherwise(pass_expr)

    df = df.with_columns(pass_expr.alias("pass"))

    if had_rush:
        rush_expr = pl.col("rush")
    else:
        rush_expr = (pl.col("rusher").is_not_null() & (pl.col("qb_kneel") == 0) & (pl.col("pass") == 0)).cast(pl.Int64)

    df = df.with_columns(
        rush=rush_expr,
        first_down=(
            (pl.col("first_down_rush") == 1) | (pl.col("first_down_pass") == 1) | (pl.col("first_down_penalty") == 1)
        ).cast(pl.Int64),
        special=pl.col("play_type").is_in(["extra_point", "field_goal", "kickoff", "punt"]).cast(pl.Int64),
        play=(
            pl.col("epa").is_not_null()
            & pl.col("posteam").is_not_null()
            & (pl.col("desc") != "*** play under review ***")
            & (pl.col("desc").str.slice(0, 8) != "Timeout ")
            & pl.col("play_type").is_in(["no_play", "pass", "run"])
        ).cast(pl.Int64),
    )

    # Hardcoded name-fix tables -- transcribed verbatim (parity-critical, not
    # illustrative). `posteam` here is still the RAW (pre-team_name_fn) value,
    # matching R's mutate ordering (this whole block runs before the
    # `mutate_at(..., team_name_fn)` step), though none of the three
    # `posteam == "..."` conditions reference a code team_name_fn would touch.
    df = df.with_columns(
        passer=pl.when(pl.col("passer") == "Jos.Allen")
        .then(pl.lit("J.Allen"))
        .when(pl.col("passer").is_in(["Alex Smith", "Ale.Smith"]))
        .then(pl.lit("A.Smith"))
        .when((pl.col("passer") == "Ryan") & (pl.col("posteam") == "ATL"))
        .then(pl.lit("M.Ryan"))
        .when(pl.col("passer") == "Tr.Brown")
        .then(pl.lit("T.Brown"))
        .when(pl.col("passer") == "Sh.Hill")
        .then(pl.lit("S.Hill"))
        .when(pl.col("passer").is_in(["Matt.Moore", "Mat.Moore"]))
        .then(pl.lit("M.Moore"))
        .when(pl.col("passer") == "Jo.Freeman")
        .then(pl.lit("J.Freeman"))
        .when(pl.col("passer") == "G.Minshew")
        .then(pl.lit("G.Minshew II"))
        .when(pl.col("passer") == "R.Griffin")
        .then(pl.lit("R.Griffin III"))
        .when(pl.col("passer").is_in(["Randel El", "Randle El"]))
        .then(pl.lit("A.Randle El"))
        .when((pl.col("season") <= 2003) & (pl.col("passer") == "Van Pelt"))
        .then(pl.lit("A.Van Pelt"))
        .when((pl.col("season") > 2003) & (pl.col("passer") == "Van Pelt"))
        .then(pl.lit("B.Van Pelt"))
        .when(pl.col("passer") == "Dom.Davis")
        .then(pl.lit("D.Davis"))
        .otherwise(pl.col("passer")),
        rusher=pl.when(
            (pl.col("rusher") == "D.Johnson")
            & (pl.col("posteam") == "HOU")
            & (pl.col("season") == 2020)
            & (pl.col("rusher_jersey_number") == 31)
        )
        .then(pl.lit("Da.Johnson"))
        .when(
            (pl.col("rusher") == "D.Johnson")
            & (pl.col("posteam") == "HOU")
            & (pl.col("season") == 2020)
            & (pl.col("rusher_jersey_number") == 25)
        )
        .then(pl.lit("Du.Johnson"))
        .when(pl.col("rusher") == "Jos.Allen")
        .then(pl.lit("J.Allen"))
        .when(pl.col("rusher").is_in(["Alex Smith", "Ale.Smith"]))
        .then(pl.lit("A.Smith"))
        .when((pl.col("rusher") == "Ryan") & (pl.col("posteam") == "ATL"))
        .then(pl.lit("M.Ryan"))
        .when(pl.col("rusher") == "Tr.Brown")
        .then(pl.lit("T.Brown"))
        .when(pl.col("rusher") == "Sh.Hill")
        .then(pl.lit("S.Hill"))
        .when(pl.col("rusher").is_in(["Matt.Moore", "Mat.Moore"]))
        .then(pl.lit("M.Moore"))
        .when(pl.col("rusher") == "Jo.Freeman")
        .then(pl.lit("J.Freeman"))
        .when(pl.col("rusher") == "G.Minshew")
        .then(pl.lit("G.Minshew II"))
        .when(pl.col("rusher") == "R.Griffin")
        .then(pl.lit("R.Griffin III"))
        .when(pl.col("rusher").is_in(["Randel El", "Randle El"]))
        .then(pl.lit("A.Randle El"))
        .when((pl.col("season") <= 2003) & (pl.col("rusher") == "Van Pelt"))
        .then(pl.lit("A.Van Pelt"))
        .when((pl.col("season") > 2003) & (pl.col("rusher") == "Van Pelt"))
        .then(pl.lit("B.Van Pelt"))
        .when(pl.col("rusher") == "Dom.Davis")
        .then(pl.lit("D.Davis"))
        .otherwise(pl.col("rusher")),
        receiver=pl.when(pl.col("receiver") == "F.R")
        .then(pl.lit("F.Jones"))
        .when((pl.col("receiver_player_name") == "D.Wells") & (pl.col("receiver_player_id") == "00-0017421"))
        .then(pl.lit("D.Wells"))
        .when((pl.col("receiver_player_name") == "D.Hayes") & (pl.col("receiver_player_id") == "00-0007144"))
        .then(pl.lit("D.Hayes"))
        .when(pl.col("receiver_player_name") == "DanielThomas")
        .then(pl.lit("D.Thomas"))
        .when(pl.col("receiver_player_name") == "JulioJones")
        .then(pl.lit("J.Jones"))
        .when(pl.col("receiver_player_name") == "Andre' Davis")
        .then(pl.lit("A.Davis"))
        .when(pl.col("receiver_player_name") == "A.al-Jabbar")
        .then(pl.lit("A.al-Jabbar"))
        .when(pl.col("receiver_player_name") == "A.St. Brown")
        .then(pl.lit("A.St. Brown"))
        .otherwise(pl.col("receiver")),
    )

    # Team-abbreviation normalization (only touch columns actually present).
    df = df.with_columns([team_name_fn(pl.col(c)) for c in TEAM_COLUMNS if c in df.columns])

    # "Seb's stuff" -- per-name-per-team-per-season id mode-vote, then a
    # per-id mode-vote back onto the name. A stable row index restores
    # original row order once after all three passes (R restores order once
    # too, via `arrange(index)` right after `ungroup()`).
    df = df.with_row_index("_orig_idx")
    df = _resolve_name_id(df, "passer", "passer_id", "passer_player_id", ["posteam", "season"])
    df = _resolve_name_id(df, "rusher", "rusher_id", "rusher_player_id", ["posteam", "season"])
    df = _resolve_name_id(df, "receiver", "receiver_id", "receiver_player_id", ["posteam", "season"])
    df = df.sort("_orig_idx").drop("_orig_idx")

    # Aborted-snap fumble override (after all custom_mode resolution, exactly
    # as in R -- doing it earlier would get "messed up" by the mode votes).
    df = df.with_columns(
        rusher=pl.when(
            (pl.col("aborted_play") == 1) & pl.col("passer").is_null() & pl.col("fumbled_1_player_name").is_not_null()
        )
        .then(pl.col("fumbled_1_player_name"))
        .otherwise(pl.col("rusher")),
        rusher_id=pl.when(
            (pl.col("aborted_play") == 1) & pl.col("passer").is_null() & pl.col("fumbled_1_player_id").is_not_null()
        )
        .then(pl.col("fumbled_1_player_id"))
        .otherwise(pl.col("rusher_id")),
    )

    df = df.with_columns(
        name=pl.when(pl.col("passer").is_not_null()).then(pl.col("passer")).otherwise(pl.col("rusher")),
        jersey_number=pl.when(pl.col("passer_jersey_number").is_not_null())
        .then(pl.col("passer_jersey_number"))
        .otherwise(pl.col("rusher_jersey_number")),
        id=pl.when(pl.col("passer_id").is_not_null()).then(pl.col("passer_id")).otherwise(pl.col("rusher_id")),
    )

    # Fantasy fallback -- `fantasy_player_name`/`_id` use the RAW (uncleaned)
    # rusher_player_name/receiver_player_name/*_id columns; `fantasy`/`_id`
    # use the CLEANED rusher/receiver/passer + id (matches R exactly).
    df = df.with_columns(
        fantasy_player_name=pl.when(pl.col("rusher_player_name").is_not_null())
        .then(pl.col("rusher_player_name"))
        .when(pl.col("receiver_player_name").is_not_null())
        .then(pl.col("receiver_player_name"))
        .otherwise(None),
        fantasy_player_id=pl.when(pl.col("rusher_player_id").is_not_null())
        .then(pl.col("rusher_player_id"))
        .when(pl.col("receiver_player_id").is_not_null())
        .then(pl.col("receiver_player_id"))
        .otherwise(None),
        fantasy=pl.when(pl.col("rusher").is_not_null())
        .then(pl.col("rusher"))
        .when(pl.col("receiver").is_not_null())
        .then(pl.col("receiver"))
        .when(pl.col("qb_scramble") == 1)
        .then(pl.col("passer"))
        .otherwise(None),
        fantasy_id=pl.when(pl.col("rusher_id").is_not_null())
        .then(pl.col("rusher_id"))
        .when(pl.col("receiver_id").is_not_null())
        .then(pl.col("receiver_id"))
        .when(pl.col("qb_scramble") == 1)
        .then(pl.col("passer_id"))
        .otherwise(None),
        out_of_bounds=pl.col("desc").str.contains(r"(?:ran ob)|(?:pushed ob)|(?:sacked ob)").cast(pl.Int64),
    )

    df = df.with_columns(
        home_opening_kickoff=(
            pl.col("home_team") == pl.col("posteam").filter(pl.col("posteam").is_not_null()).first().over("game_id")
        ).cast(pl.Int64)
    )

    return df.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else df
