"""bigballR possession segmenter — one row per stamped possession.

Faithful polars port of bigballR's ``get_possessions``
(``bigballR/R/all_functions.R:3686-3745``). This is a PURE transform: it
consumes a bigballR-contract play-by-play frame (the 35-column snake_case
contract produced by ``parse_ncaa_bb_game_pbp``) and aggregates it into one
row per possession, keyed by the ``poss_num`` / ``poss_team`` / lineup
columns already stamped by the scrape engine.

This module is deliberately DISTINCT from
``sportsdataverse/mbb/mbb_ncaa_possessions.py``, which implements the
cbb-explorer/hoop-explorer possession COUNTING rules (event-driven possession
detection from raw pbp). ``ncaa_mbb_possessions`` here does NOT detect
possessions — it only segments/groups by the possession keys bigballR
stamped upstream. Do not merge the two.

Fixed R bugs (flags default to the CORRECT behavior; the parity tests pass the
faithful value — same convention as ``mbb_ncaa_lineups.fix_tip_in``):

* ``fix_cross_game_leak`` — R's ``End = dplyr::lag(Event_Type)``
  (``all_functions.R:3698``) runs UNGROUPED, so in a multi-game frame every
  game's possession #1 inherits the PREVIOUS game's last event as its
  ``start_event_type`` (``dev/bigballr_port/possession_engine_reconciliation.md``
  BUG-4). The fix windows the lag with ``.over("game_id")``.

The technical/flagrant possession rule (BUG-3) is NOT here — it lives in the
chain that stamps ``poss_num`` / ``poss_team``
(``mbb_ncaa_game_pbp.parse_ncaa_bb_game_pbp``, ``fix_technicals=``); this
module only groups by the keys that chain already stamped.

Deliberate deviations from the R output (documented, semantics preserved):

* R's full variant round-trips every row through ``apply()`` (chr coercion),
  so ``Half_Status`` comes back character and ``ID`` is re-cast numeric.
  We keep ``period`` as Int64 and ``game_id`` as Utf8 (repo ID-dtype rule)
  and reproduce only the sorted-lineup semantics.
* ``is_transition`` / ``is_garbage_time`` / ``is_assisted`` are emitted as
  Int64 (R emits numeric 0/1 and a numeric count) rather than chr.
"""

from __future__ import annotations

from typing import Literal, Union, overload

import pandas as pd
import polars as pl

__all__ = [
    "POSSESSIONS_RENAME",
    "POSSESSION_SEG_SCHEMA",
    "POSSESSIONS_SIMPLE_SCHEMA",
    "SHOT_TYPES",
    "ncaa_mbb_possessions",
]

#: Field-goal-attempt event types (all_functions.R:3710-3711). NOTE:
#: ``get_possessions`` uses the CORRECT ``"Tip In"`` (space) spelling —
#: unlike ``get_lineups`` / ``get_player_stats``, whose ``"Tip-In"`` never
#: matches the scraper's vocabulary and silently drops tip-ins.
SHOT_TYPES: tuple[str, ...] = (
    "Layup",
    "Dunk",
    "Tip In",
    "Hook",
    "Two Point Jumper",
    "Three Point Jumper",
)

#: bigballR R output columns -> sdv-py snake_case, in the R full-variant
#: header order (group keys first, then the summarise columns). The simple
#: variant is the keyed subset without ``period`` + the score/summary tail.
POSSESSIONS_RENAME: dict[str, str] = {
    "ID": "game_id",
    "Date": "game_date",
    "Home": "home",
    "Away": "away",
    "Half_Status": "period",
    "Poss_Num": "poss_num",
    "Poss_Team": "poss_team",
    "Home.1": "home_1",
    "Home.2": "home_2",
    "Home.3": "home_3",
    "Home.4": "home_4",
    "Home.5": "home_5",
    "Away.1": "away_1",
    "Away.2": "away_2",
    "Away.3": "away_3",
    "Away.4": "away_4",
    "Away.5": "away_5",
    "Home_Score": "home_score",
    "Away_Score": "away_score",
    "PTS": "pts",
    "isAssisted": "is_assisted",
    "isTransition": "is_transition",
    "isGarbageTime": "is_garbage_time",
    "startEventType": "start_event_type",
    "firstShotTime": "first_shot_time",
    "firstShotType": "first_shot_type",
    "lastEventTime": "last_event_time",
    "lastEventType": "last_event_type",
}

_HOME_COLS: tuple[str, ...] = tuple(f"home_{i}" for i in range(1, 6))
_AWAY_COLS: tuple[str, ...] = tuple(f"away_{i}" for i in range(1, 6))
_LINEUP_COLS: tuple[str, ...] = _HOME_COLS + _AWAY_COLS

#: Group keys, full variant (all_functions.R:3700-3701) — includes period.
_KEYS_FULL: tuple[str, ...] = (
    "game_id",
    "game_date",
    "home",
    "away",
    "period",
    "poss_num",
    "poss_team",
    *_LINEUP_COLS,
)

#: Group keys, simple variant (all_functions.R:3689-3690) — no period.
_KEYS_SIMPLE: tuple[str, ...] = (
    "game_id",
    "game_date",
    "home",
    "away",
    "poss_num",
    "poss_team",
    *_LINEUP_COLS,
)

#: Output contract, full variant (28 columns).
POSSESSION_SEG_SCHEMA: pl.Schema = pl.Schema(
    {
        "game_id": pl.Utf8,
        "game_date": pl.Utf8,
        "home": pl.Utf8,
        "away": pl.Utf8,
        "period": pl.Int64,
        "poss_num": pl.Int64,
        "poss_team": pl.Utf8,
        **dict.fromkeys(_LINEUP_COLS, pl.Utf8),
        "home_score": pl.Int64,
        "away_score": pl.Int64,
        "pts": pl.Int64,
        "is_assisted": pl.Int64,
        "is_transition": pl.Int64,
        "is_garbage_time": pl.Int64,
        "start_event_type": pl.Utf8,
        "first_shot_time": pl.Int64,
        "first_shot_type": pl.Utf8,
        "last_event_time": pl.Int64,
        "last_event_type": pl.Utf8,
    }
)

#: Output contract, simple variant (17 columns).
POSSESSIONS_SIMPLE_SCHEMA: pl.Schema = pl.Schema(
    {
        "game_id": pl.Utf8,
        "game_date": pl.Utf8,
        "home": pl.Utf8,
        "away": pl.Utf8,
        "poss_num": pl.Int64,
        "poss_team": pl.Utf8,
        **dict.fromkeys(_LINEUP_COLS, pl.Utf8),
        "pts": pl.Int64,
    }
)


def _pts() -> pl.Expr:
    """``PTS = sum(Shot_Value * (Event_Result == "made"), na.rm = T)``.

    Null propagation matches R: a null ``event_result`` (or ``shot_value``)
    nulls the product, and the sum skips nulls (empty/all-null -> 0, exactly
    R's ``sum(..., na.rm = T)``).
    """
    made = (pl.col("event_result") == "made").cast(pl.Int64)
    return (pl.col("shot_value") * made).sum().alias("pts")


def _r_max_int(name: str) -> pl.Expr:
    """R ``max(x)`` WITHOUT ``na.rm`` — any NA in the group yields NA.

    polars ``.max()`` skips nulls, so the null-poisoning is reinstated
    explicitly. Logical max in R returns integer 0/1; cast to Int64.
    """
    return (
        pl.when(pl.col(name).is_null().any())
        .then(pl.lit(None, dtype=pl.Boolean))
        .otherwise(pl.col(name).max())
        .cast(pl.Int64)
        .alias(name)
    )


@overload
def ncaa_mbb_possessions(
    pbp: pl.DataFrame,
    *,
    simple: bool = ...,
    fix_cross_game_leak: bool = ...,
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...


@overload
def ncaa_mbb_possessions(
    pbp: pl.DataFrame,
    *,
    simple: bool = ...,
    fix_cross_game_leak: bool = ...,
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...


@overload
def ncaa_mbb_possessions(
    pbp: pl.DataFrame,
    *,
    simple: bool = False,
    fix_cross_game_leak: bool = True,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]: ...
def ncaa_mbb_possessions(
    pbp: pl.DataFrame,
    *,
    simple: bool = False,
    fix_cross_game_leak: bool = True,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Aggregate bigballR-contract play-by-play into one row per possession.

    Port of bigballR ``get_possessions`` (``all_functions.R:3686-3745``).
    Groups by the possession keys stamped upstream by the scrape engine
    (``poss_num``, ``poss_team``, the ten on-court lineup columns, plus game
    identity), drops possessions with any missing on-court player, and — in
    the full variant — sorts each row's home/away lineup alphabetically so a
    given lineup always occupies the same columns.

    Args:
        pbp: Play-by-play frame in the sdv-py 35-column snake_case bigballR
            contract (``parse_ncaa_bb_game_pbp`` output). May span multiple
            games; rows must be in scrape order.
        simple: When True, return only the 17-column possession/points frame
            (``all_functions.R:3687-3694``) with lineups in on-court order.
            When False (default), return the full 28-column frame with
            per-possession context columns and alpha-sorted lineups.
        fix_cross_game_leak: When True (default, and the CORRECT behavior),
            window the ``start_event_type`` lag with ``.over("game_id")`` so a
            game's first possession has a null start event instead of
            inheriting the PREVIOUS game's last event. When False, reproduce
            R's ungrouped ``dplyr::lag`` (``all_functions.R:3698``) and its
            cross-game leak. Parity tests pass False. Ignored when
            ``simple=True`` (that variant emits no ``start_event_type``).
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        ``pl.DataFrame`` (or ``pd.DataFrame``) with one row per possession —
        28 columns per ``POSSESSION_SEG_SCHEMA`` (full) or 17 per
        ``POSSESSIONS_SIMPLE_SCHEMA`` (simple). Empty input yields an empty
        frame carrying the documented schema.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_possession_seg import ncaa_mbb_possessions
            poss = ncaa_mbb_possessions(pbp)
            print(poss.shape)

        Simple points-per-possession variant::

            poss_pd = ncaa_mbb_possessions(pbp, simple=True, return_as_pandas=True)

        Pipeline next step (one line)::

            poss.group_by("poss_team").agg(pl.col("pts").mean())

    See Also:
        * `bigballR`_ -- R source of the possession segmenter.
        * `hoopR`_ -- men's college basketball data in R.

    .. _bigballR: https://github.com/jflancer/bigballR
    .. _hoopR: https://hoopR.sportsdataverse.org
    """
    if simple:
        keys = list(_KEYS_SIMPLE)
        df = pbp
        aggs: list[pl.Expr] = [_pts()]
        schema: pl.Schema = POSSESSIONS_SIMPLE_SCHEMA
    else:
        keys = list(_KEYS_FULL)
        # BUG-4: R's `End = dplyr::lag(Event_Type)` (all_functions.R:3698) runs
        # UNGROUPED over the whole frame, so in a multi-game frame each game's
        # first possession leaks the PREVIOUS game's last event type (null only
        # on the very first row). fix_cross_game_leak=False reproduces that —
        # oracle parity depends on the leak.
        end = pl.col("event_type").shift(1)
        if fix_cross_game_leak:
            end = end.over("game_id")
        df = pbp.with_columns(end.alias("_end"))
        is_shot = pl.col("event_type").is_in(SHOT_TYPES)
        aggs = [
            pl.col("home_score").first(),
            pl.col("away_score").first(),
            _pts(),
            # R: `sum(!is.na(Player_2) > 0)` — operator precedence makes the
            # `> 0` a no-op, so this is a COUNT of rows with a Player_2, not
            # a boolean (the name lies; kept for parity).
            pl.col("player_2").is_not_null().sum().cast(pl.Int64).alias("is_assisted"),
            _r_max_int("is_transition"),
            _r_max_int("is_garbage_time"),
            pl.col("_end").first().alias("start_event_type"),
            pl.col("poss_length").filter(is_shot).first().alias("first_shot_time"),
            pl.col("event_type").filter(is_shot).first().alias("first_shot_type"),
            pl.col("poss_length").last().alias("last_event_time"),
            pl.col("event_type").last().alias("last_event_type"),
        ]
        schema = POSSESSION_SEG_SCHEMA

    out = (
        df.group_by(keys)
        .agg(aggs)
        # dplyr::summarise(.groups="drop") emits groups sorted by the keys
        # (C-locale byte order, NA groups last) — matched by polars' byte-wise
        # sort with nulls_last.
        .sort(keys, nulls_last=True)
    )

    # all_functions.R:3719-3723 — drop possessions with any missing on-court
    # player ("Forced to remove N rows due to missing players in on/off").
    any_missing = pl.any_horizontal([pl.col(c).is_null() for c in _LINEUP_COLS])
    out = out.filter(any_missing == False)  # noqa: E712 — explicit bool mask

    if not simple:
        # all_functions.R:3726-3741 — alphabetical within-row lineup sort so a
        # given lineup always occupies the same columns. R does this via
        # apply(), chr-coercing the whole frame (Half_Status comes back chr);
        # only the sort semantics are reproduced here — dtypes are kept.
        out = (
            out.with_columns(
                pl.concat_list([pl.col(c) for c in _HOME_COLS]).list.sort().alias("_hs"),
                pl.concat_list([pl.col(c) for c in _AWAY_COLS]).list.sort().alias("_as"),
            )
            .with_columns(
                [pl.col("_hs").list.get(i).alias(c) for i, c in enumerate(_HOME_COLS)]
                + [pl.col("_as").list.get(i).alias(c) for i, c in enumerate(_AWAY_COLS)]
            )
            .drop("_hs", "_as")
        )

    out = out.select(list(schema))
    if return_as_pandas:
        return out.to_pandas()
    return out
