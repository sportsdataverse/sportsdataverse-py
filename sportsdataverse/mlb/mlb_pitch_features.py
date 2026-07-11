"""Pitch-level feature substrate — the sole ``mlb_statcast_search`` consumer.

Every one of the seven pitching models (T6.1) reads the tidy per-pitch frame
built here instead of re-parsing the raw Savant CSV. :func:`pitch_features`
adds physics standardization + location-vs-zone features;
:func:`add_sequence_features` layers on within-game sequence, times-through-
order, and cumulative-workload features. Run value is Savant's own per-pitch
``delta_run_exp`` — passed through unchanged as ``run_value``, per the spine's
"one run-value label" design (see ``sportsdataverse.mlb.mlb_run_expectancy``
for the sibling RE24 spine that this label is intentionally decoupled from).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, List, Union

import polars as pl

if TYPE_CHECKING:  # pragma: no cover -- annotation-only imports
    import pandas as pd

__all__ = ["pitch_features", "add_sequence_features"]

#: id columns pinned to Int64 at the substrate boundary (join-key discipline).
_ID_INT: tuple = ("pitcher", "batter", "game_pk", "at_bat_number", "pitch_number", "balls", "strikes", "inning")

#: raw physics columns standardized within ``pitcher`` -> their z-score name.
_STD_ZNAME: dict = {
    "release_speed": "velo_z",
    "release_spin_rate": "spin_z",
    "pfx_x": "pfx_x_z",
    "pfx_z": "pfx_z_z",
    "release_pos_x": "release_pos_x_z",
    "release_pos_z": "release_pos_z_z",
    "release_extension": "extension_z",
}

#: full output schema (input columns are passed through; these are the columns
#: :func:`pitch_features` adds) -- used to build the zero-row empty frame.
_ADDED_SCHEMA: dict = {
    "velo_z": pl.Float64,
    "spin_z": pl.Float64,
    "pfx_x_z": pl.Float64,
    "pfx_z_z": pl.Float64,
    "release_pos_x_z": pl.Float64,
    "release_pos_z_z": pl.Float64,
    "extension_z": pl.Float64,
    "run_value": pl.Float64,
    "plate_x_abs": pl.Float64,
    "plate_z_norm": pl.Float64,
    "in_zone": pl.Int8,
    "dist_from_heart": pl.Float64,
}

#: sequence-feature schema added by :func:`add_sequence_features`.
_SEQ_SCHEMA: dict = {
    "prev_pitch_type": pl.Utf8,
    "prev_release_pos_x": pl.Float64,
    "prev_release_pos_z": pl.Float64,
    "prev_plate_x": pl.Float64,
    "prev_plate_z": pl.Float64,
    "cum_pitches_game": pl.Int64,
    "batter_faced_index": pl.Int64,
    "times_through_order": pl.Int64,
}


def _standardize(col: str) -> pl.Expr:
    mu = pl.col(col).mean().over("pitcher")
    sd = pl.col(col).std().over("pitcher")
    return pl.when(sd > 0).then((pl.col(col) - mu) / sd).otherwise(0.0).alias(_STD_ZNAME[col])


def _empty_features(base_schema: "dict | None" = None) -> pl.DataFrame:
    schema = dict(base_schema or {})
    schema.update(_ADDED_SCHEMA)
    return pl.DataFrame(schema=schema)


def _empty_sequence(base_schema: "dict | None" = None) -> pl.DataFrame:
    schema = dict(base_schema or {})
    schema.update(_ADDED_SCHEMA)
    schema.update(_SEQ_SCHEMA)
    return pl.DataFrame(schema=schema)


def pitch_features(pitches: pl.DataFrame, *, return_as_pandas: bool = False) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Build the per-pitch feature substrate every pitching model consumes.

    Standardizes physics (velocity/spin/movement/release/extension) within
    ``pitcher``, derives strike-zone-relative location features, pins id
    columns to ``Int64``, and passes Savant's per-pitch ``delta_run_exp``
    through unchanged as ``run_value`` (the single run-value label used by
    Stuff+/Command+/TTO/tunneling).

    Args:
        pitches: Raw Savant pitch frame (e.g. from
            :func:`sportsdataverse.mlb.mlb_statcast_search`), one row per
            pitch, carrying ``pitcher``, ``release_speed``,
            ``release_spin_rate``, ``pfx_x``, ``pfx_z``, ``release_pos_x``,
            ``release_pos_z``, ``release_extension``, ``plate_x``,
            ``plate_z``, ``sz_top``, ``sz_bot``, ``delta_run_exp``.
        return_as_pandas: When ``True``, return a ``pandas.DataFrame``.

    Returns:
        One row per pitch with the input columns plus ``velo_z``, ``spin_z``,
        ``pfx_x_z``, ``pfx_z_z``, ``release_pos_x_z``, ``release_pos_z_z``,
        ``extension_z`` (standardized within ``pitcher``), ``plate_z_norm``,
        ``plate_x_abs``, ``in_zone``, ``dist_from_heart``, and ``run_value``.
        Empty/malformed input returns a zero-row frame carrying the added
        schema.

    Example:
        Quick start::

            from sportsdataverse.mlb import mlb_statcast_search
            from sportsdataverse.mlb.mlb_pitch_features import pitch_features
            raw = mlb_statcast_search("2024-06-15", "2024-06-15", player_type="pitcher")
            feats = pitch_features(raw)
            print(feats.select("pitch_type", "in_zone", "run_value").head())

        Pipeline next step::

            feats.filter(pl.col("in_zone") == 1).group_by("pitch_type").agg(pl.col("run_value").mean())

        See Also:
            * `baseballr`_ -- companion R package for Statcast-based pitching analysis.

        .. _baseballr: https://baseballr.sportsdataverse.org
    """
    if pitches is None or pitches.height == 0:
        out = _empty_features(pitches.schema if pitches is not None else None)
        return out.to_pandas() if return_as_pandas else out

    df = pitches.with_columns([pl.col(c).cast(pl.Int64) for c in _ID_INT if c in pitches.columns])
    df = df.with_columns([_standardize(c) for c in _STD_ZNAME if c in df.columns])

    zone_h = pl.col("sz_top") - pl.col("sz_bot")
    df = df.with_columns(
        pl.col("delta_run_exp").alias("run_value"),
        pl.col("plate_x").abs().alias("plate_x_abs"),
        pl.when(zone_h > 0).then((pl.col("plate_z") - pl.col("sz_bot")) / zone_h).otherwise(None).alias("plate_z_norm"),
    )
    df = df.with_columns(
        ((pl.col("plate_x_abs") <= 0.83) & (pl.col("plate_z_norm") >= 0.0) & (pl.col("plate_z_norm") <= 1.0))
        .cast(pl.Int8)
        .alias("in_zone"),
        ((pl.col("plate_x_abs") ** 2 + (pl.col("plate_z_norm") - 0.5) ** 2) ** 0.5).alias("dist_from_heart"),
    )
    return df.to_pandas() if return_as_pandas else df


def add_sequence_features(
    feats: pl.DataFrame, *, return_as_pandas: bool = False
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Add within-game sequence, times-through-order, and workload features.

    Consumes the output of :func:`pitch_features`. Within each plate
    appearance (``game_pk``, ``pitcher``, ``at_bat_number``, sorted by
    ``pitch_number``), adds ``prev_pitch_type``/``prev_release_pos_x``/
    ``prev_release_pos_z``/``prev_plate_x``/``prev_plate_z`` via
    ``shift(1)``. Within each game (``game_pk``, ``pitcher``, sorted by
    ``at_bat_number`` then ``pitch_number``), adds ``cum_pitches_game``
    (running pitch count), ``batter_faced_index`` (distinct-``at_bat_number``
    rank), and ``times_through_order`` (``min(3, (batter_faced_index-1)//9+1)``).
    Every lag/rank is ``.over(...)`` scoped to avoid cross-game leakage.

    Args:
        feats: Output of :func:`pitch_features`.
        return_as_pandas: When ``True``, return a ``pandas.DataFrame``.

    Returns:
        ``feats`` plus the sequence/TTO/workload columns described above.
        Empty input returns a zero-row frame carrying the full schema.

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_pitch_features import pitch_features, add_sequence_features
            feats = add_sequence_features(pitch_features(raw))
            print(feats.select("times_through_order", "cum_pitches_game").tail())

        See Also:
            * `baseballr`_ -- companion R package for Statcast-based pitching analysis.

        .. _baseballr: https://baseballr.sportsdataverse.org
    """
    if feats is None or feats.height == 0:
        out = _empty_sequence(feats.schema if feats is not None else None)
        return out.to_pandas() if return_as_pandas else out

    df = feats.sort(["game_pk", "pitcher", "at_bat_number", "pitch_number"])

    pa_keys: List[str] = ["game_pk", "pitcher", "at_bat_number"]
    df = df.with_columns(
        pl.col("pitch_type").shift(1).over(pa_keys).alias("prev_pitch_type"),
        pl.col("release_pos_x").shift(1).over(pa_keys).alias("prev_release_pos_x"),
        pl.col("release_pos_z").shift(1).over(pa_keys).alias("prev_release_pos_z"),
        pl.col("plate_x").shift(1).over(pa_keys).alias("prev_plate_x"),
        pl.col("plate_z").shift(1).over(pa_keys).alias("prev_plate_z"),
    )

    game_keys: List[str] = ["game_pk", "pitcher"]
    df = df.with_columns(
        (pl.col("pitch_number").cum_count().over(game_keys)).alias("cum_pitches_game"),
        (pl.col("at_bat_number").rank(method="dense").over(game_keys)).cast(pl.Int64).alias("batter_faced_index"),
    )
    df = df.with_columns(
        (((pl.col("batter_faced_index") - 1) // 9 + 1).clip(1, 3)).alias("times_through_order"),
    )
    return df.to_pandas() if return_as_pandas else df
