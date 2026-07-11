"""Pitch sequencing / tunneling ⑥ — deterministic geometry + sequence run value.

Two compute-on-demand functions, no fitted model:

- :func:`mlb_pitch_tunneling` — per-pitch release-point and plate-location
  distance from the previous pitch in the same plate appearance, and the
  ``tunnel_ratio`` (plate distance / release distance) — a high ratio means
  two pitches that looked alike out of the hand but separated at the plate
  (a good "tunnel"). Follows the Baseball Prospectus "Pitch Tunnels"
  methodology (cited as a reference; no code copied, so no license
  obligation).
- :func:`mlb_sequence_run_value` — mean Savant run value grouped by the
  ordered ``(prev_pitch_type, pitch_type)`` pair.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Union

import polars as pl

if TYPE_CHECKING:  # pragma: no cover -- annotation-only imports
    import pandas as pd

__all__ = ["mlb_pitch_tunneling", "mlb_sequence_run_value"]

_TUNNEL_SCHEMA: dict = {
    "pitcher": pl.Int64,
    "game_pk": pl.Int64,
    "at_bat_number": pl.Int64,
    "pitch_number": pl.Int64,
    "release_dist": pl.Float64,
    "plate_dist": pl.Float64,
    "tunnel_ratio": pl.Float64,
}
_SEQUENCE_RV_SCHEMA: dict = {
    "prev_pitch_type": pl.Utf8,
    "pitch_type": pl.Utf8,
    "mean_run_value": pl.Float64,
    "n": pl.Int64,
}


def mlb_pitch_tunneling(
    pitches: pl.DataFrame, *, eps: float = 0.01, return_as_pandas: bool = False
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Per-pitch release/plate distance from the previous pitch + tunnel ratio.

    Args:
        pitches: Output of
            :func:`sportsdataverse.mlb.mlb_pitch_features.add_sequence_features`
            (needs ``release_pos_x``/``release_pos_z``, ``prev_release_pos_x``/
            ``prev_release_pos_z``, ``plate_x``/``plate_z``,
            ``prev_plate_x``/``prev_plate_z``).
        eps: Minimum ``release_dist`` denominator (avoids divide-by-zero).
        return_as_pandas: When ``True``, return a ``pandas.DataFrame``.

    Returns:
        ``pitcher``, ``game_pk``, ``at_bat_number``, ``pitch_number``,
        ``release_dist``, ``plate_dist``, ``tunnel_ratio``. First pitch of a
        plate appearance (no previous pitch) has null geometry. Empty input
        returns a zero-row frame with this schema.

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_pitch_features import pitch_features, add_sequence_features
            from sportsdataverse.mlb.mlb_pitch_sequencing import mlb_pitch_tunneling
            feats = add_sequence_features(pitch_features(raw_pitches))
            out = mlb_pitch_tunneling(feats)
            print(out.select("tunnel_ratio").describe())

        See Also:
            * `baseballr`_ -- companion R package for Statcast-based pitching analysis.

        .. _baseballr: https://baseballr.sportsdataverse.org
    """
    required = (
        "release_pos_x",
        "release_pos_z",
        "prev_release_pos_x",
        "prev_release_pos_z",
        "plate_x",
        "plate_z",
        "prev_plate_x",
        "prev_plate_z",
    )
    if pitches is None or pitches.height == 0 or not all(c in pitches.columns for c in required):
        out = pl.DataFrame(schema=_TUNNEL_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    release_dist_sq = (pl.col("release_pos_x") - pl.col("prev_release_pos_x")) ** 2 + (
        pl.col("release_pos_z") - pl.col("prev_release_pos_z")
    ) ** 2
    plate_dist_sq = (pl.col("plate_x") - pl.col("prev_plate_x")) ** 2 + (
        pl.col("plate_z") - pl.col("prev_plate_z")
    ) ** 2
    df = pitches.with_columns(
        (release_dist_sq**0.5).alias("release_dist"),
        (plate_dist_sq**0.5).alias("plate_dist"),
    )
    df = df.with_columns(
        pl.when(pl.col("release_dist").is_not_null())
        .then(pl.col("plate_dist") / pl.max_horizontal(pl.col("release_dist"), pl.lit(eps)))
        .otherwise(None)
        .alias("tunnel_ratio")
    )
    out = df.select("pitcher", "game_pk", "at_bat_number", "pitch_number", "release_dist", "plate_dist", "tunnel_ratio")
    return out.to_pandas() if return_as_pandas else out


def mlb_sequence_run_value(
    pitches: pl.DataFrame, *, return_as_pandas: bool = False
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Mean run value grouped by the ordered ``(prev_pitch_type, pitch_type)`` sequence.

    Args:
        pitches: Output of
            :func:`sportsdataverse.mlb.mlb_pitch_features.add_sequence_features`
            (needs ``prev_pitch_type``, ``pitch_type``, ``run_value`` /
            ``delta_run_exp``).
        return_as_pandas: When ``True``, return a ``pandas.DataFrame``.

    Returns:
        ``prev_pitch_type``, ``pitch_type``, ``mean_run_value``, ``n`` — one
        row per observed ordered pair (rows with a null ``prev_pitch_type``,
        i.e. the first pitch of a PA, are dropped). Empty input returns a
        zero-row frame with this schema.

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_pitch_sequencing import mlb_sequence_run_value
            out = mlb_sequence_run_value(feats)
            print(out.sort("mean_run_value").head())

        See Also:
            * `baseballr`_ -- companion R package for Statcast-based pitching analysis.

        .. _baseballr: https://baseballr.sportsdataverse.org
    """
    rv_col = "run_value" if "run_value" in (pitches.columns if pitches is not None else []) else "delta_run_exp"
    if (
        pitches is None
        or pitches.height == 0
        or "prev_pitch_type" not in pitches.columns
        or rv_col not in pitches.columns
    ):
        out = pl.DataFrame(schema=_SEQUENCE_RV_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    df = pitches.filter(pl.col("prev_pitch_type").is_not_null())
    out = df.group_by("prev_pitch_type", "pitch_type").agg(
        pl.col(rv_col).mean().alias("mean_run_value"),
        pl.len().alias("n"),
    )
    return out.to_pandas() if return_as_pandas else out
