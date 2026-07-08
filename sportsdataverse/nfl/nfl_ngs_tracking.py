"""NFL Next Gen Stats over-expected models (aggregate grain).

Three player-season "over-expected" models on the pre-aggregated NGS feed
(:func:`sportsdataverse.nfl.load_nfl_nextgen_stats`) plus a descriptive
man/zone coverage-rate summary from participation labels:

* :func:`nfl_ngs_yac_oe` — YAC over expected (NGS residual, EB-shrunk).
* :func:`nfl_ngs_ryoe` — rush yards over expected (NGS residual, EB-shrunk).
* :func:`nfl_ngs_separation_oe` — separation over a *context* expectation
  (built ridge; no NGS-shipped expected field exists for separation).
* :func:`nfl_ngs_man_zone_rates` — descriptive charted-label rates,
  NOT a trained classifier.

Blocked (needs snap tracking):
    Per-play YAC-OE / RYOE / separation-OE and a *trained* man/zone
    coverage classifier all require the snap-level ``(x, y)`` player
    tracking feed, which is not public and is not pulled by
    ``load_nfl_nextgen_stats`` (that loader ships season/week aggregates
    only). The man/zone *labels* exist in
    ``load_nfl_pbp_participation`` for charted seasons (2016-2023) but
    the tracking *features* needed to train on them do not, so a
    classifier is untrainable from this package's data surface.
    Unblock: ingest a Big-Data-Bowl-style tracking feed in a separate
    plan; the aggregate models here are designed so the per-play
    versions can reuse the same shrinkage engine
    (:mod:`sportsdataverse.nfl.nfl_ngs_constants`) when that lands.
"""

from __future__ import annotations

from typing import Callable, Optional

import polars as pl

from sportsdataverse.nfl.nfl_loaders import load_nfl_nextgen_stats


def _ngs_panel(
    seasons: list,
    stat_type: str,
    *,
    level: str = "season",
    _loader: Optional[Callable[..., pl.DataFrame]] = None,
) -> pl.DataFrame:
    """Load and normalise an NGS aggregate panel.

    Args:
        seasons (list): Seasons to load (2016 is the earliest NGS season).
        stat_type (str): ``"receiving"`` | ``"rushing"`` | ``"passing"``.
        level (str): ``"season"`` keeps only the season-summary rows
            (``week == 0``); ``"week"`` keeps only weekly rows.
        _loader (Optional[Callable]): Injectable loader for offline tests;
            defaults to :func:`load_nfl_nextgen_stats`.

    Returns:
        pl.DataFrame: Panel with ``player_gsis_id`` pinned to ``Utf8`` and
        ``season`` pinned to ``Int64``. Empty/malformed input returns a
        zero-row frame that still carries both id columns.
    """
    loader = _loader or load_nfl_nextgen_stats
    df = loader(seasons=seasons, stat_type=stat_type)
    if not isinstance(df, pl.DataFrame):
        df = pl.from_pandas(df)
    if df.height == 0 or "week" not in df.columns:
        schema = dict(df.schema)
        schema.setdefault("player_gsis_id", pl.Utf8)
        schema.setdefault("season", pl.Int64)
        return pl.DataFrame(schema=schema)
    df = df.filter(pl.col("week") == 0) if level == "season" else df.filter(pl.col("week") > 0)
    return df.with_columns(
        # int-origin ids stringify via Int64 so "123" never becomes "123.0"
        pl.col("player_gsis_id").cast(pl.Int64, strict=False).cast(pl.Utf8)
        if df.schema["player_gsis_id"].is_numeric()
        else pl.col("player_gsis_id").cast(pl.Utf8),
        pl.col("season").cast(pl.Int64),
    )
