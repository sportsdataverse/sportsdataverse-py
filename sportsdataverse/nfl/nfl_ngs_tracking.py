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

from typing import Callable, List, Optional, Sequence, Union

import numpy as np
import pandas as pd
import polars as pl

from sportsdataverse.nfl.nfl_loaders import load_nfl_nextgen_stats
from sportsdataverse.nfl.nfl_ngs_constants import (
    MIN_RECEPTIONS,
    empirical_bayes_shrink,
)


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


_YAC_SCHEMA = {
    "season": pl.Int64,
    "player_gsis_id": pl.Utf8,
    "player_display_name": pl.Utf8,
    "team_abbr": pl.Utf8,
    "position": pl.Utf8,
    "receptions": pl.Float64,
    "avg_yac": pl.Float64,
    "avg_expected_yac": pl.Float64,
    "yac_oe_raw": pl.Float64,
    "yac_oe_shrunk": pl.Float64,
    "reliability": pl.Float64,
    "yac_oe_rank": pl.Int64,
}


def _shrink_over_season(
    panel: pl.DataFrame,
    raw_col: str,
    weight_col: str,
    min_w: float,
    out_raw: str,
    out_shrunk: str,
) -> pl.DataFrame:
    """Apply empirical-Bayes shrinkage per season; prior fit on qualified rows.

    The prior mean is the weight-averaged ``raw_col`` over rows with
    ``weight_col >= min_w`` (falls back to the all-row weighted mean when no
    row qualifies); ``tau2``/``sigma2`` come from
    :func:`sportsdataverse.nfl.nfl_ngs_constants.empirical_bayes_shrink`.
    """
    frames = []
    for (_season,), grp in panel.group_by("season", maintain_order=True):
        x = grp[raw_col].to_numpy().astype(float)
        n = grp[weight_col].to_numpy().astype(float)
        qualified = n >= min_w
        prior_mean = float(np.average(x[qualified], weights=n[qualified])) if qualified.any() else None
        shrunk, rel = empirical_bayes_shrink(x, n, prior_mean=prior_mean)
        frames.append(
            grp.with_columns(
                pl.Series(out_raw, x, dtype=pl.Float64),
                pl.Series(out_shrunk, shrunk, dtype=pl.Float64),
                pl.Series("reliability", rel, dtype=pl.Float64),
            )
        )
    return pl.concat(frames) if frames else panel


def _qualified_rank(shrunk_col: str, weight_col: str, min_w: float, out: str) -> pl.Expr:
    """Dense descending rank of ``shrunk_col`` within season over qualified rows only.

    Unqualified rows (``weight_col < min_w``) keep a null rank but are still
    returned; nulls are excluded from the rank by construction.
    """
    masked = pl.when(pl.col(weight_col) >= min_w).then(pl.col(shrunk_col)).otherwise(None)
    return masked.rank("dense", descending=True).over("season").cast(pl.Int64).alias(out)


def _season_list(seasons: Union[int, Sequence[int]]) -> List[int]:
    """Normalise ``seasons`` to a list of ints."""
    if isinstance(seasons, int):
        return [seasons]
    return [int(s) for s in seasons]


def _yac_oe_impl(
    seasons: Union[int, Sequence[int]],
    min_receptions: int,
    _loader: Optional[Callable[..., pl.DataFrame]],
) -> pl.DataFrame:
    panel = _ngs_panel(_season_list(seasons), "receiving", level="season", _loader=_loader)
    if panel.height == 0:
        return pl.DataFrame(schema=_YAC_SCHEMA)
    out = _shrink_over_season(
        panel,
        "avg_yac_above_expectation",
        "receptions",
        float(min_receptions),
        "yac_oe_raw",
        "yac_oe_shrunk",
    )
    out = (
        out.rename({"player_position": "position"})
        .with_columns(_qualified_rank("yac_oe_shrunk", "receptions", float(min_receptions), "yac_oe_rank"))
        .select(list(_YAC_SCHEMA.keys()))
    )
    return out.cast(_YAC_SCHEMA)


def nfl_ngs_yac_oe(
    seasons: Union[int, Sequence[int]],
    *,
    min_receptions: int = MIN_RECEPTIONS,
    return_as_pandas: bool = False,
    _loader: Optional[Callable[..., pl.DataFrame]] = None,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """YAC over expected per receiver-season, stabilised with EB shrinkage.

    ``yac_oe_raw`` is the NGS-shipped ``avg_yac_above_expectation`` passed
    through unchanged (per-reception yards after catch minus the NGS
    tracking-model expectation). ``yac_oe_shrunk`` applies per-season
    Efron-Morris empirical-Bayes shrinkage toward the reception-weighted
    league mean, weighted by ``receptions``, so small-sample extremes are
    pulled in. The shrinkage prior is fit at call time on rows with
    ``receptions >= min_receptions`` — no bundled artifact.

    Args:
        seasons (Union[int, Sequence[int]]): Season(s) to compute, 2016+.
        min_receptions (int): Qualification threshold for the prior fit and
            for receiving a ``yac_oe_rank``. Defaults to
            :data:`sportsdataverse.nfl.nfl_ngs_constants.MIN_RECEPTIONS`.
        return_as_pandas (bool): If True, returns a pandas DataFrame.
        _loader (Optional[Callable]): Injectable loader for offline tests.

    Returns:
        Union[pl.DataFrame, pd.DataFrame]: One row per
        ``(season, player_gsis_id)`` with raw + shrunk YAC-OE,
        ``reliability`` in [0, 1], and a dense descending
        ``yac_oe_rank`` over qualified rows (null for unqualified rows).
        Empty input returns a zero-row frame with the documented schema.

    Example:
        Top stabilised YAC-over-expected receivers::

            from sportsdataverse.nfl import nfl_ngs_yac_oe
            df = nfl_ngs_yac_oe([2023])
            print(df.sort("yac_oe_rank").head())

        Pandas output::

            df_pd = nfl_ngs_yac_oe(2023, return_as_pandas=True)

    See Also:
        * `nflreadpy`_ -- source loader parity (``load_nextgen_stats``)
        * `nflfastR`_ -- NFL play-by-play ecosystem (R)

    .. _nflreadpy: https://github.com/nflverse/nflreadpy
    .. _nflfastR: https://www.nflfastr.com
    """
    out = _yac_oe_impl(seasons, min_receptions, _loader)
    return out.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else out
