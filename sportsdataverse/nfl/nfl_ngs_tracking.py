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

from sportsdataverse.nfl.nfl_loaders import (
    load_nfl_nextgen_stats,
    load_nfl_pbp_participation,
)
from sportsdataverse.nfl.nfl_ngs_constants import (
    MIN_ATTEMPTS,
    MIN_RECEPTIONS,
    MIN_TARGETS,
    RECEIVER_POSITIONS,
    empirical_bayes_shrink,
    expected_separation_ridge,
    weekly_sigma2,
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
    weekly: Optional[pl.DataFrame] = None,
    weekly_raw_col: Optional[str] = None,
) -> pl.DataFrame:
    """Apply empirical-Bayes shrinkage per season; prior fit on qualified rows.

    The prior mean is the weight-averaged ``raw_col`` over rows with
    ``weight_col >= min_w`` (falls back to the all-row weighted mean when no
    row qualifies). When ``weekly`` rows are available, the sampling variance
    ``sigma2`` is identified per season from within-player across-week
    variation (:func:`sportsdataverse.nfl.nfl_ngs_constants.weekly_sigma2`) —
    the season-panel OLS is weakly identified when all players carry similar
    sample sizes (observed on the rushing panel: tau2 floored, reliability
    collapsed to ~0).
    """
    frames = []
    for (_season,), grp in panel.group_by("season", maintain_order=True):
        x = grp[raw_col].to_numpy().astype(float)
        n = grp[weight_col].to_numpy().astype(float)
        qualified = n >= min_w
        prior_mean = float(np.average(x[qualified], weights=n[qualified])) if qualified.any() else None
        sigma2 = None
        if weekly is not None and weekly.height > 0:
            sigma2 = weekly_sigma2(
                weekly.filter(pl.col("season") == _season),
                weekly_raw_col or raw_col,
                weight_col,
            )
        shrunk, rel = empirical_bayes_shrink(x, n, prior_mean=prior_mean, sigma2=sigma2)
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
    season_list = _season_list(seasons)
    panel = _ngs_panel(season_list, "receiving", level="season", _loader=_loader)
    if panel.height == 0:
        return pl.DataFrame(schema=_YAC_SCHEMA)
    weekly = _ngs_panel(season_list, "receiving", level="week", _loader=_loader)
    out = _shrink_over_season(
        panel,
        "avg_yac_above_expectation",
        "receptions",
        float(min_receptions),
        "yac_oe_raw",
        "yac_oe_shrunk",
        weekly=weekly,
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


_RYOE_SCHEMA = {
    "season": pl.Int64,
    "player_gsis_id": pl.Utf8,
    "player_display_name": pl.Utf8,
    "team_abbr": pl.Utf8,
    "position": pl.Utf8,
    "rush_attempts": pl.Float64,
    "rush_yards": pl.Float64,
    "expected_rush_yards": pl.Float64,
    "ryoe_total": pl.Float64,
    "ryoe_per_att_raw": pl.Float64,
    "ryoe_per_att_shrunk": pl.Float64,
    "pct_stacked_box": pl.Float64,
    "reliability": pl.Float64,
    "ryoe_rank": pl.Int64,
}


def _ryoe_impl(
    seasons: Union[int, Sequence[int]],
    min_attempts: int,
    _loader: Optional[Callable[..., pl.DataFrame]],
) -> pl.DataFrame:
    season_list = _season_list(seasons)
    panel = _ngs_panel(season_list, "rushing", level="season", _loader=_loader)
    if panel.height == 0:
        return pl.DataFrame(schema=_RYOE_SCHEMA)
    weekly = _ngs_panel(season_list, "rushing", level="week", _loader=_loader)
    out = _shrink_over_season(
        panel,
        "rush_yards_over_expected_per_att",
        "rush_attempts",
        float(min_attempts),
        "ryoe_per_att_raw",
        "ryoe_per_att_shrunk",
        weekly=weekly,
    )
    out = (
        out.rename(
            {
                "player_position": "position",
                "rush_yards_over_expected": "ryoe_total",
                "percent_attempts_gte_eight_defenders": "pct_stacked_box",
            }
        )
        .with_columns(_qualified_rank("ryoe_per_att_shrunk", "rush_attempts", float(min_attempts), "ryoe_rank"))
        .select(list(_RYOE_SCHEMA.keys()))
    )
    return out.cast(_RYOE_SCHEMA)


def nfl_ngs_ryoe(
    seasons: Union[int, Sequence[int]],
    *,
    min_attempts: int = MIN_ATTEMPTS,
    return_as_pandas: bool = False,
    _loader: Optional[Callable[..., pl.DataFrame]] = None,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Rush yards over expected per rusher-season, stabilised with EB shrinkage.

    ``ryoe_per_att_raw`` is the NGS-shipped
    ``rush_yards_over_expected_per_att`` passed through unchanged (the NGS
    tracking-model residual); ``ryoe_total`` is the season total
    ``rush_yards_over_expected``. ``ryoe_per_att_shrunk`` applies per-season
    Efron-Morris empirical-Bayes shrinkage toward the attempt-weighted league
    mean, weighted by ``rush_attempts``. ``pct_stacked_box``
    (``percent_attempts_gte_eight_defenders``) is reported as a context
    covariate — v1 does not adjust on it. The prior is fit at call time on
    rows with ``rush_attempts >= min_attempts`` — no bundled artifact.

    Args:
        seasons (Union[int, Sequence[int]]): Season(s) to compute, 2016+.
        min_attempts (int): Qualification threshold for the prior fit and for
            receiving a ``ryoe_rank``. Defaults to
            :data:`sportsdataverse.nfl.nfl_ngs_constants.MIN_ATTEMPTS`.
        return_as_pandas (bool): If True, returns a pandas DataFrame.
        _loader (Optional[Callable]): Injectable loader for offline tests.

    Returns:
        Union[pl.DataFrame, pd.DataFrame]: One row per
        ``(season, player_gsis_id)`` with raw + shrunk RYOE/attempt,
        ``reliability`` in [0, 1], and a dense descending ``ryoe_rank`` over
        qualified rows (null for unqualified rows). Empty input returns a
        zero-row frame with the documented schema.

    Example:
        Stabilised RYOE leaders::

            from sportsdataverse.nfl import nfl_ngs_ryoe
            df = nfl_ngs_ryoe([2023])
            print(df.sort("ryoe_rank").head())

        Pandas output::

            df_pd = nfl_ngs_ryoe(2023, return_as_pandas=True)

    See Also:
        * `nflreadpy`_ -- source loader parity (``load_nextgen_stats``)
        * `nflfastR`_ -- NFL play-by-play ecosystem (R)

    .. _nflreadpy: https://github.com/nflverse/nflreadpy
    .. _nflfastR: https://www.nflfastr.com
    """
    out = _ryoe_impl(seasons, min_attempts, _loader)
    return out.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else out


_SEP_SCHEMA = {
    "season": pl.Int64,
    "player_gsis_id": pl.Utf8,
    "player_display_name": pl.Utf8,
    "team_abbr": pl.Utf8,
    "position": pl.Utf8,
    "targets": pl.Float64,
    "avg_cushion": pl.Float64,
    "avg_separation": pl.Float64,
    "avg_intended_air_yards": pl.Float64,
    "expected_separation": pl.Float64,
    "sep_oe_raw": pl.Float64,
    "sep_oe_shrunk": pl.Float64,
    "reliability": pl.Float64,
    "sep_oe_rank": pl.Int64,
}

_SEP_FEATURE_COLS = ("avg_cushion", "avg_intended_air_yards")


def _sep_design_matrix(grp: pl.DataFrame) -> np.ndarray:
    """Numeric features + one-hot position (RECEIVER_POSITIONS, else other)."""
    numeric = np.column_stack([grp[c].to_numpy().astype(float) for c in _SEP_FEATURE_COLS])
    pos = grp["position"].to_numpy()
    onehot = np.column_stack(
        [(pos == p).astype(float) for p in RECEIVER_POSITIONS] + [(~np.isin(pos, RECEIVER_POSITIONS)).astype(float)]
    )
    return np.column_stack([numeric, onehot])


def _separation_oe_impl(
    seasons: Union[int, Sequence[int]],
    min_targets: int,
    _loader: Optional[Callable[..., pl.DataFrame]],
) -> pl.DataFrame:
    season_list = _season_list(seasons)
    panel = _ngs_panel(season_list, "receiving", level="season", _loader=_loader)
    if panel.height == 0:
        return pl.DataFrame(schema=_SEP_SCHEMA)
    panel = panel.rename({"player_position": "position"}).drop_nulls(["avg_separation", "targets", *_SEP_FEATURE_COLS])
    if panel.height == 0:
        return pl.DataFrame(schema=_SEP_SCHEMA)
    weekly = _ngs_panel(season_list, "receiving", level="week", _loader=_loader)
    frames = []
    for (_season,), grp in panel.group_by("season", maintain_order=True):
        y = grp["avg_separation"].to_numpy().astype(float)
        w = grp["targets"].to_numpy().astype(float)
        expected, _beta = expected_separation_ridge(y, _sep_design_matrix(grp), w)
        frames.append(
            grp.with_columns(
                pl.Series("expected_separation", expected, dtype=pl.Float64),
                pl.Series("sep_oe_raw", y - expected, dtype=pl.Float64),
            )
        )
    out = pl.concat(frames)
    # weekly avg_separation variation identifies sigma2 for the residual
    # (the smooth expectation contributes ~nothing to within-player variance)
    out = _shrink_over_season(
        out,
        "sep_oe_raw",
        "targets",
        float(min_targets),
        "sep_oe_raw",
        "sep_oe_shrunk",
        weekly=weekly,
        weekly_raw_col="avg_separation",
    )
    out = out.with_columns(_qualified_rank("sep_oe_shrunk", "targets", float(min_targets), "sep_oe_rank")).select(
        list(_SEP_SCHEMA.keys())
    )
    return out.cast(_SEP_SCHEMA)


def nfl_ngs_separation_oe(
    seasons: Union[int, Sequence[int]],
    *,
    min_targets: int = MIN_TARGETS,
    return_as_pandas: bool = False,
    _loader: Optional[Callable[..., pl.DataFrame]] = None,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Separation over a built context expectation, per receiver-season.

    Unlike YAC-OE and RYOE, NGS ships no expected-separation field, so this
    model BUILDS one: a per-season weighted ridge
    (:func:`sportsdataverse.nfl.nfl_ngs_constants.expected_separation_ridge`)
    of ``avg_separation`` on ``avg_cushion``, ``avg_intended_air_yards`` and
    a position one-hot, weighted by ``targets``. ``sep_oe_raw`` is the
    residual — a CONTEXT residual (role/scheme proxies), not a
    tracking-model expectation; treat it as descriptive, not causal.
    ``sep_oe_shrunk`` applies the same per-season empirical-Bayes shrinkage
    as the sibling models, weighted by ``targets``. All parameters are fit
    from the requested seasons at call time — no bundled artifact.

    Args:
        seasons (Union[int, Sequence[int]]): Season(s) to compute, 2016+.
        min_targets (int): Qualification threshold for the shrinkage prior
            and for receiving a ``sep_oe_rank``. Defaults to
            :data:`sportsdataverse.nfl.nfl_ngs_constants.MIN_TARGETS`.
        return_as_pandas (bool): If True, returns a pandas DataFrame.
        _loader (Optional[Callable]): Injectable loader for offline tests.

    Returns:
        Union[pl.DataFrame, pd.DataFrame]: One row per
        ``(season, player_gsis_id)`` with the built ``expected_separation``,
        raw + shrunk separation-over-expected, ``reliability`` in [0, 1],
        and a dense descending ``sep_oe_rank`` over qualified rows. Rows
        with null separation/cushion/air-yards inputs are dropped before
        the fit. Empty input returns a zero-row frame with the documented
        schema.

    Example:
        Separation over expected, 2023::

            from sportsdataverse.nfl import nfl_ngs_separation_oe
            df = nfl_ngs_separation_oe([2023])
            print(df.sort("sep_oe_rank").head())

        Pandas output::

            df_pd = nfl_ngs_separation_oe(2023, return_as_pandas=True)

    See Also:
        * `nflreadpy`_ -- source loader parity (``load_nextgen_stats``)
        * `nflfastR`_ -- NFL play-by-play ecosystem (R)

    .. _nflreadpy: https://github.com/nflverse/nflreadpy
    .. _nflfastR: https://www.nflfastr.com
    """
    out = _separation_oe_impl(seasons, min_targets, _loader)
    return out.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else out


_COVERAGE_TYPES = tuple(f"COVER_{i}" for i in range(7))

_MAN_ZONE_SCHEMA = {
    "season": pl.Int64,
    "defteam": pl.Utf8,
    "plays": pl.Int64,
    "man_rate": pl.Float64,
    "zone_rate": pl.Float64,
    **{f"cover_{i}_rate": pl.Float64 for i in range(7)},
}


def nfl_ngs_man_zone_rates(
    seasons: Union[int, Sequence[int]],
    *,
    return_as_pandas: bool = False,
    _loader: Optional[Callable[..., pl.DataFrame]] = None,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Descriptive man/zone coverage rates from NGS-charted labels — NOT a trained classifier.

    This is a group-by of the ``defense_man_zone_type`` /
    ``defense_coverage_type`` labels that ship in
    :func:`sportsdataverse.nfl.load_nfl_pbp_participation` for charted
    seasons (2016-2023). A *trained* coverage classifier is data-blocked —
    see the module docstring's "Blocked (needs snap tracking)" section.
    Unlabelled plays are dropped before rates are computed; ``2_MAN`` and
    ``PREVENT`` calls stay in the ``plays`` denominator but have no
    dedicated rate column, so the ``cover_*_rate`` columns sum to slightly
    under 1 while ``man_rate + zone_rate == 1`` exactly.

    Args:
        seasons (Union[int, Sequence[int]]): Season(s), charted 2016-2023.
            Seasons are loaded one at a time and concatenated
            ``diagonal_relaxed`` (the participation feed drifts schema
            across seasons).
        return_as_pandas (bool): If True, returns a pandas DataFrame.
        _loader (Optional[Callable]): Injectable loader for offline tests.

    Returns:
        Union[pl.DataFrame, pd.DataFrame]: One row per ``(season, defteam)``
        with ``plays`` (labelled plays only), ``man_rate``, ``zone_rate``
        and ``cover_0_rate`` ... ``cover_6_rate``. Un-charted seasons (all
        labels null, e.g. 2024+) return a zero-row frame with the
        documented schema.

    Example:
        League man/zone tendencies, 2022::

            from sportsdataverse.nfl import nfl_ngs_man_zone_rates
            df = nfl_ngs_man_zone_rates([2022])
            print(df.sort("man_rate", descending=True).head())

    See Also:
        * `nflreadpy`_ -- participation loader parity
        * `nflfastR`_ -- NFL play-by-play ecosystem (R)

    .. _nflreadpy: https://github.com/nflverse/nflreadpy
    .. _nflfastR: https://www.nflfastr.com
    """
    loader = _loader or load_nfl_pbp_participation
    frames = []
    for season in _season_list(seasons):
        # load per season: the participation feed's schema drifts across
        # seasons and the loader crashes on a multi-season list
        df = loader([season])
        if not isinstance(df, pl.DataFrame):
            df = pl.from_pandas(df)
        if df.height > 0:
            frames.append(df)
    if not frames:
        return _man_zone_empty(return_as_pandas)
    part = pl.concat(frames, how="diagonal_relaxed")
    required = {"nflverse_game_id", "possession_team", "defense_man_zone_type"}
    if not required.issubset(part.columns):
        return _man_zone_empty(return_as_pandas)
    part = part.drop_nulls(["defense_man_zone_type", "possession_team"])
    if part.height == 0:
        return _man_zone_empty(return_as_pandas)
    id_parts = pl.col("nflverse_game_id").str.split("_")
    part = part.with_columns(
        id_parts.list.get(0).cast(pl.Int64).alias("season"),
        pl.when(pl.col("possession_team") == id_parts.list.get(2))
        .then(id_parts.list.get(3))
        .otherwise(id_parts.list.get(2))
        .alias("defteam"),
        pl.col("defense_man_zone_type").str.to_uppercase().str.contains("MAN").alias("_is_man"),
    )
    out = part.group_by("season", "defteam").agg(
        pl.len().cast(pl.Int64).alias("plays"),
        (pl.col("_is_man") == True).mean().alias("man_rate"),  # noqa: E712
        (pl.col("_is_man") == False).mean().alias("zone_rate"),  # noqa: E712
        *[
            (pl.col("defense_coverage_type") == cov).fill_null(False).mean().alias(f"cover_{i}_rate")
            for i, cov in enumerate(_COVERAGE_TYPES)
        ],
    )
    out = out.sort("season", "defteam").select(list(_MAN_ZONE_SCHEMA.keys())).cast(_MAN_ZONE_SCHEMA)
    return out.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else out


def _man_zone_empty(return_as_pandas: bool) -> Union[pl.DataFrame, pd.DataFrame]:
    out = pl.DataFrame(schema=_MAN_ZONE_SCHEMA)
    return out.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else out
