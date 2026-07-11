"""Native opponent-adjusted EPA power ratings for the NFL (model 1 of T4.2).

Fits an opponent-adjusted ridge on the **already-computed** ``epa`` column
from :func:`sportsdataverse.nfl.nfl_loaders.load_nfl_pbp` (owned by
``ep_wp.py`` -- this module never re-scores plays), producing per-team
offense / defense / special-teams components and ``adj_net``.

The solver, :func:`opponent_adjusted_ridge`, is re-exported from the
league-agnostic shared core :mod:`sportsdataverse._common.ratings` (T7.2) --
this module keeps its NFL-specific plumbing (loaders, competitive-play
filter, ``RatingsConfig``) and delegates the pure ridge math inward.

Non-market discipline (binding): nothing in this module reads
``spread_line`` / ``total_line`` / ``vegas_wp``; the competitive-play filter
uses the naive ``wp``.
"""

from __future__ import annotations

import datetime
from typing import Literal, overload

import pandas as pd
import polars as pl

from sportsdataverse._common.ratings import opponent_adjusted_ridge
from sportsdataverse.nfl.nfl_loaders import load_nfl_pbp, load_nfl_schedule
from sportsdataverse.nfl.nfl_prediction_constants import RatingsConfig, as_of_ratings_split

__all__ = ["efficiency_ratings", "nfl_ratings", "opponent_adjusted_ridge", "special_teams_ratings"]


_EFFICIENCY_OUTPUT_SCHEMA: dict[str, pl.PolarsDataType] = {
    "team_id": pl.Utf8,
    "adj_off_epa": pl.Float64,
    "adj_def_epa": pl.Float64,
    "adj_net": pl.Float64,
    "games": pl.Int64,
}

_ST_OUTPUT_SCHEMA: dict[str, pl.PolarsDataType] = {
    "team_id": pl.Utf8,
    "adj_st_epa": pl.Float64,
}


def efficiency_ratings(plays: pl.DataFrame, *, config: RatingsConfig | None = None) -> pl.DataFrame:
    """One row per team: opponent-adjusted offense/defense EPA per play.

    Filters ``plays`` to competitive non-special-teams scrimmage plays
    (``special != 1``, ``qb_kneel != 1``, ``qb_spike != 1``,
    ``min_competitive_wp <= wp <= max_competitive_wp``, non-null
    ``epa``/``posteam``/``defteam``) and fits
    :func:`opponent_adjusted_ridge` on ``epa``. Callers pass an already
    as-of-date-filtered frame (the public ``nfl_ratings`` entry point does
    the date filter) -- this function is pure.

    Args:
        plays: An ``load_nfl_pbp``-schema frame carrying ``game_id``,
            ``posteam``, ``defteam``, ``home_team``, ``epa``, ``wp``,
            ``special``, ``qb_kneel``, ``qb_spike``.
        config: Tuning knobs (``ridge_lambda`` + the competitive-``wp``
            window); defaults to :class:`RatingsConfig`.

    Returns:
        pl.DataFrame: One row per ``team_id`` (Utf8) with ``adj_off_epa`` /
        ``adj_def_epa`` / ``adj_net`` (Float64, ``adj_net = adj_off_epa -
        adj_def_epa``) and ``games`` (Int64). Zero-row, correctly-typed on
        empty/fully-filtered input.

    Example:
        Quick start::

            from sportsdataverse.nfl.nfl_ratings import efficiency_ratings
            ratings = efficiency_ratings(pbp)
            ratings.sort("adj_net", descending=True).head()
    """
    cfg = config or RatingsConfig()
    clean = plays.filter(
        (pl.col("epa").is_not_null())
        & (pl.col("posteam").is_not_null())
        & (pl.col("defteam").is_not_null())
        & (pl.col("special") != 1)
        & (pl.col("qb_kneel") != 1)
        & (pl.col("qb_spike") != 1)
        & (pl.col("wp") >= cfg.min_competitive_wp)
        & (pl.col("wp") <= cfg.max_competitive_wp)
    )
    if clean.height == 0:
        return pl.DataFrame(schema=_EFFICIENCY_OUTPUT_SCHEMA)

    frame, _intercept, _home = opponent_adjusted_ridge(
        clean,
        off_col="posteam",
        def_col="defteam",
        home_col="home_team",
        resp_col="epa",
        lam=cfg.ridge_lambda,
    )
    games = clean.group_by(pl.col("posteam").cast(pl.Utf8).alias("team_id")).agg(
        pl.col("game_id").n_unique().cast(pl.Int64).alias("games")
    )
    assert frame.schema["team_id"] == games.schema["team_id"]
    return (
        frame.join(games, on="team_id", how="left")
        .with_columns(pl.col("games").fill_null(0))
        .rename({"off_coef": "adj_off_epa", "def_coef": "adj_def_epa"})
        .with_columns(adj_net=pl.col("adj_off_epa") - pl.col("adj_def_epa"))
        .select("team_id", "adj_off_epa", "adj_def_epa", "adj_net", "games")
    )


def special_teams_ratings(plays: pl.DataFrame, *, config: RatingsConfig | None = None) -> pl.DataFrame:
    """One row per team: opponent-adjusted special-teams EPA per play.

    Reuses :func:`opponent_adjusted_ridge` (no forked solver) restricted to
    ``special == 1`` plays with ``resp_col="epa"``; ``adj_st_epa`` is the
    ``off_coef`` (the special-teams unit acting as "offense" on the play).
    Teams appearing anywhere in ``plays`` but on no special-teams play get
    the documented neutral fill ``adj_st_epa = 0.0``.

    Args:
        plays: An ``load_nfl_pbp``-schema frame carrying ``posteam``,
            ``defteam``, ``home_team``, ``epa``, ``special``. Not
            pre-filtered -- this function selects the ST plays itself.
        config: Tuning knobs (only ``ridge_lambda`` is consulted); defaults
            to :class:`RatingsConfig`.

    Returns:
        pl.DataFrame: One row per ``team_id`` (Utf8) with ``adj_st_epa``
        (Float64). Zero-row, correctly-typed when ``plays`` is empty.

    Example:
        Quick start::

            from sportsdataverse.nfl.nfl_ratings import special_teams_ratings
            st = special_teams_ratings(pbp)
            st.sort("adj_st_epa", descending=True).head()
    """
    cfg = config or RatingsConfig()
    if plays.height == 0:
        return pl.DataFrame(schema=_ST_OUTPUT_SCHEMA)
    roster = plays.select(pl.col("posteam").cast(pl.Utf8).alias("team_id")).drop_nulls().unique()
    st_plays = plays.filter(
        (pl.col("special") == 1)
        & (pl.col("epa").is_not_null())
        & (pl.col("posteam").is_not_null())
        & (pl.col("defteam").is_not_null())
    )
    if st_plays.height == 0:
        return roster.with_columns(pl.lit(0.0).alias("adj_st_epa")).select("team_id", "adj_st_epa")
    frame, _intercept, _home = opponent_adjusted_ridge(
        st_plays,
        off_col="posteam",
        def_col="defteam",
        home_col="home_team",
        resp_col="epa",
        lam=cfg.ridge_lambda,
    )
    assert roster.schema["team_id"] == frame.schema["team_id"]
    return (
        roster.join(frame.select("team_id", "off_coef"), on="team_id", how="left")
        .with_columns(pl.col("off_coef").fill_null(0.0).alias("adj_st_epa"))
        .select("team_id", "adj_st_epa")
    )


def _add_ranks(df: pl.DataFrame) -> pl.DataFrame:
    """Append dense ranks + a net z-score to a ratings frame.

    Adds ``off_rank`` (dense on ``adj_off_epa`` descending), ``def_rank``
    (dense on ``adj_def_epa`` **ascending** -- lower allowed EPA is better),
    ``net_rank`` (dense on ``adj_net`` descending), and ``net_z`` (z-score of
    ``adj_net``; 0.0 when the standard deviation is zero). Pure frame->frame.
    """
    out = df.with_columns(
        off_rank=pl.col("adj_off_epa").rank(method="dense", descending=True).cast(pl.Int64),
        def_rank=pl.col("adj_def_epa").rank(method="dense", descending=False).cast(pl.Int64),
        net_rank=pl.col("adj_net").rank(method="dense", descending=True).cast(pl.Int64),
    )
    std_val = out["adj_net"].std()
    if not std_val:
        return out.with_columns(net_z=pl.lit(0.0).cast(pl.Float64))
    mean_net = float(out["adj_net"].mean() or 0.0)
    return out.with_columns(net_z=(pl.col("adj_net") - mean_net) / float(std_val))


# Down-select applied to the loaded pbp BEFORE the ridge so no market column
# (spread_line / total_line / vegas_wp) can leak into the rating fit -- the
# binding non-market boundary of this spine.
_RIDGE_INPUT_COLUMNS: tuple[str, ...] = (
    "game_id",
    "posteam",
    "defteam",
    "home_team",
    "epa",
    "wp",
    "special",
    "qb_kneel",
    "qb_spike",
    "play_type",
)

_RATINGS_OUTPUT_SCHEMA: dict[str, pl.PolarsDataType] = {
    "season": pl.Int64,
    "team_id": pl.Utf8,
    "adj_off_epa": pl.Float64,
    "adj_def_epa": pl.Float64,
    "adj_st_epa": pl.Float64,
    "adj_net": pl.Float64,
    "games": pl.Int64,
    "off_rank": pl.Int64,
    "def_rank": pl.Int64,
    "net_rank": pl.Int64,
    "net_z": pl.Float64,
}


@overload
def nfl_ratings(
    seasons: int | list[int],
    *,
    as_of_date: datetime.date | None = ...,
    config: RatingsConfig | None = ...,
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...
@overload
def nfl_ratings(
    seasons: int | list[int],
    *,
    as_of_date: datetime.date | None = ...,
    config: RatingsConfig | None = ...,
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...
def nfl_ratings(
    seasons: int | list[int],
    *,
    as_of_date: datetime.date | None = None,
    config: RatingsConfig | None = None,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """One row per team: the native NFL ratings spine (off/def/ST EPA).

    Public orchestrator over :func:`efficiency_ratings` +
    :func:`special_teams_ratings`. Loads play-by-play + schedule via
    ``load_nfl_pbp`` / ``load_nfl_schedule``, joins each game's ``gameday``
    onto the plays, optionally applies the as-of-date leakage boundary
    (only plays from games with ``gameday < as_of_date`` are used), then
    fits both components and reshapes into one wide per-team table with
    dense ranks and a net z-score.

    The loaded pbp is down-selected to the ridge columns *before* any fit so
    no market column (``spread_line`` / ``vegas_wp``) can leak into the
    ratings (the binding non-market boundary).

    Args:
        seasons: A single season (e.g. ``2023``) or a list of seasons pooled
            into one combined fit.
        as_of_date: When given, only plays from games strictly before this
            date are used (mirrors what was knowable heading into that date).
            ``None`` (default) uses the full season(s).
        config: Tuning knobs forwarded to both component fits; defaults to
            :class:`RatingsConfig`.
        return_as_pandas: If True, returns a pandas DataFrame.

    Returns:
        A DataFrame with one row per ``team_id``: ``season`` (Int64 -- the
        single passed season, ``null`` for a pooled multi-season call),
        ``team_id`` (Utf8), ``adj_off_epa`` / ``adj_def_epa`` / ``adj_st_epa``
        / ``adj_net`` (Float64; ``adj_net`` is offense minus defense --
        special teams stays a separate column), ``games`` (Int64),
        ``off_rank`` / ``def_rank`` / ``net_rank`` (Int64; ``def_rank``
        ascends -- fewer EPA allowed ranks better), ``net_z`` (Float64).
        Zero-row, correctly-typed when the seasons have no data or
        ``as_of_date`` filters out every play.

    Example:
        Quick start::

            from sportsdataverse.nfl import nfl_ratings
            ratings = nfl_ratings(2023)
            ratings.sort("net_rank").head()

        As-of-date leakage boundary::

            import datetime as dt
            week6 = nfl_ratings(2023, as_of_date=dt.date(2023, 10, 12))

    See Also:
        * `nflfastR`_ -- the R package whose EPA model feeds these ratings.

    .. _nflfastR: https://www.nflfastr.com
    """
    cfg = config or RatingsConfig()
    season_list: list[int] = [seasons] if isinstance(seasons, int) else list(seasons)

    plays = load_nfl_pbp(season_list)
    schedule = load_nfl_schedule(season_list)
    if plays.is_empty() or schedule.is_empty():
        empty = pl.DataFrame(schema=_RATINGS_OUTPUT_SCHEMA)
        return empty.to_pandas() if return_as_pandas else empty

    plays = plays.select(_RIDGE_INPUT_COLUMNS).with_columns(pl.col("game_id").cast(pl.Utf8))
    schedule = schedule.with_columns(pl.col("game_id").cast(pl.Utf8))
    assert plays.schema["game_id"] == schedule.schema["game_id"]

    dated = plays.join(schedule.select("game_id", pl.col("gameday").cast(pl.Date)), on="game_id", how="left")
    if as_of_date is not None:
        dated = as_of_ratings_split(dated, as_of_date)

    eff = efficiency_ratings(dated, config=cfg)
    if eff.height == 0:
        empty = pl.DataFrame(schema=_RATINGS_OUTPUT_SCHEMA)
        return empty.to_pandas() if return_as_pandas else empty
    st = special_teams_ratings(dated, config=cfg)
    assert eff.schema["team_id"] == st.schema["team_id"]

    season_value: int | None = season_list[0] if len(season_list) == 1 else None
    out = (
        eff.join(st, on="team_id", how="left")
        .with_columns(
            pl.col("adj_st_epa").fill_null(0.0),
            pl.lit(season_value).cast(pl.Int64).alias("season"),
        )
        .pipe(_add_ranks)
        .select(*_RATINGS_OUTPUT_SCHEMA.keys())
    )
    return out.to_pandas() if return_as_pandas else out
