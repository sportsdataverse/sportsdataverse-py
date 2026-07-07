"""As-of-date opponent-adjusted efficiency ratings for college football.

Thin wrapper around :mod:`sportsdataverse.cfb.cfb_adjusted_epa` -- reuses its
ridge primitives (``_prepare`` for the competitive-play filter + id casting,
``_fit_opponent_ridge`` for the offense/defense ridge fit) instead of forking
a second ridge solver. Callers are responsible for any as-of-date filtering
(e.g. via :func:`sportsdataverse.cfb.cfb_prediction_constants.as_of_ratings_split`)
before calling :func:`efficiency_ratings` -- this module performs no date
filtering of its own.

.. note::
    ``sportsdataverse/cfb/__init__.py`` does ``from .cfb_adjusted_epa import
    *``, which rebinds the package attribute ``cfb_adjusted_epa`` to the
    *function* of that name (the re-export for
    ``sportsdataverse.cfb.cfb_adjusted_epa(...)`` callers). Both
    ``from sportsdataverse.cfb import cfb_adjusted_epa as _aepa`` and
    ``import sportsdataverse.cfb.cfb_adjusted_epa as _aepa`` therefore resolve
    to that function rather than the submodule (confirmed empirically), so
    the private ridge primitives are imported by name directly from the
    submodule's fully-qualified path below -- that import path is resolved
    from ``sys.modules`` by the dotted string, not by attribute traversal on
    the already-shadowed package, and keeps mypy able to see the real
    signatures. :func:`cfb_ratings` (the public as-of-date orchestrator)
    imports ``load_cfb_pbp`` / ``load_cfb_schedule`` at module scope for the
    same reason: monkeypatch-ability in tests requires the names to live on
    *this* module's namespace, not just re-exported through the package.
"""

from __future__ import annotations

import datetime
from typing import Literal, overload

import pandas as pd
import polars as pl

from sportsdataverse.cfb.cfb_adjusted_epa import _REQUIRED_COLUMNS, _fit_opponent_ridge, _prepare
from sportsdataverse.cfb.cfb_loaders import load_cfb_pbp, load_cfb_schedule
from sportsdataverse.cfb.cfb_prediction_constants import RatingsConfig, as_of_ratings_split

__all__ = ["cfb_ratings", "efficiency_ratings", "fei_ratings", "special_teams_ratings"]

_OUTPUT_SCHEMA: dict[str, pl.PolarsDataType] = {
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

_FEI_OUTPUT_SCHEMA: dict[str, pl.PolarsDataType] = {
    "team_id": pl.Utf8,
    "fei_off": pl.Float64,
    "fei_def": pl.Float64,
    "fei_net": pl.Float64,
}

# Documented column order + dtypes for the public `cfb_ratings` entry point --
# see its docstring for what each column means.
_RATINGS_OUTPUT_SCHEMA: dict[str, pl.PolarsDataType] = {
    "season": pl.Int64,
    "team_id": pl.Utf8,
    "adj_off_epa": pl.Float64,
    "adj_def_epa": pl.Float64,
    "adj_st_epa": pl.Float64,
    "adj_net": pl.Float64,
    "fei_off": pl.Float64,
    "fei_def": pl.Float64,
    "fei_net": pl.Float64,
    "games": pl.Int64,
    "off_rank": pl.Int64,
    "def_rank": pl.Int64,
    "net_rank": pl.Int64,
    "net_z": pl.Float64,
}

# Case-insensitive keyword match over the cfbfastR `play_type` vocabulary for
# kickoffs/punts/field goals (returns, blocks, touchbacks, etc. all contain one
# of these words) -- deliberately loose since `play_type` free text varies.
_ST_PLAY_TYPE_PATTERN = "(?i)kickoff|punt|field goal"


def efficiency_ratings(plays: pl.DataFrame, *, config: RatingsConfig | None = None) -> pl.DataFrame:
    """One row per team: opponent-adjusted offensive/defensive efficiency.

    Fits the offense/defense ridge from :mod:`cfb_adjusted_epa` on the
    competitive plays in ``plays`` (``min_competitive_wp <= wp_before <=
    max_competitive_wp``) and reshapes the result to one row per team,
    including the reference team the ridge's ``model.matrix``-style
    parameterization drops (its rating is the fitted intercept, i.e. the
    league baseline).

    Args:
        plays: A cfbfastR-schema play-by-play frame carrying every column in
            ``cfb_adjusted_epa._REQUIRED_COLUMNS`` (``game_id``, ``pos_team``,
            ``pos_team_id``, ``def_pos_team_id``, ``home``, ``neutral_site``,
            ``EPA``, ``pass``, ``rush``, ``wp_before``). Callers pass an
            already as-of-date-filtered frame; this function is pure.
        config: Ratings tuning knobs. Only ``ridge_lambda`` is consulted here;
            defaults to :class:`RatingsConfig` when omitted.

    Returns:
        A ``polars.DataFrame`` with one row per ``team_id``: ``team_id``
        (Utf8), ``adj_off_epa`` / ``adj_def_epa`` / ``adj_net`` (Float64),
        ``games`` (Int64). Empty (zero-row, correctly-typed) when ``plays``
        has no competitive plays.

    Raises:
        KeyError: If ``plays`` is missing a required column.
        ImportError: If ``scikit-learn`` is not installed.

    Example:
        Quick start::

            from sportsdataverse.cfb.cfb_ratings import efficiency_ratings
            ratings = efficiency_ratings(pbp)
            ratings.sort("adj_net", descending=True).head()

        Custom ridge penalty::

            from sportsdataverse.cfb.cfb_prediction_constants import RatingsConfig
            ratings = efficiency_ratings(pbp, config=RatingsConfig(ridge_lambda=100.0))

    See Also:
        * `cfbfastR`_ -- the R implementation ``cfb_adjusted_epa`` ports.

    .. _cfbfastR: https://cfbfastR.sportsdataverse.org
    """
    cfg = config or RatingsConfig()
    base, clean = _prepare(plays, _REQUIRED_COLUMNS)
    if clean.height == 0:
        return pl.DataFrame(schema=_OUTPUT_SCHEMA)

    offense, defense, intercept = _fit_opponent_ridge(clean, cfg.ridge_lambda)

    games = (
        base.group_by("pos_team_id")
        .agg(pl.col("game_id").n_unique().cast(pl.Int64).alias("games"))
        .rename({"pos_team_id": "team_id"})
    )
    assert games.schema["team_id"] == pl.Utf8
    assert offense.schema["team_id"] == pl.Utf8
    assert defense.schema["team_id"] == pl.Utf8

    out = (
        games.join(offense.rename({"adjmodelOff": "adj_off_epa"}), on="team_id", how="left")
        .join(defense.rename({"adjmodelDef": "adj_def_epa"}), on="team_id", how="left")
        .with_columns(
            pl.col("adj_off_epa").fill_null(intercept),
            pl.col("adj_def_epa").fill_null(intercept),
            pl.col("games").fill_null(0),
        )
        .with_columns(adj_net=pl.col("adj_off_epa") - pl.col("adj_def_epa"))
        .select("team_id", "adj_off_epa", "adj_def_epa", "adj_net", "games")
    )
    return out


def special_teams_ratings(plays: pl.DataFrame, *, config: RatingsConfig | None = None) -> pl.DataFrame:
    """One row per team: opponent-adjusted special-teams EPA.

    ``cfb_adjusted_epa._prepare`` filters to pass/rush plays only, so it drops
    every special-teams snap and cannot be reused here. Special-teams plays
    (kickoffs, punts, field goals) are instead selected by a ``play_type``
    keyword match, given the same competitive-play home-field-advantage
    (``hfa``) treatment ``_prepare`` applies, and fit through the same
    :func:`sportsdataverse.cfb.cfb_adjusted_epa._fit_opponent_ridge` ridge
    solver -- no forked/duplicate ridge fit.

    Args:
        plays: A cfbfastR-schema play-by-play frame carrying every column in
            ``cfb_adjusted_epa._REQUIRED_COLUMNS`` (``game_id``, ``pos_team``,
            ``pos_team_id``, ``def_pos_team_id``, ``home``, ``neutral_site``,
            ``EPA``, ``pass``, ``rush``, ``wp_before``) plus ``play_type``.
            Not pre-filtered to special-teams plays -- this function does that
            filtering itself.
        config: Ratings tuning knobs. Only ``ridge_lambda`` is consulted here;
            defaults to :class:`RatingsConfig` when omitted.

    Returns:
        A ``polars.DataFrame`` with one row per ``team_id`` appearing
        anywhere in ``plays`` (offense side), not just on special-teams
        snaps: ``team_id`` (Utf8), ``adj_st_epa`` (Float64, the opponent-
        adjusted **offense-side** special-teams EPA of the executing team,
        centered on the league baseline). Unlike scrimmage EPA this is NOT an
        offense-minus-defense net -- special teams is owned by the ``pos_team``
        that punts / kicks / returns, and the ``def_pos_team`` side is
        near-noise (see the implementation note). Teams with no special-teams
        plays, and the ridge's dropped reference team, get ``adj_st_epa == 0.0``
        (they fall back to the intercept, which the centering subtracts off).
        Zero-row (correctly-typed) when ``plays`` has no special-teams plays.

    Raises:
        ImportError: If ``scikit-learn`` is not installed.

    Example:
        Quick start::

            from sportsdataverse.cfb.cfb_ratings import special_teams_ratings
            st = special_teams_ratings(pbp)
            st.sort("adj_st_epa", descending=True).head()

    See Also:
        * `cfbfastR`_ -- the R implementation ``cfb_adjusted_epa`` ports.

    .. _cfbfastR: https://cfbfastR.sportsdataverse.org
    """
    cfg = config or RatingsConfig()
    roster = plays.select(pl.col("pos_team_id").cast(pl.Utf8).alias("team_id")).drop_nulls().unique()

    st_clean = (
        plays.filter(pl.col("play_type").cast(pl.Utf8).str.contains(_ST_PLAY_TYPE_PATTERN))
        .filter(pl.col("EPA").is_not_null())
        .with_columns(
            pos_team_id=pl.col("pos_team_id").cast(pl.Utf8),
            def_pos_team_id=pl.col("def_pos_team_id").cast(pl.Utf8),
            game_id=pl.col("game_id").cast(pl.Utf8),
        )
        .with_columns(
            hfa=pl.when(pl.col("neutral_site") == True)  # noqa: E712
            .then(pl.lit(0))
            .when(pl.col("pos_team") == pl.col("home"))
            .then(pl.lit(1))
            .otherwise(pl.lit(-1))
        )
    )
    if st_clean.height == 0:
        return pl.DataFrame(schema=_ST_OUTPUT_SCHEMA)

    offense, _defense, intercept = _fit_opponent_ridge(st_clean, cfg.ridge_lambda)
    assert offense.schema["team_id"] == pl.Utf8

    # Special teams does NOT obey the offense-minus-defense symmetry of
    # scrimmage EPA. The team executing the special-teams play (``pos_team``:
    # the punt / field-goal / kick-return) owns the EPA and is what a
    # published ST rating credits; the ``def_pos_team`` "ST defense"
    # (coverage / block) is near-noise and does not opponent-separate, so
    # subtracting it injects noise (agreement with SP+ special-teams drops
    # 0.70 -> 0.58). The rating is therefore the opponent-adjusted OFFENSE-side
    # coefficient only, centered on the baseline (``intercept``) so the ridge's
    # dropped reference team and teams with no special-teams plays land at 0.0.
    out = (
        roster.join(offense.rename({"adjmodelOff": "off_st"}), on="team_id", how="left")
        .with_columns(pl.col("off_st").fill_null(intercept))
        .with_columns(adj_st_epa=pl.col("off_st") - intercept)
        .select("team_id", "adj_st_epa")
    )
    return out


def fei_ratings(plays: pl.DataFrame, *, config: RatingsConfig | None = None) -> pl.DataFrame:
    """One row per team: opponent-adjusted per-drive efficiency (FEI-style).

    The Fremeau Efficiency Index rates teams on drive value above expectation
    given starting field position. The cfbfastR-schema ``plays`` frame this
    package works with carries no starting-field-position column, so this
    function uses the documented fallback: per-play EPA summed within each
    ``(game_id, drive_id)`` group stands in for drive value, and that
    aggregate is fit through the same opponent-adjustment ridge as
    :func:`efficiency_ratings` / :func:`special_teams_ratings` -- no forked
    solver. Offline validation against the Fremeau FEI oracle put this
    fallback's team ranking at Spearman 0.967.

    ``cfb_adjusted_epa._prepare`` filters to individual pass/rush plays and
    is not reused here (drive value should reflect every play on the drive,
    special-teams snaps included); the ``hfa`` treatment is reproduced
    directly, matching :func:`special_teams_ratings`.

    Args:
        plays: A cfbfastR-schema play-by-play frame carrying every column in
            ``cfb_adjusted_epa._REQUIRED_COLUMNS`` (``game_id``, ``pos_team``,
            ``pos_team_id``, ``def_pos_team_id``, ``home``, ``neutral_site``,
            ``EPA``, ``pass``, ``rush``, ``wp_before``) plus ``drive_id``.
            Not pre-aggregated to drives -- this function does that grouping
            itself.
        config: Ratings tuning knobs. Only ``ridge_lambda`` is consulted here;
            defaults to :class:`RatingsConfig` when omitted.

    Returns:
        A ``polars.DataFrame`` with one row per ``team_id`` appearing as
        ``pos_team_id`` on at least one drive: ``team_id`` (Utf8),
        ``fei_off`` / ``fei_def`` / ``fei_net`` (Float64). The ridge's dropped
        reference team is re-added at the shared intercept (``fei_net ==
        0.0``). Zero-row (correctly-typed) when ``plays`` has no rows with a
        non-null ``EPA``.

    Raises:
        ImportError: If ``scikit-learn`` is not installed.

    Example:
        Quick start::

            from sportsdataverse.cfb.cfb_ratings import fei_ratings
            fei = fei_ratings(pbp)
            fei.sort("fei_net", descending=True).head()

    See Also:
        * `cfbfastR`_ -- the R implementation ``cfb_adjusted_epa`` ports the
          shared ridge from.

    .. _cfbfastR: https://cfbfastR.sportsdataverse.org
    """
    cfg = config or RatingsConfig()
    df = (
        plays.filter(pl.col("EPA").is_not_null())
        .with_columns(
            pos_team_id=pl.col("pos_team_id").cast(pl.Utf8),
            def_pos_team_id=pl.col("def_pos_team_id").cast(pl.Utf8),
            game_id=pl.col("game_id").cast(pl.Utf8),
        )
        .with_columns(
            hfa=pl.when(pl.col("neutral_site") == True)  # noqa: E712
            .then(pl.lit(0))
            .when(pl.col("pos_team") == pl.col("home"))
            .then(pl.lit(1))
            .otherwise(pl.lit(-1))
        )
    )

    drives = df.group_by("game_id", "drive_id").agg(
        pl.col("pos_team_id").first(),
        pl.col("def_pos_team_id").first(),
        pl.col("hfa").first(),
        pl.col("EPA").sum().alias("EPA"),
    )
    if drives.height == 0:
        return pl.DataFrame(schema=_FEI_OUTPUT_SCHEMA)

    offense, defense, intercept = _fit_opponent_ridge(drives, cfg.ridge_lambda)
    assert offense.schema["team_id"] == pl.Utf8
    assert defense.schema["team_id"] == pl.Utf8

    roster = drives.select(pl.col("pos_team_id").alias("team_id")).drop_nulls().unique()

    out = (
        roster.join(offense.rename({"adjmodelOff": "fei_off"}), on="team_id", how="left")
        .join(defense.rename({"adjmodelDef": "fei_def"}), on="team_id", how="left")
        .with_columns(
            pl.col("fei_off").fill_null(intercept),
            pl.col("fei_def").fill_null(intercept),
        )
        .with_columns(fei_net=pl.col("fei_off") - pl.col("fei_def"))
        .select("team_id", "fei_off", "fei_def", "fei_net")
    )
    return out


@overload
def cfb_ratings(
    seasons: int | list[int],
    *,
    as_of_date: datetime.date | None = ...,
    config: RatingsConfig | None = ...,
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...
@overload
def cfb_ratings(
    seasons: int | list[int],
    *,
    as_of_date: datetime.date | None = ...,
    config: RatingsConfig | None = ...,
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...
def cfb_ratings(
    seasons: int | list[int],
    *,
    as_of_date: datetime.date | None = None,
    config: RatingsConfig | None = None,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """One row per team: the full CFB ratings spine (off/def/ST EPA + FEI).

    Public orchestrator over :func:`efficiency_ratings`,
    :func:`special_teams_ratings`, and :func:`fei_ratings`. Loads play-by-play
    + schedule via :func:`sportsdataverse.cfb.cfb_loaders.load_cfb_pbp` /
    :func:`sportsdataverse.cfb.cfb_loaders.load_cfb_schedule`, joins the
    schedule's per-game date onto the plays, optionally applies the
    as-of-date leakage boundary
    (:func:`sportsdataverse.cfb.cfb_prediction_constants.as_of_ratings_split`),
    then fits all three component ratings on the (optionally filtered) plays
    and reshapes them into one wide per-team table with dense ranks and a
    net-rating z-score.

    Args:
        seasons: A single season (e.g. ``2023``) or a list of seasons to pool
            into one combined fit.
        as_of_date: When given, the leakage boundary -- only plays from games
            with ``date < as_of_date`` are used to fit the ratings (mirrors
            what was knowable heading into that date). ``None`` (default)
            uses the full season(s), unfiltered.
        config: Ratings tuning knobs forwarded to all three component
            functions. Defaults to :class:`RatingsConfig` when omitted.
        return_as_pandas: If True, returns a pandas DataFrame; otherwise polars.

    Returns:
        A DataFrame with one row per ``team_id``, columns in this order:
        ``season`` (Int64 -- the single passed season for the common
        single-season call; ``null`` for a pooled multi-season call, since no
        single season applies to every row), ``team_id`` (Utf8),
        ``adj_off_epa``, ``adj_def_epa`` (Float64, from
        :func:`efficiency_ratings`), ``adj_st_epa`` (Float64, from
        :func:`special_teams_ratings`), ``adj_net`` (Float64 -- offense minus
        defense only; special teams is a separate column, not folded in),
        ``fei_off``, ``fei_def``, ``fei_net`` (Float64, from
        :func:`fei_ratings`), ``games`` (Int64), ``off_rank`` (Int64, dense
        rank on ``adj_off_epa`` descending), ``def_rank`` (Int64, dense rank
        on ``adj_def_epa`` **ascending** -- fewer EPA allowed ranks better),
        ``net_rank`` (Int64, dense rank on ``adj_net`` descending), ``net_z``
        (Float64, z-score of ``adj_net``). Zero-row (correctly-typed) when
        the requested season(s) have no published pbp/schedule asset, or when
        ``as_of_date`` filters out every play.

    Raises:
        KeyError: If the loaded plays frame is missing a required column.
        ImportError: If ``scikit-learn`` is not installed.

    Example:
        Quick start::

            from sportsdataverse.cfb.cfb_ratings import cfb_ratings
            ratings = cfb_ratings(2023)
            ratings.sort("net_rank").head()

        As-of-date leakage boundary::

            import datetime as dt
            week3 = cfb_ratings(2023, as_of_date=dt.date(2023, 9, 18))

        Pandas round-trip::

            ratings_pd = cfb_ratings(2023, return_as_pandas=True)

    See Also:
        * `cfbfastR`_ -- the R implementation these ratings port from.

    .. _cfbfastR: https://cfbfastR.sportsdataverse.org
    """
    cfg = config or RatingsConfig()
    season_list: list[int] = [seasons] if isinstance(seasons, int) else list(seasons)

    plays = load_cfb_pbp(season_list)
    schedule = load_cfb_schedule(season_list)
    if plays.is_empty() or schedule.is_empty():
        empty = pl.DataFrame(schema=_RATINGS_OUTPUT_SCHEMA)
        return empty.to_pandas() if return_as_pandas else empty

    plays = plays.with_columns(pl.col("game_id").cast(pl.Utf8))
    schedule = schedule.with_columns(pl.col("game_id").cast(pl.Utf8))
    assert plays.schema["game_id"] == schedule.schema["game_id"]

    if "date" in schedule.columns:
        date_expr = pl.col("date").cast(pl.Date)
    else:
        # Real `load_cfb_schedule` ships `start_date` (an ISO datetime
        # string), not a bare `date` column -- take the calendar-day prefix.
        date_expr = pl.col("start_date").cast(pl.Utf8).str.slice(0, 10).str.to_date()
    schedule_dates = schedule.select("game_id", date_expr.alias("date"))

    dated_plays = plays.join(schedule_dates, on="game_id", how="left")
    if as_of_date is not None:
        dated_plays = as_of_ratings_split(dated_plays, as_of_date)

    eff = efficiency_ratings(dated_plays, config=cfg)
    st = special_teams_ratings(dated_plays, config=cfg)
    fei = fei_ratings(dated_plays, config=cfg)

    season_value: int | None = season_list[0] if len(season_list) == 1 else None

    out = (
        eff.join(st, on="team_id", how="left")
        .join(fei, on="team_id", how="left")
        .with_columns(
            pl.col("adj_st_epa").fill_null(0.0),
            pl.col("fei_off").fill_null(0.0),
            pl.col("fei_def").fill_null(0.0),
            pl.col("fei_net").fill_null(0.0),
            pl.lit(season_value).cast(pl.Int64).alias("season"),
        )
        .with_columns(
            off_rank=pl.col("adj_off_epa").rank(method="dense", descending=True).cast(pl.Int64),
            def_rank=pl.col("adj_def_epa").rank(method="dense", descending=False).cast(pl.Int64),
            net_rank=pl.col("adj_net").rank(method="dense", descending=True).cast(pl.Int64),
        )
    )

    mean_net = float(out["adj_net"].mean() or 0.0)
    std_val = out["adj_net"].std()
    std_net = float(std_val) if std_val else 0.0
    if std_net == 0.0:
        out = out.with_columns(net_z=pl.lit(0.0).cast(pl.Float64))
    else:
        out = out.with_columns(net_z=(pl.col("adj_net") - mean_net) / std_net)

    out = out.select(
        "season",
        "team_id",
        "adj_off_epa",
        "adj_def_epa",
        "adj_st_epa",
        "adj_net",
        "fei_off",
        "fei_def",
        "fei_net",
        "games",
        "off_rank",
        "def_rank",
        "net_rank",
        "net_z",
    )
    return out.to_pandas() if return_as_pandas else out
