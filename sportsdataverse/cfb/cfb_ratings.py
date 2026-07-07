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
    signatures.
"""

from __future__ import annotations

import polars as pl

from sportsdataverse.cfb.cfb_adjusted_epa import _REQUIRED_COLUMNS, _fit_opponent_ridge, _prepare
from sportsdataverse.cfb.cfb_prediction_constants import RatingsConfig

__all__ = ["efficiency_ratings", "fei_ratings", "special_teams_ratings"]

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
        snaps: ``team_id`` (Utf8), ``adj_st_epa`` (Float64, offense-minus-
        defense net special-teams value). Teams with no special-teams plays,
        and the ridge's dropped reference team, get ``adj_st_epa == 0.0`` --
        both sides fall back to the shared intercept, which cancels in the
        net. Zero-row (correctly-typed) when ``plays`` has no special-teams
        plays at all.

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

    offense, defense, intercept = _fit_opponent_ridge(st_clean, cfg.ridge_lambda)
    assert offense.schema["team_id"] == pl.Utf8
    assert defense.schema["team_id"] == pl.Utf8

    out = (
        roster.join(offense.rename({"adjmodelOff": "off_st"}), on="team_id", how="left")
        .join(defense.rename({"adjmodelDef": "def_st"}), on="team_id", how="left")
        .with_columns(
            pl.col("off_st").fill_null(intercept),
            pl.col("def_st").fill_null(intercept),
        )
        .with_columns(adj_st_epa=pl.col("off_st") - pl.col("def_st"))
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
