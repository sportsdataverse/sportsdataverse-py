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

__all__ = ["efficiency_ratings"]

_OUTPUT_SCHEMA: dict[str, pl.PolarsDataType] = {
    "team_id": pl.Utf8,
    "adj_off_epa": pl.Float64,
    "adj_def_epa": pl.Float64,
    "adj_net": pl.Float64,
    "games": pl.Int64,
}


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
