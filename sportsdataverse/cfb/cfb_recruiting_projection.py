"""Recruiting-composite -> team performance projection for CFB (T2.2 model ①).

An on-demand ridge maps preseason-known roster features (talent composite,
blue-chip ratio, returning production, prior-season wins) to team-season
outcomes (wins, scoring margin). The fit is strictly as-of: projecting season S
trains only on seasons < S.

Cross-source team identity: the three inputs live in three id spaces (247 team
key, normalized school name, ESPN id). The loader seams (``_load_talent`` /
``_load_returning`` / ``_load_results``) each resolve to the canonical ESPN
team id (Utf8) before the matrix join; tests monkeypatch the seams directly.

``pred_net_epa`` is emitted as null: the adjusted-EPA target needs the hosted
pbp dataset, whose loader currently 404s (documented data block, not faked).
"""

from __future__ import annotations

import warnings

import numpy as np
import pandas as pd
import polars as pl

from sportsdataverse.cfb.cfb_loaders import load_cfb_schedule, load_cfb_teams_crosswalk
from sportsdataverse.cfb.cfb_projection_constants import fit_ridge, predict_ridge
from sportsdataverse.cfb.cfb_returning_production import cfb_returning_production
from sportsdataverse.cfb.cfb_loaders import load_cfb_returning_production, load_cfb_team_talent
from sportsdataverse.cfb.cfb_roster_talent import cfb_roster_talent

__all__ = ["cfb_recruiting_projection"]

#: Model inputs. `def_returning` is DELIBERATELY ABSENT.
#:
#: It is a hard requirement via `drop_nulls(FEATURES)`, and its coverage is
#: driven by ESPN's defensive player box, which is sparse before ~2023 --
#: measured non-null teams: 2016 0, 2018 61, 2021 47, 2024 148, 2025 229.
#: Requiring it made the projection's output track that column almost exactly
#: (2016 -> 0 rows, 2018 -> 60, 2024 -> 144) and collapsed 2016/2017 entirely,
#: because their training history has no defensive box at all.
#:
#: It also carries no weight downstream: `returning_prod_weights` is
#: {offense: 1.0, defense: 0.0}, so `overall_returning` never saw it either. A
#: feature that is null across most of the training history cannot inform a
#: model fit across that history -- it can only delete rows.
FEATURES = ["talent_composite", "blue_chip_ratio", "off_returning", "prior_wins"]

_PROJECTION_SCHEMA: dict[str, pl.PolarsDataType] = {
    "season": pl.Int64,
    "team_id": pl.Utf8,
    "pred_wins": pl.Float64,
    "pred_margin": pl.Float64,
    "pred_net_epa": pl.Float64,
}


def _crosswalk_names_to_espn(seasons: list[int]) -> pl.DataFrame:
    """``norm_key`` (school+mascot, normalized) -> ESPN team id (Utf8).

    The crosswalk asset trails the calendar (capped at 2025 while a 2026
    projection is already meaningful after early signing), and a missing
    season comes back from the loader as a column-less empty frame -- so walk
    back season by season to the newest crosswalk that exists (team name ->
    ESPN id identity barely changes year to year) instead of crashing on the
    select. All-missing degrades to a typed empty per the empty-in/empty-out
    convention.
    """
    for season in range(max(seasons), 2013, -1):  # crosswalk asset floor is 2014
        xw = load_cfb_teams_crosswalk(season)
        assert isinstance(xw, pl.DataFrame)
        if xw.height > 0 and "norm_key" in xw.columns:
            return (
                xw.select(
                    pl.col("norm_key").cast(pl.Utf8),
                    pl.col("espn_team_id").cast(pl.Int64).cast(pl.Utf8).alias("espn_id"),
                )
                .drop_nulls()
                .unique(subset=["norm_key"])
            )
    return pl.DataFrame(schema={"norm_key": pl.Utf8, "espn_id": pl.Utf8})


def _load_talent(seasons: list[int], division: str) -> pl.DataFrame:
    """Roster talent for the projection, read from the PUBLISHED dataset.

    Reads `cfb_team_talent` rather than calling `cfb_roster_talent`, which
    fetches 247 live: a 4-season talent window is ~96 pages / 20 minutes per
    season, so a 20-season rebuild of this projection cost hours and hammered a
    host that resets connections under sustained paging. The release is the
    same computation over the same inputs -- the producer builds it through
    `cfb_roster_talent(recruits=...)` -- so this is a cost change, not a
    semantics change.

    `cfb_roster_talent` itself stays live for callers who want the in-progress
    signing cycle; only this producer path reads the release.

    Falls back to the live call when the release has no rows for a season,
    which is what happens for a season newer than the last publish.
    """
    tal = load_cfb_team_talent(seasons)
    if isinstance(tal, pd.DataFrame):
        tal = pl.from_pandas(tal)
    if tal is not None and tal.height:
        return tal.with_columns(pl.col("team_id").cast(pl.Int64).cast(pl.Utf8))
    warnings.warn(
        f"cfb_team_talent has no rows for {seasons}; falling back to the live 247 "
        "fetch (slow). Publish the release for these seasons to avoid it.",
        UserWarning,
        stacklevel=2,
    )
    out = cfb_roster_talent(seasons, division=division)
    assert isinstance(out, pl.DataFrame)
    return out


def _load_returning(seasons: list[int], division: str) -> pl.DataFrame:
    """Returning production for the projection, read from the PUBLISHED dataset.

    Same rationale as :func:`_load_talent`: the release is the same computation,
    and reading it keeps a rebuild cheap and reproducible. Falls back to the
    live computation when the release does not cover a season.
    """
    rp = load_cfb_returning_production(seasons)
    if isinstance(rp, pd.DataFrame):
        rp = pl.from_pandas(rp)
    if rp is not None and rp.height:
        return rp.with_columns(pl.col("team_id").cast(pl.Int64).cast(pl.Utf8))
    warnings.warn(
        f"cfb_returning_production has no rows for {seasons}; computing live.",
        UserWarning,
        stacklevel=2,
    )
    out = cfb_returning_production(seasons, division=division)
    assert isinstance(out, pl.DataFrame)
    return out


def _load_results(seasons: list[int]) -> pl.DataFrame:
    """Realized per-team-season wins + scoring margin from the schedule loader."""
    sched = load_cfb_schedule(seasons)
    assert isinstance(sched, pl.DataFrame)
    if sched.height == 0:
        return pl.DataFrame(
            schema={"season": pl.Int64, "team_id": pl.Utf8, "wins": pl.Int64, "points_margin": pl.Float64}
        )
    done = sched.filter(pl.col("home_points").is_not_null() & pl.col("away_points").is_not_null())
    home = done.select(
        pl.col("season").cast(pl.Int64),
        pl.col("home_id").cast(pl.Int64).cast(pl.Utf8).alias("team_id"),
        (pl.col("home_points") > pl.col("away_points")).cast(pl.Int64).alias("win"),
        (pl.col("home_points") - pl.col("away_points")).cast(pl.Float64).alias("m"),
    )
    away = done.select(
        pl.col("season").cast(pl.Int64),
        pl.col("away_id").cast(pl.Int64).cast(pl.Utf8).alias("team_id"),
        (pl.col("away_points") > pl.col("home_points")).cast(pl.Int64).alias("win"),
        (pl.col("away_points") - pl.col("home_points")).cast(pl.Float64).alias("m"),
    )
    return (
        pl.concat([home, away])
        .group_by("season", "team_id")
        .agg(pl.col("win").sum().alias("wins"), pl.col("m").mean().alias("points_margin"))
    )


def _build_projection_matrix(seasons: list[int], *, division: str = "fbs") -> pl.DataFrame:
    """Feature + target matrix per (season, team_id); target columns null when unplayed."""
    talent = _load_talent(seasons, division)
    returning = _load_returning(seasons, division)
    # min(seasons)-1 backs the earliest season's prior_wins lag — without it the
    # earliest training season silently drops out of the fit (null prior_wins)
    results = _load_results(sorted({*seasons, min(seasons) - 1}))
    if talent.height == 0:
        return pl.DataFrame(
            schema={
                "season": pl.Int64,
                "team_id": pl.Utf8,
                **{f: pl.Float64 for f in FEATURES},
                "wins": pl.Float64,
                "points_margin": pl.Float64,
            }
        )
    assert talent.schema["team_id"] == returning.schema["team_id"] == pl.Utf8
    m = talent.select("season", "team_id", "talent_composite", "blue_chip_ratio").join(
        returning.select("season", "team_id", "off_returning", "def_returning"),
        on=["season", "team_id"],
        how="left",
    )
    if results.height > 0:
        assert m.schema["team_id"] == results.schema["team_id"]
        prior = results.select(
            (pl.col("season") + 1).alias("season"),
            "team_id",
            pl.col("wins").cast(pl.Float64).alias("prior_wins"),
        )
        m = m.join(prior, on=["season", "team_id"], how="left")
        m = m.join(
            results.select("season", "team_id", pl.col("wins").cast(pl.Float64), "points_margin"),
            on=["season", "team_id"],
            how="left",
        )
    else:
        m = m.with_columns(
            pl.lit(None, dtype=pl.Float64).alias("prior_wins"),
            pl.lit(None, dtype=pl.Float64).alias("wins"),
            pl.lit(None, dtype=pl.Float64).alias("points_margin"),
        )
    return m


def cfb_recruiting_projection(
    target_season: int,
    *,
    division: str = "fbs",
    history_seasons: list[int] | None = None,
    alpha: float = 1.0,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Project team wins / scoring margin for a season from preseason roster features.

    Fits a ridge regression of realized wins (and average scoring margin) on
    ``[talent_composite, blue_chip_ratio, off_returning, def_returning,
    prior_wins]`` over strictly-prior seasons, then predicts the target season
    from its preseason-known features. The as-of boundary is enforced
    internally: rows with ``season >= target_season`` never enter training even
    if ``history_seasons`` includes them.

    Args:
        target_season: Season to project.
        division: Division slug for constants lookups.
        history_seasons: Seasons to draw training rows from (default: the six
            seasons before ``target_season``).
        alpha: Ridge L2 penalty.
        return_as_pandas: If True, return a pandas DataFrame; otherwise polars.

    Returns:
        Per team: ``season`` (Int64, = target), ``team_id`` (Utf8 ESPN id),
        ``pred_wins``, ``pred_margin`` (Float64), ``pred_net_epa`` (Float64,
        currently null -- the adjusted-EPA target's hosted pbp source 404s).
        Zero-row (typed) when no history is available.

    Example:
        Quick start::

            from sportsdataverse.cfb import cfb_recruiting_projection
            proj = cfb_recruiting_projection(2024)
            proj.sort("pred_wins", descending=True).head(10)

    See Also:
        * `recruitR`_ -- the R companion for CFB recruiting data.
        * `cfbfastR`_ -- R sister package (hosted data producer).

    .. _recruitR: https://github.com/sportsdataverse/recruitR
    .. _cfbfastR: https://cfbfastR.sportsdataverse.org
    """
    hist = history_seasons or list(range(target_season - 6, target_season))
    seasons = sorted({*hist, target_season})
    m = _build_projection_matrix(seasons, division=division)
    empty = pl.DataFrame(schema=_PROJECTION_SCHEMA)
    if m.height == 0:
        return empty.to_pandas() if return_as_pandas else empty
    complete = m.drop_nulls(FEATURES)
    train = complete.filter((pl.col("season") < target_season) & pl.col("wins").is_not_null())  # the as-of boundary
    target = complete.filter(pl.col("season") == target_season)
    if train.height == 0 or target.height == 0:
        return empty.to_pandas() if return_as_pandas else empty
    x_train = train.select(FEATURES).to_numpy().astype(float)
    x_target = target.select(FEATURES).to_numpy().astype(float)
    preds: dict[str, np.ndarray] = {}
    for target_col, out_col in (("wins", "pred_wins"), ("points_margin", "pred_margin")):
        icept, coef = fit_ridge(x_train, train[target_col].to_numpy().astype(float), alpha=alpha)
        preds[out_col] = predict_ridge(icept, coef, x_target)
    out = target.select(
        pl.col("season").cast(pl.Int64),
        "team_id",
    ).with_columns(
        pl.Series("pred_wins", preds["pred_wins"], dtype=pl.Float64),
        pl.Series("pred_margin", preds["pred_margin"], dtype=pl.Float64),
        pl.lit(None, dtype=pl.Float64).alias("pred_net_epa"),
    )
    return out.to_pandas() if return_as_pandas else out
