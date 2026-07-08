"""Recruiting/production -> NFL draft projection for CFB (T2.2 model ⑤).

Draft outcomes come from the nflverse draft-picks dataset
(:func:`sportsdataverse.nfl.load_nfl_draft_picks`) rather than the ESPN
season-draft endpoint, which 404s for recent years. The picks carry the
college name, the PFR player name, and (for recent drafts) the ESPN
``cfb_player_id`` — the join keys the projection matches recruits on.
"""

from __future__ import annotations

import pandas as pd
import polars as pl

from sportsdataverse.cfb.cfb_projection_constants import fit_logistic, predict_logistic
from sportsdataverse.nfl import load_nfl_draft_picks

__all__ = ["cfb_draft_projection", "load_draft_outcomes"]

_FEATURES = ["recruit_stars", "talent_points", "career_production_z", "class_year"]

_DRAFT_SCHEMA: dict[str, pl.PolarsDataType] = {
    "draft_year": pl.Int64,
    "college": pl.Utf8,
    "player_id": pl.Utf8,
    "player_name": pl.Utf8,
    "round": pl.Int64,
    "pick": pl.Int64,
    "position": pl.Utf8,
}


def load_draft_outcomes(years: int | list[int], *, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """NFL draft picks with the college of each pick, for the requested draft years.

    Args:
        years: A draft year or list of draft years.
        return_as_pandas: If True, return a pandas DataFrame; otherwise polars.

    Returns:
        One row per pick: ``draft_year`` (Int64), ``college`` (Utf8 PFR-style
        college name), ``player_id`` (Utf8 ESPN college athlete id; null for
        older drafts), ``player_name`` (Utf8), ``round`` / ``pick`` (Int64),
        ``position`` (Utf8). Zero-row (typed) when the source is unavailable.

    Example:
        Quick start::

            from sportsdataverse.cfb import load_draft_outcomes
            picks = load_draft_outcomes([2023, 2024])
            picks.group_by("college").len().sort("len", descending=True).head()

    See Also:
        * `nflreadpy`_ -- the picks dataset's canonical Python surface.
        * `recruitR`_ -- the R companion for CFB recruiting data.

    .. _nflreadpy: https://github.com/nflverse/nflreadpy
    .. _recruitR: https://github.com/sportsdataverse/recruitR
    """
    year_list = [years] if isinstance(years, int) else list(years)
    raw = load_nfl_draft_picks()
    if isinstance(raw, pd.DataFrame):
        raw = pl.from_pandas(raw)
    if raw.height == 0 or "season" not in raw.columns:
        empty = pl.DataFrame(schema=_DRAFT_SCHEMA)
        return empty.to_pandas() if return_as_pandas else empty
    out = (
        raw.filter(pl.col("season").is_in(year_list))
        .select(
            pl.col("season").cast(pl.Int64).alias("draft_year"),
            pl.col("college").cast(pl.Utf8),
            pl.col("cfb_player_id").cast(pl.Utf8).alias("player_id"),
            pl.col("pfr_player_name").cast(pl.Utf8).alias("player_name"),
            pl.col("round").cast(pl.Int64),
            pl.col("pick").cast(pl.Int64),
            pl.col("position").cast(pl.Utf8),
        )
        .sort("draft_year", "pick")
    )
    return out.to_pandas() if return_as_pandas else out


def _season_production(season: int) -> pl.DataFrame:
    """Per-player attributed production for one season (monkeypatchable seam).

    Delegates to the returning-production extractor over the hosted play-level
    player-stats parquet; the offline gate patches this to a committed fixture.
    """
    from sportsdataverse.cfb.cfb_returning_production import (
        _load_player_stats,
        _production_from_play_stats,
    )

    stats = _load_player_stats(season)
    if stats.height == 0:
        return pl.DataFrame(
            schema={
                "season": pl.Int64,
                "team_id": pl.Utf8,
                "player_id": pl.Utf8,
                "player_name": pl.Utf8,
                "unit": pl.Utf8,
                "prod_weight": pl.Float64,
                "position": pl.Utf8,
            }
        )
    return _production_from_play_stats(stats, season)


# a defensive splash event (sack/INT/PBU/FF) weighed against offensive yards --
# crude but monotone; the z-score is computed within the draft-year pool anyway
_SPLASH_EVENT_YARDS = 50.0


def _career_production(years: list[int]) -> pl.DataFrame:
    """Raw career production per (draft_year, case-folded player name).

    For draft year Y, sums seasons Y-3..Y-1: offensive attributed yards plus
    ``_SPLASH_EVENT_YARDS`` per defensive splash event. Name is the join key
    (recruit ids and play-stats ids are different id spaces).
    """
    seasons = sorted({y - off for y in years for off in (1, 2, 3)})
    per_season = [df for s in seasons if (df := _season_production(s)).height > 0]
    empty = pl.DataFrame(schema={"draft_year": pl.Int64, "_name": pl.Utf8, "prod_raw": pl.Float64})
    if not per_season:
        return empty
    prod = (
        pl.concat(per_season)
        .drop_nulls(["player_name"])
        .with_columns(
            pl.col("player_name").str.to_lowercase().str.strip_chars().alias("_name"),
            pl.when(pl.col("unit") == "defense")
            .then(pl.col("prod_weight") * _SPLASH_EVENT_YARDS)
            .otherwise(pl.col("prod_weight"))
            .alias("_w"),
        )
    )
    frames = []
    for y in years:
        window = prod.filter(pl.col("season").is_in([y - 1, y - 2, y - 3]))
        if window.height == 0:
            continue
        frames.append(
            window.group_by("_name")
            .agg(pl.col("_w").sum().alias("prod_raw"))
            .with_columns(pl.lit(y, dtype=pl.Int64).alias("draft_year"))
            .select("draft_year", "_name", "prod_raw")
        )
    return pl.concat(frames) if frames else empty


def _player_feature_frame(years: list[int], division: str) -> pl.DataFrame:
    """Per player-draft-year features + drafted label for the given draft years.

    Eligible pool for draft year Y = recruits from signing classes Y-6..Y-3
    (draft-eligible windows). Features: ``recruit_stars``, ``talent_points``
    (star points), ``career_production_z`` (z-scored share of attributed
    production over the player's college seasons; 0 when the production source
    is unavailable), ``class_year`` (Y minus signing year). Label ``drafted``
    comes from :func:`load_draft_outcomes` matched on the case-folded player
    name (the ESPN ``cfb_player_id`` is only present for recent drafts, and the
    247 recruit key never matches it directly).
    """
    from sportsdataverse.cfb.cfb_projection_constants import get_constants
    from sportsdataverse.cfb.cfb_roster_talent import load_recruit_classes

    consts = get_constants(division)
    class_years = sorted({y - off for y in years for off in (3, 4, 5, 6)})
    rec = load_recruit_classes(class_years, division=division)
    if isinstance(rec, pd.DataFrame):
        rec = pl.from_pandas(rec)
    if rec.height == 0:
        return pl.DataFrame(
            schema={
                "draft_year": pl.Int64,
                "team_id": pl.Utf8,
                "player_id": pl.Utf8,
                "player_name": pl.Utf8,
                **{f: pl.Float64 for f in _FEATURES},
                "drafted": pl.Int64,
            }
        )
    picks = load_draft_outcomes(years)
    assert isinstance(picks, pl.DataFrame)
    drafted_names = picks.select(
        pl.col("draft_year"),
        pl.col("player_name").str.to_lowercase().str.strip_chars().alias("_name"),
        pl.lit(1).alias("drafted"),
    ).unique(subset=["draft_year", "_name"])
    career = _career_production(years)
    frames: list[pl.DataFrame] = []
    for y in years:
        pool = rec.filter(pl.col("season").is_in([y - 3, y - 4, y - 5, y - 6]))
        if pool.height == 0:
            continue
        feats = pool.select(
            pl.lit(y, dtype=pl.Int64).alias("draft_year"),
            "team_id",
            pl.col("recruit_id").alias("player_id"),
            "player_name",
            pl.col("stars").cast(pl.Float64).fill_null(0.0).alias("recruit_stars"),
            pl.col("stars")
            .replace_strict(consts.star_points, default=consts.star_points.get(0, 0.0), return_dtype=pl.Float64)
            .alias("talent_points"),
            (pl.lit(y, dtype=pl.Int64) - pl.col("season")).cast(pl.Float64).alias("class_year"),
            pl.col("player_name").str.to_lowercase().str.strip_chars().alias("_name"),
        )
        feats = feats.join(
            career.filter(pl.col("draft_year") == y).select("_name", "prod_raw"),
            on="_name",
            how="left",
        ).with_columns(pl.col("prod_raw").fill_null(0.0).log1p().alias("_logprod"))
        feats = feats.with_columns(
            (
                (pl.col("_logprod") - pl.col("_logprod").mean())
                / pl.when(pl.col("_logprod").std() > 0).then(pl.col("_logprod").std()).otherwise(1.0)
            ).alias("career_production_z")
        ).drop("prod_raw", "_logprod")
        frames.append(
            feats.join(drafted_names, on=["draft_year", "_name"], how="left")
            .with_columns(pl.col("drafted").fill_null(0).cast(pl.Int64))
            .drop("_name")
        )
    if not frames:
        return pl.DataFrame(
            schema={
                "draft_year": pl.Int64,
                "team_id": pl.Utf8,
                "player_id": pl.Utf8,
                "player_name": pl.Utf8,
                **{f: pl.Float64 for f in _FEATURES},
                "drafted": pl.Int64,
            }
        )
    return pl.concat(frames)


def cfb_draft_projection(
    target_draft_year: int,
    *,
    division: str = "fbs",
    history_years: list[int] | None = None,
    l2: float = 1.0,
    return_as_pandas: bool = False,
) -> dict[str, pl.DataFrame] | dict[str, pd.DataFrame]:
    """Project NFL-draft probability per player + expected picks per team.

    Fits an L2 logistic of ``drafted`` on ``[recruit_stars, talent_points,
    career_production_z, class_year]`` over draft years strictly before the
    target (the as-of boundary, enforced internally), then scores the target
    year's eligible players.

    Args:
        target_draft_year: Draft year to project.
        division: Division slug for constants lookups.
        history_years: Training draft years (default: the five before target).
        l2: Logistic L2 penalty.
        return_as_pandas: If True, both frames return as pandas.

    Returns:
        ``{"players": ..., "teams": ...}`` — players: ``draft_year`` (Int64),
        ``team_id`` / ``player_id`` / ``player_name`` (Utf8), ``draft_prob``
        (Float64); teams: ``draft_year``, ``team_id``, ``proj_draft_picks``
        (Float64, the sum of member draft probabilities). Zero-row (typed)
        frames when no data is available.

    Example:
        Quick start::

            from sportsdataverse.cfb import cfb_draft_projection
            out = cfb_draft_projection(2024)
            out["teams"].sort("proj_draft_picks", descending=True).head(10)

    See Also:
        * `nflreadpy`_ -- draft-pick outcomes source.

    .. _nflreadpy: https://github.com/nflverse/nflreadpy
    """
    players_schema: dict[str, pl.PolarsDataType] = {
        "draft_year": pl.Int64,
        "team_id": pl.Utf8,
        "player_id": pl.Utf8,
        "player_name": pl.Utf8,
        "draft_prob": pl.Float64,
    }
    teams_schema: dict[str, pl.PolarsDataType] = {
        "draft_year": pl.Int64,
        "team_id": pl.Utf8,
        "proj_draft_picks": pl.Float64,
    }
    hist = history_years or list(range(target_draft_year - 5, target_draft_year))
    frame = _player_feature_frame(sorted({*hist, target_draft_year}), division)
    empty = {"players": pl.DataFrame(schema=players_schema), "teams": pl.DataFrame(schema=teams_schema)}
    if frame.height == 0:
        if return_as_pandas:
            return {k: v.to_pandas() for k, v in empty.items()}
        return empty
    train = frame.filter(pl.col("draft_year") < target_draft_year)  # the as-of boundary
    target = frame.filter(pl.col("draft_year") == target_draft_year)
    if train.height == 0 or target.height == 0:
        if return_as_pandas:
            return {k: v.to_pandas() for k, v in empty.items()}
        return empty
    icept, coef = fit_logistic(
        train.select(_FEATURES).to_numpy().astype(float),
        train["drafted"].to_numpy().astype(float),
        l2=l2,
    )
    probs = predict_logistic(icept, coef, target.select(_FEATURES).to_numpy().astype(float))
    players = target.select("draft_year", "team_id", "player_id", "player_name").with_columns(
        pl.Series("draft_prob", probs, dtype=pl.Float64)
    )
    teams = players.group_by("draft_year", "team_id").agg(pl.col("draft_prob").sum().alias("proj_draft_picks"))
    out = {"players": players, "teams": teams.sort("proj_draft_picks", descending=True)}
    if return_as_pandas:
        return {k: v.to_pandas() for k, v in out.items()}
    return out
