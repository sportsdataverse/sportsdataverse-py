"""Roster talent composite for college football (T2.2 model ③, foundational).

Turns recruiting classes into a per-team-season talent composite + blue-chip ratio,
the 247Sports Team Talent Composite methodology (accumulate recent classes, weight by
recruit star rating) and Bud Elliott's blue-chip ratio (share of 4-5 star recruits).
Sourced from the 247 RDB recruit feed (``sports247_recruits``) — the ESPN Core-v2
recruits endpoint only returns ``$ref`` links, impractical to resolve per recruit.

``sports247_recruits`` is imported at module scope so tests can monkeypatch it.
"""

from __future__ import annotations

import pandas as pd
import polars as pl

from sportsdataverse.cfb.cfb_projection_constants import get_constants
from sportsdataverse.cfb.sports247 import sports247_recruits

__all__ = ["blue_chip_ratio", "load_recruit_classes"]

_RECRUIT_SCHEMA: dict[str, pl.PolarsDataType] = {
    "season": pl.Int64,
    "team_id": pl.Utf8,
    "team": pl.Utf8,
    "recruit_id": pl.Utf8,
    "stars": pl.Int64,
    "grade": pl.Float64,
    "position": pl.Utf8,
}

_PAGE_SIZE = 500


def _int_id(col: str) -> pl.Expr:
    """Integer-origin id -> Utf8 (never float->str, so a Float64 247 key becomes "71" not "71.0")."""
    return pl.col(col).cast(pl.Int64).cast(pl.Utf8)


def _normalize_recruit_page(raw: pl.DataFrame, season: int) -> pl.DataFrame:
    """Map one 247-RDB recruit page to the per-recruit contract; drop uncommitted recruits."""
    required = {
        "key",
        "committed_institution_team_key",
        "committed_institution_full_name",
        "composite_star_rating",
        "composite_rating",
        "primary_position",
    }
    if raw.height == 0 or not required <= set(raw.columns):
        return pl.DataFrame(schema=_RECRUIT_SCHEMA)
    return (
        raw.select(
            pl.lit(season, dtype=pl.Int64).alias("season"),
            _int_id("committed_institution_team_key").alias("team_id"),
            pl.col("committed_institution_full_name").cast(pl.Utf8).alias("team"),
            _int_id("key").alias("recruit_id"),
            pl.col("composite_star_rating").cast(pl.Int64).alias("stars"),
            pl.col("composite_rating").cast(pl.Float64).alias("grade"),
            pl.col("primary_position").cast(pl.Utf8).alias("position"),
        ).drop_nulls(["team_id"])  # recruits without a committed team don't count toward roster talent
    )


def blue_chip_ratio(recruits: pl.DataFrame, *, window: int = 4, division: str = "fbs") -> pl.DataFrame:
    """Blue-chip ratio per team-season over a trailing window of recruiting classes.

    Bud Elliott's blue-chip ratio: the share of a roster's recruits rated at or above
    the division's blue-chip star floor (4+ stars for FBS). Each recruiting class
    contributes to the ``window`` seasons it is roster-eligible for, so the season-S
    ratio aggregates classes S-window+1 .. S.

    Args:
        recruits: Per-recruit frame from :func:`load_recruit_classes`
            (``season``, ``team_id``, ``recruit_id``, ``stars``, ...).
        window: Number of trailing recruiting classes eligible per season.
        division: Division slug for :func:`get_constants` (blue-chip star floor).

    Returns:
        Per ``(season, team_id)``: ``blue_chip_ratio`` (Float64), ``n_recruits``
        (Int64), ``n_blue_chip`` (Int64). Zero-row (typed) for empty input.

    Example:
        Quick start::

            from sportsdataverse.cfb.cfb_roster_talent import blue_chip_ratio, load_recruit_classes
            bcr = blue_chip_ratio(load_recruit_classes([2020, 2021, 2022, 2023]))
            bcr.filter(pl.col("season") == 2023).sort("blue_chip_ratio", descending=True).head()

    See Also:
        * `recruitR`_ -- the R companion for CFB recruiting data.

    .. _recruitR: https://github.com/sportsdataverse/recruitR
    """
    if recruits.height == 0:
        return pl.DataFrame(
            schema={
                "season": pl.Int64,
                "team_id": pl.Utf8,
                "blue_chip_ratio": pl.Float64,
                "n_recruits": pl.Int64,
                "n_blue_chip": pl.Int64,
            }
        )
    star_min = get_constants(division).blue_chip_star_min
    per_class = recruits.group_by(["season", "team_id"]).agg(
        pl.len().cast(pl.Int64).alias("n_recruits"),
        (pl.col("stars") >= star_min).sum().cast(pl.Int64).alias("n_blue_chip"),
    )
    # replicate each class row to the `window` seasons it is eligible for, then
    # re-aggregate per target season (no polars rolling over Int seasons)
    frames = [per_class.with_columns((pl.col("season") + off).alias("target_season")) for off in range(window)]
    return (
        pl.concat(frames)
        .group_by(["team_id", "target_season"])
        .agg(pl.col("n_recruits").sum(), pl.col("n_blue_chip").sum())
        .rename({"target_season": "season"})
        .with_columns((pl.col("n_blue_chip") / pl.col("n_recruits")).alias("blue_chip_ratio"))
        .select("season", "team_id", "blue_chip_ratio", "n_recruits", "n_blue_chip")
    )


def load_recruit_classes(
    seasons: int | list[int], *, division: str = "fbs", return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """Load recruiting classes as per-recruit rows from the 247 RDB feed.

    Args:
        seasons: A single recruiting-class year or a list of them.
        division: Division slug (reserved for constant lookups downstream; the feed
            itself is queried for all of college football).
        return_as_pandas: If True, return a pandas DataFrame; otherwise polars.

    Returns:
        One row per committed recruit: ``season`` (Int64), ``team_id`` (Utf8 — the 247
        committed-team key), ``team`` (Utf8 full name — the downstream name-join key,
        since the 247 recruit-team key differs from the 247 talent-composite key),
        ``recruit_id`` (Utf8), ``stars`` (Int64), ``grade`` (Float64 247 composite
        rating), ``position`` (Utf8). Zero-row (typed) when no data is available.

    Example:
        Quick start::

            from sportsdataverse.cfb.cfb_roster_talent import load_recruit_classes
            rec = load_recruit_classes([2022, 2023])
            rec.group_by("team").len().sort("len", descending=True).head()

    See Also:
        * `247Sports Team Talent Composite`_ -- the methodology this feeds.
        * `recruitR`_ -- the R companion for CFB recruiting.

    .. _247Sports Team Talent Composite: https://247sports.com/season/2023-football/collegeteamtalentcomposite/
    .. _recruitR: https://github.com/sportsdataverse/recruitR
    """
    season_list = [seasons] if isinstance(seasons, int) else list(seasons)
    frames: list[pl.DataFrame] = []
    for season in season_list:
        page = 1
        while True:
            raw = sports247_recruits(sport_key=1, year=season, page_size=_PAGE_SIZE, page=page)
            if raw is None or raw.height == 0:
                break
            frames.append(_normalize_recruit_page(raw, season))
            if raw.height < _PAGE_SIZE:
                break
            page += 1
    out = pl.concat(frames) if frames else pl.DataFrame(schema=_RECRUIT_SCHEMA)
    return out.to_pandas() if return_as_pandas else out
