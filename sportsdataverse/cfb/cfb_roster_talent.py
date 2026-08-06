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

__all__ = ["blue_chip_ratio", "cfb_roster_talent", "load_recruit_classes"]

_RECRUIT_SCHEMA: dict[str, pl.PolarsDataType] = {
    "season": pl.Int64,
    "team_id": pl.Utf8,
    "team": pl.Utf8,
    "recruit_id": pl.Utf8,
    "player_name": pl.Utf8,
    "stars": pl.Int64,
    "grade": pl.Float64,
    "position": pl.Utf8,
}

#: Recruits requested per RDB page. 500 EXCEEDS what 247 can serve inside the
#: client's 3s timeout, so every page raised curl(28) and the loop returned an
#: empty frame -- `cfb_roster_talent` had been yielding (0, 7) for every season.
#: Measured against the live feed:
#:     50 -> 50 rows   100 -> 100 rows   250 -> 250 rows   500 -> TIMEOUT
#: 250 is the largest measured-good size; it halves the request count versus
#: 100 while staying well inside the budget.
#: Recruits per class that count toward ``talent_composite``. The FBS limit on
#: initial counters is 25; 247 pages routinely list more (preferred walk-ons,
#: service-academy classes of 200), and counting them measures volume, not
#: talent. See the note in :func:`cfb_roster_talent`.
_MAX_CLASS_SIZE = 25

_PAGE_SIZE = 250


def _int_id(col: str) -> pl.Expr:
    """Integer-origin id -> Utf8 (never float->str, so a Float64 247 key becomes "71" not "71.0")."""
    return pl.col(col).cast(pl.Int64).cast(pl.Utf8)


def _normalize_recruit_page(raw: pl.DataFrame, season: int) -> pl.DataFrame:
    """Map one 247-RDB recruit page to the per-recruit contract.

    Uses the SIGNED institution (signing-day truth — what recruiting-class metrics
    like the blue-chip ratio count) with the committed institution as fallback;
    the RDB's ``committed_institution`` / ``current_institution`` drift with
    decommits and later transfers and systematically undercount signing classes.
    Recruits with neither (never signed/committed) are dropped.
    """
    required = {
        "key",
        "committed_institution_team_key",
        "committed_institution_full_name",
        "composite_star_rating",
        "composite_rating",
        "primary_position",
    }
    if raw.height == 0:
        return pl.DataFrame(schema=_RECRUIT_SCHEMA)
    missing = required - set(raw.columns)
    if missing:
        # RAISE, do not return empty. This previously returned a well-formed
        # zero-row frame on any schema drift, so one renamed 247 column would
        # have produced an empty talent table forever -- no error, no warning,
        # and every downstream consumer silently getting nothing.
        raise ValueError(
            f"247 recruit page is missing required columns {sorted(missing)}; "
            f"got {sorted(raw.columns)[:12]}... The RDB schema has drifted -- "
            "update _normalize_recruit_page rather than letting talent silently "
            "return zero rows."
        )
    has_signed = {"signed_institution_team_key", "signed_institution_full_name"} <= set(raw.columns)
    team_key = (
        pl.coalesce(pl.col("signed_institution_team_key"), pl.col("committed_institution_team_key"))
        if has_signed
        else pl.col("committed_institution_team_key")
    )
    team_name = (
        pl.coalesce(pl.col("signed_institution_full_name"), pl.col("committed_institution_full_name"))
        if has_signed
        else pl.col("committed_institution_full_name")
    )
    return (
        raw.select(
            pl.lit(season, dtype=pl.Int64).alias("season"),
            team_key.cast(pl.Int64).cast(pl.Utf8).alias("team_id"),
            team_name.cast(pl.Utf8).alias("team"),
            _int_id("key").alias("recruit_id"),
            (
                pl.col("first_name").cast(pl.Utf8).fill_null("")
                + pl.lit(" ")
                + pl.col("last_name").cast(pl.Utf8).fill_null("")
            )
            .str.strip_chars()
            .alias("player_name")
            if {"first_name", "last_name"} <= set(raw.columns)
            else pl.lit(None, dtype=pl.Utf8).alias("player_name"),
            pl.col("composite_star_rating").cast(pl.Int64).alias("stars"),
            pl.col("composite_rating").cast(pl.Float64).alias("grade"),
            pl.col("primary_position").cast(pl.Utf8).alias("position"),
        ).drop_nulls(["team_id"])  # recruits without a signed/committed team don't count toward roster talent
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


def cfb_roster_talent(
    seasons: int | list[int],
    *,
    division: str = "fbs",
    composite_247: pl.DataFrame | None = None,
    max_class_size: int = _MAX_CLASS_SIZE,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Team-talent composite per team-season (247 Team Talent Composite style).

    Talent is the class-recency-weighted sum of per-recruit star points over the
    trailing eligible recruiting classes (window = the length of the division's
    ``class_recency_weights``). When a 247 team-talent snapshot is supplied via
    ``composite_247``, its value overrides the derived composite for matched
    team-seasons (the derived value remains the fallback).

    Args:
        seasons: Target season or list of seasons to rate.
        division: Division slug for :func:`get_constants` (star points, weights).
        composite_247: Optional frame with ``season`` (Int64), ``team_id`` (Utf8),
            ``talent_247`` (Float64). Join-key dtypes are asserted.
        max_class_size: Top-N recruits per class that count toward
            ``talent_composite``, ranked by star points. Defaults to the FBS
            limit of 25 initial counters. Raise it only deliberately: an
            uncapped sum measures class VOLUME, which put Air Force 7th
            nationally on 200 signees at a 0.000 blue-chip ratio.
        return_as_pandas: If True, return a pandas DataFrame; otherwise polars.

    Returns:
        Per ``(season, team_id)``: ``team`` (Utf8), ``talent_composite`` (Float64),
        ``talent_rank`` (Int64 dense rank desc within season), ``blue_chip_ratio``
        (Float64), ``n_recruits`` (Int64). Zero-row (typed) when no recruits load.

    Raises:
        ValueError: If ``composite_247`` is supplied without the documented schema.

    Example:
        Quick start::

            from sportsdataverse.cfb.cfb_roster_talent import cfb_roster_talent
            tal = cfb_roster_talent(2023)
            tal.sort("talent_rank").head(10)

    See Also:
        * `247Sports Team Talent Composite`_ -- methodology reference.
        * `recruitR`_ -- the R companion for CFB recruiting data.

    .. _247Sports Team Talent Composite: https://247sports.com/season/2023-football/collegeteamtalentcomposite/
    .. _recruitR: https://github.com/sportsdataverse/recruitR
    """
    out_schema: dict[str, pl.PolarsDataType] = {
        "season": pl.Int64,
        "team_id": pl.Utf8,
        "team": pl.Utf8,
        "talent_composite": pl.Float64,
        "talent_rank": pl.Int64,
        "blue_chip_ratio": pl.Float64,
        "n_recruits": pl.Int64,
    }
    consts = get_constants(division)
    weights = consts.class_recency_weights
    window = len(weights)
    season_list = [seasons] if isinstance(seasons, int) else list(seasons)
    class_years = list(range(min(season_list) - window + 1, max(season_list) + 1))
    recruits = load_recruit_classes(class_years, division=division)
    if isinstance(recruits, pd.DataFrame):  # defensive; loader defaults to polars
        recruits = pl.from_pandas(recruits)
    if recruits.height == 0:
        empty = pl.DataFrame(schema=out_schema)
        return empty.to_pandas() if return_as_pandas else empty
    pointed = recruits.with_columns(
        pl.col("stars").replace_strict(consts.star_points, default=0.0, return_dtype=pl.Float64).alias("star_points")
    )
    # Only the top `max_class_size` recruits count toward a class.
    #
    # An uncapped sum measures VOLUME, not talent, and the two diverge badly at
    # the tail of the sport. Measured on the live 2021-2024 feed the uncapped
    # form ranked Air Force 7th nationally off 200 signees with a blue-chip
    # ratio of 0.000, and Washington State 6th off 142 at 0.035 -- service
    # academies and teams whose 247 pages include preferred walk-ons accumulate
    # more total star points than a 20-man class of blue chips. The FBS limit
    # of 25 initial counters is the principled cap: signees past it are not
    # scholarship talent, whatever the feed lists.
    per_class = pointed.group_by(["season", "team_id"]).agg(
        pl.col("star_points").sort(descending=True).head(max_class_size).sum().alias("class_points"),
        pl.col("team").drop_nulls().first().alias("team"),
    )
    weighted = pl.concat(
        [
            per_class.with_columns(
                (pl.col("season") + age).alias("target_season"),
                (pl.col("class_points") * weights[age]).alias("weighted_points"),
            )
            for age in range(window)
        ]
    )
    talent = (
        weighted.filter(pl.col("target_season").is_in(season_list))
        .group_by(["target_season", "team_id"])
        .agg(
            pl.col("weighted_points").sum().alias("talent_composite"),
            pl.col("team").drop_nulls().first().alias("team"),
        )
        .rename({"target_season": "season"})
    )
    bcr = blue_chip_ratio(recruits, window=window, division=division)
    assert talent.schema["team_id"] == bcr.schema["team_id"]
    talent = talent.join(
        bcr.select("season", "team_id", "blue_chip_ratio", "n_recruits"),
        on=["season", "team_id"],
        how="left",
    )
    if composite_247 is not None:
        required = {"season", "team_id", "talent_247"}
        if not required <= set(composite_247.columns):
            raise ValueError(f"composite_247 must carry columns {sorted(required)}")
        assert talent.schema["team_id"] == composite_247.schema["team_id"]
        assert talent.schema["season"] == composite_247.schema["season"]
        talent = (
            talent.join(
                composite_247.select("season", "team_id", "talent_247"),
                on=["season", "team_id"],
                how="left",
            )
            .with_columns(pl.coalesce(pl.col("talent_247"), pl.col("talent_composite")).alias("talent_composite"))
            .drop("talent_247")
        )
    out = (
        talent.with_columns(
            pl.col("talent_composite").rank("dense", descending=True).over("season").cast(pl.Int64).alias("talent_rank")
        )
        .select(*out_schema)
        .sort("season", "talent_rank")
    )
    return out.to_pandas() if return_as_pandas else out


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
    import warnings

    season_list = [seasons] if isinstance(seasons, int) else list(seasons)
    frames: list[pl.DataFrame] = []
    for season in season_list:
        page = 1
        got = 0
        while True:
            raw = sports247_recruits(sport_key=1, year=season, page_size=_PAGE_SIZE, page=page)
            if raw is None or raw.height == 0:
                break
            frames.append(_normalize_recruit_page(raw, season))
            got += raw.height
            if raw.height < _PAGE_SIZE:
                break
            page += 1
        # A season that yields nothing is a FAILURE, not an empty class. Every
        # request timing out (which is what _PAGE_SIZE=500 caused) looked
        # identical to "247 has no recruits for 2023" -- so the whole talent
        # surface returned zero rows for years without a single complaint.
        if got == 0:
            warnings.warn(
                f"247 recruit feed returned no rows for {season}. Recruiting "
                "classes are never empty, so this is a fetch or schema failure "
                "-- talent metrics downstream will be silently absent. Check "
                f"connectivity and _PAGE_SIZE (currently {_PAGE_SIZE}; 500 "
                "exceeded the client timeout).",
                UserWarning,
                stacklevel=2,
            )
    out = pl.concat(frames) if frames else pl.DataFrame(schema=_RECRUIT_SCHEMA)
    return out.to_pandas() if return_as_pandas else out
